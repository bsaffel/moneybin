"""Deferred Plaid delivery cannot invalidate elections during identity changes."""

from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest
from pytest_mock import MockerFixture
from sqlglot import exp
from sqlmesh.core.dialect import parse

from moneybin.connectors.sync_models import SyncDataResponse
from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.extractors.plaid.extractor import PlaidExtractor
from moneybin.repositories.account_links_repo import AccountLinksRepo
from moneybin.repositories.security_links_repo import SecurityLinksRepo
from moneybin.services.account_links_service import AccountLinksService
from tests.moneybin.test_extractors.test_plaid_investment_receipts import (
    receipt_payload as receipt_payload,
)
from tests.moneybin.test_services.test_account_identity_selections import (
    seed_account_selections,
)
from tests.moneybin.test_services.test_manual_identity_projection import (
    add_account_edge,
    add_account_terminal,
)


def _seed_plaid(db: Database, payload: SyncDataResponse) -> SyncDataResponse:
    seed_account_selections(db)
    payload = payload.model_copy(deep=True)
    payload.securities = []
    payload.investment_holdings = []
    buy = payload.investment_transactions[0].model_copy(deep=True)
    buy.investment_transaction_id = "buy"
    buy.transaction_date = date(2026, 1, 1)
    buy.transaction_datetime = None
    buy.quantity = Decimal("10")
    buy.amount = Decimal("100")
    buy.price = Decimal("10")
    buy.fees = Decimal("0")
    sell = buy.model_copy(deep=True)
    sell.investment_transaction_id = "sell"
    sell.transaction_date = date(2026, 2, 1)
    sell.investment_transaction_type = "sell"
    sell.investment_transaction_subtype = "sell"
    sell.quantity = Decimal("-2")
    sell.amount = Decimal("-20")
    payload.investment_transactions = [buy, sell]
    PlaidExtractor(db).load(payload, job_id="initial")
    AccountLinksRepo(db).insert(
        link_id="plaid_a",
        account_id="a",
        ref_kind="source_native",
        ref_value=buy.account_id,
        source_type="plaid",
        source_origin=buy.provider_item_id,
        decided_by="user",
        actor="cli",
    )
    SecurityLinksRepo(db).insert(
        link_id="plaid_security",
        security_id="security",
        ref_kind="plaid_security_id",
        ref_value=str(buy.security_id),
        source_type="plaid",
        decided_by="user",
        actor="cli",
    )
    path = Path(
        "src/moneybin/sqlmesh/models/prep/stg_plaid__investment_transactions.sql"
    )
    query = next(
        node for node in parse(path.read_text()) if isinstance(node, exp.Query)
    )
    db.execute("DELETE FROM core.fct_investment_transactions")
    db.execute(
        f"""INSERT INTO core.fct_investment_transactions
        (investment_transaction_id, account_id, security_id, trade_date,
         original_acquisition_date, type, quantity, price, amount, fees,
         currency_code, source_type, source_origin)
        SELECT investment_transaction_id, account_id, security_id, trade_date,
               original_acquisition_date, type, quantity, price, amount, fees,
               currency_code, source_type, source_origin
        FROM ({query.sql(dialect="duckdb")}) WHERE ledger_include"""  # noqa: S608  # parsed repository model, no caller SQL
    )
    return payload


@pytest.mark.parametrize(
    "change",
    [
        "corrected_buy",
        "earlier_sale",
        "account_route",
        "security_route",
        "opening_dependency",
    ],
)
def test_deferred_plaid_change_refuses_account_merge_atomically(
    db: Database,
    receipt_payload: SyncDataResponse,
    mocker: MockerFixture,
    change: str,
) -> None:
    payload = _seed_plaid(db, receipt_payload)
    mocker.patch.object(AccountLinksService, "rematch_after_merge", return_value=None)
    if change == "account_route":
        AccountLinksRepo(db).repoint(
            link_id="plaid_a",
            new_account_id="b",
            decided_by="user",
            actor="cli",
        )
    elif change == "security_route":
        SecurityLinksRepo(db).repoint(
            link_id="plaid_security",
            new_security_id="other",
            decided_by="user",
            actor="cli",
        )
    else:
        changed = payload.model_copy(deep=True)
        if change == "corrected_buy":
            changed.investment_transactions[0].quantity = Decimal("1")
            changed.investment_transactions[0].amount = Decimal("10")
        elif change == "opening_dependency":
            holding = receipt_payload.investment_holdings[0].model_copy(deep=True)
            holding.quantity = Decimal("10")
            holding.tax_lots = []
            changed.investment_holdings = [holding]
        else:
            earlier = changed.investment_transactions[1].model_copy(deep=True)
            earlier.investment_transaction_id = "earlier_sale"
            earlier.transaction_date = date(2026, 1, 15)
            earlier.quantity = Decimal("-10")
            earlier.amount = Decimal("-100")
            changed.investment_transactions.append(earlier)
        PlaidExtractor(db).load(changed, job_id="deferred")
    links = db.execute("SELECT * FROM app.account_links ORDER BY link_id").fetchall()
    securities = db.execute(
        "SELECT * FROM app.security_links ORDER BY link_id"
    ).fetchall()
    selections = db.execute("SELECT * FROM app.lot_selections").fetchall()
    audit = db.execute("SELECT COUNT(*) FROM app.audit_log").fetchone()
    with pytest.raises(UserError, match="selection"):
        AccountLinksService(db).set("a_b_", target_account_id="b")
    assert (
        db.execute("SELECT * FROM app.account_links ORDER BY link_id").fetchall()
        == links
    )
    assert (
        db.execute("SELECT * FROM app.security_links ORDER BY link_id").fetchall()
        == securities
    )
    assert db.execute("SELECT * FROM app.lot_selections").fetchall() == selections
    assert db.execute("SELECT COUNT(*) FROM app.audit_log").fetchone() == audit


def test_current_plaid_selection_allows_complete_account_merge(
    db: Database,
    receipt_payload: SyncDataResponse,
    mocker: MockerFixture,
) -> None:
    _seed_plaid(db, receipt_payload)
    mocker.patch.object(AccountLinksService, "rematch_after_merge", return_value=None)
    AccountLinksService(db).set("a_b_", target_account_id="b")
    assert db.execute(
        "SELECT account_id FROM app.account_links WHERE source_type = 'plaid' AND status = 'accepted'"
    ).fetchone() == ("b",)


def test_second_plaid_account_merge_before_transform_refuses_atomically(
    db: Database,
    receipt_payload: SyncDataResponse,
    mocker: MockerFixture,
) -> None:
    _seed_plaid(db, receipt_payload)
    mocker.patch.object(AccountLinksService, "rematch_after_merge", return_value=None)
    add_account_terminal(db, "c")
    add_account_edge(db, "b", "c", status="pending")
    db.execute(
        "INSERT INTO core.dim_accounts (account_id, display_name, currency_code) VALUES ('c', 'Third', 'USD')"
    )
    AccountLinksService(db).set("a_b_", target_account_id="b")
    links = db.execute("SELECT * FROM app.account_links ORDER BY link_id").fetchall()
    selections = db.execute("SELECT * FROM app.lot_selections").fetchall()
    audit = db.execute("SELECT COUNT(*) FROM app.audit_log").fetchone()
    with pytest.raises(UserError, match="selection"):
        AccountLinksService(db).set("b_c_", target_account_id="c")
    assert (
        db.execute("SELECT * FROM app.account_links ORDER BY link_id").fetchall()
        == links
    )
    assert db.execute("SELECT * FROM app.lot_selections").fetchall() == selections
    assert db.execute("SELECT COUNT(*) FROM app.audit_log").fetchone() == audit
