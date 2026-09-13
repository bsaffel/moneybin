"""Identity changes must account for every stored lot selection before writes."""

from datetime import date
from decimal import Decimal

import pytest

from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.investments.cost_basis import (
    LedgerEvent,
    PairedTransfer,
    compute_lot_id,
    compute_lots_and_gains,
)
from moneybin.repositories.account_link_decisions_repo import AccountLinkDecisionsRepo
from moneybin.repositories.account_links_repo import AccountLinksRepo
from moneybin.repositories.lot_selections_repo import LotSelectionsRepo
from moneybin.services.account_links_service import AccountLinksService
from moneybin.services.security_links_service import SecurityLinksService
from tests.moneybin.test_services.test_account_identity_selections import (
    seed_account_selections,
)
from tests.moneybin.test_services.test_security_links_service import (
    add_disposal,
    add_lot,
)
from tests.moneybin.test_services.test_security_links_service import (
    merge_setup as merge_setup,
)


def test_security_merge_remaps_survivor_disposal_selecting_absorbed_lot(
    db: Database,
    merge_setup: dict[str, str],  # injected shared pytest fixture
) -> None:
    old = add_lot(db, security_id=merge_setup["provisional"])
    add_disposal(db, "sell_survivor", merge_setup["survivor"])
    LotSelectionsRepo(db).set_for_disposal(
        investment_transaction_id="sell_survivor",
        selections=[(old, Decimal("2"))],
        actor="cli",
    )
    SecurityLinksService(db).accept_merge(
        merge_setup["decision_id"], into=merge_setup["survivor"]
    )
    expected = compute_lot_id(
        "acc_1", merge_setup["survivor"], date(2024, 3, 1), "itx_buy"
    )
    assert LotSelectionsRepo(db).list_for_disposal("sell_survivor") == [
        (expected, Decimal("2"))
    ]


def test_security_merge_refuses_when_selected_disposal_is_missing(
    db: Database,
    merge_setup: dict[str, str],  # injected shared pytest fixture
) -> None:
    lot = add_lot(db, security_id=merge_setup["provisional"])
    LotSelectionsRepo(db).set_for_disposal(
        investment_transaction_id="missing_disposal",
        selections=[(lot, Decimal("2"))],
        actor="cli",
    )
    before = db.execute("SELECT * FROM app.security_links ORDER BY link_id").fetchall()
    with pytest.raises(UserError):
        SecurityLinksService(db).accept_merge(
            merge_setup["decision_id"], into=merge_setup["survivor"]
        )
    assert (
        db.execute("SELECT * FROM app.security_links ORDER BY link_id").fetchall()
        == before
    )


def test_account_merge_refuses_unmaterialized_selections_before_identity_writes(
    db: Database,
) -> None:
    AccountLinksRepo(db).insert(
        link_id="link_a",
        account_id="a",
        ref_kind="source_native",
        ref_value="native_a",
        source_type="manual",
        source_origin="user",
        decided_by="user",
        actor="cli",
    )
    AccountLinkDecisionsRepo(db).insert(
        decision_id="merge",
        provisional_account_id="a",
        candidate_account_id="b",
        confidence_score=None,
        match_signals={},
        decided_by="user",
        actor="cli",
    )
    LotSelectionsRepo(db).set_for_disposal(
        investment_transaction_id="sell",
        selections=[("old_lot", Decimal("2"))],
        actor="cli",
    )
    before = db.execute("SELECT * FROM app.account_links ORDER BY link_id").fetchall()
    audit_count = db.execute("SELECT COUNT(*) FROM app.audit_log").fetchone()
    with pytest.raises(UserError, match="lot selection"):
        AccountLinksService(db).set("merge", target_account_id="b")
    assert (
        db.execute("SELECT * FROM app.account_links ORDER BY link_id").fetchall()
        == before
    )
    assert db.execute("SELECT COUNT(*) FROM app.audit_log").fetchone() == audit_count


@pytest.mark.parametrize("identity", ["account", "security"])
def test_identity_merge_refuses_real_transfer_lineage_atomically(
    db: Database, merge_setup: dict[str, str], identity: str
) -> None:
    """An unsupported materialized Transfer hash must not become an ordinary lot."""
    if identity == "account":
        seed_account_selections(db)
        account, security, disposal = "a", "security", "sell"
    else:
        account = "acc_1"
        security = merge_setup["provisional"]
        disposal = "itx_sell"
        add_disposal(db, disposal, security)
    events = [
        LedgerEvent(
            investment_transaction_id="transfer_buy",
            type="buy",
            account_id="origin_account",
            security_id=security,
            trade_date=date(2026, 1, 1),
            original_acquisition_date=None,
            quantity=Decimal("10"),
            price=None,
            amount=Decimal("-100"),
            fees=None,
            currency_code="USD",
        ),
        LedgerEvent(
            investment_transaction_id="transfer_out",
            type="transfer_out",
            account_id="origin_account",
            security_id=security,
            trade_date=date(2026, 1, 2),
            original_acquisition_date=None,
            quantity=Decimal("-10"),
            price=None,
            amount=None,
            fees=None,
            currency_code="USD",
        ),
        LedgerEvent(
            investment_transaction_id="transfer_in",
            type="transfer_in",
            account_id=account,
            security_id=security,
            trade_date=date(2026, 1, 2),
            original_acquisition_date=None,
            quantity=Decimal("10"),
            price=None,
            amount=None,
            fees=None,
            currency_code="USD",
        ),
    ]
    lots, gains = compute_lots_and_gains(
        events,
        method_for=lambda _account, _security: "fifo",
        selections_for=lambda _disposal: [],
        paired_transfers=[PairedTransfer("pair", "transfer_out", "transfer_in")],
    )
    moved = next(lot for lot in lots if lot.account_id == account)
    assert not gains
    assert moved.source_transfer_id == "pair"
    assert moved.lot_id != compute_lot_id(
        account, security, moved.acquisition_date, moved.source_transaction_id
    )
    for event in events:
        db.execute(
            """INSERT INTO core.fct_investment_transactions
            (investment_transaction_id, account_id, security_id, trade_date,
             type, quantity, amount, currency_code)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            [
                event.investment_transaction_id,
                event.account_id,
                event.security_id,
                event.trade_date,
                event.type,
                event.quantity,
                event.amount,
                event.currency_code,
            ],
        )
    db.execute(
        """UPDATE core.fct_investment_transactions SET trade_date = DATE '2026-02-01'
        WHERE investment_transaction_id = ?""",
        [disposal],
    )
    db.execute(
        """INSERT INTO core.fct_investment_lots
        (lot_id, account_id, security_id, acquisition_date, acquisition_type,
         source_transaction_id, original_quantity, remaining_quantity, currency_code)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        [
            moved.lot_id,
            moved.account_id,
            moved.security_id,
            moved.acquisition_date,
            moved.acquisition_type,
            moved.source_transaction_id,
            moved.original_quantity,
            moved.remaining_quantity,
            moved.currency_code,
        ],
    )
    LotSelectionsRepo(db).set_for_disposal(
        investment_transaction_id=disposal,
        selections=[(moved.lot_id, Decimal("2"))],
        actor="cli",
    )
    tables = (
        "app.account_links",
        "app.account_link_decisions",
        "app.security_links",
        "app.security_link_decisions",
        "app.securities",
        "app.lot_selections",
        "app.audit_log",
        "raw.manual_investment_transactions",
    )
    before = {
        table: db.execute(
            f"SELECT * FROM {table} ORDER BY 1, 2"  # noqa: S608  # closed fixture table set
        ).fetchall()
        for table in tables
    }
    with pytest.raises(UserError, match="selection"):
        if identity == "account":
            AccountLinksService(db).set("a_b_", target_account_id="b")
        else:
            SecurityLinksService(db).accept_merge(
                merge_setup["decision_id"], into=merge_setup["survivor"]
            )
    for table in tables:
        assert (
            db.execute(
                f"SELECT * FROM {table} ORDER BY 1, 2"  # noqa: S608  # closed fixture table set
            ).fetchall()
            == before[table]
        )
