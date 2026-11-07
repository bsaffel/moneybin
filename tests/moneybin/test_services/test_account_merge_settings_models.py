"""Integration: an account merge carries settings through the real models.

A survivor holds a manual buy; a provisional Plaid-synced account holds a Plaid
buy and a recorded source choice of ``manual``. Merging them puts both histories
on one account, which is the overlap the choice had resolved. Expectations are
hand-derived from the seeded rows, not read back from the models.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from moneybin.database import Database
from moneybin.investments.source_overlap import (
    investment_source_overlap,
    stale_source_choice_accounts,
)
from moneybin.repositories.account_link_decisions_repo import AccountLinkDecisionsRepo
from moneybin.repositories.account_links_repo import AccountLinksRepo
from moneybin.services.account_links_service import AccountLinksService
from moneybin.services.account_service import AccountService
from moneybin.services.investment_membership_service import InvestmentMembershipService
from moneybin.services.transform_service import TransformService
from tests.moneybin.test_investments.comparison_helpers import seed_account_metadata

pytestmark = pytest.mark.integration

_SURVIVOR = "acc_survivor"
_NATIVE_ABSORBED = "native_absorbed"
_ABSORBED = "acc_absorbed"
_SECURITY = "sec_merge_0001"
_PROVIDER_KEY = f"prov_{_SECURITY}"
_ORIGIN = "item_merge"
_TRADE_DATE = date(2026, 1, 5)
_DECISION = "merge_dec_001"


def _seed(db: Database) -> None:
    """Survivor with a manual buy; absorbed account (via a link) with a Plaid buy."""
    db.execute(
        "INSERT INTO app.securities (security_id, name, security_type, ticker) "
        "VALUES (?, 'Merge Fund', 'etf', 'MRG')",  # test fixture, not executing user SQL
        [_SECURITY],
    )
    db.execute(
        """
        INSERT INTO app.security_links
            (link_id, security_id, ref_kind, ref_value, source_type,
             status, decided_by, decided_at)
        VALUES (?, ?, 'plaid_security_id', ?, 'plaid',
                'accepted', 'auto', CURRENT_TIMESTAMP)
        """,  # test fixture, not executing user SQL
        [f"link_{_PROVIDER_KEY}", _SECURITY, _PROVIDER_KEY],
    )
    db.execute(
        """
        INSERT INTO raw.security_prices
            (provider_security_key, price_date, quote_currency, source_type,
             source_origin, close, price_basis, extracted_at, loaded_at)
        VALUES (?, CURRENT_DATE, 'USD', 'plaid', ?, 120.00, 'raw',
                CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """,  # test fixture, not executing user SQL
        [_PROVIDER_KEY, _ORIGIN],
    )
    for account_id in (_SURVIVOR, _NATIVE_ABSORBED):
        db.execute(
            """
            INSERT INTO raw.plaid_accounts
                (account_id, account_type, account_subtype, institution_name,
                 official_name, mask, source_file, source_type, source_origin,
                 extracted_at, loaded_at)
            VALUES (?, 'investment', 'brokerage', 'TestBank', 'Merge Brokerage',
                    '1234', 'sync_test', 'plaid', ?,
                    CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """,  # test fixture, not executing user SQL
            [account_id, _ORIGIN],
        )
    db.execute(
        """
        INSERT INTO raw.manual_investment_transactions (
            source_transaction_id, import_id, account_id, security_id,
            security_ref, type, trade_date, quantity, price, amount, fees,
            created_by, investment_transaction_id, currency_code, created_at
        ) VALUES ('manual_merge_buy', 'imp_manual_merge_buy', ?, ?, 'MRG', 'buy',
                  ?::DATE, 10, 100.00, -1000.00, 0.00, 'test',
                  'manual_merge_buy', 'USD', TIMESTAMP '2026-01-06 09:00:00')
        """,  # test fixture, not executing user SQL
        [_SURVIVOR, _SECURITY, _TRADE_DATE],
    )
    db.execute(
        """
        INSERT INTO raw.plaid_investment_transactions (
            investment_transaction_id, account_id, security_id,
            investment_transaction_type, investment_transaction_subtype,
            transaction_date, quantity, price, amount, fees, iso_currency_code,
            observation_version, source_type, source_origin
        ) VALUES ('itx_merge_buy', ?, ?, 'buy', 'buy', ?,
                  10, 100.00, 1000.00, 0.00, 'USD', 'plaid_fixture', 'plaid', ?)
        """,  # test fixture, not executing user SQL
        [_NATIVE_ABSORBED, _PROVIDER_KEY, _TRADE_DATE, _ORIGIN],
    )
    db.execute(
        """
        INSERT INTO raw.plaid_investment_transaction_receipts (
            investment_transaction_id, source_origin, source_file,
            observation_version, extracted_at, loaded_at
        ) VALUES ('itx_merge_buy', ?, 'sync_test', 'plaid_fixture',
                  CURRENT_TIMESTAMP, TIMESTAMP '2026-02-01 09:00:00')
        """,  # test fixture, not executing user SQL
        [_ORIGIN],
    )
    AccountLinksRepo(db).insert(
        link_id="link_absorbed_native",
        account_id=_ABSORBED,
        ref_kind="source_native",
        ref_value=_NATIVE_ABSORBED,
        source_type="plaid",
        source_origin=_ORIGIN,
        decided_by="auto",
        actor="system",
        status="accepted",
    )
    seed_account_metadata(db, _SURVIVOR, origin=_ORIGIN)
    AccountLinkDecisionsRepo(db).insert(
        decision_id=_DECISION,
        provisional_account_id=_ABSORBED,
        candidate_account_id=_SURVIVOR,
        confidence_score=0.9,
        match_signals={"signal": "institution_last4", "value": "***"},
        decided_by="auto",
        actor="system",
        status="pending",
    )
    result = TransformService(db).apply()
    assert result.applied, f"transform failed: {result.error}"


def _ledger(db: Database, account_id: str) -> list[tuple[str, str | None]]:
    rows = db.execute(
        """
        SELECT source_type, subtype FROM core.fct_investment_transactions
        WHERE account_id = ? ORDER BY source_type, subtype NULLS FIRST
        """,
        [account_id],
    ).fetchall()
    return [(str(r[0]), r[1]) for r in rows]


def test_the_fixture_reaches_the_overlap_when_the_absorbed_account_chose_nothing(
    db: Database,
) -> None:
    """Control: without a choice to carry, the merge does produce the overlap."""
    _seed(db)
    assert _ledger(db, _SURVIVOR) == [("manual", None)]
    assert _ledger(db, _ABSORBED) == [("plaid", None)]

    AccountLinksService(db, actor="cli").set(_DECISION, target_account_id=_SURVIVOR)

    assert _ledger(db, _SURVIVOR) == [("manual", None), ("plaid", None)]
    assert investment_source_overlap(db) == [_SURVIVOR]


def test_an_overlap_the_absorbed_choice_resolved_stays_resolved_after_the_merge(
    db: Database,
) -> None:
    _seed(db)
    AccountService(db).settings_update(
        _ABSORBED,
        actor="test",
        investment_source_type="manual",
        verify_source_choice=InvestmentMembershipService(db)
        .preview_source_choice(_ABSORBED, "manual")
        .verify,
    )

    AccountLinksService(db, actor="cli").set(_DECISION, target_account_id=_SURVIVOR)

    survivor = db.execute(
        """
        SELECT investment_source_type, investment_source_type_changed_at
        FROM app.account_settings WHERE account_id = ?
        """,
        [_SURVIVOR],
    ).fetchone()
    assert survivor is not None
    assert survivor[0] == "manual"
    assert survivor[1] is not None
    assert _ledger(db, _SURVIVOR) == [("manual", None)]
    assert investment_source_overlap(db) == []
    assert stale_source_choice_accounts(db) == []
    status = db.execute(
        "SELECT valuation_status FROM core.dim_holdings WHERE account_id = ?",
        [_SURVIVOR],
    ).fetchone()
    assert status is not None
    assert status[0] != "source_overlap"


def test_the_survivors_dim_accounts_row_shows_every_moved_value(db: Database) -> None:
    _seed(db)
    AccountService(db).settings_update(
        _ABSORBED,
        actor="test",
        display_name="Vacation Fund",
        official_name="Synthetic Brokerage Plus",
        last_four="9999",
        account_subtype="brokerage",
        holder_category="joint",
        currency_code="EUR",
        credit_limit=Decimal("2500.00"),
        default_cost_basis_method="hifo",
        investment_source_type="manual",
        verify_source_choice=InvestmentMembershipService(db)
        .preview_source_choice(_ABSORBED, "manual")
        .verify,
    )

    AccountLinksService(db, actor="cli").set(_DECISION, target_account_id=_SURVIVOR)

    row = db.execute(
        """
        SELECT display_name, official_name, last_four, account_subtype,
               holder_category, currency_code, credit_limit,
               investment_source_type, archived, include_in_net_worth
        FROM core.dim_accounts WHERE account_id = ?
        """,
        [_SURVIVOR],
    ).fetchone()
    assert row == (
        "Vacation Fund",
        "Synthetic Brokerage Plus",
        "9999",
        "brokerage",
        "joint",
        "EUR",
        Decimal("2500.00"),
        "manual",
        False,
        True,
    )
    cost_basis = db.execute(
        "SELECT default_cost_basis_method FROM app.account_settings "
        "WHERE account_id = ?",
        [_SURVIVOR],
    ).fetchone()
    assert cost_basis == ("hifo",)
