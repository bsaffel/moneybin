"""Integration: ``reconciliation_status`` on ``core.fct_investment_transactions``.

Expectations are hand-derived from the seeded rows: an account whose ledger
(after the source choice, bootstrap excluded) carries two source types is
unreconciled; anything else is reconciled.
"""

from __future__ import annotations

from datetime import datetime

import pytest

from moneybin.database import Database
from moneybin.services.account_service import CLEAR, AccountService
from moneybin.services.investment_membership_service import InvestmentMembershipService
from moneybin.services.transform_service import TransformService
from tests.moneybin.test_investments._overlap_seed import (
    ACCOUNT,
    seed_manual_buy,
    seed_manual_sell,
    seed_overlap,
    seed_plaid_account,
    seed_plaid_buy,
    seed_security,
    seed_snapshot,
)
from tests.moneybin.test_investments.comparison_helpers import seed_account_metadata

pytestmark = pytest.mark.integration


def _statuses(db: Database, account_id: str = ACCOUNT) -> set[str]:
    rows = db.execute(
        """
        SELECT reconciliation_status FROM core.fct_investment_transactions
        WHERE account_id = ?
        """,
        [account_id],
    ).fetchall()
    return {str(r[0]) for r in rows}


def _updated_ats(db: Database, source_type: str | None = None) -> list[datetime]:
    rows = db.execute(
        """
        SELECT updated_at FROM core.fct_investment_transactions
        WHERE account_id = ? AND (? IS NULL OR source_type = ?)
        ORDER BY source_type
        """,
        [ACCOUNT, source_type, source_type],
    ).fetchall()
    return [r[0] for r in rows]


def _apply(db: Database) -> None:
    result = TransformService(db).apply()
    assert result.applied, f"transform failed: {result.error}"


def test_overlapping_account_rows_are_unreconciled(db: Database) -> None:
    seed_overlap(db)

    rows = db.execute(
        "SELECT count(*) FROM core.fct_investment_transactions WHERE account_id = ?",
        [ACCOUNT],
    ).fetchone()
    assert rows is not None and rows[0] == 2
    assert _statuses(db) == {"unreconciled"}


def test_single_source_account_is_reconciled(db: Database) -> None:
    seed_security(db)
    seed_plaid_account(db)
    seed_manual_buy(db, account_id=ACCOUNT, txn_id="manual_only_buy")
    _apply(db)

    assert _statuses(db) == {"reconciled"}


def test_plaid_buy_with_its_bootstrap_is_reconciled(db: Database) -> None:
    seed_security(db)
    seed_plaid_account(db)
    seed_plaid_buy(db, loaded_at="2026-02-01 09:00:00")
    seed_snapshot(db, quantity="15")
    _apply(db)

    subtypes = db.execute(
        """
        SELECT subtype FROM core.fct_investment_transactions
        WHERE account_id = ? AND source_type = 'plaid'
        """,
        [ACCOUNT],
    ).fetchall()
    assert "opening_bootstrap" in {r[0] for r in subtypes}, "no bootstrap row seeded"
    assert _statuses(db) == {"reconciled"}


def test_bootstrap_does_not_count_as_a_second_source(db: Database) -> None:
    """Manual buy + a snapshot-only gap: the Plaid bootstrap row must not flag it."""
    seed_security(db)
    seed_plaid_account(db)
    seed_manual_buy(db, account_id=ACCOUNT, txn_id="manual_bootstrap_buy")
    seed_snapshot(db, quantity="10")
    _apply(db)

    row = db.execute(
        """
        SELECT count(*) FROM core.fct_investment_transactions
        WHERE account_id = ? AND subtype = 'opening_bootstrap'
        """,
        [ACCOUNT],
    ).fetchone()
    assert row is not None and row[0] >= 1, "no bootstrap row seeded"
    assert _statuses(db) == {"reconciled"}


def test_source_choice_reconciles_and_clear_reverts(db: Database) -> None:
    seed_overlap(db)
    before = max(_updated_ats(db))
    service = AccountService(db)

    service.settings_update(
        ACCOUNT,
        actor="test",
        investment_source_type="manual",
        verify_source_choice=InvestmentMembershipService(db)
        .preview_source_choice(ACCOUNT, "manual")
        .verify,
    )
    assert _statuses(db) == {"reconciled"}
    chosen = max(_updated_ats(db))
    assert chosen > before

    service.settings_update(
        ACCOUNT,
        actor="test",
        investment_source_type=CLEAR,
        verify_source_choice=InvestmentMembershipService(db)
        .preview_source_choice(ACCOUNT, None)
        .verify,
    )
    assert _statuses(db) == {"unreconciled"}
    assert min(_updated_ats(db)) > chosen


def test_sibling_second_source_event_advances_every_row(db: Database) -> None:
    seed_security(db)
    seed_plaid_account(db)
    seed_manual_buy(db, account_id=ACCOUNT, txn_id="manual_sibling_buy")
    _apply(db)
    solo = _updated_ats(db, "manual")[0]

    seed_plaid_buy(db, loaded_at="2026-02-01 09:00:00")
    _apply(db)

    manual = _updated_ats(db, "manual")[0]
    plaid = _updated_ats(db, "plaid")[0]
    assert manual == plaid, "the manual row did not follow the account's newest row"
    assert manual > solo


CLEAN = "acc_clean_manual"


def _statuses_of(db: Database, table: str, account_id: str) -> set[str]:
    rows = db.execute(
        f"SELECT reconciliation_status FROM {table} WHERE account_id = ?",  # noqa: S608  # fixed table names from this module
        [account_id],
    ).fetchall()
    return {str(r[0]) for r in rows}


def _seed_overlap_and_clean(db: Database) -> None:
    seed_security(db)
    seed_plaid_account(db)
    seed_manual_buy(db, account_id=ACCOUNT, txn_id="manual_choice_buy")
    seed_manual_sell(db, account_id=ACCOUNT, txn_id="manual_choice_sell")
    seed_plaid_buy(db, loaded_at="2026-02-01 09:00:00")
    seed_account_metadata(db, CLEAN)
    seed_manual_buy(db, account_id=CLEAN, txn_id="clean_buy")
    seed_manual_sell(db, account_id=CLEAN, txn_id="clean_sell")
    _apply(db)


def test_lots_and_gains_carry_the_account_status(db: Database) -> None:
    _seed_overlap_and_clean(db)

    for table in ("core.fct_investment_lots", "core.fct_realized_gains"):
        n_overlap = db.execute(
            f"SELECT count(*) FROM {table} WHERE account_id = ?",  # noqa: S608  # fixed table names
            [ACCOUNT],
        ).fetchone()
        assert n_overlap is not None and n_overlap[0] >= 1, f"no {table} rows seeded"
        assert _statuses_of(db, table, ACCOUNT) == {"unreconciled"}
        assert _statuses_of(db, table, CLEAN) == {"reconciled"}


def test_lot_figures_are_unchanged_by_the_marker(db: Database) -> None:
    projection = """
        SELECT * EXCLUDE (reconciliation_status) FROM core.fct_investment_lots
        WHERE account_id = ? ORDER BY lot_id
        """
    seed_security(db)
    seed_account_metadata(db, CLEAN)
    seed_manual_buy(db, account_id=CLEAN, txn_id="clean_buy")
    seed_manual_sell(db, account_id=CLEAN, txn_id="clean_sell")
    _apply(db)
    alone = db.execute(projection, [CLEAN]).fetchall()
    assert alone, "no clean-account lots seeded"

    seed_plaid_account(db)
    seed_manual_buy(db, account_id=ACCOUNT, txn_id="manual_choice_buy")
    seed_plaid_buy(db, loaded_at="2026-02-01 09:00:00")
    _apply(db)

    assert db.execute(projection, [CLEAN]).fetchall() == alone
