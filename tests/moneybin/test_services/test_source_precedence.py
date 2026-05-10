"""Tests for source-priority precedence enforcement on transaction_categories writes.

Per categorization-matching-mechanics.md §Source precedence, the priority order is:
user(1) > rule(2) > auto_rule(3) > migration(4) > ml(5) > plaid(6) > seed(7) > ai(8).
A higher-priority source can never be overwritten by a lower-priority source.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from moneybin.database import Database
from moneybin.services.categorization_service import CategorizationService
from tests.moneybin.db_helpers import create_core_tables


@pytest.fixture()
def real_db(tmp_path: Path) -> Database:
    """Real DB with core + app schema."""
    mock_store = MagicMock()
    mock_store.get_key.return_value = "test-key"
    db = Database(
        tmp_path / "test.duckdb", secret_store=mock_store, no_auto_upgrade=True
    )
    create_core_tables(db)
    return db


@pytest.fixture()
def fresh_txn(real_db: Database) -> str:
    """Insert one transaction in fct_transactions; return its id."""
    txn_id = "test_txn_001"
    real_db.execute(
        """
        INSERT INTO core.fct_transactions
        (transaction_id, account_id, transaction_date, amount, description,
         memo, source_type, is_transfer)
        VALUES (?, 'acct_test', '2026-05-10', -10.00, 'STARBUCKS #1234',
                NULL, 'ofx', false)
        """,  # noqa: S608  # test input, not executing user SQL
        [txn_id],
    )
    return txn_id


def _write(svc: CategorizationService, txn_id: str, source: str) -> bool:
    outcome = svc.write_categorization(
        transaction_id=txn_id,
        category="Food & Dining",
        subcategory="Coffee Shops",
        categorized_by=source,
    )
    return outcome.written


def _current_source(real_db: Database, txn_id: str) -> str | None:
    row = real_db.execute(
        "SELECT categorized_by FROM app.transaction_categories WHERE transaction_id = ?",
        [txn_id],
    ).fetchone()
    return row[0] if row else None


def test_user_overwrites_everything(real_db: Database, fresh_txn: str) -> None:
    svc = CategorizationService(real_db)
    for predecessor in ["ai", "seed", "plaid", "ml", "migration", "auto_rule", "rule"]:
        svc.write_categorization(
            transaction_id=fresh_txn,
            category="Initial",
            subcategory=None,
            categorized_by=predecessor,
        )
        assert _write(svc, fresh_txn, "user") is True
        assert _current_source(real_db, fresh_txn) == "user"
        real_db.execute(
            "DELETE FROM app.transaction_categories WHERE transaction_id = ?",
            [fresh_txn],
        )


def test_ai_cannot_overwrite_user(real_db: Database, fresh_txn: str) -> None:
    svc = CategorizationService(real_db)
    svc.write_categorization(
        transaction_id=fresh_txn,
        category="Coffee Shops",
        subcategory=None,
        categorized_by="user",
    )
    outcome = svc.write_categorization(
        transaction_id=fresh_txn,
        category="Different Category",
        subcategory=None,
        categorized_by="ai",
    )
    assert outcome.written is False
    assert outcome.skipped_reason == "lower_priority_source"
    assert _current_source(real_db, fresh_txn) == "user"


def test_same_source_overwrites_itself(real_db: Database, fresh_txn: str) -> None:
    """Same-priority writes succeed (the rule is ``<=``, not ``<``).

    Two consecutive ``ai`` categorizations both write — the second replaces
    the first.
    """
    svc = CategorizationService(real_db)
    svc.write_categorization(
        transaction_id=fresh_txn,
        category="First",
        subcategory=None,
        categorized_by="ai",
    )
    outcome = svc.write_categorization(
        transaction_id=fresh_txn,
        category="Second",
        subcategory=None,
        categorized_by="ai",
    )
    assert outcome.written is True
    row = real_db.execute(
        "SELECT category FROM app.transaction_categories WHERE transaction_id = ?",
        [fresh_txn],
    ).fetchone()
    assert row is not None
    assert row[0] == "Second"


_PRIORITY = {
    "user": 1,
    "rule": 2,
    "auto_rule": 3,
    "migration": 4,
    "ml": 5,
    "plaid": 6,
    "seed": 7,
    "ai": 8,
}


def test_full_precedence_ladder(real_db: Database, fresh_txn: str) -> None:
    """Walk every (existing, attempted) pair; verify outcome matches the table."""
    svc = CategorizationService(real_db)
    sources = list(_PRIORITY.keys())
    for existing in sources:
        for attempted in sources:
            real_db.execute(
                "DELETE FROM app.transaction_categories WHERE transaction_id = ?",
                [fresh_txn],
            )
            svc.write_categorization(
                transaction_id=fresh_txn,
                category="Initial",
                subcategory=None,
                categorized_by=existing,
            )
            outcome = svc.write_categorization(
                transaction_id=fresh_txn,
                category="Replacement",
                subcategory=None,
                categorized_by=attempted,
            )
            should_write = _PRIORITY[attempted] <= _PRIORITY[existing]
            assert outcome.written is should_write, (
                f"{attempted!r} writing over {existing!r}: "
                f"expected written={should_write}, got {outcome.written}"
            )
