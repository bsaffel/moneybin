"""Tests for V036 categorization-decision store creation and backfill."""

from __future__ import annotations

import pytest

from moneybin.database import Database
from moneybin.repositories.categorization_decisions_repo import (
    categorization_decision_id,
)
from moneybin.sql.migrations.V036__create_categorization_decisions import migrate
from tests.moneybin.migration_helpers import run_migration


def test_v036_backfills_three_existing_categories_deterministically(
    db: Database,
) -> None:
    db.execute("DROP TABLE IF EXISTS app.categorization_decisions")
    rows = [
        ("txn-a", "Food", "Dining", "cat-food", "merchant-a"),
        ("txn-b", "Income", "Salary", "cat-income", None),
        ("txn-c", "Travel", None, "cat-travel", "merchant-c"),
    ]
    for transaction_id, category, subcategory, category_id, merchant_id in rows:
        db.execute(
            """
            INSERT INTO app.transaction_categories (
                transaction_id, category, subcategory, category_id,
                categorized_by, merchant_id
            ) VALUES (?, ?, ?, ?, 'user', ?)
            """,
            [transaction_id, category, subcategory, category_id, merchant_id],
        )

    run_migration(db, migrate)

    observed = db.execute(
        """
        SELECT decision_id, transaction_id, status, category_id, merchant_id
        FROM app.categorization_decisions
        ORDER BY transaction_id
        """
    ).fetchall()
    assert observed == [
        (
            categorization_decision_id(transaction_id),
            transaction_id,
            "accepted",
            category_id,
            merchant_id,
        )
        for transaction_id, _category, _subcategory, category_id, merchant_id in rows
    ]
    audit_targets = db.execute(
        """
        SELECT target_id
        FROM app.audit_log
        WHERE action = 'categorization_decision.backfill'
        ORDER BY target_id
        """
    ).fetchall()
    assert audit_targets == sorted([
        (categorization_decision_id(row[0]),) for row in rows
    ])


def test_v036_is_idempotent(db: Database) -> None:
    db.execute("DROP TABLE IF EXISTS app.categorization_decisions")
    db.execute(
        """
        INSERT INTO app.transaction_categories (
            transaction_id, category, category_id, categorized_by
        ) VALUES ('txn-rerun', 'Food', 'cat-food', 'user')
        """
    )

    run_migration(db, migrate)
    first_rows = db.execute(
        "SELECT COUNT(*) FROM app.categorization_decisions"
    ).fetchone()
    first_audits = db.execute(
        "SELECT COUNT(*) FROM app.audit_log "
        "WHERE action = 'categorization_decision.backfill'"
    ).fetchone()
    run_migration(db, migrate)

    assert (
        db.execute("SELECT COUNT(*) FROM app.categorization_decisions").fetchone()
        == first_rows
        == (1,)
    )
    assert (
        db.execute(
            "SELECT COUNT(*) FROM app.audit_log "
            "WHERE action = 'categorization_decision.backfill'"
        ).fetchone()
        == first_audits
        == (1,)
    )


@pytest.mark.parametrize(
    ("category_id", "categorized_at", "missing_column"),
    [
        (None, "2026-07-19 12:00:00", "category_id"),
        ("cat-food", None, "categorized_at"),
    ],
)
def test_v036_stops_on_orphaned_legacy_accepted_rows(
    db: Database,
    category_id: str | None,
    categorized_at: str | None,
    missing_column: str,
) -> None:
    db.execute("DROP TABLE IF EXISTS app.categorization_decisions")
    db.execute(
        """
        INSERT INTO app.transaction_categories (
            transaction_id, category, category_id, categorized_at, categorized_by
        ) VALUES ('txn-legacy-null', 'Legacy', ?, ?, 'user')
        """,
        [category_id, categorized_at],
    )

    with pytest.raises(ValueError, match=missing_column):
        run_migration(db, migrate)

    assert (
        db.execute(
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_schema = 'app' "
            "AND table_name = 'categorization_decisions'"
        ).fetchone()
        is None
    )
