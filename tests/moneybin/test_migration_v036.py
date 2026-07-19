"""Tests for V036 categorization-decision store creation and backfill."""

from __future__ import annotations

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

    run_migration(db, migrate)
    run_migration(db, migrate)

    assert db.execute(
        "SELECT COUNT(*) FROM app.categorization_decisions"
    ).fetchone() == (0,)
