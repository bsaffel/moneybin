"""V014: add category_id FK columns across six (or seven) tables with backfill.

Phase 1 of the category-text -> category_id FK migration. The migration adds
a nullable ``category_id`` column to six app tables and also to
``app.rule_deactivations`` when that table exists (it was dropped in V018).
Backfills from existing ``(category, subcategory)`` text via JOIN against
``core.dim_categories``.

Populated-fixture pattern per ``.claude/rules/database.md``: V014 touches
existing data (ADD COLUMN + UPDATE backfill), so each test seeds >=3 rows
into the affected table covering user-created and seeded-default refs plus
the orphan-text case, then runs the migration inside a BEGIN/COMMIT wrap
mirroring ``MigrationRunner``.
"""

from __future__ import annotations

import pytest

from moneybin.database import Database
from moneybin.sql.migrations.V014__add_category_id_columns import migrate
from tests.moneybin.db_helpers import create_core_tables, seed_categories_view
from tests.moneybin.migration_helpers import run_migration


@pytest.fixture()
def v014_db(db: Database) -> Database:
    """Database with core tables, dim_categories view, and three user categories.

    The three user categories use deterministic IDs so backfill assertions can
    pin to exact values:
      USR0000001A -> ('Childcare', 'Daycare')
      USR0000002B -> ('Childcare', NULL)
      USR0000003C -> ('Groceries', NULL)
    The seeded default category 'Food & Drink' (FND) comes from
    ``seed_categories_view``.
    """
    create_core_tables(db)
    seed_categories_view(db)
    db.execute(
        "INSERT INTO app.user_categories "
        "(category_id, category, subcategory, is_active) "
        "VALUES ('USR0000001A', 'Childcare', 'Daycare', true), "
        "       ('USR0000002B', 'Childcare', NULL, true), "
        "       ('USR0000003C', 'Groceries', NULL, true)"
    )
    # Refresh the dim_categories view so it sees the user_categories rows.
    from moneybin.seeds import refresh_views

    refresh_views(db)
    return db


@pytest.mark.unit
class TestV014TransactionCategories:
    """V014 backfill for app.transaction_categories.category_id."""

    def test_backfills_user_category_refs(self, v014_db: Database) -> None:
        for i in range(3):
            v014_db.execute(
                "INSERT INTO app.transaction_categories "
                "(transaction_id, category, subcategory, categorized_by) "
                "VALUES (?, 'Childcare', 'Daycare', 'user')",
                [f"txn-{i}"],
            )
        run_migration(v014_db, migrate)
        rows = v014_db.execute(
            "SELECT transaction_id, category_id FROM app.transaction_categories "
            "ORDER BY transaction_id"
        ).fetchall()
        assert len(rows) == 3
        assert all(cid == "USR0000001A" for _, cid in rows)

    def test_backfills_default_category_refs(self, v014_db: Database) -> None:
        v014_db.execute(
            "INSERT INTO app.transaction_categories "
            "(transaction_id, category, subcategory, categorized_by) "
            "VALUES ('txn-fnd', 'Food & Drink', NULL, 'rule')"
        )
        run_migration(v014_db, migrate)
        row = v014_db.execute(
            "SELECT category_id FROM app.transaction_categories "
            "WHERE transaction_id = 'txn-fnd'"
        ).fetchone()
        assert row == ("FND",)

    def test_orphan_text_stays_null(self, v014_db: Database) -> None:
        v014_db.execute(
            "INSERT INTO app.transaction_categories "
            "(transaction_id, category, subcategory, categorized_by) "
            "VALUES ('txn-orphan', 'NoSuch', NULL, 'ai')"
        )
        run_migration(v014_db, migrate)
        row = v014_db.execute(
            "SELECT category_id, category FROM app.transaction_categories "
            "WHERE transaction_id = 'txn-orphan'"
        ).fetchone()
        assert row == (None, "NoSuch")

    def test_idempotent(self, v014_db: Database) -> None:
        v014_db.execute(
            "INSERT INTO app.transaction_categories "
            "(transaction_id, category, subcategory, categorized_by) "
            "VALUES ('txn-idemp', 'Groceries', NULL, 'user')"
        )
        run_migration(v014_db, migrate)
        run_migration(v014_db, migrate)
        row = v014_db.execute(
            "SELECT category_id FROM app.transaction_categories "
            "WHERE transaction_id = 'txn-idemp'"
        ).fetchone()
        assert row == ("USR0000003C",)


@pytest.mark.unit
class TestV014Budgets:
    """V014 backfill for app.budgets.category_id (top-level matches only)."""

    def test_backfills_top_level_budget(self, v014_db: Database) -> None:
        v014_db.execute(
            "INSERT INTO app.budgets "
            "(budget_id, category, monthly_amount, start_month) "
            "VALUES ('bdg-1', 'Groceries', '400.00', '2026-01')"
        )
        run_migration(v014_db, migrate)
        row = v014_db.execute(
            "SELECT category_id FROM app.budgets WHERE budget_id = 'bdg-1'"
        ).fetchone()
        assert row == ("USR0000003C",)

    def test_orphan_budget_text_stays_null(self, v014_db: Database) -> None:
        v014_db.execute(
            "INSERT INTO app.budgets "
            "(budget_id, category, monthly_amount, start_month) "
            "VALUES ('bdg-orphan', 'NoSuch', '50.00', '2026-01')"
        )
        run_migration(v014_db, migrate)
        row = v014_db.execute(
            "SELECT category_id FROM app.budgets WHERE budget_id = 'bdg-orphan'"
        ).fetchone()
        assert row == (None,)


@pytest.mark.unit
class TestV014UserMerchants:
    """V014 backfill for app.user_merchants.category_id."""

    def test_backfills_merchant_default_category(self, v014_db: Database) -> None:
        v014_db.execute(
            "INSERT INTO app.user_merchants "
            "(merchant_id, canonical_name, category, subcategory, created_by) "
            "VALUES ('mer-1', 'Whole Foods', 'Groceries', NULL, 'user')"
        )
        run_migration(v014_db, migrate)
        row = v014_db.execute(
            "SELECT category_id FROM app.user_merchants WHERE merchant_id = 'mer-1'"
        ).fetchone()
        assert row == ("USR0000003C",)

    def test_merchant_without_category_stays_null(self, v014_db: Database) -> None:
        v014_db.execute(
            "INSERT INTO app.user_merchants "
            "(merchant_id, canonical_name, category, subcategory, created_by) "
            "VALUES ('mer-2', 'No Category Co', NULL, NULL, 'user')"
        )
        run_migration(v014_db, migrate)
        row = v014_db.execute(
            "SELECT category_id FROM app.user_merchants WHERE merchant_id = 'mer-2'"
        ).fetchone()
        assert row == (None,)


@pytest.mark.unit
class TestV014TransactionSplits:
    """V014 backfill for app.transaction_splits.category_id."""

    def test_backfills_split_category(self, v014_db: Database) -> None:
        # Split rows reference a parent transaction; FK is logical, not enforced.
        v014_db.execute(
            "INSERT INTO app.transaction_splits "
            "(split_id, transaction_id, amount, category, subcategory, created_by) "
            "VALUES ('spl-1', 'txn-parent', '50.00', 'Childcare', 'Daycare', 'cli')"
        )
        run_migration(v014_db, migrate)
        row = v014_db.execute(
            "SELECT category_id FROM app.transaction_splits WHERE split_id = 'spl-1'"
        ).fetchone()
        assert row == ("USR0000001A",)


@pytest.mark.unit
class TestV014CategorizationRules:
    """V014 backfill for app.categorization_rules.category_id."""

    def test_backfills_rule_target_category(self, v014_db: Database) -> None:
        v014_db.execute(
            "INSERT INTO app.categorization_rules "
            "(rule_id, name, merchant_pattern, match_type, category, subcategory, "
            " is_active, priority) "
            "VALUES ('rul-1', 'Daycare rule', 'BRIGHT', 'contains', "
            "        'Childcare', 'Daycare', true, 100)"
        )
        run_migration(v014_db, migrate)
        row = v014_db.execute(
            "SELECT category_id FROM app.categorization_rules WHERE rule_id = 'rul-1'"
        ).fetchone()
        assert row == ("USR0000001A",)


@pytest.mark.unit
class TestV014ProposedRules:
    """V014 backfill for app.proposed_rules.category_id."""

    def test_backfills_proposed_target_category(self, v014_db: Database) -> None:
        v014_db.execute(
            "INSERT INTO app.proposed_rules "
            "(proposed_rule_id, merchant_pattern, match_type, category, subcategory, "
            " status, trigger_count, source, sample_txn_ids) "
            "VALUES ('prop-1', 'COFFEE', 'contains', 'Groceries', NULL, "
            "        'pending', 1, 'pattern_detection', ['t1'])"
        )
        run_migration(v014_db, migrate)
        row = v014_db.execute(
            "SELECT category_id FROM app.proposed_rules "
            "WHERE proposed_rule_id = 'prop-1'"
        ).fetchone()
        assert row == ("USR0000003C",)


@pytest.mark.unit
class TestV014RuleDeactivations:
    """V014 backfill for app.rule_deactivations.new_category_id (existing-install path).

    app.rule_deactivations was dropped in V018. This test simulates an existing
    install where the table exists before V014 runs, verifying the migration
    correctly backfills new_category_id when the table is present.
    """

    def test_backfills_new_category_id(self, v014_db: Database) -> None:
        # Create the table manually — schema.py no longer includes it (dropped in V018).
        # This simulates an existing install where the table was created before V018.
        v014_db.execute(  # noqa: S608  # building test fixture DDL, not executing user SQL
            """
            CREATE TABLE IF NOT EXISTS app.rule_deactivations (
                deactivation_id VARCHAR PRIMARY KEY,
                rule_id VARCHAR NOT NULL,
                reason VARCHAR NOT NULL,
                override_count INTEGER,
                new_category VARCHAR,
                new_subcategory VARCHAR,
                deactivated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        v014_db.execute(
            "INSERT INTO app.rule_deactivations "
            "(deactivation_id, rule_id, reason, override_count, "
            " new_category, new_subcategory, deactivated_at) "
            "VALUES ('deact-1', 'rul-99', 'override_threshold', 5, "
            "        'Groceries', NULL, CURRENT_TIMESTAMP)"
        )
        run_migration(v014_db, migrate)
        row = v014_db.execute(
            "SELECT new_category_id FROM app.rule_deactivations "
            "WHERE deactivation_id = 'deact-1'"
        ).fetchone()
        assert row == ("USR0000003C",)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
