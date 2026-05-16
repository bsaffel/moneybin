"""Tests for V010: add updated_at to user tables; tighten category_overrides to NOT NULL.

V010 adds `updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP` to
`app.user_categories` and `app.user_merchants`, and tightens the existing
`updated_at` column on `app.category_overrides` to NOT NULL.

The user_categories / user_merchants path is the load-bearing one: ADD COLUMN
with DEFAULT backfills every pre-existing row, then SET NOT NULL needs that
backfill committed before DuckDB will create the implicit index. Tests
populate both tables with >=3 realistic rows so the backfill+tighten path is
exercised end-to-end and any "Cannot create index with outstanding updates"
regression is caught immediately.

(V010 also tightens `app.merchant_overrides.updated_at` for any historical DB
that still has that table; the table itself is dropped in V012. Tests for
that branch were removed when the table was retired.)

See `docs/specs/core-updated-at-convention.md` — App-table schema changes.
"""

from __future__ import annotations

import pytest

from moneybin.database import Database
from moneybin.sql.migrations.V010__add_updated_at_to_user_tables import migrate
from tests.moneybin.migration_helpers import (
    column_exists,
    column_info,
    insert_rows,
    run_migration,
)


def _reset_to_pre_v010_state(db: Database) -> None:
    """Reverse the V010 end-state so the migration has work to do.

    `init_schemas` produces the end-state shape; drop the new columns and
    relax the override column back to nullable.
    """
    for table in ("user_categories", "user_merchants"):
        if column_exists(db, "app", table, "updated_at"):
            db.execute(f"ALTER TABLE app.{table} DROP COLUMN updated_at")  # noqa: S608  # table is hardcoded allowlist, not user input

    db.execute(
        "ALTER TABLE app.category_overrides ALTER COLUMN updated_at DROP NOT NULL"
    )


_USER_CATEGORIES_ROWS: list[tuple[str, str, str | None, str | None]] = [
    ("c1a2b3c4d5e6", "Food & Dining", "Coffee Shops", "Cafes and espresso bars"),
    ("c2b3c4d5e6f7", "Food & Dining", "Restaurants", None),
    ("c3c4d5e6f7a8", "Transport", "Rideshare", "Lyft, Uber, etc."),
    ("c4d5e6f7a8b9", "Income", "Side Gigs", None),
]

_USER_MERCHANTS_ROWS: list[
    tuple[str, str | None, str, str, str | None, str | None, str]
] = [
    (
        "m1a2b3c4d5e6",
        "STARBUCKS",
        "contains",
        "Starbucks",
        "Food & Dining",
        "Coffee Shops",
        "user",
    ),
    (
        "m2b3c4d5e6f7",
        None,
        "oneOf",
        "Lyft",
        "Transport",
        "Rideshare",
        "ai",
    ),
    (
        "m3c4d5e6f7a8",
        "WHOLE FOODS",
        "contains",
        "Whole Foods Market",
        "Food & Dining",
        "Groceries",
        "user",
    ),
    (
        "m4d5e6f7a8b9",
        "AMZN",
        "contains",
        "Amazon",
        None,
        None,
        "plaid",
    ),
]


def _populate_user_tables(db: Database) -> None:
    insert_rows(
        db,
        "app",
        "user_categories",
        ("category_id", "category", "subcategory", "description"),
        _USER_CATEGORIES_ROWS,
    )
    insert_rows(
        db,
        "app",
        "user_merchants",
        (
            "merchant_id",
            "raw_pattern",
            "match_type",
            "canonical_name",
            "category",
            "subcategory",
            "created_by",
        ),
        _USER_MERCHANTS_ROWS,
    )


class TestV010Migration:
    """V010 migration: updated_at added/tightened on app override + user tables."""

    def test_v010_adds_updated_at_to_user_categories(self, db: Database) -> None:
        """app.user_categories.updated_at must exist as TIMESTAMP NOT NULL after migration."""
        _reset_to_pre_v010_state(db)
        _populate_user_tables(db)

        assert not column_exists(db, "app", "user_categories", "updated_at")

        run_migration(db, migrate)

        data_type, is_nullable = column_info(db, "app", "user_categories", "updated_at")
        assert data_type == "TIMESTAMP"
        assert is_nullable is False

        null_count = db.execute(
            "SELECT COUNT(*) FROM app.user_categories WHERE updated_at IS NULL"
        ).fetchone()
        assert null_count is not None
        assert null_count[0] == 0

        rows = db.execute(
            "SELECT category_id, category, subcategory, description "
            "FROM app.user_categories ORDER BY category_id"
        ).fetchall()
        assert rows == _USER_CATEGORIES_ROWS

    def test_v010_adds_updated_at_to_user_merchants(self, db: Database) -> None:
        """app.user_merchants.updated_at must exist as TIMESTAMP NOT NULL after migration."""
        _reset_to_pre_v010_state(db)
        _populate_user_tables(db)

        assert not column_exists(db, "app", "user_merchants", "updated_at")

        run_migration(db, migrate)

        data_type, is_nullable = column_info(db, "app", "user_merchants", "updated_at")
        assert data_type == "TIMESTAMP"
        assert is_nullable is False

        null_count = db.execute(
            "SELECT COUNT(*) FROM app.user_merchants WHERE updated_at IS NULL"
        ).fetchone()
        assert null_count is not None
        assert null_count[0] == 0

        rows = db.execute(
            "SELECT merchant_id, raw_pattern, match_type, canonical_name, "
            "category, subcategory, created_by "
            "FROM app.user_merchants ORDER BY merchant_id"
        ).fetchall()
        assert rows == _USER_MERCHANTS_ROWS

    def test_v010_tightens_category_overrides_updated_at(self, db: Database) -> None:
        """app.category_overrides.updated_at must be NOT NULL after migration."""
        _reset_to_pre_v010_state(db)
        _populate_user_tables(db)

        _, pre_nullable = column_info(db, "app", "category_overrides", "updated_at")
        assert pre_nullable is True

        run_migration(db, migrate)

        _, post_nullable = column_info(db, "app", "category_overrides", "updated_at")
        assert post_nullable is False

    def test_v010_idempotent_on_second_run(self, db: Database) -> None:
        """Second migrate() must leave the same end-state — all three columns NOT NULL TIMESTAMP."""
        _reset_to_pre_v010_state(db)
        _populate_user_tables(db)

        run_migration(db, migrate)
        run_migration(db, migrate)

        for table in (
            "user_categories",
            "user_merchants",
            "category_overrides",
        ):
            data_type, is_nullable = column_info(db, "app", table, "updated_at")
            assert data_type == "TIMESTAMP", f"{table}.updated_at type drift"
            assert is_nullable is False, f"{table}.updated_at nullability drift"

    def test_v010_idempotent_on_fresh_install(self, db: Database) -> None:
        """On a fresh DB where init_schemas already produced the end-state, migrate() is a no-op."""
        # No _reset call — db comes from init_schemas with the final shape.
        run_migration(db, migrate)

        for table in (
            "user_categories",
            "user_merchants",
            "category_overrides",
        ):
            data_type, is_nullable = column_info(db, "app", table, "updated_at")
            assert data_type == "TIMESTAMP"
            assert is_nullable is False


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
