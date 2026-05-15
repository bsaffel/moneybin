"""Tests for V010: add updated_at to user tables; tighten override columns to NOT NULL.

V010 adds `updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP` to
`app.user_categories` and `app.user_merchants`, and tightens the existing
`updated_at` columns on `app.category_overrides` and `app.merchant_overrides`
to NOT NULL.

See `docs/specs/core-updated-at-convention.md` — App-table schema changes.
"""

from __future__ import annotations

import pytest

from moneybin.database import Database
from moneybin.sql.migrations.V010__add_updated_at_to_user_tables import migrate


def _column_info(db: Database, schema: str, table: str, column: str) -> tuple[str, bool]:
    """Return (data_type, is_nullable_bool) for a column from duckdb_columns()."""
    row = db.execute(
        """
        SELECT data_type, is_nullable
        FROM duckdb_columns()
        WHERE schema_name = ? AND table_name = ? AND column_name = ?
        """,
        [schema, table, column],
    ).fetchone()
    assert row is not None, f"{schema}.{table}.{column} not found"
    data_type, is_nullable = row
    return data_type, bool(is_nullable)


def _reset_to_pre_v010_state(db: Database) -> None:
    """Reverse the V010 end-state on a freshly-initialised DB.

    `init_schemas` already creates the end-state shape. To exercise the migration
    we drop the new columns and re-add the override columns as nullable.
    """
    # user_categories / user_merchants: drop the updated_at column if present.
    for table in ("user_categories", "user_merchants"):
        cols = db.execute(
            "SELECT column_name FROM duckdb_columns() "
            "WHERE schema_name = 'app' AND table_name = ?",
            [table],
        ).fetchall()
        col_names = {c[0] for c in cols}
        if "updated_at" in col_names:
            db.execute(f"ALTER TABLE app.{table} DROP COLUMN updated_at")  # noqa: S608  # table is hardcoded allowlist, not user input

    # category_overrides / merchant_overrides: relax NOT NULL on updated_at.
    for table in ("category_overrides", "merchant_overrides"):
        db.execute(f"ALTER TABLE app.{table} ALTER COLUMN updated_at DROP NOT NULL")  # noqa: S608  # table is hardcoded allowlist


class TestV010Migration:
    """V010 migration: updated_at added/tightened on app override + user tables."""

    def test_v010_adds_updated_at_to_user_categories(self, db: Database) -> None:
        """app.user_categories.updated_at must exist as TIMESTAMP NOT NULL after migration."""
        _reset_to_pre_v010_state(db)

        # Confirm the column is absent before the migration runs (TDD anchor).
        pre = db.execute(
            "SELECT COUNT(*) FROM duckdb_columns() "
            "WHERE schema_name = 'app' AND table_name = 'user_categories' "
            "AND column_name = 'updated_at'"
        ).fetchone()
        assert pre is not None
        assert pre[0] == 0

        migrate(db._conn)  # pyright: ignore[reportPrivateUsage]

        data_type, is_nullable = _column_info(db, "app", "user_categories", "updated_at")
        assert data_type == "TIMESTAMP"
        assert is_nullable is False

    def test_v010_adds_updated_at_to_user_merchants(self, db: Database) -> None:
        """app.user_merchants.updated_at must exist as TIMESTAMP NOT NULL after migration."""
        _reset_to_pre_v010_state(db)

        pre = db.execute(
            "SELECT COUNT(*) FROM duckdb_columns() "
            "WHERE schema_name = 'app' AND table_name = 'user_merchants' "
            "AND column_name = 'updated_at'"
        ).fetchone()
        assert pre is not None
        assert pre[0] == 0

        migrate(db._conn)  # pyright: ignore[reportPrivateUsage]

        data_type, is_nullable = _column_info(db, "app", "user_merchants", "updated_at")
        assert data_type == "TIMESTAMP"
        assert is_nullable is False

    def test_v010_tightens_category_overrides_updated_at(self, db: Database) -> None:
        """app.category_overrides.updated_at must be NOT NULL after migration."""
        _reset_to_pre_v010_state(db)

        _, pre_nullable = _column_info(db, "app", "category_overrides", "updated_at")
        assert pre_nullable is True

        migrate(db._conn)  # pyright: ignore[reportPrivateUsage]

        _, post_nullable = _column_info(db, "app", "category_overrides", "updated_at")
        assert post_nullable is False

    def test_v010_tightens_merchant_overrides_updated_at(self, db: Database) -> None:
        """app.merchant_overrides.updated_at must be NOT NULL after migration."""
        _reset_to_pre_v010_state(db)

        _, pre_nullable = _column_info(db, "app", "merchant_overrides", "updated_at")
        assert pre_nullable is True

        migrate(db._conn)  # pyright: ignore[reportPrivateUsage]

        _, post_nullable = _column_info(db, "app", "merchant_overrides", "updated_at")
        assert post_nullable is False

    def test_v010_idempotent_on_second_run(self, db: Database) -> None:
        """Second migrate() must leave the same end-state — all four columns NOT NULL TIMESTAMP."""
        _reset_to_pre_v010_state(db)

        migrate(db._conn)  # pyright: ignore[reportPrivateUsage]
        migrate(db._conn)  # pyright: ignore[reportPrivateUsage]

        for table in ("user_categories", "user_merchants", "category_overrides", "merchant_overrides"):
            data_type, is_nullable = _column_info(db, "app", table, "updated_at")
            assert data_type == "TIMESTAMP", f"{table}.updated_at type drift"
            assert is_nullable is False, f"{table}.updated_at nullability drift"

    def test_v010_idempotent_on_fresh_install(self, db: Database) -> None:
        """On a fresh DB where init_schemas already produced the end-state, migrate() is a no-op."""
        # No _reset call — db comes from init_schemas with the final shape.
        migrate(db._conn)  # pyright: ignore[reportPrivateUsage]

        for table in ("user_categories", "user_merchants", "category_overrides", "merchant_overrides"):
            data_type, is_nullable = _column_info(db, "app", table, "updated_at")
            assert data_type == "TIMESTAMP"
            assert is_nullable is False


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
