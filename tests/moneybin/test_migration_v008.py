"""Tests for V008: add exemplars to app.user_merchants and relax raw_pattern NOT NULL.

V008 adds `exemplars VARCHAR[] DEFAULT []` (backfills every existing row) and
relaxes `raw_pattern` from NOT NULL to nullable. Tests populate user_merchants
with >=3 realistic rows so the backfill path is exercised against real data.

See `docs/specs/categorization-matching-mechanics.md` — Schema changes.
"""

from __future__ import annotations

import pytest

from moneybin.database import Database
from moneybin.sql.migrations.V008__user_merchants_exemplars import migrate
from tests.moneybin.migration_helpers import (
    column_exists,
    column_info,
    insert_rows,
    run_migration,
)


def _reset_to_pre_v008_state(db: Database) -> None:
    """Reverse the V008 end-state: drop `exemplars`, restore raw_pattern NOT NULL."""
    if column_exists(db, "app", "user_merchants", "exemplars"):
        db.execute("ALTER TABLE app.user_merchants DROP COLUMN exemplars")
    # raw_pattern was NOT NULL pre-V008. The fresh schema relaxes it; tighten
    # it back so the migration's DROP NOT NULL branch has work to do.
    _, raw_pattern_nullable = column_info(db, "app", "user_merchants", "raw_pattern")
    if raw_pattern_nullable:
        db.execute(
            "ALTER TABLE app.user_merchants ALTER COLUMN raw_pattern SET NOT NULL"
        )


_PRE_V008_ROWS: list[tuple[str, str, str, str, str | None, str | None, str]] = [
    (
        "m_pre_v008_a",
        "STARBUCKS",
        "contains",
        "Starbucks",
        "Food & Dining",
        "Coffee Shops",
        "user",
    ),
    (
        "m_pre_v008_b",
        "WHOLE FOODS",
        "contains",
        "Whole Foods Market",
        "Food & Dining",
        "Groceries",
        "user",
    ),
    (
        "m_pre_v008_c",
        "LYFT",
        "exact",
        "Lyft",
        "Transport",
        "Rideshare",
        "rule",
    ),
    (
        "m_pre_v008_d",
        "AMZN",
        "contains",
        "Amazon",
        None,
        None,
        "plaid",
    ),
]


def _populate_user_merchants(db: Database) -> None:
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
        _PRE_V008_ROWS,
    )


class TestV008Migration:
    """V008 migration: exemplars added, raw_pattern relaxed to nullable."""

    def test_v008_adds_exemplars_and_backfills_empty_list(self, db: Database) -> None:
        """Exemplars column exists, every pre-existing row gets the empty-list default."""
        _reset_to_pre_v008_state(db)
        _populate_user_merchants(db)

        run_migration(db, migrate)

        data_type, _ = column_info(db, "app", "user_merchants", "exemplars")
        assert data_type == "VARCHAR[]"

        rows = db.execute(
            "SELECT merchant_id, exemplars FROM app.user_merchants ORDER BY merchant_id"
        ).fetchall()
        assert len(rows) == len(_PRE_V008_ROWS)
        for _merchant_id, exemplars in rows:
            assert exemplars == []

    def test_v008_relaxes_raw_pattern_to_nullable(self, db: Database) -> None:
        """raw_pattern becomes nullable; pre-existing values are preserved."""
        _reset_to_pre_v008_state(db)
        _populate_user_merchants(db)

        _, pre_nullable = column_info(db, "app", "user_merchants", "raw_pattern")
        assert pre_nullable is False

        run_migration(db, migrate)

        _, post_nullable = column_info(db, "app", "user_merchants", "raw_pattern")
        assert post_nullable is True

        rows = db.execute(
            "SELECT merchant_id, raw_pattern FROM app.user_merchants ORDER BY merchant_id"
        ).fetchall()
        assert [(r[0], r[1]) for r in rows] == [(r[0], r[1]) for r in _PRE_V008_ROWS]

    def test_v008_idempotent_on_second_run(self, db: Database) -> None:
        """Second migrate() leaves the same end-state."""
        _reset_to_pre_v008_state(db)
        _populate_user_merchants(db)

        run_migration(db, migrate)
        run_migration(db, migrate)

        data_type, _ = column_info(db, "app", "user_merchants", "exemplars")
        assert data_type == "VARCHAR[]"
        _, raw_pattern_nullable = column_info(
            db, "app", "user_merchants", "raw_pattern"
        )
        assert raw_pattern_nullable is True

    def test_v008_idempotent_on_fresh_install(self, db: Database) -> None:
        """On a fresh DB where init_schemas already produced the end-state, migrate() is a no-op."""
        run_migration(db, migrate)

        data_type, _ = column_info(db, "app", "user_merchants", "exemplars")
        assert data_type == "VARCHAR[]"
        _, raw_pattern_nullable = column_info(
            db, "app", "user_merchants", "raw_pattern"
        )
        assert raw_pattern_nullable is True


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
