"""Tests for V011: add updated_at to app.balance_assertions.

V011 adds `updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP` to
`app.balance_assertions`. Same bug class as V010: ADD COLUMN with DEFAULT
backfills every pre-existing row, then SET NOT NULL needs that backfill
committed before DuckDB will create the implicit index. The test populates
the table with >=3 realistic balance assertions so the backfill+tighten
path is exercised end-to-end and the "Cannot create index with outstanding
updates" regression cannot slip through.

See `docs/specs/core-updated-at-convention.md` — App-table schema changes.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from moneybin.database import Database
from moneybin.sql.migrations.V011__add_updated_at_to_balance_assertions import (
    migrate,
)
from tests.moneybin.migration_helpers import (
    column_exists,
    column_info,
    insert_rows,
    run_migration,
)


def _reset_to_pre_v011_state(db: Database) -> None:
    """Drop the V011 `updated_at` column so the migration has work to do."""
    if column_exists(db, "app", "balance_assertions", "updated_at"):
        db.execute("ALTER TABLE app.balance_assertions DROP COLUMN updated_at")


_BALANCE_ROWS: list[tuple[str, date, Decimal, str | None]] = [
    (
        "acct_amex_gold_002",
        date(2026, 1, 31),
        Decimal("-1284.92"),
        "billing cycle close",
    ),
    (
        "acct_chase_chk_001",
        date(2026, 1, 31),
        Decimal("4823.17"),
        "from paper statement",
    ),
    ("acct_chase_chk_001", date(2026, 2, 28), Decimal("5102.44"), None),
    ("acct_vanguard_003", date(2026, 2, 28), Decimal("76430.00"), "monthly snapshot"),
]


def _populate_balance_assertions(db: Database) -> None:
    insert_rows(
        db,
        "app",
        "balance_assertions",
        ("account_id", "assertion_date", "balance", "notes"),
        _BALANCE_ROWS,
    )


class TestV011Migration:
    """V011 migration: updated_at added to app.balance_assertions."""

    def test_v011_adds_updated_at_with_backfill(self, db: Database) -> None:
        """updated_at exists as TIMESTAMP NOT NULL and every pre-existing row is backfilled."""
        _reset_to_pre_v011_state(db)
        _populate_balance_assertions(db)

        assert not column_exists(db, "app", "balance_assertions", "updated_at")

        run_migration(db, migrate)

        data_type, is_nullable = column_info(
            db, "app", "balance_assertions", "updated_at"
        )
        assert data_type == "TIMESTAMP"
        assert is_nullable is False

        null_count = db.execute(
            "SELECT COUNT(*) FROM app.balance_assertions WHERE updated_at IS NULL"
        ).fetchone()
        assert null_count is not None
        assert null_count[0] == 0

        rows = db.execute(
            "SELECT account_id, assertion_date, balance, notes "
            "FROM app.balance_assertions "
            "ORDER BY account_id, assertion_date"
        ).fetchall()
        assert rows == _BALANCE_ROWS

    def test_v011_idempotent_on_second_run(self, db: Database) -> None:
        """Second migrate() leaves the same end-state — column stays NOT NULL TIMESTAMP."""
        _reset_to_pre_v011_state(db)
        _populate_balance_assertions(db)

        run_migration(db, migrate)
        run_migration(db, migrate)

        data_type, is_nullable = column_info(
            db, "app", "balance_assertions", "updated_at"
        )
        assert data_type == "TIMESTAMP"
        assert is_nullable is False

    def test_v011_idempotent_on_fresh_install(self, db: Database) -> None:
        """On a fresh DB where init_schemas already produced the end-state, migrate() is a no-op."""
        run_migration(db, migrate)

        data_type, is_nullable = column_info(
            db, "app", "balance_assertions", "updated_at"
        )
        assert data_type == "TIMESTAMP"
        assert is_nullable is False


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
