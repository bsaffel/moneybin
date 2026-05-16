"""Tests for V009: purge V004/V005 schema_migrations rows.

V009 deletes the V004 and V005 rows from `app.schema_migrations` because the
corresponding files were removed (they were pure CREATE-TABLE-IF-NOT-EXISTS
duplicates of their schema counterparts). Databases that applied V004/V005
would otherwise emit "File missing" drift warnings from `check_drift()`.

Test populates schema_migrations with a realistic spread of migration
history rows so the DELETE's selectivity is verified — only V004/V005 are
removed, all other rows survive.
"""

from __future__ import annotations

from datetime import datetime

import pytest

from moneybin.database import Database
from moneybin.sql.migrations.V009__purge_redundant_table_migrations import migrate
from tests.moneybin.migration_helpers import insert_rows, run_migration

_SCHEMA_MIGRATIONS_COLS = (
    "version",
    "filename",
    "checksum",
    "success",
    "execution_ms",
    "applied_at",
)

_SEED_ROWS: list[tuple[int, str, str, bool, int, datetime]] = [
    (
        1,
        "V001__rename_ofx_transaction_id.py",
        "a" * 64,
        True,
        12,
        datetime(2025, 11, 1, 10, 0, 0),
    ),
    (
        3,
        "V003__ofx_import_batch_columns.py",
        "b" * 64,
        True,
        18,
        datetime(2025, 11, 2, 10, 0, 0),
    ),
    (
        4,
        "V004__create_app_account_settings.sql",
        "c" * 64,
        True,
        9,
        datetime(2025, 11, 3, 10, 0, 0),
    ),
    (
        5,
        "V005__create_app_balance_assertions.sql",
        "d" * 64,
        True,
        7,
        datetime(2025, 11, 4, 10, 0, 0),
    ),
    (
        6,
        "V006__migrate_app_merchants_to_user_merchants.py",
        "e" * 64,
        True,
        24,
        datetime(2025, 11, 5, 10, 0, 0),
    ),
    (
        7,
        "V007__transaction_curation.py",
        "f" * 64,
        True,
        31,
        datetime(2025, 11, 6, 10, 0, 0),
    ),
]


def _populate_schema_migrations(db: Database) -> None:
    # schema_migrations is empty after init_schemas; reset defensively in case
    # a future change pre-seeds it.
    db.execute("DELETE FROM app.schema_migrations")
    insert_rows(db, "app", "schema_migrations", _SCHEMA_MIGRATIONS_COLS, _SEED_ROWS)


class TestV009Migration:
    """V009 migration: V004/V005 history rows purged; other rows preserved."""

    def test_v009_deletes_v004_and_v005_rows(self, db: Database) -> None:
        """V004 and V005 rows are removed; non-target rows untouched."""
        _populate_schema_migrations(db)

        run_migration(db, migrate)

        remaining = db.execute(
            "SELECT version FROM app.schema_migrations ORDER BY version"
        ).fetchall()
        assert [r[0] for r in remaining] == [1, 3, 6, 7]

    def test_v009_preserves_other_row_contents(self, db: Database) -> None:
        """Non-target rows keep their filenames, checksums, and timestamps."""
        _populate_schema_migrations(db)

        run_migration(db, migrate)

        rows = db.execute(
            "SELECT version, filename, checksum, success, execution_ms, applied_at "
            "FROM app.schema_migrations ORDER BY version"
        ).fetchall()
        expected = [row for row in _SEED_ROWS if row[0] not in {4, 5}]
        assert rows == expected

    def test_v009_noop_when_v004_v005_absent(self, db: Database) -> None:
        """Fresh installs that never ran V004/V005 see no error and no row churn."""
        db.execute("DELETE FROM app.schema_migrations")
        insert_rows(
            db,
            "app",
            "schema_migrations",
            _SCHEMA_MIGRATIONS_COLS,
            [r for r in _SEED_ROWS if r[0] not in {4, 5}],
        )

        run_migration(db, migrate)

        rows = db.execute(
            "SELECT version FROM app.schema_migrations ORDER BY version"
        ).fetchall()
        assert [r[0] for r in rows] == [1, 3, 6, 7]

    def test_v009_idempotent_on_second_run(self, db: Database) -> None:
        """Second migrate() is a clean no-op — rows stay as the first run left them."""
        _populate_schema_migrations(db)

        run_migration(db, migrate)
        run_migration(db, migrate)

        remaining = db.execute(
            "SELECT version FROM app.schema_migrations ORDER BY version"
        ).fetchall()
        assert [r[0] for r in remaining] == [1, 3, 6, 7]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
