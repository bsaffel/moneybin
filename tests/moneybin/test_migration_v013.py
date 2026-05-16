"""Tests for V013: add content_hash to app.schema_migrations and backfill.

V013 adds a nullable content_hash VARCHAR column and best-effort backfills
existing rows from on-disk migration files. Rows whose file no longer exists
stay NULL and are treated as "unknown" by the self-heal guard.

Populated-fixture pattern per `.claude/rules/database.md`: V013 touches
existing data (UPDATE during backfill), so we seed schema_migrations with a
realistic mix of rows — some whose files exist on disk, some whose files
don't — and assert the post-migration state row-by-row.
"""

from __future__ import annotations

import pytest

from moneybin.database import Database
from moneybin.migrations import short_hash
from moneybin.sql.migrations.V013__add_content_hash_to_schema_migrations import (
    MIGRATIONS_DIR,
    migrate,
)
from tests.moneybin.migration_helpers import column_exists, insert_rows, run_migration

_SEED_COLUMNS = ("version", "filename", "checksum", "success")


def _reset_to_pre_v013_state(db: Database) -> None:
    """Drop content_hash so V013 has work to do."""
    if column_exists(db, "app", "schema_migrations", "content_hash"):
        db.execute("ALTER TABLE app.schema_migrations DROP COLUMN content_hash")


def _expected_hash_for(filename: str) -> str:
    """SHA-256 truncated to 16 hex chars for a real migration file on disk."""
    return short_hash((MIGRATIONS_DIR / filename).read_bytes())


class TestV013Migration:
    """V013: schema_migrations.content_hash added and backfilled."""

    def test_v013_adds_nullable_content_hash_column(self, db: Database) -> None:
        """content_hash exists as VARCHAR (nullable) after migration."""
        _reset_to_pre_v013_state(db)
        run_migration(db, migrate)

        row = db.execute(
            """
            SELECT data_type, is_nullable FROM duckdb_columns()
            WHERE schema_name = 'app'
              AND table_name = 'schema_migrations'
              AND column_name = 'content_hash'
            """
        ).fetchone()
        assert row is not None
        assert row[0] == "VARCHAR"
        assert bool(row[1]) is True

    def test_v013_backfills_rows_whose_file_exists(self, db: Database) -> None:
        """Rows whose filename matches a file on disk get the truncated SHA-256."""
        _reset_to_pre_v013_state(db)

        # Seed three real migrations + one synthetic missing-file row.
        insert_rows(
            db,
            "app",
            "schema_migrations",
            _SEED_COLUMNS,
            [
                (1, "V001__rename_ofx_transaction_id.py", "seed", True),
                (2, "V002__backfill_gold_keys.sql", "seed", True),
                (3, "V003__ofx_import_batch_columns.py", "seed", True),
                (999, "V999__never_existed.sql", "seed", True),
            ],
        )

        run_migration(db, migrate)

        rows = dict(
            db.execute(
                "SELECT version, content_hash FROM app.schema_migrations "
                "WHERE version IN (1, 2, 3, 999) ORDER BY version"
            ).fetchall()
        )
        assert rows[1] == _expected_hash_for("V001__rename_ofx_transaction_id.py")
        assert rows[2] == _expected_hash_for("V002__backfill_gold_keys.sql")
        assert rows[3] == _expected_hash_for("V003__ofx_import_batch_columns.py")

    def test_v013_leaves_missing_file_rows_null(self, db: Database) -> None:
        """Rows whose filename does not exist on disk are left NULL — unknown to self-heal."""
        _reset_to_pre_v013_state(db)
        insert_rows(
            db,
            "app",
            "schema_migrations",
            _SEED_COLUMNS,
            [(888, "V888__gone.sql", "seed", True)],
        )

        run_migration(db, migrate)

        row = db.execute(
            "SELECT content_hash FROM app.schema_migrations WHERE version = 888"
        ).fetchone()
        assert row is not None
        assert row[0] is None

    def test_v013_idempotent_on_second_run(self, db: Database) -> None:
        """Re-running migrate() leaves the same end-state."""
        _reset_to_pre_v013_state(db)

        insert_rows(
            db,
            "app",
            "schema_migrations",
            _SEED_COLUMNS,
            [(1, "V001__rename_ofx_transaction_id.py", "seed", True)],
        )

        run_migration(db, migrate)
        run_migration(db, migrate)

        row = db.execute(
            "SELECT content_hash FROM app.schema_migrations WHERE version = 1"
        ).fetchone()
        assert row is not None
        assert row[0] == _expected_hash_for("V001__rename_ofx_transaction_id.py")

    def test_v013_idempotent_on_fresh_install(self, db: Database) -> None:
        """Fresh install: init_schemas already added the column; migrate() is a no-op."""
        # No _reset call — `db` comes from init_schemas with content_hash already
        # in the schema DDL.
        run_migration(db, migrate)

        assert column_exists(db, "app", "schema_migrations", "content_hash")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
