"""Tests for V046 content-derived re-import detection."""

from __future__ import annotations

from moneybin.database import Database
from moneybin.sql.migrations.V046__add_file_sha256_to_import_log import migrate
from tests.moneybin.migration_helpers import run_migration

_COLUMN_SHAPE_SQL = """
SELECT column_name, data_type, is_nullable
  FROM information_schema.columns
 WHERE table_schema = 'raw' AND table_name = 'import_log'
 ORDER BY ordinal_position
"""


def _create_legacy_raw_import_log_without_file_sha256(db: Database) -> None:
    """Simulate a pre-V046 (and pre-MB-255-move) database.

    A fresh install no longer creates raw.import_log at all (MB-255 moved
    the table to app.import_log with file_sha256 already in its DDL) --
    V046 now only has work to do on a database still carrying the legacy
    raw table.
    """
    db.execute(
        """
        CREATE TABLE raw.import_log (
            import_id VARCHAR PRIMARY KEY,
            source_file VARCHAR NOT NULL,
            source_type VARCHAR NOT NULL,
            source_origin VARCHAR NOT NULL,
            account_names JSON NOT NULL,
            status VARCHAR NOT NULL DEFAULT 'importing'
        )
        """
    )


def test_v046_adds_file_sha256_to_a_legacy_raw_import_log(db: Database) -> None:
    _create_legacy_raw_import_log_without_file_sha256(db)

    run_migration(db, migrate)

    columns = {row[0]: row[2] for row in db.execute(_COLUMN_SHAPE_SQL).fetchall()}
    assert columns["file_sha256"] == "YES"


def test_v046_leaves_existing_batches_matchable_by_path(db: Database) -> None:
    """Rows that predate the column keep NULL and keep their path behavior.

    A backfill is impossible here — the source file may be long gone — so the
    upgrade has to leave old batches on the path predicate alone.
    """
    _create_legacy_raw_import_log_without_file_sha256(db)
    db.execute(
        "INSERT INTO raw.import_log "
        "(import_id, source_file, source_type, source_origin, "
        " account_names, status) "
        "VALUES ('legacy-01', '/tmp/legacy.ofx', 'ofx', 'synthetic_credit_union', "  # test fixture path
        " '[\"checking\"]', 'complete')"
    )

    run_migration(db, migrate)

    assert db.execute(
        "SELECT file_sha256 FROM raw.import_log WHERE import_id = 'legacy-01'"
    ).fetchone() == (None,)


def test_v046_is_a_no_op_on_a_fresh_install(db: Database) -> None:
    """A fresh install never creates raw.import_log -- migrate() must not fail.

    MB-255 moved the table to app.import_log, whose schema file already
    declares file_sha256, so there is nothing left for V046 to add.
    """
    assert (
        db.execute(
            "SELECT 1 FROM duckdb_tables() WHERE schema_name = 'raw' AND table_name = 'import_log'"
        ).fetchone()
        is None
    )

    run_migration(db, migrate)  # should not raise

    columns = {
        row[0]
        for row in db.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'app' AND table_name = 'import_log'"
        ).fetchall()
    }
    assert "file_sha256" in columns


def test_v046_is_idempotent(db: Database) -> None:
    """Fresh installs and migration upgrades may both invoke the DDL."""
    _create_legacy_raw_import_log_without_file_sha256(db)
    run_migration(db, migrate)
    run_migration(db, migrate)

    assert db.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.columns
        WHERE table_schema = 'raw'
          AND table_name = 'import_log'
          AND column_name = 'file_sha256'
        """
    ).fetchone() == (1,)
