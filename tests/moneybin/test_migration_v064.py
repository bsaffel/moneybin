"""V064: drop the retired raw.ofx_institutions table (MB-256)."""

from __future__ import annotations

from moneybin.database import Database
from moneybin.sql.migrations.V064__drop_ofx_institutions import migrate
from tests.moneybin.migration_helpers import run_migration


def _table_exists(db: Database, schema: str, table: str) -> bool:
    row = db.execute(
        "SELECT 1 FROM duckdb_tables() WHERE schema_name = ? AND table_name = ?",
        [schema, table],
    ).fetchone()
    return row is not None


def test_v064_drops_a_pre_existing_table(db: Database) -> None:
    """An upgraded database that still has the table loses it."""
    db.execute(
        """
        CREATE TABLE raw.ofx_institutions (
            organization VARCHAR,
            fid VARCHAR,
            source_file VARCHAR,
            extracted_at TIMESTAMP,
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            import_id VARCHAR,
            source_type VARCHAR DEFAULT 'ofx',
            PRIMARY KEY (organization, fid)
        )
        """
    )
    db.execute(
        "INSERT INTO raw.ofx_institutions (organization, fid, source_file, extracted_at) "
        "VALUES ('SAMPLE BANK', '9999', 'test.ofx', CURRENT_TIMESTAMP)"
    )
    assert _table_exists(db, "raw", "ofx_institutions")

    run_migration(db, migrate)

    assert not _table_exists(db, "raw", "ofx_institutions")


def test_v064_is_a_no_op_on_a_fresh_install(db: Database) -> None:
    """A fresh install never creates the table -- migrate() must not fail on it."""
    assert not _table_exists(db, "raw", "ofx_institutions")

    run_migration(db, migrate)  # should not raise

    assert not _table_exists(db, "raw", "ofx_institutions")


def test_v064_idempotent_on_second_run(db: Database) -> None:
    run_migration(db, migrate)
    run_migration(db, migrate)  # should not raise
