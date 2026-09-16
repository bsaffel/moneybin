"""V064: drop raw.ofx_institutions + its orphaned view; reconcile V003."""

from __future__ import annotations

from pathlib import Path

from moneybin.database import Database
from moneybin.migrations import Migration, short_hash
from moneybin.sql.migrations.V064__drop_ofx_institutions import migrate
from tests.moneybin.migration_helpers import run_migration

_V003_PATH = (
    Path(__file__).resolve().parents[2]
    / "src"
    / "moneybin"
    / "sql"
    / "migrations"
    / "V003__ofx_import_batch_columns.py"
)


def _table_exists(db: Database, schema: str, table: str) -> bool:
    row = db.execute(
        "SELECT 1 FROM duckdb_tables() WHERE schema_name = ? AND table_name = ?",
        [schema, table],
    ).fetchone()
    return row is not None


def _view_exists(db: Database, schema: str, view: str) -> bool:
    row = db.execute(
        "SELECT 1 FROM duckdb_views() WHERE schema_name = ? AND view_name = ?",
        [schema, view],
    ).fetchone()
    return row is not None


def _create_ofx_institutions(db: Database) -> None:
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
        """
        INSERT INTO raw.ofx_institutions
            (organization, fid, source_file, extracted_at, import_id)
        VALUES
            ('SAMPLE CREDIT UNION', '1001', 'sample1.ofx', '2026-01-05 08:00:00', 'imp-aaa111'),
            ('SYNTHETIC SAVINGS BANK', '2002', 'sample2.ofx', '2026-02-11 09:30:00', 'imp-bbb222'),
            ('DEMO NATIONAL BANK', '3003', 'sample3.ofx', '2026-03-20 14:15:00', 'imp-ccc333')
        """
    )


def test_v064_drops_a_pre_existing_table(db: Database) -> None:
    """An upgraded database that still has the table loses it."""
    _create_ofx_institutions(db)
    assert _table_exists(db, "raw", "ofx_institutions")

    run_migration(db, migrate)

    assert not _table_exists(db, "raw", "ofx_institutions")


def test_v064_is_a_no_op_on_a_fresh_install(db: Database) -> None:
    """A fresh install never creates the table -- migrate() must not fail on it."""
    assert not _table_exists(db, "raw", "ofx_institutions")

    run_migration(db, migrate)  # should not raise

    assert not _table_exists(db, "raw", "ofx_institutions")


def test_v064_idempotent_on_second_run(db: Database) -> None:
    _create_ofx_institutions(db)

    run_migration(db, migrate)
    run_migration(db, migrate)  # should not raise

    assert not _table_exists(db, "raw", "ofx_institutions")


def test_v064_does_not_touch_a_previously_materialized_staging_view(
    db: Database,
) -> None:
    """A pre-existing prep view over the table is deliberately left alone.

    `prep` is a SQLMesh-owned schema -- migrations may not DROP/ALTER a
    relation there (tests/moneybin/test_migration_schema_ownership.py). DuckDB
    also does not drop a dependent view when its base table is dropped, so the
    view survives this migration and would raise on SELECT until a later
    SQLMesh plan prunes it. That transient window is real and out of scope for
    this migration; this test locks in that the migration doesn't attempt (and
    isn't expected) to touch prep.* at all.
    """
    _create_ofx_institutions(db)
    db.execute("CREATE SCHEMA IF NOT EXISTS prep")
    db.execute(
        "CREATE VIEW prep.stg_ofx__institutions AS "
        "SELECT organization, fid FROM raw.ofx_institutions"
    )
    assert _view_exists(db, "prep", "stg_ofx__institutions")

    run_migration(db, migrate)  # should not raise, and must not touch prep.*

    assert not _table_exists(db, "raw", "ofx_institutions")
    assert _view_exists(db, "prep", "stg_ofx__institutions")


def test_v064_reconciles_stale_v003_checksum(db: Database) -> None:
    """An upgraded DB's stale V003 checksum row is rewritten to match the file.

    V003's `_TABLE_COLUMNS` dropped its raw.ofx_institutions entry in the same
    change that adds V064, which changes V003's on-disk checksum. A database
    that already applied the old V003 body carries the old checksum in
    app.schema_migrations, and check_drift() would report a false "modified
    since it was applied" warning until this migration rewrites it.
    """
    stale_checksum = "0" * 64
    stale_content_hash = "0" * 16
    db.execute(
        "INSERT INTO app.schema_migrations "
        "(version, filename, checksum, success, execution_ms, content_hash) "
        "VALUES (3, 'V003__ofx_import_batch_columns.py', ?, TRUE, 12, ?)",
        [stale_checksum, stale_content_hash],
    )

    run_migration(db, migrate)

    row = db.execute(
        "SELECT checksum, content_hash FROM app.schema_migrations WHERE version = 3"
    ).fetchone()
    assert row is not None
    stored_checksum, stored_content_hash = row

    # Derive the expectation from the runner's own parser, not from a second
    # copy of the hashing logic: V064 hardcodes its hash computation on
    # purpose (a frozen migration must not import live module code), so the
    # value that matters is whether that frozen copy still agrees with what
    # check_drift() will compare against. Re-hashing here with hashlib would
    # pass even if both sides drifted away from the runner together.
    expected_checksum = Migration.from_file(_V003_PATH).checksum
    assert stored_checksum == expected_checksum
    assert stored_checksum != stale_checksum
    assert stored_content_hash == short_hash(_V003_PATH.read_bytes())


def test_v064_v003_reconciliation_is_a_no_op_when_row_absent(db: Database) -> None:
    """A fresh install has no version-3 row yet -- migrate() must not fail on it."""
    row = db.execute("SELECT 1 FROM app.schema_migrations WHERE version = 3").fetchone()
    assert row is None

    run_migration(db, migrate)  # should not raise

    row = db.execute("SELECT 1 FROM app.schema_migrations WHERE version = 3").fetchone()
    assert row is None
