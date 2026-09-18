"""V066: move raw.import_log to app.import_log (MB-255); reconcile V046."""

from __future__ import annotations

from pathlib import Path

from moneybin.database import Database
from moneybin.migrations import Migration, short_hash
from moneybin.sql.migrations.V066__move_import_log_to_app_schema import migrate
from tests.moneybin.migration_helpers import run_migration

_V046_PATH = (
    Path(__file__).resolve().parents[2]
    / "src"
    / "moneybin"
    / "sql"
    / "migrations"
    / "V046__add_file_sha256_to_import_log.py"
)


def _table_exists(db: Database, schema: str, table: str) -> bool:
    row = db.execute(
        "SELECT 1 FROM duckdb_tables() WHERE schema_name = ? AND table_name = ?",
        [schema, table],
    ).fetchone()
    return row is not None


def _create_legacy_raw_import_log(db: Database) -> None:
    """Simulate a pre-move database still carrying data in raw.import_log."""
    db.execute(
        """
        CREATE TABLE raw.import_log (
            import_id VARCHAR PRIMARY KEY,
            source_file VARCHAR NOT NULL,
            source_type VARCHAR NOT NULL,
            source_origin VARCHAR NOT NULL,
            format_name VARCHAR,
            format_source VARCHAR,
            account_names JSON NOT NULL,
            status VARCHAR NOT NULL DEFAULT 'importing',
            rows_total INTEGER,
            rows_imported INTEGER,
            rows_rejected INTEGER DEFAULT 0,
            rows_skipped_trailing INTEGER DEFAULT 0,
            rejection_details JSON,
            detection_confidence VARCHAR,
            number_format VARCHAR,
            date_format VARCHAR,
            sign_convention VARCHAR,
            balance_validated BOOLEAN,
            started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            completed_at TIMESTAMP,
            reverted_at TIMESTAMP,
            file_sha256 VARCHAR
        )
        """
    )
    db.execute(
        "INSERT INTO raw.import_log "
        "(import_id, source_file, source_type, source_origin, account_names, status) "
        "VALUES ('legacy-01', '/tmp/legacy.ofx', 'ofx', 'synthetic_credit_union', "  # test fixture path
        " '[\"checking\"]', 'complete')"
    )


def test_v066_moves_data_from_raw_to_app(db: Database) -> None:
    """An upgraded database's raw.import_log rows land in app.import_log."""
    _create_legacy_raw_import_log(db)
    assert _table_exists(db, "raw", "import_log")

    run_migration(db, migrate)

    assert not _table_exists(db, "raw", "import_log")
    row = db.execute(
        "SELECT source_file, status FROM app.import_log WHERE import_id = 'legacy-01'"
    ).fetchone()
    assert row == ("/tmp/legacy.ofx", "complete")  # noqa: S108  # test fixture path


def test_v066_is_a_no_op_on_a_fresh_install(db: Database) -> None:
    """A fresh install never creates raw.import_log -- migrate() must not fail."""
    assert not _table_exists(db, "raw", "import_log")
    assert _table_exists(db, "app", "import_log")

    run_migration(db, migrate)  # should not raise

    assert not _table_exists(db, "raw", "import_log")
    assert db.execute("SELECT COUNT(*) FROM app.import_log").fetchone() == (0,)


def test_v066_idempotent_on_second_run(db: Database) -> None:
    _create_legacy_raw_import_log(db)

    run_migration(db, migrate)
    run_migration(db, migrate)  # should not raise

    assert not _table_exists(db, "raw", "import_log")
    assert db.execute("SELECT COUNT(*) FROM app.import_log").fetchone() == (1,)


def test_v066_reconciles_stale_v046_checksum(db: Database) -> None:
    """An upgraded DB's stale V046 checksum row is rewritten to match the file.

    V046 gained a table-existence guard in the same change that adds V066,
    which changes V046's on-disk checksum. A database that already applied
    the old V046 body carries the old checksum in app.schema_migrations, and
    check_drift() would report a false "modified since it was applied"
    warning until this migration rewrites it.
    """
    _create_legacy_raw_import_log(db)
    stale_checksum = "0" * 64
    stale_content_hash = "0" * 16
    db.execute(
        "INSERT INTO app.schema_migrations "
        "(version, filename, checksum, success, execution_ms, content_hash) "
        "VALUES (46, 'V046__add_file_sha256_to_import_log.py', ?, TRUE, 12, ?)",
        [stale_checksum, stale_content_hash],
    )

    run_migration(db, migrate)

    row = db.execute(
        "SELECT checksum, content_hash FROM app.schema_migrations WHERE version = 46"
    ).fetchone()
    assert row is not None
    stored_checksum, stored_content_hash = row

    # Derive the expectation from the runner's own parser, not a second copy
    # of the hashing logic — see V064's identical rationale for V003.
    expected_checksum = Migration.from_file(_V046_PATH).checksum
    assert stored_checksum == expected_checksum
    assert stored_checksum != stale_checksum
    assert stored_content_hash == short_hash(_V046_PATH.read_bytes())


def test_v066_v046_reconciliation_is_a_no_op_when_raw_import_log_absent(
    db: Database,
) -> None:
    """A fresh install has nothing to move -- the V046 row is untouched too."""
    row = db.execute(
        "SELECT 1 FROM app.schema_migrations WHERE version = 46"
    ).fetchone()

    run_migration(db, migrate)  # should not raise

    row_after = db.execute(
        "SELECT 1 FROM app.schema_migrations WHERE version = 46"
    ).fetchone()
    assert row_after == row
