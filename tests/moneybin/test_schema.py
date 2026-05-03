"""Tests for schema initialization and inline-comment application."""

from pathlib import Path
from unittest.mock import MagicMock

from moneybin.database import Database


def test_init_does_not_fail_when_existing_table_missing_new_columns(
    tmp_path: Path, mock_secret_store: MagicMock
) -> None:
    """Reopening a stale DB must not crash during schema-comment application.

    Reopening a DB whose live table is missing columns added by a later
    migration must not raise BinderException during schema-comment application.

    Reproduces the bug where Database.__init__ ran _apply_comments BEFORE
    migrations: a pre-V003 ofx_institutions table (no import_id, no
    source_type) caused COMMENT ON COLUMN to fail because the column did
    not yet exist on the live table — even though V003 would add it
    moments later.
    """
    db_path = tmp_path / "moneybin.duckdb"

    db = Database(db_path, secret_store=mock_secret_store, no_auto_upgrade=True)
    try:
        db.execute("ALTER TABLE raw.ofx_institutions DROP COLUMN import_id")
        db.execute("ALTER TABLE raw.ofx_institutions DROP COLUMN source_type")
        db.execute("DELETE FROM app.schema_migrations WHERE version >= 3")
    finally:
        db.close()

    db2 = Database(db_path, secret_store=mock_secret_store, no_auto_upgrade=False)
    try:
        cols = {
            row[0]
            for row in db2.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = 'raw' AND table_name = 'ofx_institutions'"
            ).fetchall()
        }
        assert "import_id" in cols
        assert "source_type" in cols
    finally:
        db2.close()
