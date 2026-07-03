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

    db = Database(
        db_path, secret_store=mock_secret_store, no_auto_upgrade=True, read_only=False
    )
    try:
        db.execute("ALTER TABLE raw.ofx_institutions DROP COLUMN import_id")
        db.execute("ALTER TABLE raw.ofx_institutions DROP COLUMN source_type")
        # Drop seeds.categories (and the dependent dim view) so V014 recreates
        # it fresh on replay with its own era shape (plaid_detailed, no class).
        # The first open's refresh_views built it in the current shape (no
        # plaid_detailed); leaving it there makes V014's frozen
        # `SELECT s.plaid_detailed` view rebuild fail on reopen.
        db.execute("DROP VIEW IF EXISTS core.dim_categories")
        db.execute("DROP TABLE IF EXISTS seeds.categories")
        db.execute("DELETE FROM app.schema_migrations WHERE version >= 3")
    finally:
        db.close()

    db2 = Database(
        db_path, secret_store=mock_secret_store, no_auto_upgrade=False, read_only=False
    )
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


def test_init_does_not_fail_when_proposed_rules_missing_rule_id(
    tmp_path: Path, mock_secret_store: MagicMock
) -> None:
    """Reopening a pre-V016 DB must not crash on the rule_id index DDL.

    Schema DDL runs before migrations. A pre-V016 ``app.proposed_rules``
    table has no ``rule_id`` column; any ``CREATE INDEX`` against that
    column in the schema file binds before V016 adds the column and
    raises BinderException. Index creation for migration-added columns
    belongs in the migration, not the schema file.
    """
    db_path = tmp_path / "moneybin.duckdb"

    db = Database(
        db_path, secret_store=mock_secret_store, no_auto_upgrade=True, read_only=False
    )
    try:
        # DuckDB refuses ALTER ... DROP COLUMN while any index exists on
        # the table, so drop both indexes; the schema file recreates the
        # unrelated pattern_status index on reopen.
        db.execute("DROP INDEX IF EXISTS app.idx_proposed_rules_rule_id")
        db.execute("DROP INDEX IF EXISTS app.idx_proposed_rules_pattern_status")
        db.execute("ALTER TABLE app.proposed_rules DROP COLUMN rule_id")
        # Drop seeds.categories (and the dependent dim view) so V014 recreates
        # it fresh on replay with its own era shape (plaid_detailed, no class).
        # The first open's refresh_views built it in the current shape (no
        # plaid_detailed); leaving it there makes V014's frozen
        # `SELECT s.plaid_detailed` view rebuild fail on reopen.
        db.execute("DROP VIEW IF EXISTS core.dim_categories")
        db.execute("DROP TABLE IF EXISTS seeds.categories")
        db.execute("DELETE FROM app.schema_migrations WHERE version >= 16")
    finally:
        db.close()

    db2 = Database(
        db_path, secret_store=mock_secret_store, no_auto_upgrade=False, read_only=False
    )
    try:
        cols = {
            row[0]
            for row in db2.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = 'app' AND table_name = 'proposed_rules'"
            ).fetchall()
        }
        assert "rule_id" in cols
        indexes = {
            row[0]
            for row in db2.execute(
                "SELECT index_name FROM duckdb_indexes() "
                "WHERE schema_name = 'app' AND table_name = 'proposed_rules'"
            ).fetchall()
        }
        assert "idx_proposed_rules_rule_id" in indexes
    finally:
        db2.close()
