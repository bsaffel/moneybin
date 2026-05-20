"""Database schema initialization.

Creates all schemas and tables required by MoneyBin. Every DDL statement
uses ``CREATE … IF NOT EXISTS`` so the function is idempotent and safe to
call on every startup.

Table and column comments are written as inline SQL comments in each schema
file and applied to DuckDB's catalog after each file executes. sqlglot parses
the SQL and extracts comments from the AST — the same mechanism SQLMesh uses
internally via ``register_comments`` for its own model files.

Table comments
--------------
A ``/* description */`` block comment on the line immediately before
``CREATE TABLE`` is attached by sqlglot to the ``Create`` expression and
applied as ``COMMENT ON TABLE``.

Column comments
---------------
A trailing ``-- text`` on a column definition line is attached by sqlglot to
the ``ColumnDef`` expression and applied as ``COMMENT ON COLUMN``.
"""

import logging
from pathlib import Path

import duckdb
import sqlglot
import sqlglot.expressions as exp

from moneybin.database import escape_sql_literal
from moneybin.privacy.comment_sync import sync_classification_comments
from moneybin.privacy.taxonomy import strip_sigil

logger = logging.getLogger(__name__)

_SQL_DIR = Path(__file__).resolve().parent / "sql" / "schema"
_OFX_SCHEMA_DIR = Path(__file__).resolve().parent / "extractors" / "ofx" / "schema"
_PLAID_SCHEMA_DIR = Path(__file__).resolve().parent / "extractors" / "plaid" / "schema"
_TABULAR_SCHEMA_DIR = (
    Path(__file__).resolve().parent / "extractors" / "tabular" / "schema"
)


# Entries are filenames resolved against ``_SQL_DIR`` by default; tuples of
# (directory, filename) point at provider-bundled schema directories. The
# tuple form is a stopgap until Task 6 fully decentralizes schema discovery
# via Provider.schema_files().
_SchemaEntry = str | tuple[Path, str]


_SCHEMA_FILES: list[_SchemaEntry] = [
    "raw_schema.sql",
    "core_schema.sql",
    "app_schema.sql",
    "analytics_schema.sql",
    "meta_schema.sql",
    "reports_schema.sql",
    (_OFX_SCHEMA_DIR, "raw_ofx_institutions.sql"),
    (_OFX_SCHEMA_DIR, "raw_ofx_accounts.sql"),
    (_OFX_SCHEMA_DIR, "raw_ofx_transactions.sql"),
    (_OFX_SCHEMA_DIR, "raw_ofx_balances.sql"),
    (_PLAID_SCHEMA_DIR, "raw_plaid_accounts.sql"),
    (_PLAID_SCHEMA_DIR, "raw_plaid_balances.sql"),
    (_PLAID_SCHEMA_DIR, "raw_plaid_transactions.sql"),
    (_TABULAR_SCHEMA_DIR, "raw_tabular_transactions.sql"),
    (_TABULAR_SCHEMA_DIR, "raw_tabular_accounts.sql"),
    "raw_import_log.sql",
    "raw_manual_transactions.sql",
    "app_categories.sql",
    "app_user_merchants.sql",
    "app_categorization_rules.sql",
    "app_transaction_categories.sql",
    "app_budgets.sql",
    "app_transaction_notes.sql",
    "app_metrics.sql",
    "app_schema_migrations.sql",
    "app_versions.sql",
    "app_tabular_formats.sql",
    "app_match_decisions.sql",
    "app_seed_source_priority.sql",
    "app_proposed_rules.sql",
    "app_account_settings.sql",
    "app_balance_assertions.sql",
    "app_audit_log.sql",
    "app_transaction_tags.sql",
    "app_transaction_splits.sql",
    "app_imports.sql",
]


def _apply_comments(
    conn: duckdb.DuckDBPyConnection,
    sql: str,
    table_snapshot: dict[tuple[str, str], str | None],
    column_snapshot: dict[tuple[str, str, str], str | None],
) -> None:
    """Parse SQL with sqlglot and apply table and column comments to DuckDB catalog.

    sqlglot attaches SQL comments to adjacent AST nodes during parsing:

    - A ``/* description */`` block comment immediately before ``CREATE TABLE``
      is attached to the ``Create`` expression and applied as
      ``COMMENT ON TABLE``.
    - A trailing ``-- text`` on a column definition line is attached to the
      ``ColumnDef`` expression and applied as ``COMMENT ON COLUMN``.

    This is the same mechanism SQLMesh uses internally for its own models.
    Tables that do not exist yet (e.g. core tables before SQLMesh has run) are
    silently skipped.

    The snapshots are pre-loop catalog reads (``duckdb_tables()`` /
    ``duckdb_columns()``) so the comparison is a dict lookup, not a
    per-column ``SELECT``. A row whose human-prefix already matches the
    DDL comment is skipped so the privacy sigil suffix written by
    ``sync_classification_comments`` survives across startups.
    """
    for statement in sqlglot.parse(sql, dialect="duckdb"):
        if not isinstance(statement, exp.Create) or statement.kind != "TABLE":
            continue

        table = statement.find(exp.Table)
        if table is None:
            continue
        table_name = table.sql(dialect="duckdb")
        schema_str = table.args["db"].name if table.args.get("db") else None
        table_str = table.name
        if schema_str is None:
            continue

        # Table-level comment: /* description */ on the line before CREATE TABLE.
        # Use [-1] (the closest comment) to match SQLMesh's own pattern and avoid
        # picking up unrelated -- notes that may also precede the /* */ block.
        if statement.comments:
            description = statement.comments[-1].strip()
            existing = table_snapshot.get((schema_str, table_str))
            if description and strip_sigil(existing or "") != description:
                try:
                    safe_desc = escape_sql_literal(description)
                    conn.execute(f"COMMENT ON TABLE {table_name} IS '{safe_desc}'")
                    logger.debug(f"Applied table comment to {table_name}")
                except duckdb.CatalogException:
                    logger.debug(
                        f"Skipping table comment for {table_name} — table does not exist yet"
                    )

        # Column-level comments: trailing -- text on each column definition
        for col_def in statement.find_all(exp.ColumnDef):
            if not col_def.comments:
                continue
            comment = col_def.comments[-1].strip()
            if not comment:
                continue
            existing = column_snapshot.get((schema_str, table_str, col_def.name))
            if strip_sigil(existing or "") == comment:
                continue
            try:
                safe_comment = escape_sql_literal(comment)
                conn.execute(
                    f"COMMENT ON COLUMN {table_name}.{col_def.name} IS '{safe_comment}'"
                )
                logger.debug(f"Applied column comment to {table_name}.{col_def.name}")
            except (duckdb.CatalogException, duckdb.BinderException):
                # Column may not exist yet — either the table is created later
                # (e.g. SQLMesh-managed core tables) or a pending migration will
                # add the column. Comments will reapply on the next startup.
                logger.debug(
                    f"Skipping column comment for {table_name}.{col_def.name}"
                    " — column or table does not exist yet"
                )


def _snapshot_catalog_comments(
    conn: duckdb.DuckDBPyConnection,
) -> tuple[
    dict[tuple[str, str], str | None],
    dict[tuple[str, str, str], str | None],
]:
    """Read every table and column comment in one pair of queries."""
    table_rows = conn.execute(
        "SELECT schema_name, table_name, comment FROM duckdb_tables()"
    ).fetchall()
    column_rows = conn.execute(
        "SELECT schema_name, table_name, column_name, comment FROM duckdb_columns()"
    ).fetchall()
    table_snapshot: dict[tuple[str, str], str | None] = {
        (s, t): c for s, t, c in table_rows
    }
    column_snapshot: dict[tuple[str, str, str], str | None] = {
        (s, t, col): c for s, t, col, c in column_rows
    }
    return table_snapshot, column_snapshot


def init_schemas(conn: duckdb.DuckDBPyConnection) -> None:
    """Create all database schemas and tables, then apply inline comments.

    Args:
        conn: An active read-write DuckDB connection.
    """
    table_snapshot, column_snapshot = _snapshot_catalog_comments(conn)
    for entry in _SCHEMA_FILES:
        if isinstance(entry, tuple):
            sql_dir, sql_file = entry
            sql_path = sql_dir / sql_file
        else:
            sql_file = entry
            sql_path = _SQL_DIR / sql_file
        if not sql_path.exists():
            logger.warning(f"Schema file not found, skipping: {sql_file}")
            continue
        sql = sql_path.read_text()
        conn.execute(sql)
        _apply_comments(conn, sql, table_snapshot, column_snapshot)
        logger.debug(f"Executed {sql_file}")

    logger.debug(f"Executed {len(_SCHEMA_FILES)} schema files from {_SQL_DIR}")

    # Mirror the DataClass registry into the catalog (suffix comments
    # with `[class: ...]`).
    try:
        sync_classification_comments(conn)
    except duckdb.CatalogException:
        # Core tables managed by SQLMesh may not exist yet on a fresh
        # DB — they appear after the first `sqlmesh run`. The sync
        # runs again from sqlmesh_context() once those tables land.
        logger.debug("Skipping classification sync — core tables not yet present")
