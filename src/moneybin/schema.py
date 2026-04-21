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

logger = logging.getLogger(__name__)

_SQL_DIR = Path(__file__).resolve().parent / "sql" / "schema"

# Execution order: schemas first, then tables
_SCHEMA_FILES: list[str] = [
    "raw_schema.sql",
    "core_schema.sql",
    "app_schema.sql",
    "raw_ofx_institutions.sql",
    "raw_ofx_accounts.sql",
    "raw_ofx_transactions.sql",
    "raw_ofx_balances.sql",
    "raw_w2_forms.sql",
    "raw_csv_accounts.sql",
    "raw_csv_transactions.sql",
    "app_categories.sql",
    "app_merchants.sql",
    "app_categorization_rules.sql",
    "app_transaction_categories.sql",
    "app_budgets.sql",
    "app_transaction_notes.sql",
    "app_metrics.sql",
]


def _apply_comments(conn: duckdb.DuckDBPyConnection, sql: str) -> None:
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

    Args:
        conn: An active read-write DuckDB connection.
        sql: Full SQL text of a schema file.
    """
    for statement in sqlglot.parse(sql, dialect="duckdb"):
        if not isinstance(statement, exp.Create) or statement.kind != "TABLE":
            continue

        table = statement.find(exp.Table)
        if table is None:
            continue
        table_name = table.sql(dialect="duckdb")

        # Table-level comment: /* description */ on the line before CREATE TABLE.
        # Use [-1] (the closest comment) to match SQLMesh's own pattern and avoid
        # picking up unrelated -- notes that may also precede the /* */ block.
        if statement.comments:
            description = statement.comments[-1].strip()
            if description:
                try:
                    safe_desc = description.replace("'", "''")
                    conn.execute(f"COMMENT ON TABLE {table_name} IS '{safe_desc}'")
                    logger.debug("Applied table comment to %s", table_name)
                except duckdb.CatalogException:
                    logger.debug(
                        "Skipping table comment for %s — table does not exist yet",
                        table_name,
                    )

        # Column-level comments: trailing -- text on each column definition
        for col_def in statement.find_all(exp.ColumnDef):
            if not col_def.comments:
                continue
            comment = col_def.comments[-1].strip()
            if not comment:
                continue
            try:
                safe_comment = comment.replace("'", "''")
                conn.execute(
                    f"COMMENT ON COLUMN {table_name}.{col_def.name} IS '{safe_comment}'"
                )
                logger.debug(
                    "Applied column comment to %s.%s", table_name, col_def.name
                )
            except duckdb.CatalogException:
                logger.debug(
                    "Skipping column comment for %s.%s — table does not exist yet",
                    table_name,
                    col_def.name,
                )


def init_schemas(conn: duckdb.DuckDBPyConnection) -> None:
    """Create all database schemas and tables, then apply inline comments.

    Args:
        conn: An active read-write DuckDB connection.
    """
    for sql_file in _SCHEMA_FILES:
        sql_path = _SQL_DIR / sql_file
        if not sql_path.exists():
            logger.warning("Schema file not found, skipping: %s", sql_file)
            continue
        sql = sql_path.read_text()
        conn.execute(sql)
        _apply_comments(conn, sql)
        logger.debug("Executed %s", sql_file)

    logger.debug("Executed %d schema files from %s", len(_SCHEMA_FILES), _SQL_DIR)
