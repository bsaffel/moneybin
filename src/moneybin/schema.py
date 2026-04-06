"""Database schema initialization.

Creates all schemas and tables required by MoneyBin. Every DDL statement
uses ``CREATE … IF NOT EXISTS`` so the function is idempotent and safe to
call on every startup.

Column comments are written as inline SQL comments (``-- text``) on each
column definition inside ``CREATE TABLE`` blocks. This module extracts those
comments and applies them to the DuckDB catalog via ``COMMENT ON COLUMN``
after each file executes, so they are always up to date without requiring a
separate comment-maintenance file.
"""

import logging
import re
from pathlib import Path

import duckdb

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
]

# Matches: CREATE TABLE [IF NOT EXISTS] schema.table (
_TABLE_RE = re.compile(
    r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([\w.]+)\s*\(",
    re.IGNORECASE,
)

# Matches a column definition line with a trailing inline comment:
#   column_name TYPE [constraints][,]  -- comment text
#   column_name TYPE [constraints][,]  /* comment text */
_COL_COMMENT_RE = re.compile(
    r"^\s+(\w+)\s+\S.*?(?:--\s*(.+?)\s*$|/\*\s*(.+?)\s*\*/)",
)

# Lines starting with these keywords are table-level constraints, not columns
_CONSTRAINT_KEYWORDS = frozenset({
    "PRIMARY",
    "UNIQUE",
    "FOREIGN",
    "CHECK",
    "CONSTRAINT",
    "INDEX",
})


def _apply_inline_column_comments(conn: duckdb.DuckDBPyConnection, sql: str) -> None:
    """Extract inline SQL comments from CREATE TABLE columns and apply as COMMENT ON COLUMN.

    Scans each line of ``sql`` for column definitions of the form::

        column_name TYPE [constraints],  -- comment text

    inside ``CREATE TABLE`` blocks and executes ``COMMENT ON COLUMN`` for each
    one found. Tables that do not exist yet (e.g. core tables before SQLMesh
    runs its first plan) are silently skipped.

    Args:
        conn: An active read-write DuckDB connection.
        sql: Full SQL text of a schema file.
    """
    current_table: str | None = None
    paren_depth = 0

    for line in sql.splitlines():
        # Detect start of a CREATE TABLE block
        table_match = _TABLE_RE.match(line)
        if table_match:
            current_table = table_match.group(1)
            paren_depth = line.count("(") - line.count(")")
            continue

        if current_table is None:
            continue

        paren_depth += line.count("(") - line.count(")")
        if paren_depth <= 0:
            current_table = None
            continue

        col_match = _COL_COMMENT_RE.match(line)
        if not col_match:
            continue

        col_name = col_match.group(1)
        if col_name.upper() in _CONSTRAINT_KEYWORDS:
            continue

        comment = col_match.group(2) or col_match.group(3)
        if not comment:
            continue

        try:
            safe_comment = comment.strip().replace("'", "''")
            conn.execute(
                f"COMMENT ON COLUMN {current_table}.{col_name} IS '{safe_comment}'"
            )
            logger.debug("Applied comment to %s.%s", current_table, col_name)
        except duckdb.CatalogException:
            logger.debug(
                "Skipping column comment for %s.%s — table does not exist yet",
                current_table,
                col_name,
            )


def init_schemas(conn: duckdb.DuckDBPyConnection) -> None:
    """Create all database schemas and tables, then apply inline column comments.

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
        _apply_inline_column_comments(conn, sql)
        logger.debug("Executed %s", sql_file)

    logger.debug("Executed %d schema files from %s", len(_SCHEMA_FILES), _SQL_DIR)
