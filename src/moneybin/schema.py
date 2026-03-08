"""Database schema initialization.

Creates all schemas and tables required by MoneyBin. Every DDL statement
uses ``CREATE … IF NOT EXISTS`` so the function is idempotent and safe to
call on every startup.
"""

import logging
from pathlib import Path

import duckdb

logger = logging.getLogger(__name__)

_SQL_DIR = Path(__file__).resolve().parent / "sql" / "schema"

# Execution order: schemas first, then tables
_SCHEMA_FILES: list[str] = [
    "raw_schema.sql",
    "core_schema.sql",
    "user_schema.sql",
    "raw_ofx_institutions.sql",
    "raw_ofx_accounts.sql",
    "raw_ofx_transactions.sql",
    "raw_ofx_balances.sql",
    "raw_w2_forms.sql",
    "core_dim_accounts.sql",
    "core_fct_transactions.sql",
]


def init_schemas(conn: duckdb.DuckDBPyConnection) -> None:
    """Create all database schemas and tables.

    Args:
        conn: An active read-write DuckDB connection.
    """
    for sql_file in _SCHEMA_FILES:
        sql_path = _SQL_DIR / sql_file
        if not sql_path.exists():
            logger.warning("Schema file not found, skipping: %s", sql_file)
            continue
        conn.execute(sql_path.read_text())
        logger.debug("Executed %s", sql_file)

    logger.debug("Executed %d schema files from %s", len(_SCHEMA_FILES), _SQL_DIR)
