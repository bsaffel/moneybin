"""MCP server definition with DuckDB lifecycle management.

This module creates the FastMCP server instance and manages the read-only
DuckDB connection used by all tools and resources.

Documentation: https://modelcontextprotocol.github.io/python-sdk/
"""

import logging
from pathlib import Path
from typing import NamedTuple

import duckdb
from mcp.server.fastmcp import FastMCP

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Table registry — single source of truth for schema-qualified table names
# ---------------------------------------------------------------------------


class TableRef(NamedTuple):
    """Reference to a database table with schema and name."""

    schema: str
    name: str

    @property
    def full_name(self) -> str:
        """Schema-qualified table name for use in SQL queries."""
        return f"{self.schema}.{self.name}"


# -- Core / Gold layer (canonical tables built by dbt) --
DIM_ACCOUNTS = TableRef("core", "dim_accounts")
FCT_TRANSACTIONS = TableRef("core", "fct_transactions")

# -- Raw tables (used until core models are built for these entities) --
OFX_BALANCES = TableRef("raw", "ofx_balances")
OFX_INSTITUTIONS = TableRef("raw", "ofx_institutions")
W2_FORMS = TableRef("raw", "w2_forms")


# Global server instance — tools/resources/prompts register against this
mcp = FastMCP(
    "MoneyBin Financial Data",
    instructions=(
        "MoneyBin provides read-only access to personal financial data stored "
        "in a local DuckDB database. You can query transactions, accounts, "
        "balances, tax forms, and database schema. All data stays local — "
        "nothing is sent to any external service."
    ),
)

# Module-level DuckDB connection — set by init_db() before the server starts
_db: duckdb.DuckDBPyConnection | None = None


def get_db() -> duckdb.DuckDBPyConnection:
    """Get the read-only DuckDB connection.

    Returns:
        The active DuckDB connection.

    Raises:
        RuntimeError: If the database has not been initialized.
    """
    if _db is None:
        raise RuntimeError("DuckDB connection not initialized. Call init_db() first.")
    return _db


def table_exists(table: TableRef) -> bool:
    """Check if a table exists in the database.

    Args:
        table: Table reference to check.

    Returns:
        True if the table exists.
    """
    db = get_db()
    try:
        result = db.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = ? AND table_name = ?
            """,
            [table.schema, table.name],
        ).fetchone()
        return bool(result and result[0] > 0)
    except Exception:
        return False


def init_db(db_path: Path) -> None:
    """Open DuckDB in read-only mode for the given profile database.

    Args:
        db_path: Path to the DuckDB database file.

    Raises:
        FileNotFoundError: If the database file does not exist.
        duckdb.IOException: If the database cannot be opened.
    """
    global _db  # noqa: PLW0603 — module-level singleton is intentional

    if not db_path.exists():
        raise FileNotFoundError(
            f"Database file not found: {db_path}\n"
            "Run 'moneybin load' to create and populate the database first."
        )

    logger.info("Opening DuckDB in read-only mode: %s", db_path)
    _db = duckdb.connect(str(db_path), read_only=True)
    logger.info("DuckDB connection established")


def close_db() -> None:
    """Close the DuckDB connection if open."""
    global _db  # noqa: PLW0603 — module-level singleton is intentional

    if _db is not None:
        _db.close()
        _db = None
        logger.info("DuckDB connection closed")
