"""MCP server definition with DuckDB connection management.

This module creates the FastMCP server instance and manages the DuckDB
connection used by all tools and resources. The server uses the shared
``Database`` singleton from ``moneybin.database``, which provides a single
long-lived read-write connection per process with encryption, schema init,
and migrations handled transparently.

Documentation: https://modelcontextprotocol.github.io/python-sdk/
"""

import logging
from pathlib import Path

import duckdb
from mcp.server.fastmcp import FastMCP

from moneybin.tables import TableRef

logger = logging.getLogger(__name__)


# Global server instance — tools/resources/prompts register against this
mcp = FastMCP(
    "MoneyBin",
    instructions=(
        "MoneyBin is an AI-powered personal finance app. You can import bank "
        "statements (OFX/QFX files), query transactions and accounts, categorize "
        "spending, set budgets, and get financial insights. All data stays local "
        "in a DuckDB database — nothing is sent to any external service.\n\n"
        "IMPORTANT: When categorizing transactions or creating rules/merchant "
        "mappings, always prefer the bulk tools (bulk_categorize, "
        "bulk_create_categorization_rules, bulk_create_merchant_mappings) over "
        "their single-item equivalents. Fetch a batch with a read tool, reason "
        "about all items, then submit the full list in one bulk call."
    ),
)


def get_db() -> duckdb.DuckDBPyConnection:
    """Get the DuckDB connection for queries.

    Returns:
        The active DuckDB connection from the Database singleton.
    """
    from moneybin.database import get_database

    return get_database().conn


def get_db_path() -> Path:
    """Get the path to the DuckDB database file.

    Returns:
        The database file path from the Database singleton.
    """
    from moneybin.database import get_database

    return get_database().path


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


def init_db() -> None:
    """Initialize the database singleton.

    The Database class handles encryption, schema initialization, and
    migrations transparently via ``get_database()``, which reads the
    database path from ``get_settings().database.path``.
    """
    from moneybin.database import get_database

    db = get_database()
    logger.info(f"Database initialized: {db.path}")


def close_db() -> None:
    """Close the DuckDB connection if open."""
    from moneybin.database import close_database

    close_database()
    try:
        logger.info("DuckDB connection closed")
    except ValueError:
        pass  # stderr already closed during MCP stdio shutdown
