"""MCP server definition with DuckDB connection management.

This module creates the FastMCP server instance and manages the DuckDB
connection used by all tools and resources. The server uses the shared
``Database`` singleton from ``moneybin.database``, which provides a single
long-lived read-write connection per process with encryption, schema init,
and migrations handled transparently.

Documentation: https://modelcontextprotocol.github.io/python-sdk/
"""

from __future__ import annotations

import logging
from pathlib import Path

import duckdb
from fastmcp import FastMCP
from fastmcp.server.transforms import Visibility

from moneybin.tables import TableRef

logger = logging.getLogger(__name__)


# Extended-namespace domain names. Each tool tagged with one of these starts
# hidden by a Visibility(False, tags={domain}) transform and is re-enabled
# per-session via moneybin.discover.
EXTENDED_DOMAINS: frozenset[str] = frozenset({
    "categorize",
    "budget",
    "tax",
    "privacy",
    "transactions.matches",
})


# Global server instance — tools/resources/prompts register against this
mcp = FastMCP(
    "MoneyBin",
    instructions=(
        "MoneyBin is an AI-powered personal finance app. Tools use "
        "dot-separated namespaces (spending.summary, accounts.balances, etc.). "
        "Core tools are available immediately. Extended namespaces "
        "(categorize, budget, tax, privacy) can be loaded with "
        "moneybin.discover. All data stays local in DuckDB.\n\n"
        "IMPORTANT: Prefer bulk tools (categorize.bulk, categorize.create_rules) "
        "over single-item operations. Fetch a batch, reason about all items, "
        "then submit in one call.\n\n"
        "Every tool returns {summary, data, actions}. Check summary.has_more "
        "for pagination and actions[] for suggested next steps."
    ),
    mask_error_details=True,  # Per ADR-008: masks unclassified exceptions to a generic
    # ToolError. Domain exceptions are caught by the mcp_tool decorator (Task 4) and
    # converted to error envelopes before they reach the server boundary.
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
    """Initialize the database and register MCP tools."""
    from moneybin.database import get_database

    db = get_database()
    logger.info(f"Database initialized: {db.path}")
    register_core_tools()


def close_db() -> None:
    """Close the DuckDB connection if open."""
    from moneybin.database import close_database

    close_database()
    try:
        logger.info("DuckDB connection closed")
    except ValueError:
        pass  # stderr already closed during MCP stdio shutdown


def register_core_tools() -> None:
    """Register all MCP tools and install per-domain Visibility transforms.

    Tools tagged with an extended-namespace domain (categorize, budget, tax,
    privacy, transactions.matches) are hidden globally by Visibility transforms
    installed below. moneybin.discover re-enables them per-session.
    """
    from moneybin.mcp.tools.accounts import register_accounts_tools
    from moneybin.mcp.tools.budget import register_budget_tools
    from moneybin.mcp.tools.categorize import register_categorize_tools
    from moneybin.mcp.tools.discover import register_discover_tool
    from moneybin.mcp.tools.import_tools import register_import_tools
    from moneybin.mcp.tools.spending import register_spending_tools
    from moneybin.mcp.tools.sql import register_sql_tools
    from moneybin.mcp.tools.tax import register_tax_tools
    from moneybin.mcp.tools.transactions import register_transactions_tools

    register_spending_tools(mcp)
    register_accounts_tools(mcp)
    register_transactions_tools(mcp)
    register_import_tools(mcp)
    register_categorize_tools(mcp)
    register_budget_tools(mcp)
    register_tax_tools(mcp)
    register_sql_tools(mcp)
    register_discover_tool(mcp)

    # Hide each extended namespace globally; sessions re-enable via discover.
    for domain in EXTENDED_DOMAINS:
        mcp.add_transform(Visibility(False, tags={domain}))

    logger.info(
        f"Registered tools; {len(EXTENDED_DOMAINS)} extended namespaces hidden by default"
    )
