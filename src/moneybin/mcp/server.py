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


# Extended-namespace domains. Each tool tagged with one of these starts hidden
# by the Visibility transform installed in register_core_tools() and is
# re-enabled per-session via moneybin_discover.
EXTENDED_DOMAIN_DESCRIPTIONS: dict[str, str] = {
    "categorize": "Rules, merchant mappings, bulk categorization",
    "budget": "Budget targets, status, rollovers",
    "tax": "W-2 data, deductible expense search",
    "privacy": "Consent status, grants, revocations, audit log",
    "transactions_matches": "Match review workflow",
}

EXTENDED_DOMAINS: frozenset[str] = frozenset(EXTENDED_DOMAIN_DESCRIPTIONS)


# Global server instance — tools/resources/prompts register against this
mcp = FastMCP(
    "MoneyBin",
    instructions=(
        "MoneyBin is an AI-powered personal finance app. Tools use "
        "underscore-joined namespaces (spending_summary, accounts_balances, etc.). "
        "Core tools are available immediately. Extended namespaces "
        "(categorize, budget, tax, privacy) can be loaded with "
        "moneybin_discover. All data stays local in DuckDB.\n\n"
        "IMPORTANT: Prefer bulk tools (categorize_bulk, categorize_create_rules) "
        "over single-item operations. Fetch a batch, reason about all items, "
        "then submit in one call.\n\n"
        "Every tool returns {summary, data, actions}. Check summary.has_more "
        "for pagination and actions[] for suggested next steps."
    ),
    # mask_error_details wraps unclassified exceptions in a generic ToolError.
    # Classified domain exceptions are caught by mcp_tool and returned as error
    # envelopes before reaching this boundary.
    mask_error_details=True,
)


_tools_registered = False


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
    """Register all MCP tools and install the extended-namespace visibility guard.

    Tools tagged with an extended-namespace domain are hidden globally by a
    single Visibility transform; moneybin_discover re-enables them per-session.

    Idempotent — safe to call multiple times within a process.
    """
    global _tools_registered
    if _tools_registered:
        return

    from moneybin.mcp.tools.accounts import register_accounts_tools
    from moneybin.mcp.tools.budget import register_budget_tools
    from moneybin.mcp.tools.categorize import register_categorize_tools
    from moneybin.mcp.tools.discover import register_discover_tool
    from moneybin.mcp.tools.import_inbox import register_inbox_tools
    from moneybin.mcp.tools.import_tools import register_import_tools
    from moneybin.mcp.tools.spending import register_spending_tools
    from moneybin.mcp.tools.sql import register_sql_tools
    from moneybin.mcp.tools.tax import register_tax_tools
    from moneybin.mcp.tools.transactions import register_transactions_tools

    register_spending_tools(mcp)
    register_accounts_tools(mcp)
    register_transactions_tools(mcp)
    register_import_tools(mcp)
    register_inbox_tools(mcp)
    register_categorize_tools(mcp)
    register_budget_tools(mcp)
    register_tax_tools(mcp)
    register_sql_tools(mcp)
    register_discover_tool(mcp)

    from moneybin.config import get_settings

    if get_settings().mcp.progressive_disclosure:
        # Single Visibility transform with OR-match semantics across all extended
        # domains: a tool tagged with ANY of these tags is hidden until enabled by
        # moneybin_discover. Verified against fastmcp 3.1.x; see
        # tests/moneybin/test_mcp/test_visibility.py::test_visibility_or_match_semantics
        # which guards against an upstream change to AND-match.
        mcp.add_transform(Visibility(False, tags=set(EXTENDED_DOMAINS)))
        logger.info(
            f"Registered tools; {len(EXTENDED_DOMAINS)} extended namespaces hidden by default"
        )
    else:
        logger.info(
            "Registered tools; progressive disclosure disabled — all namespaces visible"
        )

    _tools_registered = True
