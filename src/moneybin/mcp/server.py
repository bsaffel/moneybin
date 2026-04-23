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
from typing import TYPE_CHECKING

import duckdb
from mcp.server.fastmcp import FastMCP

from moneybin.tables import TableRef

if TYPE_CHECKING:
    from moneybin.mcp.namespaces import NamespaceRegistry

logger = logging.getLogger(__name__)


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


_registry: NamespaceRegistry | None = None


def get_registry() -> NamespaceRegistry:
    """Get the namespace registry singleton."""
    global _registry  # noqa: PLW0603 — module-level singleton
    if _registry is None:
        _registry = _build_registry()
    return _registry


def _build_registry() -> NamespaceRegistry:
    """Build and populate the namespace registry with all tool modules."""
    from moneybin.mcp.namespaces import NamespaceRegistry
    from moneybin.mcp.tools.accounts import register_accounts_tools
    from moneybin.mcp.tools.budget import register_budget_tools
    from moneybin.mcp.tools.categorize import register_categorize_tools
    from moneybin.mcp.tools.discover import register_discover_tool
    from moneybin.mcp.tools.import_tools import register_import_tools
    from moneybin.mcp.tools.spending import register_spending_tools
    from moneybin.mcp.tools.sql import register_sql_tools
    from moneybin.mcp.tools.tax import register_tax_tools
    from moneybin.mcp.tools.transactions import register_transactions_tools

    registry = NamespaceRegistry()
    register_spending_tools(registry)
    register_accounts_tools(registry)
    register_transactions_tools(registry)
    register_import_tools(registry)
    register_categorize_tools(registry)
    register_budget_tools(registry)
    register_tax_tools(registry)
    register_sql_tools(registry)
    register_discover_tool(registry)
    return registry


def register_core_tools() -> None:
    """Register core namespace tools with FastMCP at startup."""
    from moneybin.config import get_settings
    from moneybin.mcp.namespaces import CORE_NAMESPACES_DEFAULT

    registry = get_registry()
    cfg = get_settings().mcp

    # Determine core namespaces from config or defaults
    if cfg.core_namespaces and cfg.core_namespaces == ["*"]:
        core_ns = registry.all_namespaces()
    elif cfg.core_namespaces:
        core_ns = set(cfg.core_namespaces)
    else:
        core_ns = set(CORE_NAMESPACES_DEFAULT)

    # Register core tools with FastMCP
    for tool in registry.get_core_tools(core_ns):
        mcp.tool(name=tool.name, description=tool.description)(tool.fn)
        registry.mark_loaded(tool.namespace)

    # moneybin.discover is always registered
    discover_tools = registry.get_namespace_tools("moneybin")
    for tool in discover_tools:
        if not registry.is_loaded("moneybin"):
            mcp.tool(name=tool.name, description=tool.description)(tool.fn)
    registry.mark_loaded("moneybin")

    logger.info(
        f"Registered {sum(len(registry.get_namespace_tools(ns)) for ns in core_ns)} "
        f"core tools from {len(core_ns)} namespaces"
    )
