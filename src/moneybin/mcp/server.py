"""MCP server definition with DuckDB connection management.

This module creates the FastMCP server instance. Each tool call opens a
short-lived ``Database`` connection via ``get_database()`` and closes it
when done. There is no long-lived singleton held by the server.

Documentation: https://modelcontextprotocol.github.io/python-sdk/
"""

from __future__ import annotations

import logging
import textwrap
from pathlib import Path

from fastmcp import FastMCP
from fastmcp.server.transforms import Visibility

from moneybin.tables import TableRef

logger = logging.getLogger(__name__)


# Extended-namespace domains. Each tool tagged with one of these starts hidden
# by the Visibility transform installed in register_core_tools() and is
# re-enabled per-session via moneybin_discover.
EXTENDED_DOMAIN_DESCRIPTIONS: dict[str, str] = {
    "categorize": "Rules, merchant mappings, categorization",
    "budget": "Budget targets, status, rollovers",
    "tax": "W-2 data, deductible expense search",
    "privacy": "Consent status, grants, revocations, audit log",
    "transactions_matches": "Match review workflow",
}

EXTENDED_DOMAINS: frozenset[str] = frozenset(EXTENDED_DOMAIN_DESCRIPTIONS)


# Global server instance — tools/resources/prompts register against this
mcp = FastMCP(
    "MoneyBin",
    instructions=textwrap.dedent(
        """\
        MoneyBin is a local-first personal finance platform. All data lives in DuckDB on the user's machine.

        Top-level groups:
        - accounts (balance) — financial accounts and per-account workflows
        - transactions (matches, categorize) — transactions and workflows on them
        - assets — physical assets (real estate, vehicles, valuables)
        - categories, merchants — taxonomy reference data
        - reports — cross-domain analytical and aggregation views (networth, spending, cashflow, financial health, budget vs actual)
        - tax — tax forms, deductions, future capital gains
        - system — data status
        - import, sync — data ingestion (sync_pull/status/connect available; OAuth flows return URLs the client opens)
        - privacy — consent and audit

        Tool names mirror the hierarchy with underscores, verb at end: accounts_balance_assert, transactions_matches_confirm, reports_networth_get, reports_spending_get.

        Read surface:
        - transactions_get — primary transaction read tool; filter by account, date, category, amount, description; returns notes/tags/splits; cursor pagination

        Curation surface (visible at connect):
        - transactions_create — bulk manual entry (1..100 atomic)
        - transactions_notes_{add,edit,delete} — note threads on a transaction
        - transactions_tags_set, transactions_tags_rename — declarative tagging + global rename
        - transactions_splits_set — declarative split replacement
        - import_labels_set — declarative labels on an import_id
        - system_audit_list — unified audit log (filter by actor, action_pattern, target, time)

        Getting oriented:
        - system_status — what data exists, freshness, pending review queues
        - reports_spending_get — monthly spending trend with MoM/YoY/trailing deltas

        Conventions:
        - Every tool returns {summary, data, actions}. Check summary.has_more for pagination; actions[] suggests next steps.
        - Prefer batch tools (transactions_categorize_apply, transactions_categorize_rules_create).
        - Sensitivity tiers: low / medium / high. Without consent, tools degrade to aggregates — they never fail.

        Cold-start workflow:
        When the user has uncategorized transactions (visible via system_status or after
        import_inbox_sync), use moneybin_discover('categorize') to enable the categorize.*
        namespace. Then: transactions_categorize_assist returns redacted descriptions for
        you to propose categories on; the user reviews; transactions_categorize_apply
        commits accepted proposals. Privacy: assist sends only redacted descriptions —
        no amounts, dates, or account IDs. Only invoke when uncategorized count is
        non-trivial and the user has indicated interest in AI-assisted categorization.
        """
    ),
    # mask_error_details wraps unclassified exceptions in a generic ToolError.
    # Classified domain exceptions are caught by mcp_tool and returned as error
    # envelopes before reaching this boundary.
    mask_error_details=True,
)


_tools_registered = False


def get_db_path() -> Path:
    """Get the path to the DuckDB database file."""
    from moneybin.config import get_settings

    return get_settings().database.path


def table_exists(table: TableRef) -> bool:
    """Check if a table exists in the database.

    Args:
        table: Table reference to check.

    Returns:
        True if the table exists.
    """
    from moneybin.database import get_database

    try:
        with get_database(read_only=True) as db:
            result = db.execute(
                """
                SELECT COUNT(*)
                FROM information_schema.tables
                WHERE table_schema = ? AND table_name = ?
                """,
                [table.schema, table.name],
            ).fetchone()
            return bool(result and result[0] > 0)
    except Exception:  # noqa: BLE001
        return False


def init_db() -> None:
    """Register MCP tools."""
    register_core_tools()


def close_db() -> None:
    """Flush metrics if the database was accessed during the session."""
    from moneybin.database import database_was_accessed

    if database_was_accessed():
        from moneybin.observability import flush_metrics

        flush_metrics()
    try:
        logger.info("MCP session closing")
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
    from moneybin.mcp.tools.categories import register_categories_tools
    from moneybin.mcp.tools.curation import register_curation_tools
    from moneybin.mcp.tools.discover import register_discover_tool
    from moneybin.mcp.tools.import_inbox import register_inbox_tools
    from moneybin.mcp.tools.import_tools import register_import_tools
    from moneybin.mcp.tools.merchants import register_merchants_tools
    from moneybin.mcp.tools.reports import register_reports_tools
    from moneybin.mcp.tools.sql import register_sql_tools
    from moneybin.mcp.tools.sync import register_sync_tools
    from moneybin.mcp.tools.system import register_system_tools
    from moneybin.mcp.tools.tax import register_tax_tools
    from moneybin.mcp.tools.transactions import register_transactions_tools
    from moneybin.mcp.tools.transactions_categorize import (
        register_transactions_categorize_tools,
    )
    from moneybin.mcp.tools.transactions_categorize_assist import (
        register_transactions_categorize_assist_tools,
    )
    from moneybin.mcp.tools.transform import register_transform_tools

    register_system_tools(mcp)
    register_reports_tools(mcp)
    register_accounts_tools(mcp)
    register_transactions_tools(mcp)
    register_transactions_categorize_tools(mcp)
    register_transactions_categorize_assist_tools(mcp)
    register_curation_tools(mcp)
    register_categories_tools(mcp)
    register_merchants_tools(mcp)
    register_import_tools(mcp)
    register_inbox_tools(mcp)
    register_budget_tools(mcp)
    register_tax_tools(mcp)
    register_sync_tools(mcp)
    register_transform_tools(mcp)
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
