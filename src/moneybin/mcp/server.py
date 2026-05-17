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

from moneybin.mcp.middleware import ValidationErrorMiddleware
from moneybin.tables import TableRef

logger = logging.getLogger(__name__)


# Global server instance — tools/resources/prompts register against this
mcp = FastMCP(
    "MoneyBin",
    instructions=textwrap.dedent(
        """\
        MoneyBin is a local-first personal finance platform. All data lives in DuckDB on the user's machine.

        Top-level domains:
        - accounts, transactions (query/correct/annotate/match/categorize), assets (physical), reports (cross-domain analytics: networth, spending, cashflow, financial health, budget vs actual)
        - categories, merchants (taxonomy reference data)
        - tax (forms, deductions), system (status, audit), import, sync, privacy (consent, audit)

        Tool names: domain_<sub>_verb, verb at end — transactions_categorize_apply, reports_networth, accounts_balance_assert.

        Start with system_status — shows what data exists, freshness, pending review queues, and whether core.* tables need a refresh (system_status.data.transforms.pending → call transform_apply).

        Every tool returns {summary, data, actions}. Pagination via summary.has_more; actions[] suggests next steps and explains how to widen capped defaults. Prefer batch tools; list parameters are capped per-call.

        Money amounts are JSON numbers in `summary.display_currency`; negative = expense, positive = income (transfers exempt). Month-bucket fields (year_month, period) are 'YYYY-MM' strings.

        Sensitivity tiers low/medium/high. Without consent, tools degrade to aggregates — they never fail.
        """
    ),
    # mask_error_details wraps unclassified exceptions in a generic ToolError.
    # Classified domain exceptions are caught by mcp_tool and returned as error
    # envelopes before reaching this boundary.
    mask_error_details=True,
)

# Convert pydantic ValidationError on tool-arg binding into a friendly
# response envelope so agents see "Accepted parameters: ..." instead of a
# raw pydantic_core stack-string. Tool body code never sees the bad call.
mcp.add_middleware(ValidationErrorMiddleware(server=mcp))


_tools_registered = False


def get_db_path() -> Path:
    """Get the path to the DuckDB database file."""
    from moneybin.database import get_settings

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


def check_schema_at_boot() -> None:
    """Verify core.* materialized tables aren't stale vs. EXPECTED_CORE_COLUMNS.

    Drift detected on a read-only check triggers one synchronous
    ``TransformService.apply()`` self-heal attempt before raising. The MCP
    transform_apply tool is the intended recovery path but lives inside this
    server, so users hitting drift on reconnect would otherwise be stuck
    behind a chicken-and-egg: the server can't boot to expose the fix.
    Re-verifies with a fresh read-only connection after the heal; raises
    SchemaDriftError only if drift persists.
    """
    from moneybin.database import (
        DatabaseNotInitializedError,
        SchemaDriftError,
        check_core_schema_drift,
        get_database,
    )

    try:
        with get_database(read_only=True) as db:
            drift = check_core_schema_drift(db)
    except DatabaseNotInitializedError:
        # No DB yet means no drift to check. moneybin mcp serve already
        # surfaces a clean error for this case via classify_user_error.
        return
    if not drift:
        return

    from moneybin.services.transform_service import TransformService

    logger.info(
        f"Stale snapshots detected for {sorted(drift)}; "
        "running transform apply to self-heal."
    )
    # Plain apply (no restate_models): a regular SQLMesh plan picks up
    # model-fingerprint changes, which is exactly the production drift
    # trigger (code extends a core model; existing snapshot lacks the new
    # columns). SQLMesh's restatement mode explicitly ignores local file
    # changes (see sqlmesh.core.context.Context.plan_builder
    # `always_include_local_changes` docstring), so it would no-op against
    # the very drift we need to fix.
    with get_database() as db:
        result = TransformService(db).apply()
    if not result.applied:
        # apply() soft-fails by returning applied=False with the SQLMesh
        # error type name (apply() itself drops the full message for PII
        # reasons). Raise SchemaDriftError so classify_user_error maps it
        # to the standard "run moneybin transform apply" hint — the user's
        # recovery path is to re-run apply manually and see the real
        # SQLMesh error in the terminal.
        raise SchemaDriftError(
            f"Auto-heal failed: TransformService.apply() reported "
            f"error={result.error} after {result.duration_seconds:.2f}s"
        )
    logger.info(f"Self-heal completed in {result.duration_seconds:.2f}s")

    with get_database(read_only=True) as db:
        post_heal_drift = check_core_schema_drift(db)
    if post_heal_drift:
        tables = ", ".join(sorted(post_heal_drift))
        logger.error(f"Schema drift persists after auto-heal: {tables}")
        raise SchemaDriftError(
            f"Stale materialized snapshots persist after auto-heal: {tables}"
        )


def close_db() -> None:
    """Flush metrics on session close — flush_metrics() no-ops for read-only sessions."""
    from moneybin.observability import flush_metrics

    flush_metrics()
    try:
        logger.info("MCP session closing")
    except ValueError:
        pass  # stderr already closed during MCP stdio shutdown


def register_core_tools() -> None:
    """Register all MCP tools.

    Full registered surface is visible at connect — client-driven progressive
    disclosure was retired 2026-05-17 (see docs/specs/mcp-architecture.md §3).
    The ``@mcp_tool(domain=...)`` tag is preserved as dormant metadata for a
    possible future first-party client.

    Idempotent — safe to call multiple times within a process.
    """
    global _tools_registered
    if _tools_registered:
        return

    from moneybin.mcp.tools.accounts import register_accounts_tools
    from moneybin.mcp.tools.budget import register_budget_tools
    from moneybin.mcp.tools.categories import register_categories_tools
    from moneybin.mcp.tools.curation import register_curation_tools
    from moneybin.mcp.tools.import_inbox import register_inbox_tools
    from moneybin.mcp.tools.import_tools import register_import_tools
    from moneybin.mcp.tools.merchants import register_merchants_tools
    from moneybin.mcp.tools.reports import register_reports_tools
    from moneybin.mcp.tools.sql import register_sql_tools
    from moneybin.mcp.tools.sync import register_sync_prompts, register_sync_tools
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
    register_sync_prompts(mcp)
    register_transform_tools(mcp)
    register_sql_tools(mcp)

    logger.info("Registered MCP tools — full surface visible at connect")
    _tools_registered = True
