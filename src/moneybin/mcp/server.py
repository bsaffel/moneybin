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

from moneybin.mcp.middleware import ValidationErrorMiddleware
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

        Tool names mirror the hierarchy with underscores, verb at end: accounts_balance_assert, transactions_matches_confirm, reports_networth, reports_spending.

        Read surface:
        - transactions_get — primary transaction read tool. Filter parameters: `accounts` (list of IDs or display names), `date_from` / `date_to` ('YYYY-MM-DD'), `categories` (list), `amount_min` / `amount_max` (decimal strings), `description` (case-insensitive pattern), `uncategorized_only` (bool). Returns notes/tags/splits. Cursor pagination via `cursor` + `next_cursor`.
        - reports_spending, reports_cashflow — monthly aggregates. Bounds are `from_month` / `to_month` as 'YYYY-MM' (defaults to the last 12 months when both are omitted).

        Curation surface (visible at connect):
        - transactions_create — bulk manual entry (1..100 atomic)
        - transactions_notes_{add,edit,delete} — note threads on a transaction
        - transactions_tags_set, transactions_tags_rename — declarative tagging + global rename
        - transactions_splits_set — declarative split replacement
        - import_labels_set — declarative labels on an import_id
        - system_audit_list — unified audit log (filter by actor, action_pattern, target, time)

        Getting oriented:
        - system_status — what data exists, freshness, pending review queues, transforms-pending signal
        - reports_spending — monthly spending trend with MoM/YoY/trailing deltas

        Refreshing derived tables:
        - import_files and import_inbox_sync apply transforms once at end of batch by default.
        - When system_status.data.transforms.pending is true, call transform_apply to rebuild core.* tables.

        Conventions:
        - Every tool returns {summary, data, actions}. Check summary.has_more for pagination; actions[] suggests next steps and explains how to widen capped defaults.
        - Money amounts are JSON numbers in `summary.display_currency`. Negative = expense, positive = income (transfers exempt).
        - Month-bucket fields (year_month, period) are 'YYYY-MM' strings.
        - When a tool rejects a kwarg with a Pydantic 'unexpected_keyword_argument' error, the error envelope (when present) lists accepted parameter names; otherwise call the tool with no arguments and read its docstring/schema.
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
