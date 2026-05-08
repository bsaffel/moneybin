# src/moneybin/mcp/resources.py
"""MCP v1 resource definitions.

Resources provide ambient context loaded when the AI connects — schema
information, account list, privacy status, data freshness. They are
read-only, compact, and change infrequently.

See ``mcp-tool-surface.md`` section 15.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from moneybin.database import get_database
from moneybin.services.account_service import AccountService
from moneybin.services.networth_service import NetworthService
from moneybin.services.schema_catalog import build_schema_doc
from moneybin.tables import DIM_ACCOUNTS, FCT_TRANSACTIONS

from .server import get_db, mcp, table_exists

logger = logging.getLogger(__name__)


@mcp.resource("moneybin://status")
def resource_status() -> str:
    """Data freshness: row counts, date ranges, last import, categorization coverage."""
    logger.info("Resource read: moneybin://status")
    db = get_db()
    status: dict[str, Any] = {}

    if table_exists(FCT_TRANSACTIONS):
        row = db.execute(f"""
            SELECT COUNT(*), MIN(transaction_date), MAX(transaction_date)
            FROM {FCT_TRANSACTIONS.full_name}
        """).fetchone()
        if row:
            status["transactions"] = {
                "total": row[0],
                "date_range_start": str(row[1]) if row[1] else None,
                "date_range_end": str(row[2]) if row[2] else None,
            }

    if table_exists(DIM_ACCOUNTS):
        row = db.execute(f"SELECT COUNT(*) FROM {DIM_ACCOUNTS.full_name}").fetchone()
        status["accounts"] = {"total": row[0] if row else 0}

    return json.dumps(status, indent=2, default=str)


@mcp.resource("moneybin://accounts")
def resource_accounts() -> str:
    """Account list with types, institutions, currencies. No balances."""
    logger.info("Resource read: moneybin://accounts")
    db = get_db()

    if not table_exists(DIM_ACCOUNTS):
        return json.dumps({"accounts": []})

    result = db.execute(f"""
        SELECT account_id, account_type, institution_name, source_type
        FROM {DIM_ACCOUNTS.full_name}
        ORDER BY institution_name, account_type
    """)
    columns = [desc[0] for desc in result.description]
    rows = result.fetchall()
    records = [dict(zip(columns, row, strict=False)) for row in rows]
    return json.dumps({"accounts": records}, indent=2, default=str)


@mcp.resource("moneybin://privacy")
def resource_privacy() -> str:
    """Active consent grants and configured AI backend. Stub until privacy specs land."""
    logger.info("Resource read: moneybin://privacy")
    return json.dumps(
        {
            "consent_grants": [],
            "configured_backend": None,
            "consent_mode": "opt-in",
            "unmask_critical": False,
        },
        indent=2,
    )


@mcp.resource("moneybin://schema")
def resource_schema() -> str:
    """Curated schema for ad-hoc SQL: interface tables, columns, comments, example queries."""
    logger.info("Resource read: moneybin://schema")
    doc = build_schema_doc()
    return json.dumps(doc, indent=2, default=str)


_CORE_NAMESPACE_DESCRIPTIONS: dict[str, str] = {
    "accounts": "Account listing, balances, net worth",
    "categories": "Category taxonomy reference data",
    "import": "File import, status, format management",
    "merchants": "Merchant name mapping reference data",
    "reports": "Spending analysis, budget vs actual, financial summaries",
    "sql": "Direct read-only SQL queries",
    "transactions": "Search, corrections, annotations, recurring",
}


def _description_for(ns: str) -> str:
    from moneybin.mcp.server import EXTENDED_DOMAIN_DESCRIPTIONS

    return _CORE_NAMESPACE_DESCRIPTIONS.get(ns) or EXTENDED_DOMAIN_DESCRIPTIONS.get(
        ns, ""
    )


def _namespace_for(tool_name: str) -> str:
    """Extract the namespace prefix from an underscore-joined tool name.

    First-underscore split: ``reports_spending_by_category`` → ``reports``,
    ``accounts_balance_list`` → ``accounts``. Multi-segment namespaces
    (e.g. ``transactions_matches``) are matched explicitly via longest-prefix
    lookup so tools like ``transactions_matches_pending`` group correctly.
    """
    from moneybin.mcp.server import EXTENDED_DOMAIN_DESCRIPTIONS

    for ns in EXTENDED_DOMAIN_DESCRIPTIONS:
        if "_" in ns and tool_name.startswith(f"{ns}_"):
            return ns
    head, sep, _ = tool_name.partition("_")
    return head if sep else tool_name


@mcp.resource("moneybin://tools")
async def resource_tools() -> str:
    """Available tool namespaces with descriptions and loaded status.

    "Loaded" here means visible by default — i.e. the namespace is not in
    ``EXTENDED_DOMAINS``. Extended namespaces are hidden globally and only
    enabled per-session via ``moneybin_discover``.
    """
    logger.info("Resource read: moneybin://tools")
    from moneybin.mcp.server import EXTENDED_DOMAINS

    # Use the unfiltered provider listing so hidden (extended-domain) tools
    # are still counted in their namespace summary. Re-verify on any fastmcp
    # version bump beyond 3.1.x.
    tools = await mcp._list_tools()  # noqa: SLF001  # fastmcp internal — public list_tools() filters by visibility  # pyright: ignore[reportPrivateUsage]

    # Group registered tools by namespace. ``moneybin_discover`` is the
    # meta-tool — tracked separately so it doesn't appear under "core".
    namespaces: dict[str, int] = {}
    for tool in tools:
        if tool.name == "moneybin_discover":
            continue
        ns = _namespace_for(tool.name)
        namespaces[ns] = namespaces.get(ns, 0) + 1

    # Extended namespaces aren't visible at connect time but we still list
    # them so discoverers know what to call moneybin_discover with.
    all_namespaces = set(namespaces.keys()) | set(EXTENDED_DOMAINS)

    core_list: list[dict[str, Any]] = []
    extended_list: list[dict[str, Any]] = []
    for ns in sorted(all_namespaces):
        entry = {
            "namespace": ns,
            "tools": namespaces.get(ns, 0),
            "loaded": ns not in EXTENDED_DOMAINS,
            "description": _description_for(ns),
        }
        if ns in EXTENDED_DOMAINS:
            extended_list.append(entry)
        else:
            core_list.append(entry)

    data = {
        "core": core_list,
        "extended": extended_list,
        "discover_tool": "moneybin_discover",
    }
    return json.dumps(data, indent=2)


@mcp.resource("accounts://summary")
def resource_accounts_summary() -> str:
    """High-level account snapshot for AI conversation context.

    Returns total counts, counts by type and subtype, count archived,
    count excluded from net worth, count with recent activity (30 days).
    No per-account data, no balances, no PII.
    """
    logger.info("Resource read: accounts://summary")
    return json.dumps(AccountService(get_database()).summary(), default=str)


@mcp.resource("moneybin://recent-curation")
def resource_recent_curation() -> str:
    """Last 50 audit events — ambient context for curation workflows.

    Resources are enhancement-only (some MCP clients are tools-only); the
    canonical read path for audit events is the ``system_audit_list`` tool.
    Sensitivity: medium — audit before/after values can carry row-level data.
    """
    logger.info("Resource read: moneybin://recent-curation")
    from moneybin.services.audit_service import AuditService

    events = AuditService(get_database()).list_events(limit=50)
    payload = [
        {
            "audit_id": e.audit_id,
            "occurred_at": e.occurred_at,
            "actor": e.actor,
            "action": e.action,
            "target_schema": e.target_schema,
            "target_table": e.target_table,
            "target_id": e.target_id,
            "before_value": e.before_value,
            "after_value": e.after_value,
            "parent_audit_id": e.parent_audit_id,
            "context_json": e.context_json,
        }
        for e in events
    ]
    return json.dumps({"events": payload}, default=str)


@mcp.resource("net-worth://summary")
def resource_networth_summary() -> str:
    """Current net worth snapshot for AI conversation context.

    Returns total net worth, total assets, total liabilities, account count,
    and as-of date. Does not include per-account breakdown; use the
    reports_networth_get tool for that.
    """
    logger.info("Resource read: net-worth://summary")
    snapshot = NetworthService(get_database()).current()
    return json.dumps(snapshot.to_dict(include_per_account=False), default=str)
