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
    """Core and app table schemas with column names, types, and descriptions."""
    logger.info("Resource read: moneybin://schema")
    db = get_db()

    result = db.execute("""
        SELECT
            schema_name,
            table_name,
            column_name,
            data_type,
            comment
        FROM duckdb_columns()
        WHERE schema_name IN ('core', 'app', 'raw')
        ORDER BY schema_name, table_name, column_index
    """)
    rows = result.fetchall()

    tables: dict[str, Any] = {}
    for row in rows:
        key = f"{row[0]}.{row[1]}"
        if key not in tables:
            tables[key] = {"schema": row[0], "table": row[1], "columns": []}
        tables[key]["columns"].append({
            "name": row[2],
            "type": row[3],
            "description": row[4],
        })

    return json.dumps({"tables": list(tables.values())}, indent=2, default=str)


_CORE_NAMESPACE_DESCRIPTIONS: dict[str, str] = {
    "overview": "Data status and financial health snapshot",
    "spending": "Expense analysis, trends, category breakdowns",
    "cashflow": "Income vs outflows, net cash position",
    "accounts": "Account listing, balances, net worth",
    "transactions": "Search, corrections, annotations, recurring",
    "import": "File import, status, format management",
    "sql": "Direct read-only SQL queries",
}


def _description_for(ns: str) -> str:
    from moneybin.mcp.server import EXTENDED_DOMAIN_DESCRIPTIONS

    return _CORE_NAMESPACE_DESCRIPTIONS.get(ns) or EXTENDED_DOMAIN_DESCRIPTIONS.get(
        ns, ""
    )


def _namespace_for(tool_name: str) -> str:
    """Extract the namespace from a dot-separated tool name.

    For ``transactions.matches.pending`` returns ``transactions.matches``.
    For ``spending.summary`` returns ``spending``.
    """
    parts = tool_name.rsplit(".", 1)
    return parts[0] if len(parts) == 2 else tool_name


@mcp.resource("moneybin://tools")
async def resource_tools() -> str:
    """Available tool namespaces with descriptions and loaded status.

    "Loaded" here means visible by default — i.e. the namespace is not in
    ``EXTENDED_DOMAINS``. Extended namespaces are hidden globally and only
    enabled per-session via ``moneybin.discover``.
    """
    logger.info("Resource read: moneybin://tools")
    from moneybin.mcp.server import EXTENDED_DOMAINS

    # Use the unfiltered provider listing so hidden (extended-domain) tools
    # are still counted in their namespace summary.
    # fastmcp 3.1.x internal — public list_tools() filters by visibility,
    # which would hide extended-domain tools from this summary. Re-verify on
    # any fastmcp version bump.
    tools = await mcp._list_tools()  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]

    # Group registered tools by namespace. ``moneybin.discover`` is the
    # meta-tool — tracked separately so it doesn't appear under "core".
    namespaces: dict[str, int] = {}
    for tool in tools:
        if tool.name == "moneybin.discover":
            continue
        ns = _namespace_for(tool.name)
        namespaces[ns] = namespaces.get(ns, 0) + 1

    # Extended namespaces aren't visible at connect time but we still list
    # them so discoverers know what to call moneybin.discover with.
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
        "discover_tool": "moneybin.discover",
    }
    return json.dumps(data, indent=2)
