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


@mcp.resource("moneybin://tools")
def resource_tools() -> str:
    """Available tool namespaces with descriptions and loaded status."""
    logger.info("Resource read: moneybin://tools")
    from moneybin.mcp.namespaces import CORE_NAMESPACES_DEFAULT, NAMESPACE_DESCRIPTIONS

    core = [
        {
            "namespace": ns,
            "loaded": True,
            "description": NAMESPACE_DESCRIPTIONS.get(ns, ""),
        }
        for ns in sorted(CORE_NAMESPACES_DEFAULT)
    ]
    return json.dumps({"core": core, "discover_tool": "moneybin.discover"}, indent=2)
