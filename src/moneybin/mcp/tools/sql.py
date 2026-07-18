# src/moneybin/mcp/tools/sql.py
"""SQL namespace tools — direct read-only SQL queries.

Tools:
    - sql_query — Execute a read-only SQL query with per-column privacy classification
    - sql_schema — Return the curated schema doc (low sensitivity)
"""

from __future__ import annotations

from typing import Any

from fastmcp import FastMCP

from moneybin import error_codes
from moneybin.database import get_database
from moneybin.errors import UserError
from moneybin.mcp._registration import register
from moneybin.mcp.decorator import mcp_tool
from moneybin.mcp.privacy import get_max_rows, tier_to_sensitivity
from moneybin.privacy.sql_query import execute_sql_query
from moneybin.protocol.envelope import (
    ResponseEnvelope,
    build_envelope,
    build_error_envelope,
)
from moneybin.services.schema_catalog import build_schema_doc


@mcp_tool(dynamic_classification=True)
def sql_query(query: str) -> ResponseEnvelope[Any]:
    """Execute a read-only SQL query against the core, app, and reports schemas.

    Only SELECT, WITH, DESCRIBE, SHOW, PRAGMA, and EXPLAIN queries are allowed.
    Writes and file-access functions are blocked. Data queries may reference
    the ``core``, ``app``, and ``reports`` schemas; other schemas are refused.

    Amounts use the accounting convention: negative = expense, positive = income.
    Currency is named in summary.display_currency.

    Privacy: each output column is resolved to its data class via SQL lineage
    (sqlglot). CRITICAL columns (account/routing numbers) are ALWAYS masked
    (****<last4>) — exactly as the typed account tools mask them — so raw SQL
    is not a bypass around privacy enforcement. Other tiers (amounts, descriptions,
    dates) are returned in the clear, matching the typed tools' current behavior.

    Results are capped; when truncated, summary.has_more is true and total_count
    exceeds returned_count. For schema, columns, and example queries, read
    resource `moneybin://schema` before composing non-trivial queries.

    Args:
        query: The SQL query to execute.
    """
    # execute_sql_query raises UserError (with an SQL_* code) on rejected,
    # unparseable, out-of-scope, unknown-table, or failed queries; the
    # @mcp_tool decorator converts that to a low-sensitivity error envelope.
    with get_database(read_only=True) as db:
        result = execute_sql_query(db, query, max_rows=get_max_rows())
    return build_envelope(
        data=result.records,
        sensitivity=tier_to_sensitivity(result.tier).value,
        total_count=result.total_count,
        classes_returned=result.classes_returned,
    )


@mcp_tool(dynamic_classification=True)
def sql_schema(table: str | None = None) -> ResponseEnvelope[Any]:
    """Return the curated database schema for ad-hoc SQL composition.

    Equivalent to reading the ``moneybin://schema`` MCP resource. Provided
    as a tool for hosts that don't surface MCP resources to the model
    (e.g. Claude.ai chat).

    Defaults to a compact catalog (table names + purposes + column counts)
    so agents don't pay for the full ~50KB schema document on every call.
    Pass ``table='<schema.name>'`` to get columns, comments, and example
    queries for one table. Pass ``table='*'`` to get the full document.

    Args:
        table: ``None`` (default) returns the compact catalog; a full table
            name like ``'core.fct_transactions'`` returns details for that
            table; ``'*'`` returns the full schema document.
    """
    doc = build_schema_doc()
    tables: list[dict[str, Any]] = doc["tables"]

    if table is None:
        compact = [
            {
                "name": t["name"],
                "purpose": t["purpose"],
                "column_count": len(t["columns"]),
                "example_count": len(t["examples"]),
            }
            for t in tables
        ]
        # Preserve `beyond_the_interface` (the catalog-query pointer for
        # non-curated tables) — it's a few hundred bytes and is the kind of
        # orientation hint the compact view exists to surface.
        return build_envelope(
            data={
                "version": doc["version"],
                "generated_at": doc["generated_at"],
                "conventions": doc["conventions"],
                "tables": compact,
                "beyond_the_interface": doc.get("beyond_the_interface"),
            },
            sensitivity="low",
            classes_returned=["aggregate"],
            actions=[
                "Pass table='<schema.name>' (e.g. 'core.fct_transactions') to "
                "fetch columns, comments, and example queries for one table.",
                "Pass table='*' for the full schema document (~50KB).",
            ],
        )

    if table == "*":
        return build_envelope(
            data=doc, sensitivity="low", classes_returned=["aggregate"]
        )

    matches = [t for t in tables if t["name"] == table]
    if not matches:
        available = [t["name"] for t in tables]
        known = ", ".join(available)
        return build_error_envelope(
            error=UserError(
                f"Unknown table: {table}",
                code=error_codes.SQL_UNKNOWN_TABLE,
                hint=f"Available tables: {known}",
                details={"available_tables": available},
            ),
            sensitivity="low",
            actions=[f"Call again with table=<one of>: {known}"],
        )
    return build_envelope(
        data={
            "version": doc["version"],
            "generated_at": doc["generated_at"],
            "conventions": doc["conventions"],
            "tables": matches,
        },
        sensitivity="low",
        classes_returned=["aggregate"],
    )


def register_sql_tools(mcp: FastMCP) -> None:
    """Register all sql namespace tools with the FastMCP server."""
    register(
        mcp,
        sql_query,
        "sql_query",
        "Execute a read-only SQL query against the database. "
        "Supports SELECT, WITH, DESCRIBE, SHOW, PRAGMA, EXPLAIN. "
        "Each output column is classified via SQL lineage; CRITICAL columns "
        "(account/routing numbers) are ALWAYS masked (****<last4>), exactly like "
        "the typed tools — raw SQL is not a privacy bypass. "
        "Amounts use the accounting convention (negative=expense, positive=income); "
        "currency is in summary.display_currency. "
        "Call sql_schema (or read resource moneybin://schema) for tables, "
        "columns, and example queries.",
    )
    register(
        mcp,
        sql_schema,
        "sql_schema",
        "Return the curated database schema. Default call returns a compact "
        "catalog (table names + purposes + column counts). Pass "
        "table='<schema.name>' for one table's columns/examples, or "
        "table='*' for the full ~50KB document. Mirrors the moneybin://schema "
        "resource for hosts that don't expose MCP resources.",
    )
