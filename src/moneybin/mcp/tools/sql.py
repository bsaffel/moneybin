# src/moneybin/mcp/tools/sql.py
"""SQL namespace tools — direct read-only SQL queries.

Tools:
    - sql_query — Execute a read-only SQL query (medium sensitivity)
    - sql_schema — Return the curated schema doc (low sensitivity)
"""

from __future__ import annotations

from typing import Any

from fastmcp import FastMCP

from moneybin.database import get_database
from moneybin.mcp._registration import register
from moneybin.mcp.decorator import mcp_tool
from moneybin.mcp.privacy import get_max_rows, validate_read_only_query
from moneybin.protocol.envelope import ResponseEnvelope, build_envelope
from moneybin.services.schema_catalog import build_schema_doc


@mcp_tool(sensitivity="medium")
def sql_query(query: str) -> ResponseEnvelope:
    """Execute a read-only SQL query against the database.

    Only SELECT, WITH, DESCRIBE, SHOW, PRAGMA, and EXPLAIN queries
    are allowed. Write operations and file-access functions are blocked.

    Use this for ad-hoc analysis not covered by other tools. Results
    are limited to the configured maximum row count.

    For schema, columns, and example queries, read resource
    `moneybin://schema` before composing non-trivial queries.

    Args:
        query: The SQL query to execute.
    """
    error = validate_read_only_query(query)
    if error:
        return build_envelope(
            data={"error": error},
            sensitivity="low",
        )

    # Security: This tool intentionally executes user-provided SQL.
    # Parameterized queries are not applicable here — the entire query
    # is user input. Safety relies on validate_read_only_query() above,
    # which blocks write operations, file-access functions, and URL schemes.
    with get_database(read_only=True) as db:
        result = db.execute(query)
        columns = [desc[0] for desc in result.description]
        rows = result.fetchmany(get_max_rows())
    records = [dict(zip(columns, row, strict=False)) for row in rows]
    return build_envelope(data=records, sensitivity="medium")


@mcp_tool(sensitivity="low")
def sql_schema(table: str | None = None) -> ResponseEnvelope:
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
        return build_envelope(
            data={
                "version": doc["version"],
                "generated_at": doc["generated_at"],
                "conventions": doc["conventions"],
                "tables": compact,
            },
            sensitivity="low",
            actions=[
                "Pass table='<schema.name>' (e.g. 'core.fct_transactions') to "
                "fetch columns, comments, and example queries for one table.",
                "Pass table='*' for the full schema document (~50KB).",
            ],
        )

    if table == "*":
        return build_envelope(data=doc, sensitivity="low")

    matches = [t for t in tables if t["name"] == table]
    if not matches:
        known = ", ".join(t["name"] for t in tables)
        return build_envelope(
            data={
                "version": doc["version"],
                "generated_at": doc["generated_at"],
                "conventions": doc["conventions"],
                "tables": [],
                "error": f"Unknown table: {table}",
                "available_tables": [t["name"] for t in tables],
            },
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
    )


def register_sql_tools(mcp: FastMCP) -> None:
    """Register all sql namespace tools with the FastMCP server."""
    register(
        mcp,
        sql_query,
        "sql_query",
        "Execute a read-only SQL query against the database. "
        "Supports SELECT, WITH, DESCRIBE, SHOW, PRAGMA, EXPLAIN. "
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
