# src/moneybin/mcp/tools/sql.py
"""SQL namespace tools — direct read-only SQL queries.

Tools:
    - sql_query — Execute a read-only SQL query (medium sensitivity)
    - sql_schema — Return the curated schema doc (low sensitivity)
"""

from __future__ import annotations

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

    db = get_database()
    # Security: This tool intentionally executes user-provided SQL.
    # Parameterized queries are not applicable here — the entire query
    # is user input. Safety relies on validate_read_only_query() above,
    # which blocks write operations, file-access functions, and URL schemes.
    result = db.execute(query)
    columns = [desc[0] for desc in result.description]
    rows = result.fetchmany(get_max_rows())
    records = [dict(zip(columns, row, strict=False)) for row in rows]
    return build_envelope(data=records, sensitivity="medium")


@mcp_tool(sensitivity="low")
def sql_schema() -> ResponseEnvelope:
    """Return the curated database schema for ad-hoc SQL composition.

    Equivalent to reading the ``moneybin://schema`` MCP resource. Provided
    as a tool for hosts that don't surface MCP resources to the model
    (e.g. Claude.ai chat). Returns interface tables, columns, comments,
    conventions, and example queries.
    """
    return build_envelope(data=build_schema_doc(), sensitivity="low")


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
        "Return the curated database schema: interface tables, columns, "
        "comments, conventions, and example queries. Mirrors the "
        "moneybin://schema resource for hosts that don't expose MCP resources.",
    )
