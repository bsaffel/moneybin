# src/moneybin/mcp/tools/sql.py
"""SQL namespace tools — direct read-only SQL queries.

Tools:
    - sql.query — Execute a read-only SQL query (medium sensitivity)
"""

from __future__ import annotations

import logging

from fastmcp import FastMCP

from moneybin.database import get_database
from moneybin.mcp._registration import register
from moneybin.mcp.decorator import mcp_tool
from moneybin.mcp.privacy import get_max_rows, validate_read_only_query
from moneybin.protocol.envelope import ResponseEnvelope, build_envelope

logger = logging.getLogger(__name__)


@mcp_tool(sensitivity="medium")
def sql_query(query: str) -> ResponseEnvelope:
    """Execute a read-only SQL query against the database.

    Only SELECT, WITH, DESCRIBE, SHOW, PRAGMA, and EXPLAIN queries
    are allowed. Write operations and file-access functions are blocked.

    Use this for ad-hoc analysis not covered by other tools. Results
    are limited to the configured maximum row count.

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


def register_sql_tools(mcp: FastMCP) -> None:
    """Register all sql namespace tools with the FastMCP server."""
    register(
        mcp,
        sql_query,
        "sql.query",
        "Execute a read-only SQL query against the database. "
        "Supports SELECT, WITH, DESCRIBE, SHOW, PRAGMA, EXPLAIN.",
    )
