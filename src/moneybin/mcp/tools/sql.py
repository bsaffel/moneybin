# src/moneybin/mcp/tools/sql.py
"""SQL namespace tools — direct read-only SQL queries.

Tools:
    - sql_query — Execute a read-only SQL query with per-column privacy classification
    - sql_schema — Return the curated schema doc (low sensitivity)
"""

from __future__ import annotations

from typing import Any

import duckdb
from fastmcp import FastMCP

from moneybin import error_codes
from moneybin.database import get_database
from moneybin.errors import UserError
from moneybin.mcp._registration import register
from moneybin.mcp.decorator import mcp_tool
from moneybin.mcp.privacy import (
    get_max_rows,
    tier_to_sensitivity,
    validate_read_only_query,
)
from moneybin.privacy.redaction import redact_records
from moneybin.privacy.sql_lineage import (
    SqlParseError,
    SqlSchemaError,
    derive_query_tier,
    expand_star,
    get_current_schema_snapshot,
    is_data_query,
    parse_cached,
    resolve_output_classes,
    tables_outside_schemas,
)
from moneybin.privacy.taxonomy import DataClass
from moneybin.protocol.envelope import (
    ResponseEnvelope,
    build_envelope,
    build_error_envelope,
)
from moneybin.services.schema_catalog import build_schema_doc

# sql_query is limited to the schemas the privacy CLASSIFICATION registry
# covers, so every queryable column has a known data class and the masking
# guarantee is sound. reports.* is deferred until its views are classified
# (tracked as a follow-up); raw/prep/meta are internal, non-consumer schemas.
_ALLOWED_QUERY_SCHEMAS = frozenset({"core", "app"})


def _execute_metadata_query(query: str) -> ResponseEnvelope[Any]:
    """Run a DESCRIBE/SHOW/PRAGMA/EXPLAIN statement directly at LOW.

    These return schema or query-plan text, not classified row data, so they
    bypass the lineage gate. They still go through ``validate_read_only_query``
    (caller) which blocks writes and file access.
    """
    max_rows = get_max_rows()
    try:
        with get_database(read_only=True) as db:
            result = db.execute(query)  # noqa: S608 — read-only metadata stmt, validated
            columns = [desc[0] for desc in result.description]
            # Fetch one extra to detect truncation (e.g. DESCRIBE on a wide table).
            rows = result.fetchmany(max_rows + 1)
    except duckdb.Error as e:
        return build_error_envelope(
            error=UserError(
                "Query execution failed.",
                code=error_codes.SQL_QUERY_ERROR,
                details={"detail": str(e)},
            ),
            sensitivity="low",
        )
    truncated = len(rows) > max_rows
    rows = rows[:max_rows]
    records = [dict(zip(columns, row, strict=False)) for row in rows]
    return build_envelope(
        data=records,
        sensitivity="low",
        total_count=max_rows + 1 if truncated else len(records),
        classes_returned=["aggregate"],
    )


@mcp_tool(dynamic_classification=True)
def sql_query(query: str) -> ResponseEnvelope[Any]:
    """Execute a read-only SQL query against the core and app schemas.

    Only SELECT, WITH, DESCRIBE, SHOW, PRAGMA, and EXPLAIN queries are allowed.
    Writes and file-access functions are blocked. Data queries may reference
    only the ``core`` and ``app`` schemas (use the reports_* tools for curated
    report views); other schemas are refused.

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
    error = validate_read_only_query(query)
    if error:
        return build_error_envelope(
            error=UserError(error, code=error_codes.SQL_INVALID_QUERY),
            sensitivity="low",
        )

    try:
        tree = parse_cached(query)
    except SqlParseError as e:
        return build_error_envelope(
            error=UserError(
                "Could not parse SQL.",
                code=error_codes.SQL_INVALID_QUERY,
                details={"detail": str(e)},
            ),
            sensitivity="low",
        )

    # DESCRIBE/SHOW/PRAGMA/EXPLAIN return schema/plan text, not row data —
    # execute directly at LOW; the lineage gate only applies to data queries.
    if not is_data_query(tree):
        return _execute_metadata_query(query)

    try:
        with get_database(read_only=True) as db:
            snapshot = get_current_schema_snapshot(db)
            qtree = expand_star(tree, snapshot)
            disallowed = tables_outside_schemas(qtree, snapshot, _ALLOWED_QUERY_SCHEMAS)
            if disallowed:
                return build_error_envelope(
                    error=UserError(
                        "sql_query is limited to the core and app schemas.",
                        code=error_codes.SQL_SCHEMA_NOT_ALLOWED,
                        hint="Use the reports_* tools for curated report views; "
                        "raw/prep are internal schemas.",
                        details={"disallowed": sorted(set(disallowed))},
                    ),
                    sensitivity="low",
                )
            output_classes = resolve_output_classes(qtree, snapshot, query)
            max_rows = get_max_rows()
            # Security: validate_read_only_query gates write/file-access above.
            # The entire string is intentionally user SQL and cannot be parameterized.
            result = db.execute(query)  # noqa: S608 — read-only, validated above
            columns = [desc[0] for desc in result.description]
            # Fetch one extra row to detect truncation without a second query.
            rows = result.fetchmany(max_rows + 1)
    except SqlParseError as e:
        return build_error_envelope(
            error=UserError(
                "Could not parse SQL.",
                code=error_codes.SQL_INVALID_QUERY,
                details={"detail": str(e)},
            ),
            sensitivity="low",
        )
    # SqlSchemaError comes from the lineage qualify step; CatalogException from
    # DuckDB at execute time. Both mean "table/column doesn't exist" — one envelope.
    except (SqlSchemaError, duckdb.CatalogException) as e:
        return build_error_envelope(
            error=UserError(
                "Unknown table or column.",
                code=error_codes.SQL_UNKNOWN_TABLE,
                details={"detail": str(e)},
            ),
            sensitivity="low",
        )
    except duckdb.Error as e:
        return build_error_envelope(
            error=UserError(
                "Query execution failed.",
                code=error_codes.SQL_QUERY_ERROR,
                details={"detail": str(e)},
            ),
            sensitivity="low",
        )

    truncated = len(rows) > max_rows
    rows = rows[:max_rows]
    records = [dict(zip(columns, row, strict=False)) for row in rows]
    # Key the redaction map by DuckDB's ACTUAL result-column names, aligned to
    # the lineage classes BY POSITION. sqlglot's alias_or_name for an unaliased
    # expression (e.g. MIN(account_id) → '') diverges from DuckDB's column name
    # ('min(account_id)'), so name-keying would miss the CRITICAL transform and
    # leak the value. Projection order == result-column order, so position is
    # the reliable join. On a count mismatch (rare — e.g. * expansion ordering),
    # fail closed: apply the query's max tier to every column.
    class_values = list(output_classes.values())
    if len(class_values) == len(columns):
        col_classes = dict(zip(columns, class_values, strict=True))
    else:
        floor = (
            max(class_values, key=lambda c: c.tier)
            if class_values
            else DataClass.AGGREGATE
        )
        col_classes = dict.fromkeys(columns, floor)
    redacted = redact_records(records, col_classes, consent=None)
    tier = derive_query_tier(output_classes)
    # total_count > returned_count makes build_envelope set has_more=True. We
    # don't pay for an exact COUNT(*); +1 signals "at least one more row".
    total_count = max_rows + 1 if truncated else len(records)
    return build_envelope(
        data=redacted,
        sensitivity=tier_to_sensitivity(tier).value,
        total_count=total_count,
        classes_returned=sorted({c.value for c in output_classes.values()}),
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
