# src/moneybin/mcp/tools/sql.py
"""SQL namespace tools — direct read-only SQL queries.

Tools:
    - sql_query — Execute a read-only SQL query with per-column privacy classification
    - sql_schema — Return the curated schema doc (low sensitivity)
"""

from __future__ import annotations

from typing import Any

from fastmcp import FastMCP
from sqlglot import exp

from moneybin import error_codes
from moneybin.database import get_database
from moneybin.errors import UserError
from moneybin.mcp._registration import register
from moneybin.mcp.decorator import mcp_tool
from moneybin.privacy.sensitivity import Sensitivity, get_max_rows, tier_to_sensitivity
from moneybin.privacy.sql_query import execute_sql_query
from moneybin.protocol.envelope import (
    ResponseEnvelope,
    build_envelope,
    build_error_envelope,
)
from moneybin.services.schema_catalog import build_live_catalog, build_schema_doc


@mcp_tool(dynamic_classification=True, maximum_sensitivity=Sensitivity.CRITICAL)
def sql_query(query: str) -> ResponseEnvelope[Any]:
    """Execute a read-only SQL query against the five agent-readable schemas.

    Only SELECT, WITH, DESCRIBE, and SHOW queries are allowed. Writes,
    file-access functions, PRAGMA, and EXPLAIN are blocked. The ``core``,
    ``app``, ``reports``, ``raw``, and ``prep`` schemas may be referenced;
    ``meta`` and ``seeds`` are refused — by DESCRIBE just as by SELECT, so a
    catalog statement cannot inspect a table the data path won't read.

    Amounts use the accounting convention: negative = expense, positive = income.
    Currency is named in summary.display_currency.

    Privacy: each output column is resolved to its data class via SQL lineage
    (sqlglot). CRITICAL columns (account/routing numbers) are ALWAYS masked
    (****<last4>). ``core``/``app``/``reports`` declare a class for every
    deployed column; ``raw``/``prep`` carry 34 declarations and floor the rest
    on a value-shape scan that reaches text and integer values only, masking
    SSN and 8-or-more-digit shapes. So a 4-to-7 digit account number in an
    undeclared column passes through, and a ``DECIMAL`` or ``FLOAT`` one is
    never scanned at all. Other tiers (amounts, descriptions, dates) are
    returned in the clear, matching the typed tools' current behavior.

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


@mcp_tool(dynamic_classification=True, maximum_sensitivity=Sensitivity.CRITICAL)
def sql_schema(table: str | None = None) -> ResponseEnvelope[Any]:
    """Return the curated database schema for ad-hoc SQL composition.

    Equivalent to reading the ``moneybin://schema`` MCP resource. Provided
    as a tool for hosts that don't surface MCP resources to the model
    (e.g. Claude.ai chat).

    Defaults to a compact catalog (table names + purposes + column counts)
    so agents don't pay for the full ~50KB schema document on every call.
    Pass ``table='<schema.name>'`` to get columns, comments, and example
    queries for one table. Pass ``table='*'`` to get the full document.
    Pass ``table='<schema>.*'`` to list every relation in one schema.

    Args:
        table: ``None`` (default) returns the compact catalog; a full table
            name like ``'core.fct_transactions'`` returns details for that
            table; ``'<schema>.*'`` like ``'raw.*'`` lists that schema's live
            relations as names and kinds; ``'*'`` returns the full schema
            document.
    """
    # Dispatched before the curated doc is built, not after: the listing reads
    # nothing from it, and `build_live_catalog` derives `curated` from its own
    # snapshot. Moved below the build, this branch would pay for a second
    # catalog it never opens.
    if table is not None and table.endswith(".*"):
        return _live_schema_listing(table.removesuffix(".*"))

    doc = build_schema_doc()
    tables: list[dict[str, Any]] = doc["tables"]

    if table is None:
        compact = [
            {
                "name": t["name"],
                "purpose": t["purpose"],
                "kind": t["kind"],
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
            # Every branch counts the `tables` it returns. `build_envelope`'s
            # shape heuristic reads any dict as one item, so left implicit this
            # reports a single row for a catalog of dozens.
            returned_count=len(compact),
            actions=[
                "Pass table='<schema.name>' (e.g. 'core.fct_transactions') to "
                "fetch columns, comments, and example queries for one table.",
                "Pass table='<schema>.*' (e.g. 'raw.*') to list one schema's "
                "live relations, including those with no entry above.",
                "Pass table='*' for the full schema document (~50KB).",
            ],
        )

    if table == "*":
        return build_envelope(
            data=doc,
            sensitivity="low",
            classes_returned=["aggregate"],
            returned_count=len(tables),
        )

    matches = [t for t in tables if t["name"].lower() == table.lower()]
    live = None if matches else _live_relation(table)
    if live is not None and live["curated"]:
        # `doc` predates this relation, which acquired its curated entry
        # between that read and the live one. Re-read rather than call it
        # uncurated: the second read is strictly later than the live read, so
        # it holds whatever the live catalog just saw. A relation dropped in
        # that window stays missing and falls through below.
        doc = build_schema_doc()
        tables = doc["tables"]
        matches = [t for t in tables if t["name"].lower() == table.lower()]

    if not matches:
        available = [t["name"] for t in tables]
        known = ", ".join(available)
        # "Unknown table" is a false negative for a relation that exists and
        # that `sql_query` reads — the curated doc covers only the 35 tagged
        # interface tables. An agent told "unknown" fixes the name; an agent
        # told "not curated" reaches for DESCRIBE. Different recovery, so a
        # different code.
        if live is not None:
            described = _describable(live["name"])
            return build_error_envelope(
                error=UserError(
                    f"Not in the curated catalog: {table}",
                    code=error_codes.SQL_TABLE_NOT_CURATED,
                    hint=(
                        "The relation exists and sql_query reads it; only its "
                        "curated entry (purpose and example queries) is "
                        f"missing. Run sql_query with DESCRIBE {described} for "
                        "its columns."
                    ),
                    details={"table": table},
                ),
                sensitivity="low",
                actions=[f"Call sql_query(query='DESCRIBE {described}')"],
            )
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
        returned_count=len(matches),
    )


def _describable(name: str) -> str:
    """Return a catalog name the agent can paste straight into ``DESCRIBE``.

    The hint is advice, but `sql_query` still has to parse it. `raw` holds
    whatever an operator created, so a relation carrying a space or a reserved
    word makes `DESCRIBE raw.monthly spend` a bind error — the refusal would
    promise columns and hand back a syntax failure.

    Quoted unconditionally rather than only where required: a "does this need
    quoting" predicate is one more thing to get wrong, and DuckDB reads a
    quoted identifier the same either way.

    Built from the catalog row, not the caller's string: the lookup that found
    this relation folds case, so the spelling asked for need not be the one the
    catalog holds, and only the catalog's resolves.
    """
    schema, _, relation = name.partition(".")
    return ".".join(
        exp.to_identifier(part, quoted=True).sql("duckdb")
        for part in (schema, relation)
    )


def _live_relation(table: str) -> dict[str, Any] | None:
    """Return ``table``'s row in the live catalog, or ``None`` if it has none.

    Raises ``sql_schema_not_allowed`` for a name under a schema `sql_query`
    refuses, because `build_live_catalog` is the one gate that decides which
    schemas are readable. Spelling that test a second time here is what let
    `'meta.model_freshness'` answer "unknown table" while `'meta.*'` refused
    it — one question, two answers, and the first sends the agent off to
    correct a name that was never wrong.

    An unqualified name carries no schema to refuse, so it stays unknown.

    Both sides are lowered rather than the input alone: a runtime seed view is
    named from a user-chosen alias (`raw.gsheet_<alias>`), so the catalog can
    hold casing the caller did not write.

    The row is returned rather than the bare "exists" this once answered,
    because its ``curated`` flag is read from a later snapshot than the
    caller's document — which is what lets the caller tell a genuinely
    uncurated relation from one curated since it looked.
    """
    schema, dot, _name = table.partition(".")
    if not dot:
        return None
    wanted = table.lower()
    return next(
        (r for r in build_live_catalog(schema=schema) if r["name"].lower() == wanted),
        None,
    )


def _live_schema_listing(schema: str) -> ResponseEnvelope[Any]:
    """Return every queryable relation in one schema as names and kinds.

    A disallowed schema propagates `build_live_catalog`'s ``UserError`` to the
    `@mcp_tool` decorator, which builds the identical low-sensitivity envelope
    — the same path `sql_query` uses for the same refusal.

    The echoed ``schema`` is lowered because the rows underneath it are: the
    gate normalizes before querying, so echoing the caller's spelling back
    would have `'RAW.*'` answer with a `schema` disagreeing in case with every
    row it contains, and with what `'raw.*'` returns for the same request.
    Counts are explicit because `build_envelope`'s shape heuristic reads any
    dict as a single item, which would report one relation for a schema of 21.
    """
    schema = schema.lower()
    relations = build_live_catalog(schema=schema)
    return build_envelope(
        data={"schema": schema, "tables": relations},
        sensitivity="low",
        classes_returned=["aggregate"],
        returned_count=len(relations),
        actions=[
            "Pass table='<schema.name>' for a curated table's columns and "
            "example queries.",
            "For a relation with curated=false, call "
            "sql_query(query='DESCRIBE <schema.name>') for its columns.",
        ],
    )


def register_sql_tools(mcp: FastMCP) -> None:
    """Register all sql namespace tools with the FastMCP server."""
    register(
        mcp,
        sql_query,
        "sql_query",
        "Execute a read-only SQL query against the database. "
        "Supports SELECT, WITH, DESCRIBE, SHOW (not PRAGMA or EXPLAIN). "
        "Reaches core, app, reports, raw, and prep; meta and seeds are refused. "
        "Each output column is classified via SQL lineage; CRITICAL columns "
        "(account/routing numbers) are ALWAYS masked (****<last4>). "
        "core/app/reports declare a class for every column. raw and prep declare 34 "
        "and scan every other text or integer value, masking only SSN and "
        "8-or-more-digit shapes — a 4-to-7 digit account number passes through, and "
        "a DECIMAL or FLOAT is never scanned at all, so treat those two schemas as "
        "less protected, not equally protected. "
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
        "table='<schema.name>' for one table's columns/examples, "
        "table='<schema>.*' (e.g. 'raw.*') to list one schema's live relations "
        "with table-vs-view kind and whether each is curated, or "
        "table='*' for the full ~50KB document. Only the curated tables carry "
        "purposes and examples; an uncurated relation is still readable via "
        "sql_query DESCRIBE. Mirrors the moneybin://schema "
        "resource for hosts that don't expose MCP resources.",
    )
