"""moneybin sql — privacy-safe ad-hoc SQL (lineage + CRITICAL masking).

The agent/operator path for ad-hoc SQL that runs through the SAME privacy
enforcement as the ``sql_query`` MCP tool: read-only gate, core/app schema
restriction, sqlglot column lineage, and CRITICAL masking. Contrast with
``moneybin db query`` / ``db shell`` / ``db ui``, which are raw, unmasked
direct-DB access (they emit an operator-bypass banner pointing here).
"""

from __future__ import annotations

import logging
from typing import Any

import typer

from moneybin.cli.output import (
    OutputFormat,
    json_fields_option,
    output_option,
    quiet_option,
    render_or_json,
)
from moneybin.cli.utils import handle_cli_errors
from moneybin.database import get_database
from moneybin.protocol.envelope import ResponseEnvelope, build_envelope

logger = logging.getLogger(__name__)

app = typer.Typer(
    no_args_is_help=True,
    help="Privacy-safe ad-hoc SQL (lineage classification + CRITICAL masking).",
)


@app.callback()
def _sql_group() -> None:  # pyright: ignore[reportUnusedFunction]  # Typer keeps the reference
    """Privacy-safe ad-hoc SQL (lineage classification + CRITICAL masking).

    A group callback keeps ``query`` an explicit subcommand — without it Typer
    collapses a single-command group, which would change the invocation to
    ``moneybin sql`` and break the ``moneybin sql query`` contract.
    """


@app.command("query")
def sql_query_command(
    query: str = typer.Argument(..., help="SQL query to execute (read-only)."),
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,  # noqa: ARG001 — query emits result rows, not chatter
    json_fields: str | None = json_fields_option,
) -> None:
    """Execute a read-only SQL query with privacy enforcement.

    The privacy-safe counterpart to ``moneybin db query``: only SELECT, WITH,
    DESCRIBE, SHOW, PRAGMA, and EXPLAIN are allowed, limited to the ``core``
    and ``app`` schemas. Each output column is classified via SQL lineage;
    CRITICAL columns (account/routing numbers) are ALWAYS masked (****<last4>),
    exactly like the typed tools and the ``sql_query`` MCP tool. Other tiers
    (amounts, descriptions, dates) pass through in the clear.

    Amounts use the accounting convention: negative = expense, positive = income.

    ``--json-fields`` filters ``--output json`` to a subset of the SELECT list's
    column names (the available fields depend on the query).
    """
    # Deferred: execute_sql_query pulls in sqlglot (a SQL parser); keep it off
    # the CLI cold-start path per .claude/rules/cli.md "Cold-Start Hygiene".
    from moneybin.mcp.privacy import get_max_rows, tier_to_sensitivity  # noqa: PLC0415
    from moneybin.privacy.sql_query import execute_sql_query  # noqa: PLC0415

    # render_or_json stays inside handle_cli_errors so a rendering/serialization
    # failure (e.g. an unusual DuckDB column type) surfaces as a clean CLI error
    # envelope, not an unhandled traceback.
    with handle_cli_errors(cli_actor="sql_query"):
        with get_database(read_only=True) as db:
            result = execute_sql_query(db, query, max_rows=get_max_rows())
        envelope: ResponseEnvelope[Any] = build_envelope(
            data=result.records,
            sensitivity=tier_to_sensitivity(result.tier).value,
            total_count=result.total_count,
            classes_returned=result.classes_returned,
        )

        def _render_text(_: object) -> None:
            if not result.columns:
                return
            typer.echo(" | ".join(result.columns))
            for record in result.records:
                typer.echo(
                    " | ".join(str(record.get(col, "")) for col in result.columns)
                )

        render_or_json(
            envelope,
            output,
            render_fn=_render_text,
            json_fields=json_fields,
            cli_actor="sql_query",
            classes_returned=result.classes_returned,
        )
