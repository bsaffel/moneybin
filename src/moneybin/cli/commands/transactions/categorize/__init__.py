"""Categorize transactions: rules, bulk apply, auto-rules.

Per-transaction categorization workflow — rules, bulk apply from JSON,
auto-rule review/confirm, and stats. The matcher itself (rules + merchants)
runs locally with no LLM dependency; LLM-assist for uncategorized rows is
available via the MCP server. Category taxonomy and merchant mappings live
in the top-level `categories` and `merchants` groups respectively.
"""

import dataclasses
import logging

import typer

from moneybin.cli.output import (
    OutputFormat,
    output_option,
    quiet_option,
)
from moneybin.cli.utils import handle_cli_errors
from moneybin.database import get_database
from moneybin.errors import UserError
from moneybin.protocol.envelope import ResponseEnvelope

from . import auto, ml, rules
from .commit_from_file import categorize_commit_from_file
from .export import categorize_export_uncategorized

logger = logging.getLogger(__name__)

app = typer.Typer(
    help="Categorization workflow + rules (taxonomy under top-level `categories`)",
    no_args_is_help=True,
)

app.add_typer(rules.app, name="rules")
app.add_typer(auto.app, name="auto")
app.add_typer(ml.app, name="ml")

app.command("export-uncategorized")(categorize_export_uncategorized)
app.command("commit-from-file")(categorize_commit_from_file)


@app.command("commit")
def categorize_commit(
    stdin_sentinel: str | None = typer.Argument(
        None,
        help="Pass '-' to read JSON from stdin.",
    ),
    input_path: str | None = typer.Option(
        None, "--input", help="Path to a JSON file with categorization items."
    ),
    output: OutputFormat = output_option,
) -> None:
    """Commit externally-decided categorizations from a JSON array.

    Read from a file:

      moneybin transactions categorize commit --input cats.json

    Or from stdin:

      cat cats.json | moneybin transactions categorize commit -

    Per-item validation: failures are reported in the result without aborting
    the batch. Exit code is 1 if any item failed.
    """
    import json
    import sys
    from pathlib import Path

    from moneybin.cli.output import render_or_json
    from moneybin.services.categorization import (
        CategorizationResult,
        CategorizationService,
        validate_items,
    )

    use_stdin = stdin_sentinel == "-"

    if input_path is not None and use_stdin:
        typer.echo(
            "Provide either --input <path> or '-' to read from stdin (not both).",
            err=True,
        )
        raise typer.Exit(2)

    if input_path is None and not use_stdin:
        typer.echo(
            "Provide either --input <path> or '-' to read JSON from stdin.",
            err=True,
        )
        raise typer.Exit(2)

    try:
        if input_path is not None:
            with Path(input_path).open(encoding="utf-8") as f:
                raw = json.load(f)
        else:
            raw = json.load(sys.stdin)
    except FileNotFoundError as e:
        typer.echo(f"❌ File not found: {input_path}", err=True)
        raise typer.Exit(2) from e
    except json.JSONDecodeError as e:
        typer.echo(f"❌ Invalid JSON: {e}", err=True)
        raise typer.Exit(1) from e

    try:
        items, parse_errors = validate_items(raw)
    except ValueError as e:
        typer.echo(f"❌ {e}", err=True)
        raise typer.Exit(1) from e

    if items:
        with handle_cli_errors():
            with get_database() as db:
                result = CategorizationService(db).categorize_items(items)
    else:
        result = CategorizationResult(applied=0, skipped=0, errors=0, error_details=[])
    result.merge_parse_errors(parse_errors)

    input_count = len(items) + len(parse_errors)

    def _render_table(_: ResponseEnvelope) -> None:
        logger.info(
            f"✅ Applied {result.applied} | skipped {result.skipped} | errors {result.errors}"
        )
        if result.merchants_created:
            logger.info(f"   Created {result.merchants_created} merchant mappings")
        for err in result.error_details:
            logger.warning(f"⚠️  {err['transaction_id']}: {err['reason']}")

    envelope = result.to_envelope(input_count)
    if result.errors > 0:
        envelope = dataclasses.replace(
            envelope,
            error=UserError(
                f"{result.errors} item(s) failed to categorize",
                code="categorization_errors",
            ),
        )
    render_or_json(envelope, output, render_fn=_render_table)

    if result.errors > 0 or result.skipped > 0:
        raise typer.Exit(1)


@app.command("run")
def categorize_run(
    methods: str = typer.Option(
        "rules,merchants",
        "--methods",
        help="Comma-separated engines to run in order. Default: rules,merchants.",
    ),
    output: OutputFormat = output_option,
) -> None:
    """Run the categorization engine cascade over uncategorized transactions.

    Engines available today: ``rules``, ``merchants``. Methods cascade in
    order — a rule write blocks a merchant write at the same priority.

      moneybin transactions categorize run
      moneybin transactions categorize run --methods rules
      moneybin transactions categorize run --methods rules,merchants --output json
    """
    from typing import Literal

    from moneybin.cli.output import render_or_json
    from moneybin.protocol.envelope import build_envelope
    from moneybin.services.categorization import CategorizationService

    valid: set[str] = {"rules", "merchants"}
    typed_methods: list[Literal["rules", "merchants"]] = []
    bad: list[str] = []
    for raw in methods.split(","):
        name = raw.strip()
        if not name:
            continue
        if name == "rules" or name == "merchants":
            typed_methods.append(name)
        else:
            bad.append(name)
    if bad:
        typer.echo(
            f"❌ Unknown method(s): {', '.join(bad)}. Valid: {', '.join(sorted(valid))}.",
            err=True,
        )
        raise typer.Exit(2)

    with handle_cli_errors():
        with get_database() as db:
            data = CategorizationService(db).categorize_run(methods=typed_methods)

    envelope = build_envelope(data=data, sensitivity="medium")

    def _render_table(_: ResponseEnvelope) -> None:
        breakdown = data["applied_by_method"]
        for method, count in breakdown.items():
            logger.info(f"  {method}: {count}")
        logger.info(f"✅ Applied {data['total_applied']} total")

    render_or_json(envelope, output, render_fn=_render_table)


@app.command("assist")
def categorize_assist(
    limit: int = typer.Option(
        100, "--limit", help="Maximum number of records to return (default 100)."
    ),
    account_filter: str | None = typer.Option(
        None,
        "--account-filter",
        help="Comma-separated account IDs to restrict to.",
    ),
    date_range: str | None = typer.Option(
        None,
        "--date-range",
        help="Date range as START,END (ISO dates, inclusive).",
    ),
    output: OutputFormat = output_option,
) -> None:
    """Return uncategorized transactions as redacted records for LLM categorization.

    Outputs the same redacted shape as the MCP tool transactions_categorize_assist:
    description and memo are redacted; no amount, date, or account ID is included.

      moneybin transactions categorize assist --limit 50 --output json | jq '.data[0]'
      moneybin transactions categorize assist --account-filter acct_a,acct_b --output json

    Pipe the JSON output into an LLM workflow; commit decisions back via
    `moneybin transactions categorize commit`.
    """
    from moneybin.cli.output import render_or_json
    from moneybin.mcp.privacy import audit_log
    from moneybin.metrics.registry import CATEGORIZE_ASSIST_CALLS_TOTAL
    from moneybin.protocol.envelope import build_envelope
    from moneybin.services.categorization import CategorizationService

    accounts: list[str] | None = (
        [a.strip() for a in account_filter.split(",") if a.strip()]
        if account_filter
        else None
    )
    date_tuple: tuple[str, str] | None = None
    if date_range:
        parts = [p.strip() for p in date_range.split(",")]
        if len(parts) != 2:
            typer.echo("❌ --date-range must be START,END (ISO dates).", err=True)
            raise typer.Exit(2)
        date_tuple = (parts[0], parts[1])

    with handle_cli_errors():
        with get_database(read_only=True) as db:
            redacted = CategorizationService(db).categorize_assist(
                limit=limit,
                account_filter=accounts,
                date_range=date_tuple,
            )

    CATEGORIZE_ASSIST_CALLS_TOTAL.labels(surface="cli").inc()
    audit_log(
        tool="transactions_categorize_assist",
        sensitivity="medium",
        metadata={"txn_count": len(redacted), "account_filter": accounts},
    )

    data = [r.to_dict() for r in redacted]
    envelope = build_envelope(data=data, sensitivity="medium")

    def _render_table(_: ResponseEnvelope) -> None:
        logger.info(f"Returned {len(data)} redacted record(s).")

    render_or_json(envelope, output, render_fn=_render_table)


@app.command("stats")
def stats(
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,  # noqa: ARG001 — summary has no informational chatter; only data
) -> None:
    """Show categorization coverage summary."""
    from moneybin.cli.utils import emit_json
    from moneybin.services.categorization import CategorizationService

    with handle_cli_errors():
        with get_database() as db:
            coverage = CategorizationService(db).categorization_stats()

    if output == OutputFormat.JSON:
        emit_json("summary", coverage)
        return

    total = coverage["total"]
    categorized = coverage["categorized"]
    uncategorized = coverage["uncategorized"]
    pct = coverage["pct_categorized"]

    logger.info("Categorization coverage:")
    logger.info(f"  Total transactions:   {total}")
    logger.info(f"  Categorized:          {categorized} ({pct:.1f}%)")
    logger.info(f"  Uncategorized:        {uncategorized}")

    # Show breakdown by source
    for key, value in coverage.items():
        if key.startswith("by_"):
            source = key[3:]
            logger.info(f"  By {source}:  {value}")
