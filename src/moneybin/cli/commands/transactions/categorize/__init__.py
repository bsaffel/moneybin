"""Categorize transactions: rules, bulk apply, auto-rules.

Per-transaction categorization workflow — rules, bulk apply from JSON,
auto-rule review/confirm, and stats. The matcher itself (rules + merchants)
runs locally with no LLM dependency; LLM-assist for uncategorized rows is
available via the MCP server. Category taxonomy and merchant mappings live
in the top-level `categories` and `merchants` groups respectively.
"""

import logging

import typer

from moneybin.cli.output import (
    OutputFormat,
    output_option,
    quiet_option,
)
from moneybin.cli.utils import handle_cli_errors
from moneybin.database import get_database
from moneybin.protocol.envelope import ResponseEnvelope

from . import auto, ml, rules
from .apply_from_file import categorize_apply_from_file
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
app.command("apply-from-file")(categorize_apply_from_file)


@app.command("apply")
def categorize_apply(
    stdin_sentinel: str | None = typer.Argument(
        None,
        help="Pass '-' to read JSON from stdin.",
    ),
    input_path: str | None = typer.Option(
        None, "--input", help="Path to a JSON file with categorization items."
    ),
    output: OutputFormat = output_option,
) -> None:
    """Assign categories to transactions from a JSON array.

    Read from a file:

      moneybin transactions categorize apply --input cats.json

    Or from stdin:

      cat cats.json | moneybin transactions categorize apply -

    Per-item validation: failures are reported in the result without aborting
    the batch. Exit code is 1 if any item failed.
    """
    import json
    import sys
    from pathlib import Path

    from moneybin.cli.output import render_or_json
    from moneybin.services.categorization_service import (
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

    render_or_json(result.to_envelope(input_count), output, render_fn=_render_table)

    if result.errors > 0 or result.skipped > 0:
        raise typer.Exit(1)


@app.command("stats")
def stats(
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,  # noqa: ARG001 — summary has no informational chatter; only data
) -> None:
    """Show categorization coverage summary."""
    from moneybin.cli.utils import emit_json
    from moneybin.services.categorization_service import CategorizationService

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
