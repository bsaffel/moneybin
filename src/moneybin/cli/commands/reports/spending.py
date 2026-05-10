"""moneybin reports spending — monthly spending trend with deltas."""

from __future__ import annotations

import logging

import typer

from moneybin.cli.output import OutputFormat, output_option, quiet_option
from moneybin.cli.utils import (
    emit_json,
    handle_cli_errors,
    render_rich_table,
)
from moneybin.services.reports_service import SPENDING_COMPARES, ReportsService

logger = logging.getLogger(__name__)

spending_app = typer.Typer(
    help="Spending trend with MoM/YoY/trailing deltas", no_args_is_help=True
)


@spending_app.command("show")
def reports_spending_show(
    from_month: str | None = typer.Option(None, "--from", help="ISO date YYYY-MM-01"),
    to_month: str | None = typer.Option(None, "--to", help="ISO date YYYY-MM-01"),
    category: str | None = typer.Option(
        None, "--category", help="Filter to category text"
    ),
    compare: str = typer.Option("yoy", "--compare", help="yoy | mom | trailing"),
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,  # noqa: ARG001
) -> None:
    """Show spending trend with MoM/YoY/trailing comparisons."""
    if compare not in SPENDING_COMPARES:
        raise typer.BadParameter(f"Unknown comparison: {compare}")
    with handle_cli_errors() as db:
        cols, rows = ReportsService(db).spending_trend(
            from_month=from_month, to_month=to_month, category=category, compare=compare
        )
    if output == OutputFormat.JSON:
        emit_json("spending", [dict(zip(cols, r, strict=False)) for r in rows])
        return
    render_rich_table(cols, rows)
