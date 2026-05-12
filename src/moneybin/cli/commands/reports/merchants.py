"""moneybin reports merchants — per-merchant activity rollup."""

from __future__ import annotations

import logging

import typer

from moneybin.cli.output import OutputFormat, output_option, quiet_option
from moneybin.cli.utils import (
    emit_json,
    handle_cli_errors,
    render_rich_table,
)
from moneybin.database import get_database
from moneybin.services.reports_service import MERCHANTS_SORTS, ReportsService

logger = logging.getLogger(__name__)

merchants_app = typer.Typer(
    help="Per-merchant lifetime activity totals",
    no_args_is_help=True,
)


@merchants_app.command("show")
def reports_merchants_show(
    top: int = typer.Option(25, "--top", help="Limit to top N merchants"),
    sort: str = typer.Option("spend", "--sort", help="spend | count | recent"),
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,  # noqa: ARG001
) -> None:
    """Show per-merchant activity totals."""
    if sort not in MERCHANTS_SORTS:
        raise typer.BadParameter(f"Unknown sort key: {sort}")
    with handle_cli_errors():
        with get_database() as db:
            cols, rows = ReportsService(db).merchant_activity(top=top, sort=sort)
    if output == OutputFormat.JSON:
        emit_json("merchants", [dict(zip(cols, r, strict=False)) for r in rows])
        return
    render_rich_table(cols, rows)
