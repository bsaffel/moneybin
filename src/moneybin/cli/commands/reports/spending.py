"""moneybin reports spending — monthly spending trend with deltas."""

from __future__ import annotations

import logging

import typer

from moneybin.cli.output import (
    OutputFormat,
    output_option,
    quiet_option,
    render_or_json,
)
from moneybin.cli.utils import (
    handle_cli_errors,
    render_rich_table,
)
from moneybin.protocol.envelope import build_envelope
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
    with handle_cli_errors(output=output) as db:
        cols, rows = ReportsService(db).spending_trend(
            from_month=from_month, to_month=to_month, category=category, compare=compare
        )

    def _render_text(_: object) -> None:
        render_rich_table(cols, rows)

    render_or_json(
        build_envelope(
            data=[dict(zip(cols, r, strict=False)) for r in rows], sensitivity="low"
        ),
        output,
        render_fn=_render_text,
    )
