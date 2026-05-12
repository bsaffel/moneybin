"""moneybin reports cashflow — monthly inflow/outflow/net."""

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
from moneybin.services.reports_service import CASHFLOW_GROUPINGS, ReportsService

logger = logging.getLogger(__name__)

cashflow_app = typer.Typer(help="Monthly cash flow", no_args_is_help=True)


@cashflow_app.command("show")
def reports_cashflow_show(
    from_month: str | None = typer.Option(None, "--from", help="ISO date YYYY-MM-01"),
    to_month: str | None = typer.Option(None, "--to", help="ISO date YYYY-MM-01"),
    by: str = typer.Option(
        "account-and-category",
        "--by",
        help="account | category | account-and-category",
    ),
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,  # noqa: ARG001
) -> None:
    """Show monthly cash flow rollup."""
    if by not in CASHFLOW_GROUPINGS:
        raise typer.BadParameter(f"Unknown grouping: {by}")
    with handle_cli_errors(output=output) as db:
        cols, rows = ReportsService(db).cash_flow(
            from_month=from_month, to_month=to_month, by=by
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
