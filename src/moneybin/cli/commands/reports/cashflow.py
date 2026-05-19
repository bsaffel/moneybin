"""moneybin reports cashflow — monthly inflow/outflow/net."""

from __future__ import annotations

import dataclasses
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
from moneybin.database import get_database
from moneybin.protocol.envelope import build_envelope
from moneybin.services.reports_service import CASHFLOW_GROUPINGS, ReportsService

logger = logging.getLogger(__name__)


def reports_cashflow(
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
    with handle_cli_errors():
        with get_database(read_only=True) as db:
            payload = ReportsService(db).cash_flow(
                from_month=from_month, to_month=to_month, by=by
            )

    def _render_text(_: object) -> None:
        if not payload.rows:
            return
        cols = list(dataclasses.asdict(payload.rows[0]).keys())
        rows = [tuple(dataclasses.asdict(r).values()) for r in payload.rows]
        render_rich_table(cols, rows)

    render_or_json(
        build_envelope(data=payload, sensitivity="low"),
        output,
        render_fn=_render_text,
    )
