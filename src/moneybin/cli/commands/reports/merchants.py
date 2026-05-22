"""moneybin reports merchants — per-merchant activity rollup."""

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
from moneybin.services.reports_service import MERCHANTS_SORTS, ReportsService

logger = logging.getLogger(__name__)


def reports_merchants(
    top: int = typer.Option(25, "--top", help="Limit to top N merchants"),
    sort: str = typer.Option("spend", "--sort", help="spend | count | recent"),
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,  # noqa: ARG001
) -> None:
    """Show per-merchant activity totals."""
    if sort not in MERCHANTS_SORTS:
        raise typer.BadParameter(f"Unknown sort key: {sort}")
    with handle_cli_errors():
        with get_database(read_only=True) as db:
            payload = ReportsService(db).merchant_activity(top=top, sort=sort)

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
        cli_actor="reports_merchants",
    )
