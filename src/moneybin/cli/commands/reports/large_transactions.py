"""moneybin reports large-transactions — anomaly-flavored transaction lens."""

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
from moneybin.services.reports_service import LARGE_TXN_ANOMALIES, ReportsService

logger = logging.getLogger(__name__)


def reports_large_transactions(
    top: int = typer.Option(25, "--top", help="Top N by ABS(amount)"),
    anomaly: str = typer.Option("none", "--anomaly", help="account | category | none"),
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,  # noqa: ARG001
) -> None:
    """Show large transactions, optionally filtered to z-score outliers."""
    if anomaly not in LARGE_TXN_ANOMALIES:
        raise typer.BadParameter(f"Unknown anomaly mode: {anomaly}")
    with handle_cli_errors():
        with get_database(read_only=True) as db:
            payload = ReportsService(db).large_transactions(top=top, anomaly=anomaly)

    def _render_text(_: object) -> None:
        if not payload.rows:
            return
        cols = list(dataclasses.asdict(payload.rows[0]).keys())
        rows = [tuple(dataclasses.asdict(r).values()) for r in payload.rows]
        render_rich_table(cols, rows)

    render_or_json(
        build_envelope(data=payload, sensitivity="medium"),
        output,
        render_fn=_render_text,
    )
