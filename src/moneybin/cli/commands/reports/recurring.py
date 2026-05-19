"""moneybin reports recurring — likely-recurring subscription candidates."""

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
from moneybin.services.reports_service import (
    RECURRING_CADENCES,
    RECURRING_STATUSES,
    ReportsService,
)

logger = logging.getLogger(__name__)


def reports_recurring(
    min_confidence: float = typer.Option(
        0.5,
        "--min-confidence",
        help="Filter to candidates at or above this confidence (0.0-1.0)",
    ),
    status: str = typer.Option("active", "--status", help="active | inactive | all"),
    cadence: str | None = typer.Option(
        None,
        "--cadence",
        help="weekly | biweekly | monthly | quarterly | yearly | irregular",
    ),
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,  # noqa: ARG001
) -> None:
    """Show likely-recurring subscriptions ordered by annualized cost."""
    if status not in RECURRING_STATUSES:
        raise typer.BadParameter(f"Unknown status: {status}")
    if cadence is not None and cadence not in RECURRING_CADENCES:
        raise typer.BadParameter(f"Unknown cadence: {cadence}")
    with handle_cli_errors():
        with get_database(read_only=True) as db:
            payload = ReportsService(db).recurring_subscriptions(
                min_confidence=min_confidence, status=status, cadence=cadence
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
