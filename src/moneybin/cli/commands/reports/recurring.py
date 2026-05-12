"""moneybin reports recurring — likely-recurring subscription candidates."""

from __future__ import annotations

import logging

import typer

from moneybin.cli.output import OutputFormat, output_option, quiet_option
from moneybin.cli.utils import (
    emit_json,
    handle_cli_errors,
    render_rich_table,
)
from moneybin.services.reports_service import (
    RECURRING_CADENCES,
    RECURRING_STATUSES,
    ReportsService,
)

logger = logging.getLogger(__name__)

recurring_app = typer.Typer(
    help="Likely-recurring subscription candidates with confidence scores",
    no_args_is_help=True,
)


@recurring_app.command("show")
def reports_recurring_show(
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
    with handle_cli_errors(output=output) as db:
        cols, rows = ReportsService(db).recurring_subscriptions(
            min_confidence=min_confidence, status=status, cadence=cadence
        )
    if output == OutputFormat.JSON:
        emit_json("recurring", [dict(zip(cols, r, strict=False)) for r in rows])
        return
    render_rich_table(cols, rows)
