"""moneybin reports large-transactions — anomaly-flavored transaction lens."""

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
from moneybin.services.reports_service import LARGE_TXN_ANOMALIES, ReportsService

logger = logging.getLogger(__name__)

large_transactions_app = typer.Typer(
    help="Anomaly-flavored transaction lens (top-N + z-score)",
    no_args_is_help=True,
)


@large_transactions_app.command("show")
def reports_large_transactions_show(
    top: int = typer.Option(25, "--top", help="Top N by ABS(amount)"),
    anomaly: str = typer.Option("none", "--anomaly", help="account | category | none"),
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,  # noqa: ARG001
) -> None:
    """Show large transactions, optionally filtered to z-score outliers."""
    if anomaly not in LARGE_TXN_ANOMALIES:
        raise typer.BadParameter(f"Unknown anomaly mode: {anomaly}")
    with handle_cli_errors():
        with get_database() as db:
            cols, rows = ReportsService(db).large_transactions(top=top, anomaly=anomaly)
    if output == OutputFormat.JSON:
        emit_json(
            "large_transactions", [dict(zip(cols, r, strict=False)) for r in rows]
        )
        return
    render_rich_table(cols, rows)
