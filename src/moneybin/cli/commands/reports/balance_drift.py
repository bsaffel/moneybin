"""moneybin reports balance-drift — asserted vs computed balance reconciliation."""

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
from moneybin.services.reports_service import DRIFT_STATUSES, ReportsService

logger = logging.getLogger(__name__)


def reports_balance_drift(
    account: str | None = typer.Option(
        None, "--account", help="Filter to account name"
    ),
    status: str = typer.Option(
        "all", "--status", help="drift | warning | clean | no-data | all"
    ),
    since: str | None = typer.Option(
        None, "--since", help="ISO date; only assertions on or after"
    ),
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,  # noqa: ARG001
) -> None:
    """Show balance reconciliation drift, sorted by absolute drift."""
    if status not in DRIFT_STATUSES:
        raise typer.BadParameter(f"Unknown status: {status}")
    with handle_cli_errors():
        with get_database(read_only=True) as db:
            payload = ReportsService(db).balance_drift(
                account=account, status=status, since=since
            )

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
