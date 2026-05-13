"""moneybin reports balance-drift — asserted vs computed balance reconciliation."""

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
from moneybin.services.reports_service import DRIFT_STATUSES, ReportsService

logger = logging.getLogger(__name__)

balance_drift_app = typer.Typer(
    help="Asserted vs computed balance reconciliation deltas",
    no_args_is_help=True,
)


@balance_drift_app.command("show")
def reports_balance_drift_show(
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
    with handle_cli_errors(output=output) as db:
        cols, rows = ReportsService(db).balance_drift(
            account=account, status=status, since=since
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
