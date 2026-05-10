"""moneybin reports balance-drift — asserted vs computed balance reconciliation."""

from __future__ import annotations

import logging

import typer

from moneybin.cli.output import OutputFormat, output_option, quiet_option
from moneybin.cli.utils import emit_json, handle_cli_errors
from moneybin.tables import REPORTS_BALANCE_DRIFT

logger = logging.getLogger(__name__)

balance_drift_app = typer.Typer(
    help="Asserted vs computed balance reconciliation deltas",
    no_args_is_help=True,
)

_VALID_STATUSES = {"drift", "warning", "clean", "no-data", "all"}


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
    if status not in _VALID_STATUSES:
        raise typer.BadParameter(f"Unknown status: {status}")
    with handle_cli_errors() as db:
        sql = f"""
            SELECT account_id, account_name, assertion_date, asserted_balance,
                   computed_balance, drift, drift_abs, drift_pct,
                   days_since_assertion, status
            FROM {REPORTS_BALANCE_DRIFT.full_name}
            WHERE 1=1
        """  # noqa: S608  # TableRef interpolation
        params: list[object] = []
        if account:
            sql += " AND account_name = ?"
            params.append(account)
        if status != "all":
            sql += " AND status = ?"
            params.append(status)
        if since:
            sql += " AND assertion_date >= ?"
            params.append(since)
        sql += " ORDER BY drift_abs DESC"

        cursor = db.execute(sql, params)
        rows = cursor.fetchall()
        cols = [d[0] for d in cursor.description]
    payload = [dict(zip(cols, r, strict=False)) for r in rows]
    if output == OutputFormat.JSON:
        emit_json("balance_drift", payload)
        return
    from rich.console import Console  # noqa: PLC0415 — defer heavy import
    from rich.table import Table  # noqa: PLC0415 — defer heavy import

    console = Console()
    table = Table(*cols)
    for r in rows:
        table.add_row(*[str(v) if v is not None else "" for v in r])
    console.print(table)
