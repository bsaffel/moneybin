"""Reports top-level command group.

Owns cross-domain analytical and aggregation view operations. Per
cli-restructure.md v2: cross-cutting read-only views (networth,
spending, cashflow, financial health, budget vs actual).
"""

from __future__ import annotations

import logging
from datetime import date as _date

import typer

from moneybin.cli.output import OutputFormat, output_option, quiet_option
from moneybin.cli.utils import emit_json, handle_cli_errors
from moneybin.services.networth_service import NetworthService

from ..stubs import _not_implemented
from .balance_drift import balance_drift_app
from .cashflow import cashflow_app
from .large_transactions import large_transactions_app
from .merchants import merchants_app
from .recurring import recurring_app
from .spending import spending_app
from .uncategorized import uncategorized_app

logger = logging.getLogger(__name__)

app = typer.Typer(
    help="Cross-domain analytical and aggregation views",
    no_args_is_help=True,
)

networth_app = typer.Typer(help="Net worth reports", no_args_is_help=True)
app.add_typer(networth_app, name="networth")
app.add_typer(cashflow_app, name="cashflow")
app.add_typer(spending_app, name="spending")
app.add_typer(recurring_app, name="recurring")
app.add_typer(merchants_app, name="merchants")
app.add_typer(uncategorized_app, name="uncategorized")
app.add_typer(large_transactions_app, name="large-transactions")
app.add_typer(balance_drift_app, name="balance-drift")


@networth_app.command("show")
def reports_networth_show(
    as_of: str | None = typer.Option(
        None, "--as-of", help="ISO date (YYYY-MM-DD); shows networth on or before"
    ),
    account: list[str] | None = typer.Option(
        None,
        "--account",
        help="Filter per-account breakdown to specific account_id(s); repeatable",
    ),
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,  # noqa: ARG001 — show prints a snapshot, not informational chatter
) -> None:
    """Show current or as-of net worth + per-account breakdown."""
    with handle_cli_errors() as db:
        as_of_date = _date.fromisoformat(as_of) if as_of else None
        snapshot = NetworthService(db).current(
            as_of_date=as_of_date, account_ids=account
        )
    payload = snapshot.to_dict()
    if output == OutputFormat.JSON:
        emit_json("networth", payload)
        return
    typer.echo(f"Net worth as of {snapshot.balance_date}: {snapshot.net_worth}")
    typer.echo(f"  Assets:      {snapshot.total_assets}")
    typer.echo(f"  Liabilities: {snapshot.total_liabilities}")
    typer.echo(f"  Accounts:    {snapshot.account_count}")
    if snapshot.per_account:
        typer.echo("Per-account breakdown:")
        for row in snapshot.per_account:
            typer.echo(
                f"  {row['display_name']:<40} {row['balance']:>14}  "
                f"({row['observation_source']})"
            )


@networth_app.command("history")
def reports_networth_history(
    from_date: str = typer.Option(..., "--from", help="ISO date (YYYY-MM-DD)"),
    to_date: str = typer.Option(..., "--to", help="ISO date (YYYY-MM-DD)"),
    interval: str = typer.Option(
        "monthly", "--interval", help="daily | weekly | monthly"
    ),
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,  # noqa: ARG001 — history prints a series, not informational chatter
) -> None:
    """Net worth time series with period-over-period change."""
    with handle_cli_errors() as db:
        parsed_from = _date.fromisoformat(from_date)
        parsed_to = _date.fromisoformat(to_date)
        rows = NetworthService(db).history(parsed_from, parsed_to, interval=interval)
    if output == OutputFormat.JSON:
        emit_json("history", rows)
        return
    typer.echo("period      net_worth     change_abs    change_pct")
    for row in rows:
        change_abs = row["change_abs"] if row["change_abs"] is not None else "-"
        change_pct = (
            f"{row['change_pct']:.2%}" if row["change_pct"] is not None else "-"
        )
        typer.echo(
            f"{row['period']:<12} {row['net_worth']:>12} {change_abs!s:>13} "
            f"{change_pct:>10}"
        )


@app.command("budget")
def reports_budget() -> None:
    """Budget vs actual report."""
    _not_implemented("budget-tracking.md")


@app.command("health")
def reports_health(months: int = typer.Option(1, "--months")) -> None:
    """Cross-domain financial health snapshot."""
    _not_implemented("net-worth.md")
