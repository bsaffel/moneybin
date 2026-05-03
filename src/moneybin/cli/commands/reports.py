"""CLI commands for the v2 reports namespace.

Top-level reports group: cross-domain analytical views.

Created by net-worth.md. Future report specs (spending, cashflow, tax,
budget vs actual) add subcommands to this group per cli-restructure.md v2.
"""

from __future__ import annotations

import logging
from datetime import date as _date

import typer

from moneybin.cli.output import OutputFormat, output_option, quiet_option
from moneybin.cli.utils import emit_json, handle_cli_errors
from moneybin.services.networth_service import NetworthService

logger = logging.getLogger(__name__)

app = typer.Typer(help="Cross-domain analytical reports", no_args_is_help=True)
networth_app = typer.Typer(help="Net worth reports", no_args_is_help=True)
app.add_typer(networth_app, name="networth")


@networth_app.command("show")
def networth_show_cmd(
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
    as_of_date = _date.fromisoformat(as_of) if as_of else None
    with handle_cli_errors() as db:
        snapshot = NetworthService(db).current(
            as_of_date=as_of_date, account_ids=account
        )
    payload = {
        "balance_date": snapshot.balance_date.isoformat(),
        "net_worth": snapshot.net_worth,
        "total_assets": snapshot.total_assets,
        "total_liabilities": snapshot.total_liabilities,
        "account_count": snapshot.account_count,
        "per_account": snapshot.per_account,
    }
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
def networth_history_cmd(
    from_date: str = typer.Option(..., "--from", help="ISO date (YYYY-MM-DD)"),
    to_date: str = typer.Option(..., "--to", help="ISO date (YYYY-MM-DD)"),
    interval: str = typer.Option(
        "monthly", "--interval", help="daily | weekly | monthly"
    ),
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,  # noqa: ARG001 — history prints a series, not informational chatter
) -> None:
    """Net worth time series with period-over-period change."""
    parsed_from = _date.fromisoformat(from_date)
    parsed_to = _date.fromisoformat(to_date)
    with handle_cli_errors() as db:
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
