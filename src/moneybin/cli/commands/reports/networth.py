"""moneybin reports networth — current snapshot and history series."""

from __future__ import annotations

import logging
from datetime import date as _date

import typer

from moneybin.cli.output import (
    OutputFormat,
    output_option,
    quiet_option,
    render_or_json,
)
from moneybin.cli.utils import handle_cli_errors
from moneybin.database import get_database
from moneybin.protocol.envelope import build_envelope
from moneybin.services.networth_service import NetworthService

logger = logging.getLogger(__name__)


def reports_networth(
    as_of: str | None = typer.Option(
        None, "--as-of", help="ISO date (YYYY-MM-DD); shows networth on or before"
    ),
    account: list[str] | None = typer.Option(
        None,
        "--account",
        help="Filter per-account breakdown to specific account_id(s); repeatable",
    ),
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,  # noqa: ARG001 — networth prints a snapshot, not informational chatter
) -> None:
    """Show current or as-of net worth + per-account breakdown."""
    with handle_cli_errors():
        with get_database(read_only=True) as db:
            as_of_date = _date.fromisoformat(as_of) if as_of else None
            snapshot = NetworthService(db).current(
                as_of_date=as_of_date, account_ids=account
            )
    payload = snapshot.to_dict()

    def _render_text(_: object) -> None:
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

    render_or_json(
        build_envelope(data=payload, sensitivity="low"),
        output,
        render_fn=_render_text,
    )


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
    with handle_cli_errors():
        with get_database(read_only=True) as db:
            parsed_from = _date.fromisoformat(from_date)
            parsed_to = _date.fromisoformat(to_date)
            rows = NetworthService(db).history(
                parsed_from, parsed_to, interval=interval
            )

    def _render_text(_: object) -> None:
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

    render_or_json(
        build_envelope(data=rows, sensitivity="low"),
        output,
        render_fn=_render_text,
    )
