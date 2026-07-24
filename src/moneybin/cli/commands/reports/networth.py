"""moneybin reports networth — current snapshot and history series."""

from __future__ import annotations

from typing import cast

import typer
from pydantic import JsonValue

from moneybin.cli.output import (
    OutputFormat,
    output_option,
    quiet_option,
    render_or_json,
)
from moneybin.cli.utils import handle_cli_errors
from moneybin.database import get_database
from moneybin.reports._framework.cli_register import (
    _CLI_MAX_ROWS,  # pyright: ignore[reportPrivateUsage]  # noqa: PLC2701 — shared report cap
)


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
        from moneybin.reports._framework.catalog import (  # noqa: PLC0415 — defer catalog import
            get_report_catalog,
        )

        with get_database(read_only=True) as db:
            result = get_report_catalog().execute(
                db,
                report_id="core:networth",
                parameters={
                    "as_of": as_of,
                    "account_ids": cast(JsonValue, account),
                },
                limit=_CLI_MAX_ROWS,
            )

    def _render_text(_: object) -> None:
        if not result.records or result.records[0]["balance_date"] is None:
            typer.echo("No net worth data available.")
            return
        snapshot = result.records[0]
        typer.echo(
            f"Net worth as of {snapshot['balance_date']}: {snapshot['net_worth']}"
        )
        typer.echo(f"  Assets:      {snapshot['total_assets']}")
        typer.echo(f"  Liabilities: {snapshot['total_liabilities']}")
        typer.echo(f"  Accounts:    {snapshot['account_count']}")
        accounts = [row for row in result.records if row["account_id"] is not None]
        if accounts:
            typer.echo("Per-account breakdown:")
            for row in accounts:
                typer.echo(
                    f"  {row['account_name']!s:<40} {row['account_balance']!s:>14}  "
                    f"({row['observation_source']})"
                )

    render_or_json(
        result.to_envelope(),
        output,
        render_fn=_render_text,
        cli_actor="reports_networth",
        classes_returned=result.classes_returned,
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
        from moneybin.reports._framework.catalog import (  # noqa: PLC0415 — defer catalog import
            get_report_catalog,
        )

        with get_database(read_only=True) as db:
            result = get_report_catalog().execute(
                db,
                report_id="core:networth_history",
                parameters={
                    "from_date": from_date,
                    "to_date": to_date,
                    "interval": interval,
                },
                limit=_CLI_MAX_ROWS,
            )

    def _render_text(_: object) -> None:
        typer.echo("period      net_worth     change_abs    change_pct")
        for point in result.records:
            change_abs = point["change_abs"] if point["change_abs"] is not None else "-"
            change_pct = (
                f"{point['change_pct']:.2%}" if point["change_pct"] is not None else "-"
            )
            typer.echo(
                f"{point['period']!s:<12} {point['net_worth']!s:>12} "
                f"{change_abs!s:>13} "
                f"{change_pct:>10}"
            )

    render_or_json(
        result.to_envelope(),
        output,
        render_fn=_render_text,
        cli_actor="reports_networth_history",
        classes_returned=result.classes_returned,
    )
