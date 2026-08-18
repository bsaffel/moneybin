"""moneybin reports networth — current snapshot and history series."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, cast

import typer
from pydantic import JsonValue

from moneybin.cli.output import (
    CLI_MAX_ROWS,
    OutputFormat,
    currency_label,
    display_currency_option,
    output_option,
    quiet_option,
    render_or_json,
)
from moneybin.cli.utils import handle_cli_errors
from moneybin.database import get_database
from moneybin.reports._framework.cli_register import echo_report_notes


def _summed(rows: Sequence[Mapping[str, Any]], column: str) -> Any:
    """``column`` added up across ``rows``, or None when no row states it."""
    values = [row[column] for row in rows if row[column] is not None]
    return sum(values) if values else None


def reports_networth(
    as_of: str | None = typer.Option(
        None, "--as-of", help="ISO date (YYYY-MM-DD); shows networth on or before"
    ),
    account: list[str] | None = typer.Option(
        None,
        "--account",
        help="Filter per-account breakdown to specific account_id(s); repeatable",
    ),
    display_currency: str | None = display_currency_option,
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,  # noqa: ARG001 — networth prints a snapshot, not informational chatter
) -> None:
    """Show current or as-of net worth + per-account breakdown."""
    with handle_cli_errors():
        from moneybin.reports._framework.catalog import (  # noqa: PLC0415 — defer catalog import
            get_report_catalog,
            profile_home_currency,
        )

        with get_database(read_only=True) as db:
            result = get_report_catalog().execute(
                db,
                report_id="core:networth",
                parameters={
                    "as_of": as_of,
                    "account_ids": cast(JsonValue, account),
                },
                limit=CLI_MAX_ROWS,
                display_currency=display_currency,
                home_currency=profile_home_currency(db),
            )

    def _render_text(_: object) -> None:
        if not result.records or result.records[0]["balance_date"] is None:
            typer.echo("No net worth data available.")
            echo_report_notes(result)
            return
        # One totals row per currency the profile holds. Display conversion
        # prices each of them and relabels it into the target currency, so
        # after a successful conversion they share one label and add up to the
        # whole position (multi-currency.md Requirement 16); without one they
        # stay apart and each currency keeps its own headline. Printing the
        # first row of a group would report one currency's share as the whole
        # in both cases.
        by_currency: dict[object, list[dict[str, JsonValue]]] = {}
        for row in result.records:
            if row["account_id"] is None:
                by_currency.setdefault(row["currency_code"], []).append(row)
        balance_date = result.records[0]["balance_date"]
        typer.echo(f"Net worth as of {balance_date}")
        for currency, rows in by_currency.items():
            typer.echo(f"  {currency_label(currency)}: {_summed(rows, 'net_worth')}")
            typer.echo(f"    Assets:      {_summed(rows, 'total_assets')}")
            typer.echo(f"    Liabilities: {_summed(rows, 'total_liabilities')}")
            typer.echo(f"    Accounts:    {_summed(rows, 'account_count')}")
        accounts = [row for row in result.records if row["account_id"] is not None]
        if accounts:
            typer.echo("Per-account breakdown:")
            for row in accounts:
                typer.echo(
                    f"  {row['account_name']!s:<40} {row['account_balance']!s:>14} "
                    f"{currency_label(row['currency_code']):<3} "
                    f"({row['observation_source']})"
                )
        echo_report_notes(result)

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
    display_currency: str | None = display_currency_option,
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,  # noqa: ARG001 — history prints a series, not informational chatter
) -> None:
    """Net worth time series with period-over-period change."""
    with handle_cli_errors():
        from moneybin.reports._framework.catalog import (  # noqa: PLC0415 — defer catalog import
            get_report_catalog,
            profile_home_currency,
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
                limit=CLI_MAX_ROWS,
                display_currency=display_currency,
                home_currency=profile_home_currency(db),
            )

    def _render_text(_: object) -> None:
        # Each currency is its own series, so a period appears once per
        # currency; without the column two rows for the same month read as one
        # position swinging wildly.
        typer.echo("period       cur     net_worth     change_abs    change_pct")
        for point in result.records:
            change_abs = point["change_abs"] if point["change_abs"] is not None else "-"
            change_pct = (
                f"{point['change_pct']:.2%}" if point["change_pct"] is not None else "-"
            )
            currency = currency_label(point["currency_code"])
            typer.echo(
                f"{point['period']!s:<12} {currency:<7} {point['net_worth']!s:>12} "
                f"{change_abs!s:>13} "
                f"{change_pct:>10}"
            )
        echo_report_notes(result)

    render_or_json(
        result.to_envelope(),
        output,
        render_fn=_render_text,
        cli_actor="reports_networth_history",
        classes_returned=result.classes_returned,
    )
