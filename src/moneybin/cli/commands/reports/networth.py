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
from moneybin.cli.render import Money, format_money, render_rows, render_summary
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
    quiet: bool = quiet_option,
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
            echo_report_notes(result, quiet=quiet)
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
        for currency, rows in by_currency.items():
            render_summary(
                [
                    # Every figure here is a position rather than a movement, so
                    # each is a `balance`: unsigned when it is, and keeping the
                    # `−` when it is not. Liabilities are stored negative, and a
                    # profile can hold a negative net worth outright — the one
                    # amount on this surface where a dropped minus inverts the
                    # answer.
                    ("Net worth", format_money(_summed(rows, "net_worth"), "balance")),
                    ("Assets", format_money(_summed(rows, "total_assets"), "balance")),
                    (
                        "Liabilities",
                        format_money(_summed(rows, "total_liabilities"), "balance"),
                    ),
                    ("Accounts", str(_summed(rows, "account_count") or 0)),
                ],
                title=f"{currency_label(currency)} as of {balance_date}",
            )
        accounts = [row for row in result.records if row["account_id"] is not None]
        if accounts:
            render_rows(
                ["account", "balance", "currency", "source"],
                [
                    (
                        row["account_name"],
                        row["account_balance"],
                        currency_label(row["currency_code"]),
                        row["observation_source"],
                    )
                    for row in accounts
                ],
                money={"balance": Money("balance")},
            )
        echo_report_notes(result, quiet=quiet)

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
    quiet: bool = quiet_option,
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
        if result.records:
            render_rows(
                ["period", "currency", "net_worth", "change_abs", "change_pct"],
                [
                    (
                        point["period"],
                        currency_label(point["currency_code"]),
                        point["net_worth"],
                        point["change_abs"],
                        f"{point['change_pct']:.2%}"
                        if point["change_pct"] is not None
                        else "-",
                    )
                    for point in result.records
                ],
                money={
                    "net_worth": Money("balance"),
                    # A change in a position, not in a spend magnitude: the sign
                    # says which way the position moved, and up is the favourable
                    # direction, so a rise reads as income rather than expense.
                    "change_abs": Money("delta", polarity="income"),
                },
                # Formatted to two places by the caller above rather than by
                # `format_money`, so it takes the no-fold guarantee here: a
                # folded `12.34%` reads as `12.3` and is off by a factor of ten.
                numeric=("change_pct",),
            )
        echo_report_notes(result, quiet=quiet)

    render_or_json(
        result.to_envelope(),
        output,
        render_fn=_render_text,
        cli_actor="reports_networth_history",
        classes_returned=result.classes_returned,
    )
