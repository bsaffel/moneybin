"""accounts balance — per-account balance workflow.

Subcommands: show, history, assert, list, delete, reconcile.
All delegate to BalanceService — no business logic here.
"""

from __future__ import annotations

import logging
from datetime import date as _date
from decimal import Decimal

import typer

from moneybin.cli.output import (
    OutputFormat,
    currency_label,
    emit_human_result,
    no_pager_option,
    output_option,
    quiet_option,
    render_or_json,
)
from moneybin.cli.render import (
    Money,
    MoneyWithCurrency,
    build_rows,
    build_summary,
    compose_human_result,
)
from moneybin.cli.utils import get_terminal_policy, handle_cli_errors
from moneybin.database import get_database
from moneybin.privacy.payloads.balances import (
    BalanceAssertionListPayload,
    BalanceObservationListPayload,
    BalanceObservationRow,
)
from moneybin.protocol.envelope import build_envelope
from moneybin.services.balance_service import (
    BalanceService,
    balances_display_currency,
)

logger = logging.getLogger(__name__)

app = typer.Typer(
    help="Balance assertions, history, and reconciliation",
    no_args_is_help=True,
)


def _emit_observations(
    observations: list[BalanceObservationRow], *, no_pager: bool, include_account: bool
) -> None:
    """Render balance observations as one pageable result with atomic amounts."""
    policy = get_terminal_policy(no_pager=no_pager)
    columns = (
        ["account_id", "date", "balance", "observed", "source", "delta"]
        if include_account
        else ["date", "balance", "observed", "source", "delta"]
    )
    rows: list[tuple[object, ...]] = []
    for obs in observations:
        row = [
            obs.account_id,
            obs.balance_date,
            MoneyWithCurrency(obs.balance, currency_label(obs.currency_code)),
            obs.is_observed,
            obs.observation_source,
            obs.reconciliation_delta,
        ]
        rows.append(tuple(row if include_account else row[1:]))
    human = build_rows(
        columns,
        rows,
        money={"balance": Money("balance"), "delta": Money("delta", polarity="income")},
        total_columns=len(columns),
        terminal=policy,
    )
    emit_human_result(human, policy=policy, finite_read=True, no_pager=no_pager)


@app.command("show")
def accounts_balance_show(
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,  # show has no informational chatter
    account: str | None = typer.Option(
        None, "--account", help="Filter to a single account_id"
    ),
    as_of: str | None = typer.Option(
        None, "--as-of", help="ISO date (YYYY-MM-DD); shows balance on or before"
    ),
    no_pager: bool = no_pager_option,
) -> None:
    """Show current or as-of balances per account."""
    account_ids = [account] if account else None
    with handle_cli_errors(
        cli_actor="accounts_balance_show", payload_type=BalanceObservationListPayload
    ):
        with get_database(read_only=True) as db:
            as_of_date = _date.fromisoformat(as_of) if as_of else None
            result = BalanceService(db).current_balances(
                account_ids=account_ids, as_of_date=as_of_date
            )

    envelope = build_envelope(
        data=result, display_currency=balances_display_currency(result)
    )
    if output == OutputFormat.JSON:
        render_or_json(envelope, output, cli_actor="accounts_balance_show")
        return
    _emit_observations(
        list(result.observations), no_pager=no_pager, include_account=True
    )


@app.command("history")
def accounts_balance_history(
    account: str = typer.Option(..., "--account", help="Account ID (required)"),
    from_date: str | None = typer.Option(None, "--from"),
    to_date: str | None = typer.Option(None, "--to"),
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,  # history has no informational chatter
    no_pager: bool = no_pager_option,
) -> None:
    """Per-account balance history (daily series)."""
    with handle_cli_errors(
        cli_actor="accounts_balance_history",
        payload_type=BalanceObservationListPayload,
    ):
        with get_database(read_only=True) as db:
            from_d = _date.fromisoformat(from_date) if from_date else None
            to_d = _date.fromisoformat(to_date) if to_date else None
            result = BalanceService(db).history(account, from_date=from_d, to_date=to_d)

    envelope = build_envelope(
        data=result, display_currency=balances_display_currency(result)
    )
    if output == OutputFormat.JSON:
        render_or_json(envelope, output, cli_actor="accounts_balance_history")
        return
    _emit_observations(
        list(result.observations), no_pager=no_pager, include_account=False
    )


@app.command("assert")
def accounts_balance_assert(
    account_id: str = typer.Argument(...),
    assertion_date: str = typer.Argument(..., help="ISO date (YYYY-MM-DD)"),
    amount: str = typer.Argument(..., help="Balance amount as decimal"),
    notes: str | None = typer.Option(None, "--notes"),
    yes: bool = typer.Option(
        False, "--yes", "-y"
    ),  # accepted for forward compat; no confirmation prompt today, but scripts pass --yes defensively
) -> None:
    """Assert a balance for an account on a specific date."""
    parsed_date: _date
    with handle_cli_errors():
        with get_database(read_only=False) as db:
            parsed_date = _date.fromisoformat(assertion_date)
            parsed_amount = Decimal(amount)
            result = BalanceService(db).assert_balance(
                account_id=account_id,
                assertion_date=parsed_date,
                balance=parsed_amount,
                notes=notes,
                actor="cli",
            )
    emit_human_result(
        compose_human_result([
            build_summary(
                [
                    ("Account", account_id),
                    ("Date", str(parsed_date)),
                    (
                        "Balance",
                        f"{result.assertion.balance} "
                        f"{currency_label(result.assertion.currency_code)}",
                    ),
                ],
                title="Balance asserted",
            )
        ]),
        policy=get_terminal_policy(),
        finite_read=False,
        receipt=True,
    )


@app.command("list")
def accounts_balance_list(
    account: str | None = typer.Option(None, "--account"),
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,  # list has no informational chatter
    no_pager: bool = no_pager_option,
) -> None:
    """List balance assertions, optionally filtered by account."""
    with handle_cli_errors(
        cli_actor="accounts_balance_list", payload_type=BalanceAssertionListPayload
    ):
        with get_database(read_only=True) as db:
            result = BalanceService(db).list_assertions(account)
    if output == OutputFormat.JSON:
        render_or_json(
            build_envelope(data=result),
            output,
            cli_actor="accounts_balance_list",
        )
        return
    policy = get_terminal_policy(no_pager=no_pager)
    rows: list[tuple[object, ...]] = [
        (
            assertion.account_id,
            assertion.assertion_date,
            MoneyWithCurrency(
                assertion.balance, currency_label(assertion.currency_code)
            ),
            assertion.notes,
        )
        for assertion in result.assertions
    ]
    emit_human_result(
        build_rows(
            ["account_id", "date", "balance", "notes"],
            rows,
            money={"balance": Money("balance")},
            terminal=policy,
        ),
        policy=policy,
        finite_read=True,
        no_pager=no_pager,
    )


@app.command("assertion-delete")
def accounts_balance_assertion_delete(
    account_id: str = typer.Argument(...),
    assertion_date: str = typer.Argument(..., help="ISO date (YYYY-MM-DD)"),
    yes: bool = typer.Option(
        False, "--yes", "-y"
    ),  # accepted for forward compat; no confirmation prompt today, but scripts pass --yes defensively
) -> None:
    """Delete a balance assertion. Silent no-op if no row exists."""
    parsed_date: _date
    with handle_cli_errors():
        with get_database(read_only=False) as db:
            parsed_date = _date.fromisoformat(assertion_date)
            deleted = BalanceService(db).delete_assertion(
                account_id, parsed_date, actor="cli"
            )
    emit_human_result(
        compose_human_result([
            build_summary(
                [("Account", account_id), ("Date", str(parsed_date))],
                title=(
                    "Balance assertion deleted"
                    if deleted
                    else "No balance assertion found"
                ),
            )
        ]),
        policy=get_terminal_policy(),
        finite_read=False,
        receipt=True,
    )


@app.command("reconcile")
def accounts_balance_reconcile(
    account: str | None = typer.Option(None, "--account"),
    threshold: str = typer.Option("0.01", "--threshold"),
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,  # reconcile has no informational chatter
    no_pager: bool = no_pager_option,
) -> None:
    """Show observed balance days with non-zero reconciliation delta."""
    account_ids = [account] if account else None
    with handle_cli_errors(
        cli_actor="accounts_balance_reconcile",
        payload_type=BalanceObservationListPayload,
    ):
        with get_database(read_only=True) as db:
            parsed_threshold = Decimal(threshold)
            result = BalanceService(db).reconcile(
                account_ids=account_ids, threshold=parsed_threshold
            )
    if output == OutputFormat.JSON:
        render_or_json(
            build_envelope(
                data=result, display_currency=balances_display_currency(result)
            ),
            output,
            cli_actor="accounts_balance_reconcile",
        )
        return
    _emit_observations(
        list(result.observations), no_pager=no_pager, include_account=True
    )
