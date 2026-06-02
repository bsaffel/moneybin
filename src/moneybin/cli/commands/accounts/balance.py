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
    output_option,
    quiet_option,
    render_or_json,
)
from moneybin.cli.utils import handle_cli_errors
from moneybin.database import get_database
from moneybin.privacy.payloads.balances import (
    BalanceAssertionListPayload,
    BalanceObservationListPayload,
)
from moneybin.protocol.envelope import build_envelope
from moneybin.services.balance_service import BalanceService

logger = logging.getLogger(__name__)

app = typer.Typer(
    help="Balance assertions, history, and reconciliation",
    no_args_is_help=True,
)


@app.command("show")
def accounts_balance_show(
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,  # noqa: ARG001 — show has no informational chatter
    account: str | None = typer.Option(
        None, "--account", help="Filter to a single account_id"
    ),
    as_of: str | None = typer.Option(
        None, "--as-of", help="ISO date (YYYY-MM-DD); shows balance on or before"
    ),
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

    def _render_text(_: object) -> None:
        for obs in result.observations:
            typer.echo(
                f"  {obs.account_id}  {obs.balance_date}  {obs.balance}"
                f"  observed={obs.is_observed}  source={obs.observation_source}"
                f"  delta={obs.reconciliation_delta}"
            )

    render_or_json(
        build_envelope(data=result),
        output,
        render_fn=_render_text,
        cli_actor="accounts_balance_show",
    )


@app.command("history")
def accounts_balance_history(
    account: str = typer.Option(..., "--account", help="Account ID (required)"),
    from_date: str | None = typer.Option(None, "--from"),
    to_date: str | None = typer.Option(None, "--to"),
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,  # noqa: ARG001 — history has no informational chatter
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

    def _render_text(_: object) -> None:
        for obs in result.observations:
            typer.echo(
                f"  {obs.balance_date}  {obs.balance}"
                f"  observed={obs.is_observed}  source={obs.observation_source}"
                f"  delta={obs.reconciliation_delta}"
            )

    render_or_json(
        build_envelope(data=result),
        output,
        render_fn=_render_text,
        cli_actor="accounts_balance_history",
    )


@app.command("assert")
def accounts_balance_assert(
    account_id: str = typer.Argument(...),
    assertion_date: str = typer.Argument(..., help="ISO date (YYYY-MM-DD)"),
    amount: str = typer.Argument(..., help="Balance amount as decimal"),
    notes: str | None = typer.Option(None, "--notes"),
    yes: bool = typer.Option(False, "--yes", "-y"),  # noqa: ARG001 — accepted for forward compat; no confirmation prompt today, but scripts pass --yes defensively
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
    typer.echo(
        f"✅ Asserted balance for {account_id} on {parsed_date}: {result.assertion.balance}",
        err=True,
    )


@app.command("list")
def accounts_balance_list(
    account: str | None = typer.Option(None, "--account"),
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,  # noqa: ARG001 — list has no informational chatter
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
    for assertion in result.assertions:
        typer.echo(
            f"  {assertion.account_id}  {assertion.assertion_date}  {assertion.balance}  notes={assertion.notes}"
        )


@app.command("assertion-delete")
def accounts_balance_assertion_delete(
    account_id: str = typer.Argument(...),
    assertion_date: str = typer.Argument(..., help="ISO date (YYYY-MM-DD)"),
    yes: bool = typer.Option(False, "--yes", "-y"),  # noqa: ARG001 — accepted for forward compat; no confirmation prompt today, but scripts pass --yes defensively
) -> None:
    """Delete a balance assertion. Silent no-op if no row exists."""
    parsed_date: _date
    with handle_cli_errors():
        with get_database(read_only=False) as db:
            parsed_date = _date.fromisoformat(assertion_date)
            BalanceService(db).delete_assertion(account_id, parsed_date, actor="cli")
    typer.echo(
        f"✅ Deleted balance assertion for {account_id} on {parsed_date}",
        err=True,
    )


@app.command("reconcile")
def accounts_balance_reconcile(
    account: str | None = typer.Option(None, "--account"),
    threshold: str = typer.Option("0.01", "--threshold"),
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,  # noqa: ARG001 — reconcile has no informational chatter
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
            build_envelope(data=result),
            output,
            cli_actor="accounts_balance_reconcile",
        )
        return
    for obs in result.observations:
        typer.echo(
            f"  {obs.account_id}  {obs.balance_date}  {obs.balance}"
            f"  source={obs.observation_source}  delta={obs.reconciliation_delta}"
        )
