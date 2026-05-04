"""accounts balance — per-account balance workflow.

Subcommands: show, history, assert, list, delete, reconcile.
All delegate to BalanceService — no business logic here.
"""

from __future__ import annotations

import logging
from datetime import date as _date
from decimal import Decimal

import typer

from moneybin.cli.output import OutputFormat, output_option, quiet_option
from moneybin.cli.utils import emit_json, handle_cli_errors
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
    with handle_cli_errors() as db:
        as_of_date = _date.fromisoformat(as_of) if as_of else None
        observations = BalanceService(db).current_balances(
            account_ids=account_ids, as_of_date=as_of_date
        )
    if output == OutputFormat.JSON:
        emit_json("balances", [o.to_dict() for o in observations])
        return
    for obs in observations:
        d = obs.to_dict()
        typer.echo(
            f"  {d['account_id']}  {d['balance_date']}  {d['balance']}"
            f"  observed={d['is_observed']}  source={d['observation_source']}"
            f"  delta={d['reconciliation_delta']}"
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
    with handle_cli_errors() as db:
        from_d = _date.fromisoformat(from_date) if from_date else None
        to_d = _date.fromisoformat(to_date) if to_date else None
        observations = BalanceService(db).history(
            account, from_date=from_d, to_date=to_d
        )
    if output == OutputFormat.JSON:
        emit_json("history", [o.to_dict() for o in observations])
        return
    for obs in observations:
        d = obs.to_dict()
        typer.echo(
            f"  {d['balance_date']}  {d['balance']}"
            f"  observed={d['is_observed']}  source={d['observation_source']}"
            f"  delta={d['reconciliation_delta']}"
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
    result: object
    with handle_cli_errors() as db:
        parsed_date = _date.fromisoformat(assertion_date)
        parsed_amount = Decimal(amount)
        result = BalanceService(db).assert_balance(
            account_id=account_id,
            assertion_date=parsed_date,
            balance=parsed_amount,
            notes=notes,
        )
    typer.echo(
        f"✅ Asserted balance for {account_id} on {parsed_date}: {result.balance}",  # type: ignore[union-attr]
        err=True,
    )


@app.command("list")
def accounts_balance_list(
    account: str | None = typer.Option(None, "--account"),
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,  # noqa: ARG001 — list has no informational chatter
) -> None:
    """List balance assertions, optionally filtered by account."""
    with handle_cli_errors() as db:
        assertions = BalanceService(db).list_assertions(account)
    if output == OutputFormat.JSON:
        emit_json("assertions", [a.to_dict() for a in assertions])
        return
    for assertion in assertions:
        d = assertion.to_dict()
        typer.echo(
            f"  {d['account_id']}  {d['assertion_date']}  {d['balance']}  notes={d['notes']}"
        )


@app.command("delete")
def accounts_balance_delete(
    account_id: str = typer.Argument(...),
    assertion_date: str = typer.Argument(..., help="ISO date (YYYY-MM-DD)"),
    yes: bool = typer.Option(False, "--yes", "-y"),  # noqa: ARG001 — accepted for forward compat; no confirmation prompt today, but scripts pass --yes defensively
) -> None:
    """Delete a balance assertion. Silent no-op if no row exists."""
    parsed_date: _date
    with handle_cli_errors() as db:
        parsed_date = _date.fromisoformat(assertion_date)
        BalanceService(db).delete_assertion(account_id, parsed_date)
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
    parsed_threshold = Decimal(threshold)
    account_ids = [account] if account else None
    with handle_cli_errors() as db:
        observations = BalanceService(db).reconcile(
            account_ids=account_ids, threshold=parsed_threshold
        )
    if output == OutputFormat.JSON:
        emit_json("reconcile", [o.to_dict() for o in observations])
        return
    for obs in observations:
        d = obs.to_dict()
        typer.echo(
            f"  {d['account_id']}  {d['balance_date']}  {d['balance']}"
            f"  source={d['observation_source']}  delta={d['reconciliation_delta']}"
        )
