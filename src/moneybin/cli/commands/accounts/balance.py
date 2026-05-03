"""accounts balance — per-account balance workflow stub.

Stubs delegate to net-worth.md (status: draft). When that spec is
implemented these stubs are replaced.
"""

import typer

from ..stubs import _not_implemented

app = typer.Typer(
    help="Per-account balance workflow (assert, list, reconcile, history)",
    no_args_is_help=True,
)


@app.command("show")
def accounts_balance_show(
    account: str | None = typer.Option(None, "--account"),
    as_of: str | None = typer.Option(None, "--as-of"),
) -> None:
    """Show current balance for one or all accounts."""
    _not_implemented("net-worth.md")


@app.command("assert")
def accounts_balance_assert(
    account_id: str,
    date: str,
    amount: str,
    notes: str | None = typer.Option(None, "--notes"),
    yes: bool = typer.Option(False, "--yes"),
) -> None:
    """Assert a known balance for an account on a date."""
    _not_implemented("net-worth.md")


@app.command("list")
def accounts_balance_list(
    account: str | None = typer.Option(None, "--account"),
) -> None:
    """List balance assertions, optionally filtered by account."""
    _not_implemented("net-worth.md")


@app.command("delete")
def accounts_balance_delete(
    account_id: str,
    date: str,
    yes: bool = typer.Option(False, "--yes"),
) -> None:
    """Delete a balance assertion."""
    _not_implemented("net-worth.md")


@app.command("reconcile")
def accounts_balance_reconcile(
    account: str | None = typer.Option(None, "--account"),
    threshold: float | None = typer.Option(None, "--threshold"),
) -> None:
    """Show accounts with non-zero reconciliation deltas."""
    _not_implemented("net-worth.md")


@app.command("history")
def accounts_balance_history(
    account: str | None = typer.Option(None, "--account"),
    from_: str | None = typer.Option(None, "--from"),
    to: str | None = typer.Option(None, "--to"),
    interval: str = typer.Option("daily", "--interval"),
) -> None:
    """Show balance history."""
    _not_implemented("net-worth.md")
