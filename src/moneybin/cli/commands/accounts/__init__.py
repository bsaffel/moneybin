"""Accounts top-level command group.

Owns account entity operations (list, show, rename, include) and
per-account workflows (balance) per cli-restructure.md v2.
Entity ops are stubbed; account-management.md (planned) owns the
implementation.
"""

import typer

from ..stubs import _not_implemented
from . import balance, investments

app = typer.Typer(
    help="Accounts and per-account workflows (balance, investments)",
    no_args_is_help=True,
)


@app.command("list")
def accounts_list() -> None:
    """List all accounts."""
    _not_implemented("account-management.md")


@app.command("show")
def accounts_show(account_id: str) -> None:
    """Show one account by ID."""
    _not_implemented("account-management.md")


@app.command("rename")
def accounts_rename(account_id: str, new_name: str) -> None:
    """Rename an account."""
    _not_implemented("account-management.md")


@app.command("include")
def accounts_include(
    account_id: str,
    no: bool = typer.Option(
        False, "--no", help="Exclude from net worth instead of include"
    ),
) -> None:
    """Toggle include_in_net_worth for an account."""
    _not_implemented("account-management.md")


app.add_typer(balance.app, name="balance")
app.add_typer(investments.app, name="investments")
