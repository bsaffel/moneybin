"""budget — budget target management.

Vs-actual report lives under `reports budget` per moneybin-cli.md v2.
"""

import typer

from ..stubs import _not_implemented

app = typer.Typer(
    help="Budget target management (vs-actual report lives in `reports budget`)",
    no_args_is_help=True,
)


@app.command("set", hidden=True)
def budget_set(category: str, amount: float) -> None:
    """Set or update a budget target for a category."""
    _not_implemented("budget targets")


@app.command("delete", hidden=True)
def budget_delete(category: str) -> None:
    """Delete a budget target for a category."""
    _not_implemented("budget targets")
