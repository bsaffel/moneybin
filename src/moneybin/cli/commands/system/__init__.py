"""system — system and data status meta-view."""

import typer

from ..stubs import _not_implemented

app = typer.Typer(
    help="System and data status",
    no_args_is_help=True,
)


@app.command("status")
def system_status() -> None:
    """Show data freshness and pending review queue counts."""
    _not_implemented("net-worth.md")
