"""accounts investments — placeholder for investment-tracking.md."""

import typer

from ..stubs import _not_implemented

app = typer.Typer(
    help="Investment holdings tracking (future: investment-tracking.md)",
    no_args_is_help=True,
)


@app.command("show")
def show() -> None:
    """Show investment portfolio."""
    _not_implemented("investment-tracking.md")
