"""reports networth — cross-domain net worth aggregation (accounts + assets)."""

import typer

from ..stubs import _not_implemented

app = typer.Typer(
    help="Cross-domain net worth aggregation (accounts + assets)",
    no_args_is_help=True,
)


@app.command("show")
def show(as_of: str | None = typer.Option(None, "--as-of")) -> None:
    """Show current net worth."""
    _not_implemented("net-worth.md")


@app.command("history")
def history(
    from_: str | None = typer.Option(None, "--from"),
    to: str | None = typer.Option(None, "--to"),
    interval: str = typer.Option("monthly", "--interval"),
) -> None:
    """Show net worth history."""
    _not_implemented("net-worth.md")
