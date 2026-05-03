"""ML-assisted categorization (not yet implemented)."""

import typer

from moneybin.cli.commands.stubs import _not_implemented

app = typer.Typer(
    help="ML-assisted categorization",
    no_args_is_help=True,
)


@app.command("status")
def status() -> None:
    """Show ML model status."""
    _not_implemented("categorization-ml.md")


@app.command("train")
def train() -> None:
    """Train the ML categorization model."""
    _not_implemented("categorization-ml.md")


@app.command("apply")
def apply() -> None:
    """Apply ML model to uncategorized transactions."""
    _not_implemented("categorization-ml.md")
