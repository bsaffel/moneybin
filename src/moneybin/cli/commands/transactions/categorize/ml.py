"""ML-assisted categorization (not yet implemented)."""

import typer

from ...stubs import _not_implemented

app = typer.Typer(
    help="ML-assisted categorization",
    no_args_is_help=True,
)


@app.command("status")
def ml_status() -> None:
    """Show ML model status."""
    _not_implemented("categorization-ml.md")


@app.command("train")
def ml_train() -> None:
    """Train the ML categorization model."""
    _not_implemented("categorization-ml.md")


@app.command("apply")
def ml_apply() -> None:
    """Apply ML model to uncategorized transactions."""
    _not_implemented("categorization-ml.md")
