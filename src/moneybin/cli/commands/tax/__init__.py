"""tax — tax forms, deductions, and tax-prep utilities."""

import typer

from ..stubs import _not_implemented

app = typer.Typer(
    help="Tax forms, deductions, and tax-prep utilities",
    no_args_is_help=True,
)


@app.command("w2")
def tax_w2(year: str) -> None:
    """Show W-2 form data for a tax year."""
    _not_implemented("tax-w2.md")


@app.command("deductions")
def tax_deductions(year: str) -> None:
    """Show categorized deductible expenses for a tax year."""
    _not_implemented("tax-deductions.md")
