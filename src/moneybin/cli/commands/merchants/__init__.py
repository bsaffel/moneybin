"""Merchant mapping management (list, create)."""

import typer

from moneybin.cli.commands.stubs import _not_implemented

app = typer.Typer(
    help="Merchant mappings management",
    no_args_is_help=True,
)


@app.command("list")
def list_merchants() -> None:
    """List all merchant mappings."""
    _not_implemented("categorization-overview.md")


@app.command("create")
def create(
    pattern: str = typer.Argument(..., help="Merchant name pattern"),
    canonical_name: str = typer.Argument(..., help="Canonical merchant name"),
    default_category: str | None = typer.Option(
        None, "--default-category", help="Default category for this merchant"
    ),
) -> None:
    """Create a merchant mapping."""
    _not_implemented("categorization-overview.md")
