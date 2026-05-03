"""Category taxonomy management (list, create, toggle, delete)."""

import typer

from moneybin.cli.commands.stubs import _not_implemented

app = typer.Typer(
    help="Category taxonomy management",
    no_args_is_help=True,
)


@app.command("list")
def categories_list() -> None:
    """List all categories."""
    _not_implemented("categorization-overview.md")


@app.command("create")
def categories_create(
    name: str = typer.Argument(..., help="Category name"),
    parent: str | None = typer.Option(None, "--parent", help="Parent category name"),
) -> None:
    """Create a new category."""
    _not_implemented("categorization-overview.md")


@app.command("toggle")
def categories_toggle(
    category_id: str = typer.Argument(..., help="Category ID to enable/disable"),
) -> None:
    """Enable or disable a category."""
    _not_implemented("categorization-overview.md")


@app.command("delete")
def categories_delete(
    category_id: str = typer.Argument(..., help="Category ID to delete"),
) -> None:
    """Delete a category."""
    _not_implemented("categorization-overview.md")
