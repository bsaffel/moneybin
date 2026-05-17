"""Category taxonomy management (list, create, set, delete)."""

import typer

from ..stubs import _not_implemented

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


@app.command("set")
def categories_set(
    category_id: str = typer.Argument(..., help="Category ID to update"),
    is_active: bool = typer.Option(
        True, "--active/--inactive", help="Set category active or inactive"
    ),
) -> None:
    """Update a category's settings (is_active is the only modifiable field)."""
    _not_implemented("categorization-overview.md")


@app.command("delete")
def categories_delete(
    category_id: str = typer.Argument(..., help="Category ID to delete"),
) -> None:
    """Delete a category."""
    _not_implemented("categorization-overview.md")
