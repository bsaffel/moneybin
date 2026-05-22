"""Category taxonomy management (list, create, set, delete)."""

from __future__ import annotations

import logging

import typer

from moneybin.cli.output import OutputFormat, output_option, render_or_json
from moneybin.cli.utils import handle_cli_errors
from moneybin.database import get_database
from moneybin.protocol.envelope import build_envelope

from ..stubs import _not_implemented

logger = logging.getLogger(__name__)

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
    force: bool = typer.Option(
        False,
        "--force",
        help="Cascade-delete referencing transactions and budgets instead of refusing",
    ),
    output: OutputFormat = output_option,
) -> None:
    """Hard-delete a user-created category.

    Refuses if the category is referenced by transactions or budgets unless
    --force is passed. Default (seeded) categories cannot be deleted — disable
    them with `moneybin categories set <id> --inactive` instead.
    """
    from moneybin.services.categorization import (  # noqa: PLC0415
        CategorizationService,
    )

    with handle_cli_errors():
        with get_database() as db:
            CategorizationService(db).delete_category(
                category_id, force=force, actor="cli"
            )

    envelope = build_envelope(
        data={"category_id": category_id, "action": "deleted", "force": force},
        sensitivity="low",
    )
    if output == OutputFormat.JSON:
        render_or_json(envelope, output)
        return
    logger.info(f"✅ Deleted category {category_id}")
