"""Category taxonomy management (list, create, set, delete)."""

from __future__ import annotations

import logging

import typer

from moneybin.cli.output import (
    OutputFormat,
    output_option,
    quiet_option,
    render_or_json,
)
from moneybin.cli.utils import handle_cli_errors
from moneybin.database import get_database
from moneybin.privacy.payloads.categories import (
    CategoryCreatePayload,
    CategorySetPayload,
)
from moneybin.protocol.envelope import build_envelope

logger = logging.getLogger(__name__)

app = typer.Typer(
    help="Category taxonomy management",
    no_args_is_help=True,
)


@app.command("list")
def categories_list(
    include_inactive: bool = typer.Option(
        False,
        "--include-inactive",
        help="Include inactive categories.",
    ),
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,  # noqa: ARG001 — list emits result rows only
) -> None:
    """List all categories."""
    from moneybin.services.categorization import CategorizationService

    with handle_cli_errors():
        with get_database(read_only=True) as db:
            payload = CategorizationService(db).get_all_categories(
                include_inactive=include_inactive
            )

    envelope = build_envelope(data=payload, sensitivity="low")
    if output == OutputFormat.JSON:
        render_or_json(envelope, output, cli_actor="categories_list")
        return
    for row in payload.categories:
        suffix = f" / {row.subcategory}" if row.subcategory else ""
        state = "" if row.is_active else " (inactive)"
        typer.echo(f"{row.category_id}  {row.category}{suffix}{state}")


@app.command("create")
def categories_create(
    name: str = typer.Argument(..., help="Category name"),
    parent: str | None = typer.Option(None, "--parent", help="Parent category name"),
    output: OutputFormat = output_option,
) -> None:
    """Create a new category."""
    from moneybin.services.categorization import CategorizationService

    category = parent or name
    subcategory = name if parent else None
    with handle_cli_errors():
        with get_database(read_only=False) as db:
            category_id = CategorizationService(db).create_category(
                category,
                subcategory=subcategory,
                actor="cli",
            )
    sub = f" / {subcategory}" if subcategory else ""
    payload = CategoryCreatePayload(
        category_id=category_id,
        category=category,
        subcategory=subcategory,
        action="created",
        display=f"{category}{sub}",
    )
    if output == OutputFormat.JSON:
        render_or_json(
            build_envelope(data=payload),
            output,
            cli_actor="categories_create",
        )
        return
    typer.echo(category_id)


@app.command("set")
def categories_set(
    category_id: str = typer.Argument(..., help="Category ID to update"),
    is_active: bool = typer.Option(
        True, "--active/--inactive", help="Set category active or inactive"
    ),
    output: OutputFormat = output_option,
) -> None:
    """Update a category's settings (is_active is the only modifiable field)."""
    from moneybin.services.categorization import CategorizationService

    with handle_cli_errors():
        with get_database(read_only=False) as db:
            CategorizationService(db).toggle_category(
                category_id,
                is_active=is_active,
                actor="cli",
            )
    payload = CategorySetPayload(
        category_id=category_id,
        action="enabled" if is_active else "disabled",
    )
    if output == OutputFormat.JSON:
        render_or_json(
            build_envelope(data=payload),
            output,
            cli_actor="categories_set",
        )
        return
    typer.echo(category_id)


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
    from moneybin.privacy.payloads.categories import (  # noqa: PLC0415
        CategoryDeletePayload,
    )
    from moneybin.services.categorization import (  # noqa: PLC0415
        CategorizationService,
    )

    # Both imports resolve BEFORE the delete: an import failure after a
    # committed deletion would strand the user with no confirmation/envelope.
    with handle_cli_errors():
        with get_database(read_only=False) as db:
            CategorizationService(db).delete_category(
                category_id, force=force, actor="cli"
            )

    envelope = build_envelope(
        data=CategoryDeletePayload(
            category_id=category_id,
            action="deleted",
            force=force,
        ),
        sensitivity="low",
    )
    if output == OutputFormat.JSON:
        render_or_json(envelope, output, cli_actor="categories_delete")
        return
    logger.info(f"✅ Deleted category {category_id}")
