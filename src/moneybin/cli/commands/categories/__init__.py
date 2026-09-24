"""Category taxonomy management (list, create, set, delete)."""

from __future__ import annotations

import logging

import typer

from moneybin.cli.output import (
    OutputFormat,
    emit_human_result,
    no_pager_option,
    output_option,
    quiet_option,
    render_or_json,
)
from moneybin.cli.render import build_rows, build_summary, compose_human_result
from moneybin.cli.utils import get_terminal_policy, handle_cli_errors
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
    quiet: bool = quiet_option,  # list emits result rows only
    no_pager: bool = no_pager_option,
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
    policy = get_terminal_policy(no_pager=no_pager)
    if not payload.categories:
        emit_human_result(
            build_summary(
                [("Categories", "No categories match this scope.")],
                title=(
                    "Try: moneybin categories create --help"
                    if include_inactive
                    else "Try: moneybin categories list --include-inactive"
                ),
            ),
            policy=policy,
            finite_read=True,
            no_pager=no_pager,
        )
        return
    emit_human_result(
        build_rows(
            ["category_id", "category", "subcategory", "status"],
            [
                (
                    row.category_id,
                    row.category,
                    row.subcategory or "-",
                    "active" if row.is_active else "inactive",
                )
                for row in payload.categories
            ],
            terminal=policy,
        ),
        policy=policy,
        finite_read=True,
        no_pager=no_pager,
    )


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
    emit_human_result(
        compose_human_result([
            build_summary(
                [("Category", payload.display), ("Category ID", category_id)],
                title="Category created",
            )
        ]),
        policy=get_terminal_policy(),
        finite_read=False,
        receipt=True,
    )


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
    emit_human_result(
        compose_human_result([
            build_summary(
                [("Category ID", category_id), ("Result", payload.action)],
                title="Category updated",
            )
        ]),
        policy=get_terminal_policy(),
        finite_read=False,
        receipt=True,
    )


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
    from moneybin.privacy.payloads.categories import (
        CategoryDeletePayload,
    )
    from moneybin.services.categorization import (
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
    emit_human_result(
        compose_human_result([
            build_summary(
                [("Category ID", category_id), ("Result", "deleted")],
                title="Category deleted",
            )
        ]),
        policy=get_terminal_policy(),
        finite_read=False,
        receipt=True,
    )
