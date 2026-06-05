"""Categories namespace tools — taxonomy reference data."""

from __future__ import annotations

from fastmcp import FastMCP

from moneybin.database import get_database
from moneybin.mcp._registration import register
from moneybin.mcp.decorator import mcp_tool
from moneybin.privacy.payloads.categories import (
    CategoriesPayload,
    CategoryCreatePayload,
    CategoryDeletePayload,
    CategorySetPayload,
)
from moneybin.protocol.envelope import ResponseEnvelope, build_envelope
from moneybin.services.categorization import CategorizationService


@mcp_tool()
def categories(include_inactive: bool = False) -> ResponseEnvelope[CategoriesPayload]:
    """List all categories in the taxonomy."""
    with get_database(read_only=True) as db:
        payload = CategorizationService(db).get_all_categories(
            include_inactive=include_inactive
        )
    return build_envelope(
        data=payload,
        actions=[
            "Use categories_create to add a custom category",
            "Defaults are seeded automatically by `moneybin db init` and "
            "`moneybin refresh` (or `moneybin transform seed` to re-run).",
        ],
    )


@mcp_tool(read_only=False, idempotent=False)
def categories_create(
    category: str, subcategory: str | None = None, description: str | None = None
) -> ResponseEnvelope[CategoryCreatePayload]:
    """Create a custom category or subcategory (non-default, active by default)."""
    with get_database(read_only=False) as db:
        category_id = CategorizationService(db).create_category(
            category,
            subcategory=subcategory,
            description=description,
            actor="mcp",
        )
    sub = f" / {subcategory}" if subcategory else ""
    return build_envelope(
        data=CategoryCreatePayload(
            category_id=category_id,
            category=category,
            subcategory=subcategory,
            action="created",
            display=f"{category}{sub}",
        )
    )


@mcp_tool(read_only=False)
def categories_set(
    category_id: str, is_active: bool
) -> ResponseEnvelope[CategorySetPayload]:
    """Update a category's settings (currently only is_active).

    Idempotent partial update — matches the shape-1b _set convention used
    across MoneyBin (budget_set, accounts_set). Existing categorizations
    are preserved when a category is disabled.

    For lifecycle operations: use categories_create to add, categories_delete
    to remove.

    Args:
        category_id: ID of the category to update.
        is_active: Whether the category is selectable for new
            categorizations.
    """
    with get_database(read_only=False) as db:
        CategorizationService(db).toggle_category(
            category_id,
            is_active=is_active,
            actor="mcp",
        )
    action = "enabled" if is_active else "disabled"
    return build_envelope(
        data=CategorySetPayload(category_id=category_id, action=action)
    )


@mcp_tool(read_only=False, destructive=True)
def categories_delete(
    category_id: str, force: bool = False
) -> ResponseEnvelope[CategoryDeletePayload]:
    """Hard-delete a user-created category.

    Args:
        category_id: ID of the user-created category to delete.
        force: If True, cascade-delete referencing transaction and
            budget rows; if False (default), refuse when references
            exist.
    """
    with get_database(read_only=False) as db:
        CategorizationService(db).delete_category(category_id, force=force, actor="mcp")
    return build_envelope(
        data=CategoryDeletePayload(
            category_id=category_id, action="deleted", force=force
        )
    )


def register_categories_tools(mcp: FastMCP) -> None:
    """Register all categories namespace tools with the FastMCP server."""
    register(mcp, categories, "categories", "List all categories in the taxonomy.")
    register(
        mcp,
        categories_create,
        "categories_create",
        "Create a custom category or subcategory. "
        "Writes app.user_categories; revert with categories_set (set is_active=False) or hard-remove with categories_delete.",
    )
    register(
        mcp,
        categories_set,
        "categories_set",
        "Update a category's settings (is_active is currently the only "
        "modifiable field). Shape-1b partial-update tool matching the "
        "budget_set / accounts_set convention. "
        "Writes app.user_categories.is_active for user-created categories or "
        "app.category_overrides for seeded ones; revert by calling again "
        "with the opposite is_active value. "
        "For category lifecycle: categories_create to add, categories_delete to remove.",
    )
    register(
        mcp,
        categories_delete,
        "categories_delete",
        "Hard-delete a user-created category. Refuses by default if "
        "referenced by transactions or budgets; pass force=True to "
        "cascade-delete those rows (affected transactions become "
        "uncategorized). Default (seeded) categories cannot be deleted — "
        "use categories_set with is_active=False to disable them. "
        "Mutation surface: deletes app.user_categories row; with force=True "
        "also deletes referencing app.transaction_categories rows and "
        "matching app.budgets rows. No revert path — recreate with "
        "categories_create.",
    )
