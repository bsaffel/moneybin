"""Categories namespace tools — taxonomy reference data."""

from __future__ import annotations

from fastmcp import FastMCP

from moneybin.database import get_database
from moneybin.mcp._registration import register
from moneybin.mcp.decorator import mcp_tool
from moneybin.protocol.envelope import ResponseEnvelope, build_envelope
from moneybin.services.categorization import CategorizationService


@mcp_tool(sensitivity="low")
def categories_list(include_inactive: bool = False) -> ResponseEnvelope:
    """List all categories in the taxonomy."""
    with get_database(read_only=True) as db:
        data = CategorizationService(db).get_all_categories(
            include_inactive=include_inactive
        )
    return build_envelope(
        data=data,
        sensitivity="low",
        actions=[
            "Use categories_create to add a custom category",
            "Defaults are seeded automatically by `moneybin db init` and "
            "`moneybin transform apply` (or `moneybin transform seed` to re-run).",
        ],
    )


@mcp_tool(sensitivity="low", read_only=False, idempotent=False)
def categories_create(
    category: str,
    subcategory: str | None = None,
    description: str | None = None,
) -> ResponseEnvelope:
    """Create a custom category or subcategory (non-default, active by default)."""
    with get_database() as db:
        category_id = CategorizationService(db).create_category(
            category,
            subcategory=subcategory,
            description=description,
        )
    sub = f" / {subcategory}" if subcategory else ""
    return build_envelope(
        data={
            "category_id": category_id,
            "category": category,
            "subcategory": subcategory,
            "action": "created",
            "display": f"{category}{sub}",
        },
        sensitivity="low",
    )


@mcp_tool(sensitivity="low", read_only=False)
def categories_set(
    category_id: str,
    is_active: bool,
) -> ResponseEnvelope:
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
    with get_database() as db:
        CategorizationService(db).toggle_category(
            category_id,
            is_active=is_active,
        )
    action = "enabled" if is_active else "disabled"
    return build_envelope(
        data={"category_id": category_id, "action": action},
        sensitivity="low",
    )


def register_categories_tools(mcp: FastMCP) -> None:
    """Register all categories namespace tools with the FastMCP server."""
    register(
        mcp,
        categories_list,
        "categories_list",
        "List all categories in the taxonomy.",
    )
    register(
        mcp,
        categories_create,
        "categories_create",
        "Create a custom category or subcategory. "
        "Writes app.user_categories; revert with categories_set (set is_active=False) or hard-remove with categories_delete (Group 2).",
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
