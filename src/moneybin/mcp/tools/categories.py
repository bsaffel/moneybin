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
def categories_toggle(
    category_id: str,
    is_active: bool,
) -> ResponseEnvelope:
    """Enable or disable a category. Existing categorizations are preserved."""
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
        "Writes app.user_categories; revert with categories_toggle (set is_active=False) — there is no hard-delete.",
    )
    register(
        mcp,
        categories_toggle,
        "categories_toggle",
        "Enable or disable a category in the taxonomy. "
        "Writes app.user_categories.is_active for user-created categories or app.category_overrides for seeded ones; revert by calling again with the opposite is_active value.",
    )
