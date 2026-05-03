"""Categories namespace tools — taxonomy reference data."""

from __future__ import annotations

from fastmcp import FastMCP

from moneybin.database import get_database
from moneybin.errors import UserError
from moneybin.mcp._registration import register
from moneybin.mcp.decorator import mcp_tool
from moneybin.protocol.envelope import ResponseEnvelope, build_envelope
from moneybin.services.categorization_service import CategorizationService
from moneybin.tables import CATEGORIES, CATEGORY_OVERRIDES, USER_CATEGORIES


@mcp_tool(sensitivity="low")
def categories_list(include_inactive: bool = False) -> ResponseEnvelope:
    """List all categories in the taxonomy."""
    data = CategorizationService(get_database()).get_all_categories(
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


@mcp_tool(sensitivity="low")
def categories_create(
    category: str,
    subcategory: str | None = None,
    description: str | None = None,
) -> ResponseEnvelope:
    """Create a custom category or subcategory (non-default, active by default)."""
    category_id = CategorizationService(get_database()).create_category(
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


@mcp_tool(sensitivity="low")
def categories_toggle(
    category_id: str,
    is_active: bool,
) -> ResponseEnvelope:
    """Enable or disable a category. Existing categorizations are preserved."""
    db = get_database()
    cat = db.execute(
        f"SELECT is_default FROM {CATEGORIES.full_name} WHERE category_id = ?",  # noqa: S608  # TableRef constant
        [category_id],
    ).fetchone()
    if not cat:
        raise UserError(f"Category {category_id} not found", code="CATEGORY_NOT_FOUND")

    if cat[0]:  # default category — record/upsert the override
        db.execute(
            f"""
            INSERT INTO {CATEGORY_OVERRIDES.full_name} (category_id, is_active, updated_at)
            VALUES (?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT (category_id) DO UPDATE
                SET is_active = excluded.is_active,
                    updated_at = excluded.updated_at
            """,  # noqa: S608  # TableRef constant, no user input
            [category_id, is_active],
        )
    else:
        db.execute(
            f"UPDATE {USER_CATEGORIES.full_name} SET is_active = ? WHERE category_id = ?",  # noqa: S608  # TableRef constant
            [is_active, category_id],
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
        "Create a custom category or subcategory.",
    )
    register(
        mcp,
        categories_toggle,
        "categories_toggle",
        "Enable or disable a category in the taxonomy.",
    )
