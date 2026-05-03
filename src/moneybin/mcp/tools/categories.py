"""Categories namespace tools — taxonomy reference data.

Tools:
    - categories_list — List all categories in the taxonomy (low sensitivity)
    - categories_create — Create a custom category or subcategory (low sensitivity)
    - categories_toggle — Enable or disable a category (low sensitivity)
"""

from __future__ import annotations

import uuid

import duckdb
from fastmcp import FastMCP

from moneybin.database import get_database
from moneybin.errors import UserError
from moneybin.mcp._registration import register
from moneybin.mcp.decorator import mcp_tool
from moneybin.protocol.envelope import ResponseEnvelope, build_envelope
from moneybin.services.categorization_service import CategorizationService
from moneybin.tables import CATEGORIES, CATEGORY_OVERRIDES, USER_CATEGORIES


@mcp_tool(sensitivity="low", domain="categorize")
def categories_list(
    include_inactive: bool = False,
) -> ResponseEnvelope:
    """List all categories in the taxonomy.

    Returns category ID, name, subcategory, description, and active
    status. By default only active categories are returned.

    Args:
        include_inactive: Include disabled categories (default False).
    """
    db = get_database()
    if include_inactive:
        try:
            rows = db.execute(
                f"""
                SELECT category_id, category, subcategory, description,
                       is_default, is_active, plaid_detailed
                FROM {CATEGORIES.full_name}
                ORDER BY category, subcategory
                """
            ).fetchall()
        except duckdb.CatalogException:
            rows = []

        data = [
            {
                "category_id": r[0],
                "category": r[1],
                "subcategory": r[2],
                "description": r[3],
                "is_default": r[4],
                "is_active": r[5],
                "plaid_detailed": r[6],
            }
            for r in rows
        ]
    else:
        data = CategorizationService(db).get_active_categories()

    return build_envelope(
        data=data,
        sensitivity="low",
        actions=[
            "Use categories_create to add a custom category",
            "Defaults are seeded automatically by `moneybin db init` and "
            "`moneybin transform apply` (or `moneybin transform seed` to re-run).",
        ],
    )


@mcp_tool(sensitivity="low", domain="categorize")
def categories_create(
    category: str,
    subcategory: str | None = None,
    description: str | None = None,
) -> ResponseEnvelope:
    """Create a custom category or subcategory.

    Categories created this way are marked as non-default and active.
    They can be toggled on/off with ``categories_toggle``.

    Args:
        category: Primary category name (e.g. 'Childcare').
        subcategory: Optional subcategory (e.g. 'Daycare').
        description: Optional description of this category.
    """
    cat_id = uuid.uuid4().hex[:12]
    db = get_database()

    try:
        db.execute(
            f"""
            INSERT INTO {USER_CATEGORIES.full_name}
            (category_id, category, subcategory, description,
             is_active, created_at)
            VALUES (?, ?, ?, ?, true, CURRENT_TIMESTAMP)
            """,
            [cat_id, category, subcategory, description],
        )
    except duckdb.ConstraintException:
        sub = f" / {subcategory}" if subcategory else ""
        raise UserError(
            f"Category already exists: {category}{sub}",
            code="CATEGORY_ALREADY_EXISTS",
        ) from None
    # Other DuckDB errors propagate to fastmcp's mask_error_details.

    sub = f" / {subcategory}" if subcategory else ""
    return build_envelope(
        data={
            "category_id": cat_id,
            "category": category,
            "subcategory": subcategory,
            "action": "created",
            "display": f"{category}{sub}",
        },
        sensitivity="low",
    )


@mcp_tool(sensitivity="low", domain="categorize")
def categories_toggle(
    category_id: str,
    is_active: bool,
) -> ResponseEnvelope:
    """Enable or disable a category.

    Disabled categories are hidden from the taxonomy but existing
    categorizations using them are preserved.

    Args:
        category_id: The category ID to toggle (e.g. 'FND-COF').
        is_active: True to enable, False to disable.
    """
    db = get_database()
    cat = db.execute(
        f"SELECT is_default FROM {CATEGORIES.full_name} WHERE category_id = ?",
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
            """,
            [category_id, is_active],
        )
    else:
        db.execute(
            f"UPDATE {USER_CATEGORIES.full_name} SET is_active = ? WHERE category_id = ?",
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
