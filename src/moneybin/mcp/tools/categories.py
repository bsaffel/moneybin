"""Categories namespace tools — taxonomy reference data.

These granular callbacks are internal helpers retained for standard-boundary
composition and parity; they are never individually registered. The standard
surface routes their outcomes through ``taxonomy`` and ``taxonomy_set``.
``_LEGACY_INTERNAL_CALLBACKS`` and the surface-budget guard prevent accidental
publication.
"""

from __future__ import annotations

from moneybin.database import get_database
from moneybin.privacy.payloads.categories import (
    CategoriesPayload,
    CategoryCreatePayload,
    CategoryDeletePayload,
    CategorySetPayload,
)
from moneybin.protocol.envelope import ResponseEnvelope, build_envelope
from moneybin.services.categorization import CategorizationService


def categories(include_inactive: bool = False) -> ResponseEnvelope[CategoriesPayload]:
    """List all categories in the taxonomy."""
    with get_database(read_only=True) as db:
        payload = CategorizationService(db).get_all_categories(
            include_inactive=include_inactive
        )
    return build_envelope(
        data=payload,
        actions=[
            "Use taxonomy_set with kind='category' and state='present' to add one",
            "Defaults are seeded automatically by `moneybin db init` and "
            "`moneybin refresh` (or `moneybin transform seed` to re-run).",
        ],
    )


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


_LEGACY_INTERNAL_CALLBACKS = (
    categories,
    categories_create,
    categories_set,
    categories_delete,
)
