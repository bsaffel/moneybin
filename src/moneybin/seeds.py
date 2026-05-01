"""SQLMesh seed materialization + the views that expose seeds alongside user data.

Seeds are managed by SQLMesh (``seeds.*`` schema, populated from CSV). The
application reads from ``app.*`` views that union seed rows with user
additions and apply user overrides (e.g. deactivations). This keeps seed
edits flowing through immediately while preserving user state.

To add a new seed: add a SQLMesh seed model, add its full name to
``_SEED_MODELS``, and create the corresponding view in ``_create_views``.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from moneybin.tables import (
    CATEGORIES,
    CATEGORY_OVERRIDES,
    SEED_CATEGORIES,
    USER_CATEGORIES,
)

if TYPE_CHECKING:
    from moneybin.database import Database

logger = logging.getLogger(__name__)


_SEED_MODELS: list[str] = [
    SEED_CATEGORIES.full_name,
]


def materialize_seeds(db: Database) -> None:
    """Materialize all SQLMesh seed models, then (re)create the app views.

    Idempotent. Safe to call from ``db init``, ``transform seed``, and
    ``transform apply``.
    """
    from moneybin.database import sqlmesh_context

    if _SEED_MODELS:
        logger.info("Materializing SQLMesh seed models")
        with sqlmesh_context() as ctx:
            ctx.plan(auto_apply=True, no_prompts=True, select_models=_SEED_MODELS)

    refresh_views(db)


def refresh_views(db: Database) -> None:
    """Create or replace the app views that expose seeds + user data."""
    sql = f"""
        CREATE OR REPLACE VIEW {CATEGORIES.full_name} AS
        SELECT
            s.category_id,
            s.category,
            s.subcategory,
            s.description,
            s.plaid_detailed,
            true AS is_default,
            COALESCE(o.is_active, true) AS is_active,
            NULL::TIMESTAMP AS created_at
        FROM {SEED_CATEGORIES.full_name} s
        LEFT JOIN {CATEGORY_OVERRIDES.full_name} o USING (category_id)
        UNION ALL
        SELECT
            category_id,
            category,
            subcategory,
            description,
            NULL AS plaid_detailed,
            false AS is_default,
            is_active,
            created_at
        FROM {USER_CATEGORIES.full_name}
        """  # noqa: S608  # all interpolated names are TableRef constants, not user input
    db.execute(sql)
