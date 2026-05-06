"""SQLMesh seed materialization + the views that expose seeds alongside user data.

Seeds are managed by SQLMesh (``seeds.*`` schema, populated from CSV). The
application reads from ``app.*`` views that union seed rows with user
additions and apply user overrides (e.g. deactivations). This keeps seed
edits flowing through immediately while preserving user state.

Both categories and merchants follow this pattern: seed tables are populated
by SQLMesh, then ``refresh_views`` assembles the ``app.*`` view that merges
seeds with user rows and applies overrides.

To add a new seed: add a SQLMesh seed model, add its full name to
``_SEED_MODELS``, and extend ``refresh_views`` with the corresponding view.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from moneybin.tables import (
    CATEGORIES,
    CATEGORY_OVERRIDES,
    MERCHANT_OVERRIDES,
    MERCHANTS,
    SEED_CATEGORIES,
    SEED_MERCHANTS_CA,
    SEED_MERCHANTS_GLOBAL,
    SEED_MERCHANTS_US,
    USER_CATEGORIES,
    USER_MERCHANTS,
)

if TYPE_CHECKING:
    from moneybin.database import Database

logger = logging.getLogger(__name__)


_SEED_MODELS: list[str] = [
    SEED_CATEGORIES.full_name,
    SEED_MERCHANTS_GLOBAL.full_name,
    SEED_MERCHANTS_US.full_name,
    SEED_MERCHANTS_CA.full_name,
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


def _ensure_seed_tables_exist(db: Database) -> None:
    """Create seed tables if they don't exist yet.

    In production, SQLMesh has already created and populated these tables —
    the CREATE TABLE IF NOT EXISTS calls are no-ops. In fresh test DBs (where
    SQLMesh hasn't run), the empty tables let refresh_views assemble the
    app.* views without hitting a CatalogException on missing tables.
    """
    db.execute("CREATE SCHEMA IF NOT EXISTS seeds")
    db.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {SEED_CATEGORIES.full_name} (
            category_id VARCHAR PRIMARY KEY,
            category VARCHAR,
            subcategory VARCHAR,
            description VARCHAR,
            plaid_detailed VARCHAR
        )
        """  # noqa: S608  # all interpolated names are TableRef constants, not user input
    )
    merchant_ddl = """(
            merchant_id VARCHAR PRIMARY KEY,
            raw_pattern VARCHAR,
            match_type VARCHAR,
            canonical_name VARCHAR,
            category VARCHAR,
            subcategory VARCHAR,
            country VARCHAR
        )"""
    db.execute(
        f"CREATE TABLE IF NOT EXISTS {SEED_MERCHANTS_GLOBAL.full_name} {merchant_ddl}"  # noqa: S608
    )
    db.execute(
        f"CREATE TABLE IF NOT EXISTS {SEED_MERCHANTS_US.full_name} {merchant_ddl}"  # noqa: S608
    )
    db.execute(
        f"CREATE TABLE IF NOT EXISTS {SEED_MERCHANTS_CA.full_name} {merchant_ddl}"  # noqa: S608
    )


def refresh_views(db: Database) -> None:
    """Create or replace the app views that expose seeds + user data."""
    _ensure_seed_tables_exist(db)
    # Query examples for the LLM: see src/moneybin/services/schema_catalog.py (EXAMPLES dict)
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

    merchants_sql = f"""
        CREATE OR REPLACE VIEW {MERCHANTS.full_name} AS
        -- User merchants first (user wins on overlap)
        SELECT
            merchant_id, raw_pattern, match_type, canonical_name,
            category, subcategory, created_by,
            created_at,
            true AS is_user
        FROM {USER_MERCHANTS.full_name}
        UNION ALL
        -- Global seeds
        SELECT
            s.merchant_id, s.raw_pattern, s.match_type, s.canonical_name,
            COALESCE(o.category, s.category) AS category,
            COALESCE(o.subcategory, s.subcategory) AS subcategory,
            'seed' AS created_by,
            NULL::TIMESTAMP AS created_at,
            false AS is_user
        FROM {SEED_MERCHANTS_GLOBAL.full_name} s
        LEFT JOIN {MERCHANT_OVERRIDES.full_name} o USING (merchant_id)
        WHERE COALESCE(o.is_active, true)
        UNION ALL
        -- US seeds
        SELECT
            s.merchant_id, s.raw_pattern, s.match_type, s.canonical_name,
            COALESCE(o.category, s.category) AS category,
            COALESCE(o.subcategory, s.subcategory) AS subcategory,
            'seed' AS created_by,
            NULL::TIMESTAMP AS created_at,
            false AS is_user
        FROM {SEED_MERCHANTS_US.full_name} s
        LEFT JOIN {MERCHANT_OVERRIDES.full_name} o USING (merchant_id)
        WHERE COALESCE(o.is_active, true)
        UNION ALL
        -- CA seeds
        SELECT
            s.merchant_id, s.raw_pattern, s.match_type, s.canonical_name,
            COALESCE(o.category, s.category) AS category,
            COALESCE(o.subcategory, s.subcategory) AS subcategory,
            'seed' AS created_by,
            NULL::TIMESTAMP AS created_at,
            false AS is_user
        FROM {SEED_MERCHANTS_CA.full_name} s
        LEFT JOIN {MERCHANT_OVERRIDES.full_name} o USING (merchant_id)
        WHERE COALESCE(o.is_active, true)
        """  # noqa: S608  # all interpolated names are TableRef constants, not user input
    db.execute(merchants_sql)
