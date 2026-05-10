"""SQLMesh seed materialization + the views that expose seeds alongside user data.

Seeds are managed by SQLMesh (``seeds.*`` schema, populated from CSV). The
canonical resolved dimensions — categories and merchants — are exposed as
``core.dim_categories`` and ``core.dim_merchants``. These views are also
declared as SQLMesh models in ``sqlmesh/models/core/dim_*.sql`` (the
canonical spec for column shapes). ``refresh_views`` builds equivalent
views directly via DuckDB so they are available in fresh test databases
and on every ``Database`` open without requiring a full SQLMesh ``transform
apply`` first; ``transform apply`` will subsequently ``CREATE OR REPLACE``
the same views with identical bodies.

Also drops the legacy ``app.categories`` / ``app.merchants`` views from
existing databases (retired in favor of the ``core.dim_*`` models).
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
    """Materialize all SQLMesh seed models, then (re)create the dim views.

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
    SQLMesh hasn't run), the empty tables let ``refresh_views`` assemble the
    ``core.dim_*`` views without hitting a CatalogException on missing
    source tables.
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
    """Create or replace the resolved dim views and drop retired legacy views.

    Idempotent and safe to call before migrations run.

    ``core.dim_categories`` has no V006 dependency and is always built.

    ``core.dim_merchants`` requires ``app.user_merchants`` (created by V006).
    If V006 has not yet run — ``app.merchants`` is still a TABLE because the
    operator opened the database with ``no_auto_upgrade=True`` — build a
    backward-compat passthrough that wraps the legacy table so categorization
    reads still resolve. The full union (user + seeds + overrides) lands once
    migrations complete.
    """
    _ensure_seed_tables_exist(db)
    legacy = db.execute(
        "SELECT table_type FROM information_schema.tables "
        "WHERE table_schema = 'app' AND table_name = 'merchants'"
    ).fetchone()
    is_pre_v006_table = legacy is not None and legacy[0] == "BASE TABLE"

    # Drop the retired app.categories view (no V006 dependency).
    db.execute("DROP VIEW IF EXISTS app.categories")
    # app.merchants is dropped only post-V006 — pre-V006 it's the user-data
    # TABLE we are about to wrap.
    if not is_pre_v006_table:
        db.execute("DROP VIEW IF EXISTS app.merchants")

    # Build resolved dim views. Mirrors sqlmesh/models/core/dim_*.sql so tests
    # and freshly-opened databases see the dims without requiring a full
    # SQLMesh `transform apply`. SQLMesh subsequently CREATE OR REPLACEs these
    # with identical bodies on every transform run.
    db.execute(
        f"""
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
        UNION
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
    )

    if is_pre_v006_table:
        logger.warning(
            "app.merchants exists as a TABLE (pre-V006 schema); building "
            "core.dim_merchants as a passthrough over the legacy table. "
            "Run migrations to complete the upgrade."
        )
        db.execute(
            f"""
            CREATE OR REPLACE VIEW {MERCHANTS.full_name} AS
            SELECT
                merchant_id, raw_pattern, match_type, canonical_name,
                category, subcategory, created_by, created_at,
                CAST([] AS VARCHAR[]) AS exemplars,
                true AS is_user
            FROM app.merchants
            """  # noqa: S608  # MERCHANTS is a TableRef constant; app.merchants is the legacy TABLE
        )
        return

    # Seed rows have no exemplars — exemplar accumulation is a system-created
    # merchant feature (categorization-matching-mechanics.md §Schema changes).
    # CAST([] AS VARCHAR[]) gives the empty-list literal the correct typed
    # element so the UNION ALL columns align across branches.
    db.execute(
        f"""
        CREATE OR REPLACE VIEW {MERCHANTS.full_name} AS
        -- User merchants first (user wins on overlap)
        SELECT
            merchant_id, raw_pattern, match_type, canonical_name,
            category, subcategory, created_by,
            exemplars,
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
            CAST([] AS VARCHAR[]) AS exemplars,
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
            CAST([] AS VARCHAR[]) AS exemplars,
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
            CAST([] AS VARCHAR[]) AS exemplars,
            NULL::TIMESTAMP AS created_at,
            false AS is_user
        FROM {SEED_MERCHANTS_CA.full_name} s
        LEFT JOIN {MERCHANT_OVERRIDES.full_name} o USING (merchant_id)
        WHERE COALESCE(o.is_active, true)
        """  # noqa: S608  # all interpolated names are TableRef constants, not user input
    )
