"""SQLMesh seed materialization + the views that expose seeded data.

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
    MERCHANTS,
    SEED_CATEGORIES,
    USER_CATEGORIES,
    USER_MERCHANTS,
)

if TYPE_CHECKING:
    from moneybin.database import Database

logger = logging.getLogger(__name__)


_SEED_MODELS: list[str] = [SEED_CATEGORIES.full_name]


def materialize_seeds(db: Database) -> None:
    """Materialize all SQLMesh seed models, then (re)create the dim views.

    Idempotent. Safe to call from ``db init``, ``transform seed``, and
    ``transform apply``.
    """
    from moneybin.database import sqlmesh_context

    if _SEED_MODELS:
        logger.info("Materializing SQLMesh seed models")
        with sqlmesh_context(db) as ctx:
            ctx.plan(auto_apply=True, no_prompts=True, select_models=_SEED_MODELS)

    refresh_views(db)


def _ensure_seed_tables_exist(db: Database) -> None:
    """Create the categories seed table if it doesn't exist yet.

    In production, SQLMesh has already created and populated this table —
    the CREATE TABLE IF NOT EXISTS call is a no-op. In fresh test DBs (where
    SQLMesh hasn't run), the empty table lets ``refresh_views`` assemble the
    ``core.dim_categories`` view without hitting a CatalogException on the
    missing source.
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
        """  # noqa: S608  # SEED_CATEGORIES is a TableRef constant, not user input
    )


def refresh_views(db: Database) -> None:
    """Create or replace the resolved dim views and drop retired legacy views.

    Idempotent and safe to call before migrations run.

    ``core.dim_categories`` has no V006 dependency and is always built.

    ``core.dim_merchants`` requires ``app.user_merchants`` (created by V006).
    If V006 has not yet run — ``app.merchants`` is still a TABLE because the
    operator opened the database with ``no_auto_upgrade=True`` — build a
    backward-compat passthrough that wraps the legacy table so categorization
    reads still resolve. The pure user view lands once migrations complete.
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
                CAST(NULL AS TIMESTAMP) AS updated_at
            FROM app.merchants
            """  # noqa: S608  # MERCHANTS is a TableRef constant; app.merchants is the legacy TABLE
        )
        return

    db.execute(
        f"""
        CREATE OR REPLACE VIEW {MERCHANTS.full_name} AS
        SELECT
            merchant_id, raw_pattern, match_type, canonical_name,
            category, subcategory, created_by,
            exemplars,
            created_at,
            updated_at
        FROM {USER_MERCHANTS.full_name}
        """  # noqa: S608  # all interpolated names are TableRef constants, not user input
    )
