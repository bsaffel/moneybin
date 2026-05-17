"""V014: add category_id FK columns across seven tables and backfill.

Phase 1 of the category-text -> category_id FK migration. Adds a nullable
``category_id`` column (``new_category_id`` for ``rule_deactivations`` to
preserve its ``new_`` prefix convention) to seven tables and backfills
from existing ``(category, subcategory)`` text via JOIN against the
unified ``core.dim_categories`` view. Unresolvable rows (orphaned text —
the referenced category was deleted or renamed before V014 shipped) are
left with NULL FKs and keep their text columns as display fallback.

Tables affected:
- app.transaction_categories  (per-transaction category assignment)
- app.budgets                  (monthly budget targets)
- app.user_merchants           (merchant default category)
- app.transaction_splits       (per-line category on splits)
- app.categorization_rules     (rule target category)
- app.proposed_rules           (proposed rule target category, pre-approval)
- app.rule_deactivations       (audit trail: converged category at deactivation)

Idempotent: ``ADD COLUMN IF NOT EXISTS`` is a no-op on replay; the
backfill ``UPDATE`` only touches rows where the FK is still NULL.

Each backfill uses IS NOT DISTINCT FROM on subcategory so NULL matches
NULL symmetrically. Budgets has no subcategory column at all — its
backfill matches dim rows where ``dc.subcategory IS NULL`` (the
top-level dim row).

``Database.__init__`` calls ``refresh_views()`` *after* migrations
(``init_schemas → migrations → refresh_views``), so on the auto-upgrade
path ``core.dim_categories`` does not yet exist when V014 runs. The
migration inlines a minimal copy of the view definition (kept in sync
with ``moneybin.seeds.refresh_views`` and the SQLMesh ``dim_categories``
model) before issuing any backfill. The post-migration
``refresh_views()`` call then ``CREATE OR REPLACE``s the view with the
canonical body — the inline body is a one-shot for this migration only.
"""

import logging
from typing import cast

logger = logging.getLogger(__name__)

# (fk_col, table, text_col, subcategory_predicate)
# subcategory_predicate becomes part of the UPDATE...FROM ON clause and
# must reference `dc.subcategory` (the dim alias). For tables that have
# their own subcategory column, use IS NOT DISTINCT FROM for NULL
# symmetry; for budgets (no subcategory), match only dim rows where
# subcategory IS NULL.
_BACKFILLS: tuple[tuple[str, str, str, str], ...] = (
    (
        "category_id",
        "app.transaction_categories",
        "category",
        "t.subcategory IS NOT DISTINCT FROM dc.subcategory",
    ),
    (
        "category_id",
        "app.budgets",
        "category",
        "dc.subcategory IS NULL",
    ),
    (
        "category_id",
        "app.user_merchants",
        "category",
        "t.subcategory IS NOT DISTINCT FROM dc.subcategory",
    ),
    (
        "category_id",
        "app.transaction_splits",
        "category",
        "t.subcategory IS NOT DISTINCT FROM dc.subcategory",
    ),
    (
        "category_id",
        "app.categorization_rules",
        "category",
        "t.subcategory IS NOT DISTINCT FROM dc.subcategory",
    ),
    (
        "category_id",
        "app.proposed_rules",
        "category",
        "t.subcategory IS NOT DISTINCT FROM dc.subcategory",
    ),
    (
        "new_category_id",
        "app.rule_deactivations",
        "new_category",
        "t.new_subcategory IS NOT DISTINCT FROM dc.subcategory",
    ),
)


def migrate(conn: object) -> None:
    """Add FK columns to seven tables and backfill from text via dim_categories."""
    # Database.__init__ calls refresh_views() AFTER migrations, so on the
    # auto-upgrade path core.dim_categories does not yet exist when V014 runs.
    # Inline a minimal version of the view definition here (mirrors
    # moneybin.seeds.refresh_views and sqlmesh/models/core/dim_categories.sql)
    # so the backfill JOINs resolve. The post-migration refresh_views() call
    # then CREATE OR REPLACEs this with the canonical body.
    conn.execute("CREATE SCHEMA IF NOT EXISTS seeds")  # type: ignore[union-attr]
    conn.execute(  # type: ignore[union-attr]
        """
        CREATE TABLE IF NOT EXISTS seeds.categories (
            category_id VARCHAR PRIMARY KEY,
            category VARCHAR,
            subcategory VARCHAR,
            description VARCHAR,
            plaid_detailed VARCHAR
        )
        """
    )
    conn.execute(  # type: ignore[union-attr]
        """
        CREATE OR REPLACE VIEW core.dim_categories AS
        SELECT
            s.category_id, s.category, s.subcategory, s.description,
            s.plaid_detailed,
            true AS is_default,
            COALESCE(o.is_active, true) AS is_active,
            NULL::TIMESTAMP AS created_at
        FROM seeds.categories s
        LEFT JOIN app.category_overrides o USING (category_id)
        UNION
        SELECT
            category_id, category, subcategory, description,
            NULL AS plaid_detailed,
            false AS is_default,
            is_active, created_at
        FROM app.user_categories
        """
    )

    for fk_col, table, _text_col, _subcategory_pred in _BACKFILLS:
        logger.info(f"V014: ADD COLUMN IF NOT EXISTS {table}.{fk_col}")
        conn.execute(  # type: ignore[union-attr]  # noqa: S608  # constants from _BACKFILLS, no user input
            f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {fk_col} VARCHAR"
        )

    for fk_col, table, text_col, subcategory_pred in _BACKFILLS:
        backfill_sql = f"""
            UPDATE {table} AS t
            SET {fk_col} = dc.category_id
            FROM core.dim_categories AS dc
            WHERE t.{fk_col} IS NULL
              AND t.{text_col} = dc.category
              AND {subcategory_pred}
            RETURNING 1
        """  # noqa: S608  # constants from _BACKFILLS, no user input
        rows = cast(
            list[tuple[int]],
            conn.execute(backfill_sql).fetchall(),  # type: ignore[union-attr]
        )
        logger.info(f"V014: backfilled {fk_col} for {len(rows)} {table} rows")

        orphan_row = conn.execute(  # type: ignore[union-attr]
            f"SELECT COUNT(*) FROM {table} WHERE {fk_col} IS NULL"  # noqa: S608  # constants from _BACKFILLS, no user input
        ).fetchone()
        orphan_count = orphan_row[0] if orphan_row else 0
        if orphan_count:
            logger.warning(
                f"V014: {orphan_count} {table} rows have NULL {fk_col} "
                f"after backfill (orphaned text)"
            )

    logger.info("V014 migration complete")
