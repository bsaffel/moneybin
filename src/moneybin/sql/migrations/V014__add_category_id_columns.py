"""V014: add category_id FK columns across seven tables and backfill.

Phase 1 of the category-text -> category_id FK migration. Adds a nullable
``category_id`` column to six tables (and ``new_category_id`` to
``rule_deactivations`` when that table still exists — it was dropped in
V018) and backfills from existing ``(category, subcategory)`` text via
JOIN against the unified ``core.dim_categories`` view. Unresolvable rows
(orphaned text — the referenced category was deleted or renamed before
V014 shipped) are left with NULL FKs and keep their text columns as
display fallback.

Tables affected:
- app.transaction_categories  (per-transaction category assignment)
- app.budgets                  (monthly budget targets)
- app.user_merchants           (merchant default category)
- app.transaction_splits       (per-line category on splits)
- app.categorization_rules     (rule target category)
- app.proposed_rules           (proposed rule target category, pre-approval)
- app.rule_deactivations       (only on existing installs where V018 hasn't run yet)

Idempotent: ``ADD COLUMN IF NOT EXISTS`` is a no-op on replay; the
backfill ``UPDATE`` only touches rows where the FK is still NULL; the
rule_deactivations entry is skipped on fresh installs where the table
was never created (schema.py omits it after V018 was introduced).

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
)

# rule_deactivations was dropped in V018. Guard against running V014 on a
# fresh install where the table was never created (schema.py omits it after
# V018 was introduced). On existing installs the table exists until V018 runs.
_RULE_DEACTIVATIONS_BACKFILL = (
    "new_category_id",
    "app.rule_deactivations",
    "new_category",
    "t.new_subcategory IS NOT DISTINCT FROM dc.subcategory",
)


def migrate(conn: object) -> None:
    """Add FK columns to six (or seven) tables and backfill from text via dim_categories."""
    # Database.__init__ calls refresh_views() AFTER migrations, so on the
    # auto-upgrade path core.dim_categories does not yet exist when V014 runs.
    # Inline a minimal version of the view definition here (mirrors
    # moneybin.seeds.refresh_views and src/moneybin/sqlmesh/models/core/dim_categories.sql)
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

    # Check whether rule_deactivations still exists (it was dropped in V018;
    # on fresh installs schema.py never creates it).
    rule_deact_exists = bool(
        conn.execute(  # type: ignore[union-attr]
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_schema = 'app' AND table_name = 'rule_deactivations'"
        ).fetchone()
    )
    active_backfills = list(_BACKFILLS)
    if rule_deact_exists:
        active_backfills.append(_RULE_DEACTIVATIONS_BACKFILL)

    for fk_col, table, _text_col, _subcategory_pred in active_backfills:
        logger.info(f"V014: ADD COLUMN IF NOT EXISTS {table}.{fk_col}")
        conn.execute(  # type: ignore[union-attr]  # noqa: S608  # constants from _BACKFILLS, no user input
            f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {fk_col} VARCHAR"
        )

    for fk_col, table, text_col, subcategory_pred in active_backfills:
        backfill_sql = f"""
            UPDATE {table} AS t
            SET {fk_col} = dc.category_id
            FROM core.dim_categories AS dc
            WHERE t.{fk_col} IS NULL
              AND t.{text_col} = dc.category
              AND {subcategory_pred}
        """  # noqa: S608  # constants from _BACKFILLS, no user input
        conn.execute(backfill_sql)  # type: ignore[union-attr]

        # Single COUNT scan derives both backfilled and orphan totals without
        # materializing one row per updated record (a real concern on
        # multi-million-row tables like transaction_categories).
        counts_row = conn.execute(  # type: ignore[union-attr]
            f"SELECT COUNT(*), COUNT(*) FILTER (WHERE {fk_col} IS NULL) "  # noqa: S608  # constants from _BACKFILLS
            f"FROM {table}"
        ).fetchone()
        total, orphan_count = counts_row if counts_row else (0, 0)
        logger.info(f"V014: {total - orphan_count} {table} rows resolved {fk_col}")
        if orphan_count:
            logger.warning(
                f"V014: {orphan_count} {table} rows have NULL {fk_col} "
                f"after backfill (orphaned text)"
            )

    logger.info("V014 migration complete")
