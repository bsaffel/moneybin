"""Drop the retired app.merchant_overrides table and rewrite legacy 'seed' rows.

MoneyBin no longer ships a curated seed merchant catalog. The
``app.merchant_overrides`` table (migration-owned) is dropped here. All
merchants now live in ``app.user_merchants`` (created by user, LLM-assist,
auto-rule, plaid, or migration).

The paired ``seeds.merchants_global/us/ca`` tables were SQLMesh SEED models,
so on a fully-materialized database they are exposed as *views* over the
physical snapshot — and ``DROP TABLE IF EXISTS seeds.merchants_global`` raises
``Existing object … is of type View, trying to drop type Table`` (``IF EXISTS``
suppresses "doesn't exist," not the type mismatch). This migration therefore
does NOT drop them: SQLMesh owns their teardown and removes the views on the
next ``plan`` once the models are gone from the project (they already are). This
is the same SQLMesh-owned-schema rule V032 (PR #306) had to learn — migrations
never write ``seeds`` / ``core`` / ``prep`` / etc.

Also rewrites any historical ``app.transaction_categories.categorized_by =
'seed'`` rows to ``'rule'``. The ``'seed'`` value was removed from the
``_SOURCE_PRIORITY`` ladder in the same change; without rewriting, the
SQL CASE expression in ``write_categorization`` returns NULL for those
rows and every subsequent precedence check (including ``'user'`` writes)
silently fails because ``priority <= NULL`` evaluates to NULL. Mapping to
``'rule'`` matches how merchant-matched categorizations were recorded
anyway (the auto-fan-out path always wrote ``categorized_by='rule'``
regardless of whether the merchant was user- or seed-created).

Idempotent: ``DROP TABLE IF EXISTS`` is a no-op when the table is absent, and
the UPDATE no-ops when no ``'seed'`` rows exist.
"""

import logging

logger = logging.getLogger(__name__)


def migrate(conn: object) -> None:
    """Drop app.merchant_overrides and rewrite legacy categorized_by='seed' rows."""
    # info (not debug) preserves the log level V012 shipped with — see
    # database.md "keep whatever level they shipped with" for already-applied
    # migrations. The body changed (bug fix) but the level convention holds.
    logger.info("Dropping app.merchant_overrides if present")
    conn.execute("DROP TABLE IF EXISTS app.merchant_overrides")  # type: ignore[union-attr]

    # Rewrite any historical 'seed' rows so the new 7-level precedence ladder
    # doesn't lock these transactions. See module docstring for rationale.
    legacy_count_row = conn.execute(  # type: ignore[union-attr]
        "SELECT COUNT(*) FROM app.transaction_categories WHERE categorized_by = 'seed'"
    ).fetchone()
    legacy_count = legacy_count_row[0] if legacy_count_row else 0
    if legacy_count:
        conn.execute(  # type: ignore[union-attr]
            "UPDATE app.transaction_categories SET categorized_by = 'rule' "
            "WHERE categorized_by = 'seed'"
        )
        logger.info(
            f"Rewrote {legacy_count} legacy categorized_by='seed' rows to 'rule'"
        )

    logger.info("V012 migration complete")
