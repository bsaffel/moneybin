"""Drop the retired app.merchant_overrides table and seed merchant schema.

MoneyBin no longer ships a curated seed merchant catalog. The
``seeds.merchants_global/us/ca`` tables and the ``app.merchant_overrides``
table that paired with them are removed. All merchants now live in
``app.user_merchants`` (created by user, LLM-assist, auto-rule, plaid, or
migration).

Also rewrites any historical ``app.transaction_categories.categorized_by =
'seed'`` rows to ``'rule'``. The ``'seed'`` value was removed from the
``_SOURCE_PRIORITY`` ladder in the same change; without rewriting, the
SQL CASE expression in ``write_categorization`` returns NULL for those
rows and every subsequent precedence check (including ``'user'`` writes)
silently fails because ``priority <= NULL`` evaluates to NULL. Mapping to
``'rule'`` matches how merchant-matched categorizations were recorded
anyway (the auto-fan-out path always wrote ``categorized_by='rule'``
regardless of whether the merchant was user- or seed-created).

Idempotent: ``DROP TABLE IF EXISTS`` is a no-op when the table is absent;
the ``CREATE SCHEMA IF NOT EXISTS seeds`` guards against running before
``refresh_views`` has created the schema on a fresh install. The UPDATE
no-ops when no ``'seed'`` rows exist.
"""

import logging

logger = logging.getLogger(__name__)


def migrate(conn: object) -> None:
    """Drop app.merchant_overrides + seeds.merchants_* and rewrite legacy 'seed' rows."""
    # Defensive: on a fresh install with auto-upgrade, this migration runs
    # before refresh_views() creates the seeds schema. CREATE SCHEMA IF NOT
    # EXISTS makes the subsequent DROP TABLE IF EXISTS unambiguously a no-op
    # regardless of init order.
    conn.execute("CREATE SCHEMA IF NOT EXISTS seeds")  # type: ignore[union-attr]

    for table in (
        "app.merchant_overrides",
        "seeds.merchants_global",
        "seeds.merchants_us",
        "seeds.merchants_ca",
    ):
        logger.info(f"Dropping {table} if present")
        conn.execute(f"DROP TABLE IF EXISTS {table}")  # type: ignore[union-attr]  # noqa: S608  # allowlisted literals

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
