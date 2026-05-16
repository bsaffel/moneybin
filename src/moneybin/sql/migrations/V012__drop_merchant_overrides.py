"""Drop the retired app.merchant_overrides table and seed merchant schema.

MoneyBin no longer ships a curated seed merchant catalog. The
``seeds.merchants_global/us/ca`` tables and the ``app.merchant_overrides``
table that paired with them are removed. All merchants now live in
``app.user_merchants`` (created by user, LLM-assist, auto-rule, plaid, or
migration).

Idempotent: ``DROP TABLE IF EXISTS`` is a no-op when the table is absent.
Fresh installs never see these tables because ``schema.py`` and
``sqlmesh/models/seeds/`` no longer declare them.
"""

import logging

logger = logging.getLogger(__name__)


def migrate(conn: object) -> None:
    """Drop app.merchant_overrides and seeds.merchants_* if present."""
    for table in (
        "app.merchant_overrides",
        "seeds.merchants_global",
        "seeds.merchants_us",
        "seeds.merchants_ca",
    ):
        logger.info(f"Dropping {table} if present")
        conn.execute(f"DROP TABLE IF EXISTS {table}")  # type: ignore[union-attr]  # noqa: S608  # allowlisted literals

    logger.info("V012 migration complete")
