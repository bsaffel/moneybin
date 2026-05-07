"""Move existing app.merchants rows to app.user_merchants and drop the old table.

Idempotent: detects whether the migration has already run by checking whether
app.merchants is a table (pre-migration) or a view (post-migration).
"""

import logging

logger = logging.getLogger(__name__)


def migrate(conn: object) -> None:
    """Move app.merchants data to app.user_merchants, then drop the table."""
    result = conn.execute(  # type: ignore[union-attr]
        """
        SELECT table_type FROM information_schema.tables
        WHERE table_schema = 'app' AND table_name = 'merchants'
        """
    ).fetchone()

    if result is None:
        # Neither table nor view exists yet — fresh install. seeds.py will create the view.
        return

    table_type = result[0]
    if table_type == "VIEW":
        # Already migrated.
        return

    # table_type == 'BASE TABLE' — pre-migration state. Move data.
    logger.info("Migrating app.merchants → app.user_merchants")
    conn.execute(  # type: ignore[union-attr]
        """
        INSERT INTO app.user_merchants (
            merchant_id, raw_pattern, match_type, canonical_name,
            category, subcategory, created_by, created_at
        )
        SELECT
            merchant_id, raw_pattern, match_type, canonical_name,
            category, subcategory, created_by, created_at
        FROM app.merchants
        """
    )
    conn.execute("DROP TABLE app.merchants")  # type: ignore[union-attr]
    logger.info("app.merchants → app.user_merchants migration complete")
