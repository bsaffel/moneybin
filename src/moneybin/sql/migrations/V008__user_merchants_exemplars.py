"""Add exemplars column to app.user_merchants and relax raw_pattern NOT NULL.

Per categorization-matching-mechanics.md §Schema changes — supports the
oneOf exemplar accumulator that replaces auto-generalized contains patterns
for system-created merchants.
"""

import logging

logger = logging.getLogger(__name__)


def migrate(conn: object) -> None:
    """Add exemplars column; allow NULL raw_pattern. Idempotent."""
    cols: list[tuple[str, str]] = conn.execute(  # type: ignore[union-attr]
        """
        SELECT column_name, is_nullable FROM information_schema.columns
        WHERE table_schema = 'app' AND table_name = 'user_merchants'
        """
    ).fetchall()
    col_map: dict[str, str] = {c[0]: c[1] for c in cols}

    if not col_map:
        # Table doesn't exist yet — fresh install will create it from
        # app_user_merchants.sql with exemplars already present.
        return

    if "exemplars" not in col_map:
        logger.info("Adding exemplars column to app.user_merchants")
        conn.execute(  # type: ignore[union-attr]
            "ALTER TABLE app.user_merchants ADD COLUMN exemplars VARCHAR[] DEFAULT []"
        )

    # is_nullable is 'YES' / 'NO' per the SQL standard. Only drop NOT NULL if
    # raw_pattern is still NOT NULL — idempotent on re-run.
    if col_map.get("raw_pattern") == "NO":
        logger.info("Relaxing raw_pattern NOT NULL on app.user_merchants")
        conn.execute(  # type: ignore[union-attr]
            "ALTER TABLE app.user_merchants ALTER COLUMN raw_pattern DROP NOT NULL"
        )

    logger.info("V008 migration complete")
