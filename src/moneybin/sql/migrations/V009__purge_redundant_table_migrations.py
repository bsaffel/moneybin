"""Remove V004/V005 history rows that no longer have matching files on disk.

V004 (create_app_account_settings) and V005 (create_app_balance_assertions)
were pure CREATE TABLE IF NOT EXISTS duplicates of their schema counterparts
and were deleted. Databases that ran them will see 'File missing' drift
warnings from check_drift() until these rows are purged.

Idempotent: the DELETE is a no-op when the rows are absent (fresh installs,
or databases upgraded after the files were removed).
"""

import logging

logger = logging.getLogger(__name__)


def migrate(conn: object) -> None:
    """Delete V004/V005 schema_migrations rows to suppress spurious drift warnings."""
    count = conn.execute(  # type: ignore[union-attr]
        "SELECT COUNT(*) FROM app.schema_migrations WHERE version IN (4, 5)"
    ).fetchone()[0]
    if count:
        conn.execute(  # type: ignore[union-attr]
            "DELETE FROM app.schema_migrations WHERE version IN (4, 5)"
        )
        logger.info(f"Purged {count} redundant migration row(s) (V004/V005)")
