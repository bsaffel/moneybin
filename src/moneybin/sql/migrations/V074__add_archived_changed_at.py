"""V074: add app.account_settings.archived_changed_at.

docs/specs/core-updated-at-convention.md. Appended last because DuckDB's ALTER
TABLE ADD COLUMN always appends, and init_schemas() must produce the same
column order as an upgraded database. Existing rows get NULL: no archive
change has been recorded yet.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def migrate(conn: object) -> None:
    """Add archived_changed_at."""
    conn.execute(  # type: ignore[union-attr]
        "ALTER TABLE app.account_settings "
        "ADD COLUMN IF NOT EXISTS archived_changed_at TIMESTAMP"
    )
    conn.execute(  # type: ignore[union-attr]
        "COMMENT ON COLUMN app.account_settings.archived_changed_at IS "
        "'When archived last changed, including an undo; NULL if it never "
        "changed. Folded into updated_at on core.dim_accounts and on the "
        "account''s core.fct_transactions rows, whose is_categorizable it moves'"
    )
    logger.debug("V074: added archived_changed_at to app.account_settings")
