"""V068: add app.account_settings.investment_source_type and its change time.

docs/specs/investment-source-choice.md. Appended last because DuckDB's ALTER
TABLE ADD COLUMN always appends, and init_schemas() must produce the same
column order as an upgraded database. Existing rows get NULL, which keeps
today's behavior (every source feeds the ledger).
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def migrate(conn: object) -> None:
    """Add investment_source_type and investment_source_type_changed_at."""
    conn.execute(  # type: ignore[union-attr]
        "ALTER TABLE app.account_settings "
        "ADD COLUMN IF NOT EXISTS investment_source_type VARCHAR"
    )
    conn.execute(  # type: ignore[union-attr]
        "COMMENT ON COLUMN app.account_settings.investment_source_type IS "
        "'The source type whose investment rows feed this account''s ledger "
        "(manual or plaid); NULL uses every source'"
    )
    conn.execute(  # type: ignore[union-attr]
        "ALTER TABLE app.account_settings "
        "ADD COLUMN IF NOT EXISTS investment_source_type_changed_at TIMESTAMP"
    )
    conn.execute(  # type: ignore[union-attr]
        "COMMENT ON COLUMN app.account_settings.investment_source_type_changed_at "
        "IS 'When investment_source_type last changed, including a clear and an "
        "undo; NULL if it never changed. Folded into the investment ledger''s "
        "updated_at so re-entered rows do not rewind it'"
    )
    logger.debug("V068: added investment_source_type columns to app.account_settings")
