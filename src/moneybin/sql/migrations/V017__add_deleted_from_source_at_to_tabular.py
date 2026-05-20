"""V017: add soft-delete tracking column to raw.tabular_transactions.

Adds ``deleted_from_source_at TIMESTAMP NULL`` so live-syncing tabular
sources (gsheet adapter) can mark rows that disappeared from the source
without DELETEing them. The PullService stamps this column on the
soft-delete diff path; the prep view filters NULL-only rows so core and
reports layers see only currently-live data.

NULL on every row for non-live sources (CSV, OFX, Excel one-shot imports)
and for gsheet rows that are still present in their source sheet.

Pure additive DDL — ``ADD COLUMN IF NOT EXISTS ... NULL`` with no
DEFAULT, so no backfill is required and the migration is a no-op on
replay.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def migrate(conn: object) -> None:
    """Add raw.tabular_transactions.deleted_from_source_at. Idempotent."""
    logger.info(
        "V017: ADD COLUMN IF NOT EXISTS raw.tabular_transactions.deleted_from_source_at"
    )
    conn.execute(  # type: ignore[union-attr]
        "ALTER TABLE raw.tabular_transactions "
        "ADD COLUMN IF NOT EXISTS deleted_from_source_at TIMESTAMP"
    )

    # COMMENT ON COLUMN is idempotent (replaces existing comment), so safe on replay.
    conn.execute(  # type: ignore[union-attr]
        "COMMENT ON COLUMN raw.tabular_transactions.deleted_from_source_at IS "
        "'For live tabular sources (gsheet): timestamp when this row was observed "
        "absent from the source on the most recent pull. NULL means the row is "
        "currently present in source (or the source is non-live like a one-shot "
        "CSV import).'"
    )
