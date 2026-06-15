"""V029: add source_origin to raw.ofx_balances.

Adds ``source_origin VARCHAR`` so the staging translation JOIN in
``prep.stg_ofx__balances`` can match against ``app.account_links.source_origin``
(B2). Mirrors V028 (``raw.ofx_accounts``) and the column already present on
``raw.ofx_transactions``.

Pure additive DDL — ``ADD COLUMN IF NOT EXISTS ... NULL`` with no DEFAULT,
so no backfill is required and the migration is a no-op on replay.
Existing rows land NULL until re-imported; the LEFT JOIN in the staging model
tolerates NULL (it NULLs the canonical account_id for those rows, which is
correct — they have no accepted mapping yet).
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def migrate(conn: object) -> None:
    """Add raw.ofx_balances.source_origin. Idempotent."""
    logger.info("V029: ADD COLUMN IF NOT EXISTS raw.ofx_balances.source_origin")
    conn.execute(  # type: ignore[union-attr]
        "ALTER TABLE raw.ofx_balances ADD COLUMN IF NOT EXISTS source_origin VARCHAR"
    )
    conn.execute(  # type: ignore[union-attr]
        "COMMENT ON COLUMN raw.ofx_balances.source_origin IS "
        "'Institution slug resolved at import time (e.g. wells_fargo); scopes the "
        "source_native account key against slug collisions and matches "
        "app.account_links.source_origin for the staging translation JOIN (B2). "
        "NULL for rows imported before V029 — re-import to populate.'"
    )
