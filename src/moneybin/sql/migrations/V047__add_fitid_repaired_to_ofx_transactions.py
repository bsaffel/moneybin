"""V047: add fitid_repaired to raw.ofx_transactions.

The staging supersession retires a bare FITID when a ``#``-suffixed row matches
its content. It inferred provenance from the marker, but the OFX spec does not
reserve ``#``: an institution may legitimately mint both ``X`` and
``X#reference`` for two distinct transactions, and content equality cannot
separate that from a repair — ``identifiers.md`` is explicit that two genuinely
distinct transactions can carry identical content. That inference deletes a real
transaction, silently and with no review entry. The extractor now records which
ids it rewrote, and staging suppresses only against those.

Backfilled to the marker inference for rows already imported, which is the
status quo those installs are running on. Nothing at this layer can recover the
true provenance of an id written before the flag existed, and backfilling FALSE
would strand the orphaned bare rows the supersession was built to retire —
re-creating the double-count on every database that has already hit a collision.
Re-importing a statement replaces its rows through the raw PK and writes the
real value.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def migrate(conn: object) -> None:
    """Add raw.ofx_transactions.fitid_repaired and backfill it. Idempotent."""
    logger.debug("V047: ADD COLUMN IF NOT EXISTS raw.ofx_transactions.fitid_repaired")
    conn.execute(  # type: ignore[union-attr]
        "ALTER TABLE raw.ofx_transactions "
        "ADD COLUMN IF NOT EXISTS fitid_repaired BOOLEAN DEFAULT FALSE"
    )
    logger.debug("V047: backfilling fitid_repaired from the collision marker")
    conn.execute(  # type: ignore[union-attr]
        "UPDATE raw.ofx_transactions SET fitid_repaired = TRUE "
        "WHERE CONTAINS(source_transaction_id, '#') AND NOT fitid_repaired"
    )
    conn.execute(  # type: ignore[union-attr]
        "COMMENT ON COLUMN raw.ofx_transactions.fitid_repaired IS "
        "'TRUE when the extractor rewrote source_transaction_id to break a FITID "
        "collision; the only proof staging may use to retire the id this row "
        "superseded. Rows predating this column were backfilled from the marker'"
    )
