"""V047: add fitid_repaired to raw.ofx_transactions.

The staging supersession retires a bare FITID when a ``#``-suffixed row matches
its content. It inferred provenance from the marker, but the OFX spec does not
reserve ``#``: an institution may legitimately mint both ``X`` and
``X#reference`` for two distinct transactions, and content equality cannot
separate that from a repair — ``identifiers.md`` is explicit that two genuinely
distinct transactions can carry identical content. That inference deletes a real
transaction, silently and with no review entry. The extractor now records which
ids it rewrote, and staging suppresses only against those.

Deliberately NOT backfilled. Rows imported before this column existed carry no
record of what wrote them, and every available substitute is the same unsound
inference in a narrower dress: the marker itself, or the suffix's shape.
Recomputing the extractor's hash cannot recover it either — the suffix is taken
over ``str(amount)`` as the extractor saw it, before DuckDB coerces the column to
``DECIMAL(18,2)``, so a statement amount of ``-25.5`` hashed ``"-25.5"`` and reads
back ``"-25.50"``.

So the column starts FALSE everywhere and no pre-existing row licenses a delete.
The cost is real and accepted: a database that already imported a collision keeps
showing that double-count until the statement is re-imported, at which point the
extractor writes the true value through the raw PK and the supersession resumes.
That is the trade this whole change is built on — a double-count is visible in a
total and correctable, while a wrong suppression removes a real transaction with
nothing left to notice.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def migrate(conn: object) -> None:
    """Add raw.ofx_transactions.fitid_repaired, FALSE for every existing row."""
    logger.debug("V047: ADD COLUMN IF NOT EXISTS raw.ofx_transactions.fitid_repaired")
    conn.execute(  # type: ignore[union-attr]
        "ALTER TABLE raw.ofx_transactions "
        "ADD COLUMN IF NOT EXISTS fitid_repaired BOOLEAN DEFAULT FALSE"
    )
    conn.execute(  # type: ignore[union-attr]
        "COMMENT ON COLUMN raw.ofx_transactions.fitid_repaired IS "
        "'TRUE when the extractor rewrote source_transaction_id to break a FITID "
        "collision; the only proof staging may use to retire the id this row "
        "superseded. Rows predating this column are FALSE and never licensed one'"
    )
