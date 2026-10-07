"""V069: let app.category_source_map.category_id be NULL.

A user row with no category marks its source term as ignored: the term is
known, so it leaves the pending inbox, and it categorizes nothing.

``category_id`` is not part of the primary key, so DuckDB drops the constraint
in place; no table rebuild is needed, unlike V067 and V068.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def migrate(conn: object) -> None:
    """Drop NOT NULL from app.category_source_map.category_id. Idempotent."""
    logger.debug("V069: drop NOT NULL from app.category_source_map.category_id")
    conn.execute(  # type: ignore[attr-defined]
        "ALTER TABLE app.category_source_map ALTER COLUMN category_id DROP NOT NULL"
    )
