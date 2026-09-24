"""V068: allow ``app.category_source_map.category_id`` to be ``NULL``.

A ``NULL`` on a user row now means "the user marked this source label as
carrying no useful category signal — stop asking and categorize nothing
through it" (docs/specs/category-source-map.md's map-to-null suppression).
``category_id`` is not part of the table's primary key
(``(source_type, source_category_code, source_subcategory_code)``), so this
is a plain ``ALTER COLUMN ... DROP NOT NULL`` — DuckDB supports it directly
(verified against the official ALTER TABLE docs and empirically against
DuckDB 1.5.5) and no table rebuild is needed, unlike V067's primary-key
widening.

Every existing row keeps its ``category_id`` untouched; only newly-written
ignore rows will ever carry ``NULL`` here.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def migrate(conn: object) -> None:
    """Drop the NOT NULL constraint on category_id. Idempotent."""
    logger.debug(
        "V068: ALTER TABLE app.category_source_map ALTER COLUMN category_id "
        "DROP NOT NULL"
    )
    conn.execute(  # type: ignore[attr-defined]
        "ALTER TABLE app.category_source_map ALTER COLUMN category_id DROP NOT NULL"
    )
