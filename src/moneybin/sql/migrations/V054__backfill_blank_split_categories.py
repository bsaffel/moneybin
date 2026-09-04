"""V054: null out whitespace-only category/subcategory in app.transaction_splits.

Pre-``validate_category_text`` (this PR), ``add_split`` had no non-empty
check, so a row can already carry ``category = '   '`` — text that passes a
``NULL`` check while rendering as a blank cell. That masks the parent's real
category: ``core.fct_transaction_lines`` computes
``COALESCE(s.category, t.category)``, and a whitespace-only ``s.category`` is
non-NULL, so the child's blank wins over the parent's real value instead of
falling through.

Backfill rule: a category/subcategory that is entirely whitespace (space,
tab, newline, CR — the same set the prep-layer staging models now trim on
write) becomes NULL. A merely *padded* value (``'  Gas  '``) is left as-is —
this PR deliberately does not trim padding anywhere, on the write path or
here, to avoid a second normalization pattern; only the blank case this PR
exists to close is backfilled.

Idempotent: the ``UPDATE`` only touches rows that are non-NULL and entirely
whitespace, so replay is a no-op.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def migrate(conn: object) -> None:
    """Null out whitespace-only category/subcategory in app.transaction_splits."""
    logger.debug(
        "V054: backfill whitespace-only app.transaction_splits.category/subcategory to NULL"
    )
    conn.execute(  # type: ignore[union-attr]
        """
        UPDATE app.transaction_splits
        SET
            category = CASE
                WHEN category IS NOT NULL
                    AND TRIM(category, ' ' || CHR(9) || CHR(10) || CHR(13)) = ''
                THEN NULL
                ELSE category
            END,
            subcategory = CASE
                WHEN subcategory IS NOT NULL
                    AND TRIM(subcategory, ' ' || CHR(9) || CHR(10) || CHR(13)) = ''
                THEN NULL
                ELSE subcategory
            END
        WHERE
            (category IS NOT NULL
                AND TRIM(category, ' ' || CHR(9) || CHR(10) || CHR(13)) = '')
            OR (subcategory IS NOT NULL
                AND TRIM(subcategory, ' ' || CHR(9) || CHR(10) || CHR(13)) = '')
        """
    )
