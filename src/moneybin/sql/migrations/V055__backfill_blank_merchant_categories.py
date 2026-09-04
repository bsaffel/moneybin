"""V055: blank category defaults on merchants, and what they already wrote.

V054's twin, one table over. Pre-``validate_category_text`` (this PR),
``create_merchant_core`` had no non-empty check, so ``app.user_merchants`` can
already carry ``category = '   '``. The merchant case is worse than the split
case V054 covers, and that is why it needs its own backfill rather than the
write guard alone: a blank split is one wrong row, while a blank merchant
default is a *generator*. ``apply_merchant_categories`` skips only ``None``
(``if cat is not None`` on the adopted merchant, a truthiness test on the name
match), so every ``categorize_pending`` sweep copies the blank onto each newly
matched transaction. The write guard stops new merchants; it does nothing
about the ones a legacy row keeps producing.

Backfill rule, and the blank class itself, are V054's — see
``V054__backfill_blank_split_categories`` for why "blank" is defined as
``str.strip()`` rather than a character list, why a merely padded ``'  Gas  '``
is left alone, why a blanked category takes its subcategory with it, and why
the ``WHERE`` has to name the already-NULL category to catch an orphan that
three-valued logic would otherwise exclude. ``test_blank_whitespace_definition``
holds every site to one character class, this one included.

``app.transaction_categories`` takes a DELETE rather than an UPDATE, because
its ``category`` is ``NOT NULL``: row absence is the only way this table
spells "uncategorized". That is the state such a transaction should have been
in all along — the row is precisely what hid it from
``core.uncategorized_queue`` while rendering an empty cell. Nothing worth
keeping is lost with it: the row's ``merchant_id`` is a match the next sweep
re-derives, now against a nulled default, so a real category lands where a
blank one was. A blank *subcategory* under a real category is nulled instead,
since the categorization itself is sound.

A separate migration rather than an edit to V054: V054 ships in this same PR,
but a dev database that already applied it would never re-run an amended body,
and this half would silently skip exactly the installs most likely to hold a
legacy row.

Deliberately NOT re-resolved: ``category_id``, on V054's reasoning — it is the
canonical FK and normalizing the text can make a pair resolvable that was not,
producing a row that displays as categorized while staying invisible to the
``category_id`` scan in ``plan_category_delete``.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

#: Entirely-whitespace test, defined to equal Python ``str.strip()`` exactly.
#: Byte-identical to V054's by construction — ``test_blank_whitespace_definition``
#: reads every site's source and fails when two of them disagree. Do not
#: maintain it by adding the character that last leaked; that test enumerates
#: all 29 codepoints ``str.isspace()`` accepts and names any the class misses.
_BLANK = r"[\p{Z}\s\x0B\x1C-\x1F\x85]*"


def migrate(conn: object) -> None:
    """Null blank merchant category defaults; clear what they propagated."""
    logger.debug(
        "V055: backfill whitespace-only app.user_merchants.category/subcategory "
        "to NULL and clear the categorizations they produced"
    )
    conn.execute(  # type: ignore[union-attr]
        """
        UPDATE app.user_merchants
        SET
            category = CASE
                WHEN REGEXP_FULL_MATCH(category, ?)
                THEN NULL
                ELSE category
            END,
            subcategory = CASE
                WHEN REGEXP_FULL_MATCH(subcategory, ?)
                  OR REGEXP_FULL_MATCH(category, ?)
                  OR category IS NULL
                THEN NULL
                ELSE subcategory
            END,
            -- core.dim_merchants reads this column straight through, so a
            -- service-external write that changes row content has to refresh
            -- it or the dim reports stale for exactly the rows just fixed. It
            -- rides the same WHERE, so an untouched row is not restamped.
            updated_at = CURRENT_TIMESTAMP
        WHERE
            REGEXP_FULL_MATCH(category, ?)
            OR REGEXP_FULL_MATCH(subcategory, ?)
            OR (category IS NULL AND subcategory IS NOT NULL)
        """,
        [_BLANK] * 5,
    )
    conn.execute(  # type: ignore[union-attr]
        """
        DELETE FROM app.transaction_categories
        WHERE REGEXP_FULL_MATCH(category, ?)
        """,
        [_BLANK],
    )
    conn.execute(  # type: ignore[union-attr]
        """
        UPDATE app.transaction_categories
        SET subcategory = NULL
        WHERE REGEXP_FULL_MATCH(subcategory, ?)
        """,
        [_BLANK],
    )
