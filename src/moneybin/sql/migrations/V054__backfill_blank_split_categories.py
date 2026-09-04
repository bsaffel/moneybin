"""V054: null out whitespace-only category/subcategory in app.transaction_splits.

Pre-``validate_category_text`` (this PR), ``add_split`` had no non-empty
check, so a row can already carry ``category = '   '`` — text that passes a
``NULL`` check while rendering as a blank cell. That masks the parent's real
category: ``core.fct_transaction_lines`` computes
``COALESCE(s.category, t.category)``, and a whitespace-only ``s.category`` is
non-NULL, so the child's blank wins over the parent's real value instead of
falling through.

Backfill rule: a category/subcategory that is entirely whitespace becomes
NULL. "Whitespace" here is whatever Python's ``str.strip()`` calls blank —
the service validator's definition — rather than a hand-written character
list, so the two cannot drift apart. A merely *padded* value (``'  Gas  '``)
is left as-is: this PR deliberately does not trim padding anywhere, on the
write path or here, to avoid a second normalization pattern; only the blank
case this PR exists to close is backfilled.

A blanked ``category`` takes its ``subcategory`` with it. A subcategory is a
child of a category in this taxonomy, so ``(NULL, 'Coffee')`` is not a
weaker answer than ``(NULL, NULL)`` — it is an invalid one, and
``write_contracts.SplitTarget`` refuses exactly that shape on MCP writes.
``core.fct_transaction_lines`` coalesces the two fields independently, so
leaving the orphan would render the parent's category beside this split's
subcategory: a pair nobody chose. The reverse does not hold — a blank
subcategory under a real category just nulls the subcategory.

That makes the postcondition "no split carries a subcategory without a
category", which is wider than blankness and is why the predicate names the
already-NULL category explicitly. A row can reach ``(NULL, 'Coffee')``
without any blank text ever being involved: the granular ``set_splits`` arm
accepted a bare ``subcategory`` outright until this PR, and the test that
used to pin that behavior spelled the shape out. Such a row is invisible to a
``WHERE`` written only around blankness, because
``REGEXP_FULL_MATCH(NULL, pattern)`` is ``NULL`` and ``NULL OR FALSE`` is
``NULL`` rather than ``TRUE`` — SQL three-valued logic excludes the row
instead of matching it. Stating the invariant rather than the symptom covers
both routes to the same orphan.

Idempotent: every ``CASE`` maps its matched rows to ``NULL``, and a ``NULL``
category with a ``NULL`` subcategory matches no branch of the ``WHERE``, so
replay is a no-op.

Deliberately NOT re-resolved: ``category_id``. It is the canonical FK
(``category``/``subcategory`` are V014's deprecated display snapshot), and
normalizing the text can make a pair resolvable that was not — a legacy
``('Hobbies', '   ')`` becomes ``('Hobbies', NULL)``, which matches a real
``core.dim_categories`` row, while the FK stays the ``NULL`` it was. Such a
row displays as categorized but is invisible to the reference scan in
``plan_category_delete``, which keys on ``category_id``. Re-deriving it here
would mean inlining a copy of the ``dim_categories`` view the way V014 does,
because migrations run before ``refresh_views()`` and the view may not exist
yet. That machinery is not justified for this migration: it is a no-op on
every install in existence — MoneyBin has no installed base, so there are no
legacy rows for it to touch — and the write path it protects now refuses a
blank outright, so no new row can reach this shape. A future install that
imports pre-existing data does not reach it either; the staging models null a
blank out before it is ever stored.

Deliberately NOT rewritten: historical ``app.audit_log`` images.
``BaseRepo.undo_event()`` → ``_insert_row()`` restores a captured before-image
verbatim, with no validator in the path, so undoing a pre-V054
``split.remove`` could write a blank straight back. That is a general property
of the undo mechanism for every field on every table rather than anything
specific to category text, and it needs an image captured before this PR's
guard existed — which, per the paragraph above, cannot exist. Closing it
properly means validating on the undo path, not rewriting audit history, and
that is its own change.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

#: Entirely-whitespace test, defined to equal Python ``str.strip()`` exactly.
#: The write path refuses a blank with ``str.strip()``
#: (``services._validators.validate_category_text``), so any character the two
#: disagree on is a category the validator calls blank and the import path
#: stores non-NULL — invisible in ``core.uncategorized_queue``, which is the
#: defect this migration exists to close.
#:
#: The class is the union of the four things ``str.isspace()`` covers:
#: ``\p{Z}`` every Unicode space separator, ``\s`` the C0 whitespace RE2
#: includes, ``\x0B`` the vertical tab it excludes, and ``\x1C-\x1F\x85`` the
#: information separators and NEXT LINE. Do not maintain this by adding the
#: character that last leaked — ``test_blank_whitespace_definition.py``
#: enumerates all 29 codepoints ``str.isspace()`` accepts and fails naming any
#: the class misses. Bare ``TRIM`` is not a substitute: it strips the space
#: separators but no control character, so it leaves a tab behind.
_BLANK = r"[\p{Z}\s\x0B\x1C-\x1F\x85]*"


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
            END
        WHERE
            REGEXP_FULL_MATCH(category, ?)
            OR REGEXP_FULL_MATCH(subcategory, ?)
            OR (category IS NULL AND subcategory IS NOT NULL)
        """,
        [_BLANK] * 5,
    )
