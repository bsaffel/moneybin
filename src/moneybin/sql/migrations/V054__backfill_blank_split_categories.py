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

Idempotent: the ``UPDATE`` only touches rows that are non-NULL and entirely
whitespace, so replay is a no-op.
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
        f"""
        UPDATE app.transaction_splits
        SET
            category = CASE
                WHEN REGEXP_FULL_MATCH(category, '{_BLANK}')
                THEN NULL
                ELSE category
            END,
            subcategory = CASE
                WHEN REGEXP_FULL_MATCH(subcategory, '{_BLANK}')
                THEN NULL
                ELSE subcategory
            END
        WHERE
            REGEXP_FULL_MATCH(category, '{_BLANK}')
            OR REGEXP_FULL_MATCH(subcategory, '{_BLANK}')
        """  # noqa: S608  # module constant, not caller input
    )
