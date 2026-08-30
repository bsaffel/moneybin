"""V052: add account_label to raw.tabular_accounts.

``core.dim_accounts.display_name`` had no rung for the one name a person
actually wrote. It assembled a label out of institution, account type and last
four, so a sheet whose Account column read "Retirement Plan 2024 Rewards" stored
``Unnamed account`` — the column was projected through staging and then simply
not selected by the dimension. This column is the rung's input.

Not the same string as ``account_name``, and the two are not
interchangeable. ``account_name`` is the source's own text, classed
``ACCOUNT_IDENTIFIER`` because an Account column routinely holds a whole
account number; ``account_label`` is its display-safe form — trailing last-four
token dropped, embedded numbers masked — and feeds a column that reaches
reports unmasked. One is evidence, the other is publishable, which is why they
are separate columns rather than one column that changed meaning.

Deliberately NOT backfilled. The masking and last-four-stripping rules live in
Python, and ``account_name`` is the *unparsed* label, so a SQL backfill would
have to reimplement both here and would then be the second copy of a rule whose
whole point is having one. NULL reads as "this account has no authored label"
everywhere downstream — the dimension's COALESCE skips the rung and names the
account exactly as it does today — so existing rows keep their current name
until the next import of that file writes the column through the raw primary
key.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def migrate(conn: object) -> None:
    """Add account_label to raw.tabular_accounts, NULL for every existing row."""
    logger.debug("V052: ADD COLUMN IF NOT EXISTS raw.tabular_accounts.account_label")
    conn.execute(  # type: ignore[union-attr]
        "ALTER TABLE raw.tabular_accounts ADD COLUMN IF NOT EXISTS account_label VARCHAR"
    )
    # Byte-identical to the comment in raw_tabular_accounts.sql. `_apply_comments`
    # re-runs that DDL's comments on every startup while this migration runs
    # once, so a divergent string here would be overwritten on the next open and
    # the catalog description would differ by which ran last.
    conn.execute(  # type: ignore[union-attr]
        "COMMENT ON COLUMN raw.tabular_accounts.account_label IS "
        "'Display-safe form of account_name for core.dim_accounts.display_name: "
        "trailing last-four token dropped, embedded account numbers masked. NULL "
        "when no human authored a name and the importer synthesized one from the "
        "filename'"
    )
