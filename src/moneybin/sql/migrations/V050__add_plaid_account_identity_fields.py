"""V050: add persistent_account_id and name to raw.plaid_accounts.

The broker has always sent both, but ``SyncAccount`` never declared them and
Pydantic's ``extra='ignore'`` destroyed each one at validation — silently, with
no error and no log line. Existing databases therefore hold no copy anywhere:
the values never survived past the wire.

Deliberately NOT backfilled, and the two columns are not equally cheap to guess.
``persistent_account_id`` becomes an ``AccountResolver`` strong ref, which
auto-adopts in step 1 of ``resolve()`` without surfacing a review — so a wrong
value silently merges two ledgers, which is exactly the failure the field exists
to prevent. Nothing already stored can derive it: ``account_id`` is the token a
relink reissues, and ``official_name`` is a shared product label (two Chase
cards report one ``Ultimate Rewards®`` between them).

So both columns start NULL. NULL reads as "unknown" everywhere downstream — the
resolver skips an empty strong ref, and the display fallback keeps using
``official_name`` — and the next sync writes the true values through the raw
primary key. The cost is bounded: cross-connection identity stays unavailable
for accounts not yet re-synced, which is the status quo, not a regression.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def migrate(conn: object) -> None:
    """Add both columns to raw.plaid_accounts, NULL for every existing row."""
    logger.debug(
        "V050: ADD COLUMN IF NOT EXISTS raw.plaid_accounts.persistent_account_id"
    )
    conn.execute(  # type: ignore[union-attr]
        "ALTER TABLE raw.plaid_accounts "
        "ADD COLUMN IF NOT EXISTS persistent_account_id VARCHAR"
    )
    # Byte-identical to the comment in raw_plaid_accounts.sql. `_apply_comments`
    # re-runs that DDL's comments on every startup while this migration runs
    # once, so a divergent string here would be overwritten on the next open and
    # the catalog description would differ by which ran last.
    conn.execute(  # type: ignore[union-attr]
        "COMMENT ON COLUMN raw.plaid_accounts.persistent_account_id IS "
        "'Plaid persistent_account_id; survives relink, so it is the "
        "cross-connection identity ref. NULL where the institution does not "
        "supply one'"
    )

    logger.debug("V050: ADD COLUMN IF NOT EXISTS raw.plaid_accounts.name")
    conn.execute(  # type: ignore[union-attr]
        "ALTER TABLE raw.plaid_accounts ADD COLUMN IF NOT EXISTS name VARCHAR"
    )
    conn.execute(  # type: ignore[union-attr]
        "COMMENT ON COLUMN raw.plaid_accounts.name IS "
        "'Account name as the institution reports it; distinguishes sibling "
        "accounts that share one official_name'"
    )
