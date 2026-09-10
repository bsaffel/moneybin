"""V060: retire the archive cascade; add app.account_settings.archived_at.

Prerequisite for reports-net-worth-sql-surface.md Requirement 9 (see
docs/specs/reports-net-worth-sql-surface.md, §``app.account_settings`` — new
column, and §Prerequisites). ``AccountService.settings_update`` used to force
``include_in_net_worth=False`` in the same write as ``archived=True``,
unconditionally, before an explicit caller value could apply. That cascade is
retired in this same change; ``archived_at`` lets a stock-measure report (net
worth) exclude an account only for dates after it, instead of retroactively.

Backfill, for every account currently ``archived=TRUE``:

1. Find the most recent ``account_settings.set`` audit row on this account
   whose ``archived`` transitioned FALSE -> TRUE (``app.audit_log`` is
   append-only, so this evidence has not decayed). Stamp ``archived_at`` with
   that row's date.
2. Read ``before_value.include_in_net_worth`` from that same row. ``TRUE``
   means the write that followed forced it to ``FALSE`` -- the retired
   cascade's own signature -- so restore ``include_in_net_worth=TRUE``:
   ``archived_at`` now carries the exclusion, date-scoped, so the cascade's
   effect no longer needs to live in this column too. ``FALSE`` means the
   account was already excluded before that write, by a user choice the
   cascade had nothing to do with -- leave it alone.

An archived account with no such row (pre-audit data, a seed, or a direct
write) is a one-way door: restoring ``include_in_net_worth`` on a guess would
silently change a real net-worth number with no evidence backing the guess.
The durable choice is the same one the spec makes for ``archived_at`` itself
-- leave both untouched rather than infer a cutoff or an intent that cannot be
proven. The account stays exactly as exclusionary as it is today; a user who
wants it back in net worth flips ``include_in_net_worth`` explicitly.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def migrate(conn: object) -> None:
    """Add archived_at and backfill it (+ restore cascade-forced excludes)."""
    conn.execute(  # type: ignore[attr-defined]
        "ALTER TABLE app.account_settings ADD COLUMN IF NOT EXISTS archived_at DATE"
    )
    conn.execute(  # type: ignore[attr-defined]
        "COMMENT ON COLUMN app.account_settings.archived_at IS "
        "'The date the account stopped being part of the position. NULL "
        "while active. Lets a stock-measure report (net worth) exclude the "
        "account only for dates after this one, instead of retroactively; "
        "set by AccountService on the archived FALSE->TRUE transition, "
        "cleared on unarchive'"
    )

    archived_accounts = conn.execute(  # type: ignore[attr-defined]
        "SELECT account_id FROM app.account_settings WHERE archived = TRUE"
    ).fetchall()

    restored = 0
    dated_only = 0
    no_evidence = 0
    for (account_id,) in archived_accounts:
        row = conn.execute(  # type: ignore[attr-defined]
            """
            SELECT
                CAST(occurred_at AS DATE),
                json_extract_string(before_value, '$.include_in_net_worth')
            FROM app.audit_log
            WHERE target_schema = 'app'
              AND target_table = 'account_settings'
              AND target_id = ?
              AND action = 'account_settings.set'
              AND json_extract_string(before_value, '$.archived') = 'false'
              AND json_extract_string(after_value, '$.archived') = 'true'
            ORDER BY occurred_at DESC
            LIMIT 1
            """,
            [account_id],
        ).fetchone()
        if row is None:
            no_evidence += 1
            logger.debug(
                f"V060: no archive audit row for account {account_id}; "
                "leaving archived_at NULL and include_in_net_worth untouched"
            )
            continue
        archived_on, before_include = row
        if before_include == "true":
            conn.execute(  # type: ignore[attr-defined]
                "UPDATE app.account_settings "
                "SET archived_at = ?, include_in_net_worth = TRUE "
                "WHERE account_id = ?",
                [archived_on, account_id],
            )
            restored += 1
        else:
            conn.execute(  # type: ignore[attr-defined]
                "UPDATE app.account_settings SET archived_at = ? WHERE account_id = ?",
                [archived_on, account_id],
            )
            dated_only += 1
    logger.debug(
        f"V060: backfilled {restored} cascade-restored, {dated_only} "
        f"already-excluded, {no_evidence} no-evidence archived accounts"
    )
