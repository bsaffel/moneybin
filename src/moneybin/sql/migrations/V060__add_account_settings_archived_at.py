"""V060: retire the archive cascade; add app.account_settings.archived_at.

Prerequisite for reports-net-worth-sql-surface.md Requirement 9 (see
docs/specs/reports-net-worth-sql-surface.md, §``app.account_settings`` — new
column, and §Prerequisites). ``AccountService.settings_update`` used to force
``include_in_net_worth=False`` in the same write as ``archived=True``,
unconditionally, before an explicit caller value could apply. That cascade is
retired in this same change; ``archived_at`` lets a stock-measure report (net
worth) exclude an account only for dates after it, instead of retroactively.

Backfill, for every account currently ``archived=TRUE``: find the most recent
``app.audit_log`` row on this account whose ``archived`` transitioned FALSE ->
TRUE (append-only, so this evidence has not decayed) and stamp ``archived_at``
with that row's date. The action match is ``action LIKE 'account_settings.set%'``,
not an exact ``= 'account_settings.set'`` — ``BaseRepo.undo_event`` records a
reversal as ``f"{event.action}.undo"``, so undoing a prior unarchive re-archives
via an ``account_settings.set.undo`` row (and undoing *that* undo via
``account_settings.set.undo.undo``, arbitrarily deep), each carrying the same
before/after row-image shape as a direct ``.set``. An exact-action match would
skip that evidence and fall back to an older direct-``.set`` archive row (or
none), stamping ``archived_at`` before the account's actual latest active
period. The prefix is scoped safely: ``account_settings.delete`` and its own
undo chain start with ``account_settings.delete``, never ``...set``. A
transition counts either when ``before_value.archived`` reads ``'false'`` or
when ``before_value`` is SQL ``NULL`` — ``AccountSettingsRepo.set`` captures no
prior row on an account's first-ever settings write, and the absence of a row
*is* the documented ``archived=FALSE`` default, just not in the JSON shape a
bare ``json_extract_string(...) = 'false'`` comparison expects.

The most-recent-FALSE->TRUE row is disqualified if a TRUE->FALSE (unarchive)
row exists *after* it. Without that check, an account archived long ago (D1),
audited-unarchived later (D2), then re-archived with no audit evidence at all
(a direct write, a restore, or data predating the audit log) would still be
``archived=TRUE`` today, and the naive most-recent-match query would pick D1
— dating the account before the D1-D2 active period it legitimately holds and
excluding valid balances from it. Requiring no later unarchive is exactly
"no guess beats a documented gap" applied to the current archived streak, not
just to whether any evidence exists at all: an archive row from a superseded
streak is not evidence for this one. This does not regress the undo-produced
re-archival case the broadened action match exists for — undoing an unarchive
row emits a FALSE->TRUE row *after* that unarchive's own ``occurred_at``, so it
still qualifies as the latest unsuperseded archive evidence.

"After" is ordered by ``(occurred_at, rowid)``, never ``occurred_at`` alone.
Every row written in one outer transaction (``AccountSettingsRepo.set(...,
in_outer_txn=True)`` -- a supported, not contrived, path) shares one
transaction-stable ``occurred_at``: DuckDB's ``CURRENT_TIMESTAMP`` does not
advance within a transaction. An archive -> unarchive -> re-archive sequence
run that way emits three rows with an identical timestamp, and a strict
``occurred_at >`` comparison can't tell the final re-archive from the
unarchive it follows -- it would reject the current FALSE->TRUE transition
and leave the account NULL, the exact blanket exclusion this migration exists
to remove. ``rowid`` (monotonic, append-only) is the established tiebreaker
for this: ``AuditService.events_for_operation`` uses the identical
``ORDER BY occurred_at, rowid`` for the same reason.

A TRUE->FALSE (unarchive) row is recognized the same way when it is
DELETION-shaped: undoing a pre-V060 account's first-ever settings write (that
write's own ``before_value`` was SQL ``NULL``) deletes the row
(``BaseRepo.undo_event``'s ``before is None`` branch), and the undo it emits
carries ``after_value`` as SQL ``NULL`` -- not a JSON object with
``archived: false``. Absence of a row *is* the documented ``archived=FALSE``
default on this end of a transition exactly as it is on the other, so this
also ends an archived streak. Missing it would let a stale pre-undo archive
date win over an intervening deleted-then-un-audited-re-archived period, the
same class of bug the previous paragraph's check closes.

``include_in_net_worth`` is left exactly as stored for every account this
migration touches — never restored, even when
``before_value.include_in_net_worth`` reads ``'true'`` (the retired cascade's
own signature). That signature is not unique to the cascade:
``AccountService``'s old ``if archived is True: include_in_net_worth =
False`` ran unconditionally, ahead of ``_resolve()``, so a caller who
explicitly passed ``archived=True`` *and* ``include_in_net_worth=False`` in
the *same* call — exactly what the CLI's ``accounts set --archive --exclude``
and the MCP ``accounts_set(is_archived=True, include_in_net_worth=False)``
document as the intended way to archive-and-exclude in one step — produced a
byte-identical before/after audit image to a caller who only archived and let
the cascade force the flag. ``AccountSettingsRepo.set`` records full
before/after row snapshots, not the kwargs a caller passed, so there is no way
to tell the two apart from history alone.

Per ``.claude/rules/design-principles.md`` ("Magic stays visible"), a
silent write on an ambiguous signal defaults to leaving the value alone
rather than guessing — and guessing is not forced here: both current
net-worth consumers already filter ``include_in_net_worth AND NOT archived``,
so every archived account is excluded regardless of this flag's value until
reports-net-worth-sql-surface.md's Requirement 9 replaces that blanket
exclusion with a date-scoped one. That future change is the right place to
decide how to treat the accounts this migration leaves alone (most likely by
surfacing them for user review), rather than inferring intent a second time
with no better evidence than this migration has.

An archived account with no FALSE->TRUE evidence at all (pre-audit data, a
seed, or a direct write) is left with ``archived_at`` NULL for the same
reason — no guess beats a documented gap.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def migrate(conn: object) -> None:
    """Add archived_at and backfill it from app.audit_log evidence."""
    conn.execute(  # type: ignore[union-attr]
        "ALTER TABLE app.account_settings ADD COLUMN IF NOT EXISTS archived_at DATE"
    )
    conn.execute(  # type: ignore[union-attr]
        "COMMENT ON COLUMN app.account_settings.archived_at IS "
        "'The date the account stopped being part of the position. NULL "
        "while active. Lets a stock-measure report (net worth) exclude the "
        "account only for dates after this one, instead of retroactively; "
        "set by AccountService on the archived FALSE->TRUE transition, "
        "cleared on unarchive'"
    )

    archived_accounts = conn.execute(  # type: ignore[union-attr]
        "SELECT account_id FROM app.account_settings WHERE archived = TRUE"
    ).fetchall()

    dated = 0
    no_evidence = 0
    for (account_id,) in archived_accounts:
        row = conn.execute(  # type: ignore[union-attr]
            """
            WITH transitions AS (
                SELECT
                    rowid AS rid,
                    occurred_at,
                    (
                        before_value IS NULL
                        OR json_extract_string(before_value, '$.archived') = 'false'
                    ) AS from_false,
                    json_extract_string(after_value, '$.archived') = 'true'
                        AS to_true,
                    json_extract_string(before_value, '$.archived') = 'true'
                        AS from_true,
                    (
                        after_value IS NULL
                        OR json_extract_string(after_value, '$.archived') = 'false'
                    ) AS to_false
                FROM app.audit_log
                WHERE target_schema = 'app'
                  AND target_table = 'account_settings'
                  AND target_id = ?
                  AND action LIKE 'account_settings.set%'
            )
            SELECT CAST(t.occurred_at AS DATE)
            FROM transitions AS t
            WHERE t.from_false
              AND t.to_true
              AND NOT EXISTS (
                  SELECT 1 FROM transitions AS u
                  WHERE u.from_true AND u.to_false
                    AND (u.occurred_at, u.rid) > (t.occurred_at, t.rid)
              )
            ORDER BY t.occurred_at DESC, t.rid DESC
            LIMIT 1
            """,
            [account_id],
        ).fetchone()
        if row is None:
            no_evidence += 1
            logger.debug(
                f"V060: no archive audit row for account {account_id}; "
                "leaving archived_at NULL"
            )
            continue
        (archived_on,) = row
        conn.execute(  # type: ignore[union-attr]
            "UPDATE app.account_settings SET archived_at = ? WHERE account_id = ?",
            [archived_on, account_id],
        )
        dated += 1
    logger.debug(
        f"V060: backfilled archived_at for {dated} accounts, {no_evidence} "
        "with no audit evidence; include_in_net_worth left untouched throughout"
    )
