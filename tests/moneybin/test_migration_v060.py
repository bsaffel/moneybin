"""V060: retire the archive cascade; backfill app.account_settings.archived_at.

Populated-fixture pattern per ``.claude/rules/database.md`` — V060 both adds a
column and backfills existing data (UPDATE), so the fixture is pre-V060
shaped (no ``archived_at`` column) and realistically populated, including
``app.audit_log`` evidence for the backfill and accounts with no evidence at
all (the one-way door named in
docs/specs/reports-net-worth-sql-surface.md §Prerequisites).

``include_in_net_worth`` is never restored by this migration (see the module
docstring on ``V060__add_account_settings_archived_at``): the audit image the
retired cascade produced is indistinguishable from a caller who explicitly
passed ``archived=True`` and ``include_in_net_worth=False`` in the same call,
so every fixture below keeps ``include_in_net_worth`` exactly as it was
stored before the migration runs.
"""

from __future__ import annotations

from datetime import date

import pytest

from moneybin.database import Database
from moneybin.sql.migrations.V060__add_account_settings_archived_at import migrate
from tests.moneybin.migration_helpers import column_exists, insert_rows, run_migration

_SETTINGS_COLUMNS = (
    "account_id",
    "display_name",
    "archived",
    "include_in_net_worth",
)

# account_ids covering every backfill branch.
_AMBIGUOUS_SIGNATURE = (
    "acct-ambigsignat1"  # before.include_in_net_worth=True -> untouched
)
_USER_EXCLUDED = "acct-userexclud1"  # before.include_in_net_worth=False -> leave alone
_NO_EVIDENCE = "acct-noevidence1"  # no FALSE->TRUE audit row -> leave alone entirely
_FIRST_WRITE = "acct-firstwrite1"  # archived on the account's first-ever settings row
_ACTIVE = "acct-activeacct1"  # archived=False -> untouched
_RE_ARCHIVED = "acct-rearchived1"  # two transitions -> most recent wins
_UNDO_REARCHIVED = "acct-undorearch1"  # re-archived via undo -> undo's date wins
_STALE_AFTER_UNARCHIVE = "acct-staleafterunarch1"  # archive superseded by an
# audited unarchive, then re-archived with no further audit evidence -> NULL,
# not the superseded archive's date
_STALE_AFTER_UNDO_DELETE = "acct-staleafterundo1"  # first-write archive, undone
# (a deletion-shaped unarchive with after_value NULL, not a JSON false), then
# re-archived with no further audit evidence -> NULL, not the undone archive's
# date
_STALE_AFTER_GENUINE_DELETE = "acct-staleafterdel1"  # archived via a normal
# .set, then the settings row is genuinely REMOVED via
# AccountSettingsRepo.delete() (action='account_settings.delete', not a
# '.set'-prefixed undo), then re-archived with NO further audit evidence ->
# NULL, not the superseded .set archive's date
_TIED_TIMESTAMPS = "acct-tiedtimestamp1"  # archive/unarchive/re-archive inside
# one outer transaction -> all three rows share one occurred_at; the final
# FALSE->TRUE transition must still win via the rowid tiebreak
_NEVER_UNARCHIVED = "acct-neverunarchiv1"  # archived once, no unarchive
# evidence at all -> must still be dated (pins the NOT EXISTS rewrite against
# the zero-row regression a naive scalar-CTE replacement would introduce)


def _audit_row_sql(action: str = "account_settings.set") -> str:
    return f"""
        INSERT INTO app.audit_log (
            audit_id, occurred_at, actor, action, target_schema, target_table,
            target_id, before_value, after_value, operation_id
        ) VALUES (?, ?, 'cli', '{action}', 'app', 'account_settings',
                  ?, ?, ?, ?)
    """  # noqa: S608  # test fixture SQL, action is a hardcoded literal


@pytest.fixture()
def pre_v060_db(db: Database) -> Database:
    """Pre-V060 app.account_settings (no archived_at) + realistic audit evidence."""
    db.execute("DROP TABLE app.account_settings")
    db.execute("""
        CREATE TABLE app.account_settings (
            account_id           VARCHAR NOT NULL PRIMARY KEY,
            display_name         VARCHAR,
            archived              BOOLEAN NOT NULL DEFAULT FALSE,
            include_in_net_worth BOOLEAN NOT NULL DEFAULT TRUE,
            updated_at           TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """)
    insert_rows(
        db,
        "app",
        "account_settings",
        _SETTINGS_COLUMNS,
        [
            (_AMBIGUOUS_SIGNATURE, "Ambiguous Signature", True, False),
            (_USER_EXCLUDED, "User Excluded", True, False),
            (_NO_EVIDENCE, "No Evidence", True, False),
            (_FIRST_WRITE, "First Write Archive", True, True),
            (_ACTIVE, "Active Account", False, True),
            (_RE_ARCHIVED, "Re-archived", True, False),
            (_UNDO_REARCHIVED, "Undo Re-archived", True, True),
            (_STALE_AFTER_UNARCHIVE, "Stale After Unarchive", True, True),
            (_STALE_AFTER_UNDO_DELETE, "Stale After Undo Delete", True, True),
            (_STALE_AFTER_GENUINE_DELETE, "Stale After Genuine Delete", True, True),
            (_TIED_TIMESTAMPS, "Tied Timestamps", True, True),
            (_NEVER_UNARCHIVED, "Never Unarchived", True, True),
        ],
    )

    # _AMBIGUOUS_SIGNATURE: one FALSE->TRUE transition, before.include_in_net_worth
    # was TRUE -- the retired cascade's signature, but indistinguishable from a
    # caller who explicitly asked for both archived=True and
    # include_in_net_worth=False in the same call. Must stay untouched.
    db.execute(
        _audit_row_sql(),
        [
            "aud-ambigsignat1",
            "2026-01-10 09:00:00",
            _AMBIGUOUS_SIGNATURE,
            '{"archived": false, "include_in_net_worth": true}',
            '{"archived": true, "include_in_net_worth": false}',
            "op-ambigsignat1",
        ],
    )

    # _USER_EXCLUDED: one FALSE->TRUE transition, before.include_in_net_worth was
    # already FALSE — a user choice unrelated to archiving.
    db.execute(
        _audit_row_sql(),
        [
            "aud-userexclud1",
            "2026-02-05 09:00:00",
            _USER_EXCLUDED,
            '{"archived": false, "include_in_net_worth": false}',
            '{"archived": true, "include_in_net_worth": false}',
            "op-userexclud1",
        ],
    )

    # _NO_EVIDENCE: no audit row at all (pre-audit data / seed / direct write).

    # _FIRST_WRITE: archiving was this account's first-ever settings mutation,
    # so AccountSettingsRepo.set captured before_value as SQL NULL (no prior
    # row) rather than a JSON object with archived: false.
    db.execute(
        _audit_row_sql(),
        [
            "aud-firstwrite1",
            "2026-04-01 09:00:00",
            _FIRST_WRITE,
            None,
            '{"archived": true, "include_in_net_worth": true}',
            "op-firstwrite1",
        ],
    )

    # _RE_ARCHIVED: archived on 2026-01-01, unarchived, re-archived on
    # 2026-03-20 — the migration must pick the MOST RECENT FALSE->TRUE row.
    db.execute(
        _audit_row_sql(),
        [
            "aud-rearchived1",
            "2026-01-01 09:00:00",
            _RE_ARCHIVED,
            '{"archived": false, "include_in_net_worth": true}',
            '{"archived": true, "include_in_net_worth": true}',
            "op-rearchived1a",
        ],
    )
    db.execute(
        _audit_row_sql(),
        [
            "aud-rearchived2",
            "2026-03-20 09:00:00",
            _RE_ARCHIVED,
            '{"archived": false, "include_in_net_worth": false}',
            '{"archived": true, "include_in_net_worth": false}',
            "op-rearchived2",
        ],
    )

    # _UNDO_REARCHIVED: archived on 2026-01-05, unarchived on 2026-02-10, then
    # re-archived by undoing that unarchive on 2026-03-15 -- BaseRepo.undo_event
    # records the reversal as action='account_settings.set.undo', not
    # 'account_settings.set'. The migration must still find this as the most
    # recent FALSE->TRUE transition, not fall back to the 2026-01-05 row.
    db.execute(
        _audit_row_sql(),
        [
            "aud-undorearch1a",
            "2026-01-05 09:00:00",
            _UNDO_REARCHIVED,
            '{"archived": false, "include_in_net_worth": true}',
            '{"archived": true, "include_in_net_worth": true}',
            "op-undorearch1a",
        ],
    )
    db.execute(
        _audit_row_sql(),
        [
            "aud-undorearch1b",
            "2026-02-10 09:00:00",
            _UNDO_REARCHIVED,
            '{"archived": true, "include_in_net_worth": true}',
            '{"archived": false, "include_in_net_worth": true}',
            "op-undorearch1b",
        ],
    )
    db.execute(
        _audit_row_sql("account_settings.set.undo"),
        [
            "aud-undorearch1c",
            "2026-03-15 09:00:00",
            _UNDO_REARCHIVED,
            '{"archived": false, "include_in_net_worth": true}',
            '{"archived": true, "include_in_net_worth": true}',
            "op-undorearch1c",
        ],
    )
    # _STALE_AFTER_UNARCHIVE: archived on 2026-01-01, audited-unarchived on
    # 2026-02-01, then re-archived with NO further audit evidence at all (a
    # direct write, a restore, or data predating the audit log) -- the live
    # row scans as archived=TRUE, but the only FALSE->TRUE audit row is the
    # one the unarchive superseded. The migration must leave archived_at
    # NULL, not date the account back to the superseded 2026-01-01 archive
    # and exclude its legitimate 2026-01-01..2026-02-01 active period.
    db.execute(
        _audit_row_sql(),
        [
            "aud-staleafterunarch1a",
            "2026-01-01 09:00:00",
            _STALE_AFTER_UNARCHIVE,
            '{"archived": false, "include_in_net_worth": true}',
            '{"archived": true, "include_in_net_worth": true}',
            "op-staleafterunarch1a",
        ],
    )
    db.execute(
        _audit_row_sql(),
        [
            "aud-staleafterunarch1b",
            "2026-02-01 09:00:00",
            _STALE_AFTER_UNARCHIVE,
            '{"archived": true, "include_in_net_worth": true}',
            '{"archived": false, "include_in_net_worth": true}',
            "op-staleafterunarch1b",
        ],
    )

    # _STALE_AFTER_UNDO_DELETE: archiving was this account's first-ever
    # settings write on 2026-01-01 (before_value NULL, per _FIRST_WRITE
    # above). Undoing that write on 2026-02-01 deletes the row -- a
    # DELETION-shaped unarchive whose emitted after_value is SQL NULL, not a
    # JSON `{"archived": false}` object. The account is currently archived
    # again with NO further audit evidence at all (a direct write or
    # restore). The migration must recognize the NULL after_value as ending
    # the archived streak and leave archived_at NULL, not date the account
    # back to the superseded 2026-01-01 first-write archive.
    db.execute(
        _audit_row_sql(),
        [
            "aud-staleafterundo1a",
            "2026-01-01 09:00:00",
            _STALE_AFTER_UNDO_DELETE,
            None,
            '{"archived": true, "include_in_net_worth": true}',
            "op-staleafterundo1a",
        ],
    )
    db.execute(
        _audit_row_sql("account_settings.set.undo"),
        [
            "aud-staleafterundo1b",
            "2026-02-01 09:00:00",
            _STALE_AFTER_UNDO_DELETE,
            '{"archived": true, "include_in_net_worth": true}',
            None,
            "op-staleafterundo1b",
        ],
    )

    # _STALE_AFTER_GENUINE_DELETE: archived via a normal .set on 2026-01-01,
    # then the settings row is genuinely REMOVED via AccountSettingsRepo.delete()
    # on 2026-02-01 -- action='account_settings.delete', an entirely different
    # action family from '.set', not one of its undo suffixes. That deletion
    # ends the archived streak exactly as an audited unarchive does (Codex PR
    # #596 P2, comment 3998603780): the account is currently archived again
    # with NO further audit evidence at all. The migration must recognize the
    # genuine delete's after_value NULL as superseding the 2026-01-01 archive
    # and leave archived_at NULL, not date the account back to it.
    db.execute(
        _audit_row_sql(),
        [
            "aud-staleafterdel1a",
            "2026-01-01 09:00:00",
            _STALE_AFTER_GENUINE_DELETE,
            '{"archived": false, "include_in_net_worth": true}',
            '{"archived": true, "include_in_net_worth": true}',
            "op-staleafterdel1a",
        ],
    )
    db.execute(
        _audit_row_sql("account_settings.delete"),
        [
            "aud-staleafterdel1b",
            "2026-02-01 09:00:00",
            _STALE_AFTER_GENUINE_DELETE,
            '{"archived": true, "include_in_net_worth": true}',
            None,
            "op-staleafterdel1b",
        ],
    )
    # _TIED_TIMESTAMPS: archived, unarchived, and re-archived inside ONE outer
    # transaction (AccountSettingsRepo.set(..., in_outer_txn=True)) -- DuckDB's
    # CURRENT_TIMESTAMP is transaction-stable, so all three rows share the
    # identical occurred_at. Insertion order is the append order: archive
    # (audit_id 'a'), unarchive ('b'), re-archive ('c'). A strict
    # occurred_at > comparison cannot distinguish the final re-archive from
    # the unarchive it follows and would reject it; the rowid tiebreak (this
    # table's insertion order) must recognize row 'c' as strictly after row
    # 'b' despite the tied timestamp, so archived_at is dated, not NULL.
    db.execute(
        _audit_row_sql(),
        [
            "aud-tiedtimestamp1a",
            "2026-05-01 09:00:00",
            _TIED_TIMESTAMPS,
            None,
            '{"archived": true, "include_in_net_worth": true}',
            "op-tiedtimestamp1",
        ],
    )
    db.execute(
        _audit_row_sql(),
        [
            "aud-tiedtimestamp1b",
            "2026-05-01 09:00:00",
            _TIED_TIMESTAMPS,
            '{"archived": true, "include_in_net_worth": true}',
            '{"archived": false, "include_in_net_worth": true}',
            "op-tiedtimestamp1",
        ],
    )
    db.execute(
        _audit_row_sql(),
        [
            "aud-tiedtimestamp1c",
            "2026-05-01 09:00:00",
            _TIED_TIMESTAMPS,
            '{"archived": false, "include_in_net_worth": true}',
            '{"archived": true, "include_in_net_worth": true}',
            "op-tiedtimestamp1",
        ],
    )

    # _NEVER_UNARCHIVED: archived once, no unarchive evidence at all. Pins the
    # NOT EXISTS rewrite against the regression the trap warns about: a naive
    # `ORDER BY ... LIMIT 1` scalar CTE for "last unarchive" would return zero
    # rows here (there is no unarchive), and a cross join against zero rows
    # yields nothing -- silently leaving archived_at NULL for every
    # never-unarchived account instead of correctly dating it.
    db.execute(
        _audit_row_sql(),
        [
            "aud-neverunarchiv1",
            "2026-06-01 09:00:00",
            _NEVER_UNARCHIVED,
            None,
            '{"archived": true, "include_in_net_worth": true}',
            "op-neverunarchiv1",
        ],
    )
    return db


def _settings_row(db: Database, account_id: str) -> tuple[object, bool]:
    row = db.execute(
        "SELECT archived_at, include_in_net_worth FROM app.account_settings "
        "WHERE account_id = ?",
        [account_id],
    ).fetchone()
    assert row is not None
    return row


def test_v060_adds_archived_at_column(pre_v060_db: Database) -> None:
    assert not column_exists(pre_v060_db, "app", "account_settings", "archived_at")
    run_migration(pre_v060_db, migrate)
    assert column_exists(pre_v060_db, "app", "account_settings", "archived_at")


def test_v060_never_restores_include_despite_cascade_signature(
    pre_v060_db: Database,
) -> None:
    """Cascade's audit signature == an explicit archived+exclude call; leave alone."""
    run_migration(pre_v060_db, migrate)
    archived_at, include = _settings_row(pre_v060_db, _AMBIGUOUS_SIGNATURE)
    assert archived_at == date(2026, 1, 10)
    assert include is False


def test_v060_leaves_user_exclusion_alone(pre_v060_db: Database) -> None:
    run_migration(pre_v060_db, migrate)
    archived_at, include = _settings_row(pre_v060_db, _USER_EXCLUDED)
    assert archived_at == date(2026, 2, 5)
    assert include is False


def test_v060_no_evidence_leaves_archived_at_null_and_include_untouched(
    pre_v060_db: Database,
) -> None:
    """The one-way door: no audit row means no guess — archived_at stays NULL."""
    run_migration(pre_v060_db, migrate)
    archived_at, include = _settings_row(pre_v060_db, _NO_EVIDENCE)
    assert archived_at is None
    assert include is False


def test_v060_backfills_archived_at_when_archiving_was_the_first_write(
    pre_v060_db: Database,
) -> None:
    """before_value IS NULL on a first write still counts as FALSE->TRUE evidence."""
    run_migration(pre_v060_db, migrate)
    archived_at, include = _settings_row(pre_v060_db, _FIRST_WRITE)
    assert archived_at == date(2026, 4, 1)
    assert include is True


def test_v060_active_account_untouched(pre_v060_db: Database) -> None:
    run_migration(pre_v060_db, migrate)
    archived_at, include = _settings_row(pre_v060_db, _ACTIVE)
    assert archived_at is None
    assert include is True


def test_v060_uses_most_recent_archive_transition(pre_v060_db: Database) -> None:
    run_migration(pre_v060_db, migrate)
    archived_at, include = _settings_row(pre_v060_db, _RE_ARCHIVED)
    assert archived_at == date(2026, 3, 20)
    # before.include_in_net_worth on the winning (most recent) row is False,
    # but this migration never restores it regardless.
    assert include is False


def test_v060_uses_most_recent_transition_including_undo(
    pre_v060_db: Database,
) -> None:
    """A re-archival performed via undo (action='...set.undo') must win.

    An exact `action = 'account_settings.set'` match would miss the undo row
    and fall back to the earlier direct archive, dating the account before
    its actual latest active period.
    """
    run_migration(pre_v060_db, migrate)
    archived_at, include = _settings_row(pre_v060_db, _UNDO_REARCHIVED)
    assert archived_at == date(2026, 3, 15)
    assert include is True


def test_v060_ignores_archive_evidence_superseded_by_a_later_unarchive(
    pre_v060_db: Database,
) -> None:
    """An audited unarchive invalidates every archive row before it as evidence.

    The account is currently ``archived=TRUE`` with no audit row for that
    transition -- only an old archive followed by an audited unarchive. The
    naive "most recent FALSE->TRUE row" query would pick the old archive,
    dating the account before the active period between it and the
    unarchive. The documented policy ("no guess beats a documented gap")
    applies to the CURRENT archived streak, not just to whether any FALSE->
    TRUE row exists at all -- so this must leave archived_at NULL, matching
    an account with no evidence whatsoever.
    """
    run_migration(pre_v060_db, migrate)
    archived_at, include = _settings_row(pre_v060_db, _STALE_AFTER_UNARCHIVE)
    assert archived_at is None
    assert include is True


def test_v060_ignores_archive_evidence_superseded_by_a_deleted_row(
    pre_v060_db: Database,
) -> None:
    """A NULL after_value (undo-of-first-write) ends an archived streak too.

    Undoing a pre-V060 account's first-ever settings write deletes the row --
    BaseRepo.undo_event emits that reversal with after_value SQL NULL, never
    a JSON ``{"archived": false}`` object, because there was no prior row to
    restore to. Absence of a row is the documented archived=FALSE default on
    either end of a transition, so this must be recognized as an unarchive
    exactly like an explicit JSON false, and supersede the first-write
    archive it undoes -- leaving archived_at NULL for the un-audited
    re-archive that follows, not the superseded first-write date.
    """
    run_migration(pre_v060_db, migrate)
    archived_at, include = _settings_row(pre_v060_db, _STALE_AFTER_UNDO_DELETE)
    assert archived_at is None
    assert include is True


def test_v060_ignores_archive_evidence_superseded_by_a_genuine_delete(
    pre_v060_db: Database,
) -> None:
    """A genuine account_settings.delete row ends an archived streak too.

    Codex PR #596 P2 (comment 3998603780): the prior action filter,
    ``action LIKE 'account_settings.set%'``, excludes ``account_settings.delete``
    entirely -- a different action family, not one of ``.set``'s undo suffixes.
    But deleting an archived settings row produces ``before_value.archived =
    true, after_value = NULL``, which returns the account to the default
    active state exactly as an audited unarchive does. Without matching this
    action too, the migration can't see the intervening deletion and would
    pick the superseded 2026-01-01 archive as current evidence, dating the
    account before the un-audited re-archive that actually holds today.
    """
    run_migration(pre_v060_db, migrate)
    archived_at, include = _settings_row(pre_v060_db, _STALE_AFTER_GENUINE_DELETE)
    assert archived_at is None
    assert include is True


def test_v060_orders_tied_transitions_by_append_position(
    pre_v060_db: Database,
) -> None:
    """A tied occurred_at (one outer transaction) must not reject the re-archive.

    Codex P2 (PR #596, comment 3998538957): all three rows share one
    transaction-stable occurred_at. A strict occurred_at > comparison for
    "no later unarchive" can't distinguish the final re-archive from the
    unarchive it immediately follows and rejects it, leaving archived_at
    NULL -- the exact blanket exclusion this migration exists to remove.
    The rowid tiebreak (append order) must resolve the tie correctly.
    """
    run_migration(pre_v060_db, migrate)
    archived_at, include = _settings_row(pre_v060_db, _TIED_TIMESTAMPS)
    assert archived_at == date(2026, 5, 1)
    assert include is True


def test_v060_dates_an_account_with_no_unarchive_evidence(
    pre_v060_db: Database,
) -> None:
    """The NOT EXISTS rewrite must still date an account with zero unarchives.

    A naive `ORDER BY ... LIMIT 1` scalar CTE for "last unarchive" returns
    zero rows when there is no unarchive, and a cross join against zero rows
    yields nothing -- silently NULLing every never-unarchived account. This
    pins the correct behavior: no unarchive evidence at all still dates the
    account from its one archive row.
    """
    run_migration(pre_v060_db, migrate)
    archived_at, include = _settings_row(pre_v060_db, _NEVER_UNARCHIVED)
    assert archived_at == date(2026, 6, 1)
    assert include is True


def test_v060_idempotent_on_replay(pre_v060_db: Database) -> None:
    run_migration(pre_v060_db, migrate)
    first = _settings_row(pre_v060_db, _AMBIGUOUS_SIGNATURE)
    run_migration(pre_v060_db, migrate)
    second = _settings_row(pre_v060_db, _AMBIGUOUS_SIGNATURE)
    assert first == second


@pytest.mark.fresh_db
def test_v060_upgrade_column_order_matches_fresh_schema(db: Database) -> None:
    """An upgraded table has the same ordered schema as a fresh install."""
    fresh_schema = [
        (row[1], row[2])
        for row in db.execute("PRAGMA table_info('app.account_settings')").fetchall()
    ]
    db.execute(
        "CREATE TABLE app._pre_v060_account_settings AS "
        "SELECT * EXCLUDE (archived_at) FROM app.account_settings LIMIT 0"
    )
    db.execute("DROP TABLE app.account_settings CASCADE")  # isolated test database
    db.execute("ALTER TABLE app._pre_v060_account_settings RENAME TO account_settings")

    run_migration(db, migrate)

    upgraded_schema = [
        (row[1], row[2])
        for row in db.execute("PRAGMA table_info('app.account_settings')").fetchall()
    ]
    assert upgraded_schema == fresh_schema
