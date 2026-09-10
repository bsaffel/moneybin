"""V060: retire the archive cascade; backfill app.account_settings.archived_at.

Populated-fixture pattern per ``.claude/rules/database.md`` — V060 both adds a
column and backfills existing data (UPDATE), so the fixture is pre-V060
shaped (no ``archived_at`` column) and realistically populated, including
``app.audit_log`` evidence for the two backfill branches and an account with
no evidence at all (the one-way door named in
docs/specs/reports-net-worth-sql-surface.md §Prerequisites).
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
_CASCADE_RESTORED = "acct-cascaderes1"  # before.include_in_net_worth=True -> restore
_USER_EXCLUDED = "acct-userexclud1"  # before.include_in_net_worth=False -> leave alone
_NO_EVIDENCE = "acct-noevidence1"  # no FALSE->TRUE audit row -> leave alone entirely
_ACTIVE = "acct-activeacct1"  # archived=False -> untouched
_RE_ARCHIVED = "acct-rearchived1"  # two transitions -> most recent wins


def _audit_row_sql() -> str:
    return """
        INSERT INTO app.audit_log (
            audit_id, occurred_at, actor, action, target_schema, target_table,
            target_id, before_value, after_value, operation_id
        ) VALUES (?, ?, 'cli', 'account_settings.set', 'app', 'account_settings',
                  ?, ?, ?, ?)
    """


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
            (_CASCADE_RESTORED, "Cascade Restored", True, False),
            (_USER_EXCLUDED, "User Excluded", True, False),
            (_NO_EVIDENCE, "No Evidence", True, False),
            (_ACTIVE, "Active Account", False, True),
            (_RE_ARCHIVED, "Re-archived", True, False),
        ],
    )

    # _CASCADE_RESTORED: one FALSE->TRUE transition, before.include_in_net_worth
    # was TRUE — the retired cascade's own signature.
    db.execute(
        _audit_row_sql(),
        [
            "aud-cascaderes1",
            "2026-01-10 09:00:00",
            _CASCADE_RESTORED,
            '{"archived": false, "include_in_net_worth": true}',
            '{"archived": true, "include_in_net_worth": false}',
            "op-cascaderes1",
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


def test_v060_restores_include_when_cascade_caused_it(pre_v060_db: Database) -> None:
    run_migration(pre_v060_db, migrate)
    archived_at, include = _settings_row(pre_v060_db, _CASCADE_RESTORED)
    assert archived_at == date(2026, 1, 10)
    assert include is True


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


def test_v060_active_account_untouched(pre_v060_db: Database) -> None:
    run_migration(pre_v060_db, migrate)
    archived_at, include = _settings_row(pre_v060_db, _ACTIVE)
    assert archived_at is None
    assert include is True


def test_v060_uses_most_recent_archive_transition(pre_v060_db: Database) -> None:
    run_migration(pre_v060_db, migrate)
    archived_at, include = _settings_row(pre_v060_db, _RE_ARCHIVED)
    assert archived_at == date(2026, 3, 20)
    # before.include_in_net_worth on the winning (most recent) row is False.
    assert include is False


def test_v060_idempotent_on_replay(pre_v060_db: Database) -> None:
    run_migration(pre_v060_db, migrate)
    first = _settings_row(pre_v060_db, _CASCADE_RESTORED)
    run_migration(pre_v060_db, migrate)
    second = _settings_row(pre_v060_db, _CASCADE_RESTORED)
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
