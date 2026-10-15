"""V074: add app.account_settings.archived_changed_at.

Pure additive DDL, but the fixture is still populated (3 rows, one archived) so
the upgrade is shown to leave existing settings rows at NULL.
"""

from __future__ import annotations

import pytest

from moneybin.database import Database
from moneybin.sql.migrations.V074__add_archived_changed_at import migrate
from tests.moneybin.migration_helpers import column_exists, insert_rows, run_migration

_NEW_COLUMN = "archived_changed_at"
_SETTINGS_COLUMNS = ("account_id", "display_name", "archived", "include_in_net_worth")


@pytest.fixture()
def pre_v074_db(db: Database) -> Database:
    """app.account_settings without the V074 column, holding three rows."""
    db.execute(
        f"ALTER TABLE app.account_settings DROP COLUMN {_NEW_COLUMN}"
    )  # test fixture, hardcoded column name
    insert_rows(
        db,
        "app",
        "account_settings",
        _SETTINGS_COLUMNS,
        [
            ("acc_arch_a", "Checking A", False, True),
            ("acc_arch_b", "Old Savings", True, False),
            ("acc_arch_c", None, False, True),
        ],
    )
    return db


def test_v074_adds_the_column_as_null(pre_v074_db: Database) -> None:
    assert not column_exists(pre_v074_db, "app", "account_settings", _NEW_COLUMN)
    run_migration(pre_v074_db, migrate)
    assert column_exists(pre_v074_db, "app", "account_settings", _NEW_COLUMN)
    rows = pre_v074_db.execute(
        "SELECT archived_changed_at FROM app.account_settings"
    ).fetchall()
    assert rows == [(None,), (None,), (None,)]


def test_v074_appends_last_with_a_comment(pre_v074_db: Database) -> None:
    run_migration(pre_v074_db, migrate)
    ordered = [
        r[0]
        for r in pre_v074_db.execute(
            "SELECT column_name FROM duckdb_columns() "
            "WHERE schema_name = 'app' AND table_name = 'account_settings' "
            "ORDER BY column_index"
        ).fetchall()
    ]
    assert ordered[-1] == _NEW_COLUMN
    comment = pre_v074_db.execute(
        "SELECT comment FROM duckdb_columns() "
        "WHERE schema_name = 'app' AND table_name = 'account_settings' "
        "AND column_name = ?",
        [_NEW_COLUMN],
    ).fetchone()
    assert comment is not None and comment[0]


def test_v074_is_a_no_op_on_replay(pre_v074_db: Database) -> None:
    run_migration(pre_v074_db, migrate)
    run_migration(pre_v074_db, migrate)
    assert pre_v074_db.execute(
        "SELECT COUNT(*) FROM app.account_settings"
    ).fetchone() == (3,)


@pytest.mark.fresh_db
def test_v074_upgrade_column_order_matches_fresh_schema(db: Database) -> None:
    """An upgraded table has the same ordered schema as a fresh install."""
    fresh_schema = [
        (row[1], row[2])
        for row in db.execute("PRAGMA table_info('app.account_settings')").fetchall()
    ]
    db.execute(
        f"ALTER TABLE app.account_settings DROP COLUMN {_NEW_COLUMN}"
    )  # test fixture, hardcoded column name

    run_migration(db, migrate)

    upgraded_schema = [
        (row[1], row[2])
        for row in db.execute("PRAGMA table_info('app.account_settings')").fetchall()
    ]
    assert upgraded_schema == fresh_schema
