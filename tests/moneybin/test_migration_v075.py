"""V075: add currency and cost-basis change times to app.account_settings.

Touches existing data (backfill), so the fixture holds three populated rows.
"""

from __future__ import annotations

from datetime import datetime

import pytest

from moneybin.database import Database
from moneybin.sql.migrations.V075__add_ledger_setting_change_stamps import migrate
from tests.moneybin.migration_helpers import column_exists, insert_rows, run_migration

_NEW_COLUMNS = ("currency_code_changed_at", "default_cost_basis_method_changed_at")
_SETTINGS_COLUMNS = (
    "account_id",
    "display_name",
    "currency_code",
    "default_cost_basis_method",
    "updated_at",
)
_UPDATED = (
    datetime(2026, 3, 1, 9, 30),
    datetime(2026, 4, 2, 10, 45),
    datetime(2026, 5, 3, 11, 15),
)


@pytest.fixture()
def pre_v075_db(db: Database) -> Database:
    """app.account_settings without the V075 columns, holding three rows."""
    for column in _NEW_COLUMNS:
        db.execute(
            f"ALTER TABLE app.account_settings DROP COLUMN {column}"
        )  # test fixture, hardcoded column name
    insert_rows(
        db,
        "app",
        "account_settings",
        _SETTINGS_COLUMNS,
        [
            ("acc_stamp_a", "Checking A", "EUR", None, _UPDATED[0]),
            ("acc_stamp_b", None, None, "hifo", _UPDATED[1]),
            ("acc_stamp_c", "Brokerage", "USD", "fifo", _UPDATED[2]),
        ],
    )
    return db


def test_v075_backfills_both_stamps_from_updated_at(pre_v075_db: Database) -> None:
    for column in _NEW_COLUMNS:
        assert not column_exists(pre_v075_db, "app", "account_settings", column)
    run_migration(pre_v075_db, migrate)
    rows = pre_v075_db.execute(
        """
        SELECT currency_code_changed_at, default_cost_basis_method_changed_at
        FROM app.account_settings ORDER BY account_id
        """
    ).fetchall()
    assert rows == [(stamp, stamp) for stamp in _UPDATED]


def test_v075_appends_last_with_comments(pre_v075_db: Database) -> None:
    run_migration(pre_v075_db, migrate)
    ordered = pre_v075_db.execute(
        """
        SELECT column_name, comment FROM duckdb_columns()
        WHERE schema_name = 'app' AND table_name = 'account_settings'
        ORDER BY column_index
        """
    ).fetchall()
    assert tuple(name for name, _ in ordered[-2:]) == _NEW_COLUMNS
    assert all(comment for _, comment in ordered[-2:])


def test_v075_replay_keeps_existing_stamps(pre_v075_db: Database) -> None:
    run_migration(pre_v075_db, migrate)
    pre_v075_db.execute(
        "UPDATE app.account_settings SET currency_code_changed_at = ? "
        "WHERE account_id = 'acc_stamp_a'",
        [datetime(2026, 6, 1)],
    )
    run_migration(pre_v075_db, migrate)
    assert pre_v075_db.execute(
        "SELECT currency_code_changed_at FROM app.account_settings "
        "WHERE account_id = 'acc_stamp_a'"
    ).fetchone() == (datetime(2026, 6, 1),)


@pytest.mark.fresh_db
def test_v075_upgrade_column_order_matches_fresh_schema(db: Database) -> None:
    """An upgraded table has the same ordered schema as a fresh install."""
    fresh_schema = [
        (row[1], row[2])
        for row in db.execute("PRAGMA table_info('app.account_settings')").fetchall()
    ]
    for column in _NEW_COLUMNS:
        db.execute(
            f"ALTER TABLE app.account_settings DROP COLUMN {column}"
        )  # test fixture, hardcoded column name

    run_migration(db, migrate)

    upgraded_schema = [
        (row[1], row[2])
        for row in db.execute("PRAGMA table_info('app.account_settings')").fetchall()
    ]
    assert upgraded_schema == fresh_schema
