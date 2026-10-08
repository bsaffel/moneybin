"""V069: add app.account_settings.investment_source_type and its change time.

Pure additive DDL, but the fixture is still populated (3 rows) so the upgrade is
shown to leave existing settings rows at NULL (today's behavior: every source).
"""

from __future__ import annotations

import pytest

from moneybin.database import Database
from moneybin.sql.migrations.V069__add_investment_source_type import migrate
from tests.moneybin.migration_helpers import column_exists, insert_rows, run_migration

_NEW_COLUMNS = ("investment_source_type", "investment_source_type_changed_at")
_SETTINGS_COLUMNS = ("account_id", "display_name", "archived", "include_in_net_worth")


@pytest.fixture()
def pre_v069_db(db: Database) -> Database:
    """app.account_settings without the V069 columns, holding three rows."""
    for column in _NEW_COLUMNS:
        db.execute(
            f"ALTER TABLE app.account_settings DROP COLUMN {column}"
        )  # test fixture, hardcoded column names
    insert_rows(
        db,
        "app",
        "account_settings",
        _SETTINGS_COLUMNS,
        [
            ("acc_choice_a", "Brokerage A", False, True),
            ("acc_choice_b", "Brokerage B", True, False),
            ("acc_choice_c", None, False, True),
        ],
    )
    return db


def _column_comment(db: Database, column: str) -> str | None:
    row = db.execute(
        "SELECT comment FROM duckdb_columns() "
        "WHERE schema_name = 'app' AND table_name = 'account_settings' "
        "AND column_name = ?",
        [column],
    ).fetchone()
    assert row is not None
    return row[0]


def test_v069_adds_both_columns_as_null(pre_v069_db: Database) -> None:
    for column in _NEW_COLUMNS:
        assert not column_exists(pre_v069_db, "app", "account_settings", column)
    run_migration(pre_v069_db, migrate)
    for column in _NEW_COLUMNS:
        assert column_exists(pre_v069_db, "app", "account_settings", column)
    rows = pre_v069_db.execute(
        "SELECT investment_source_type, investment_source_type_changed_at "
        "FROM app.account_settings"
    ).fetchall()
    assert len(rows) == 3
    assert all(r == (None, None) for r in rows)


def test_v069_appends_after_archived_at_with_comments(pre_v069_db: Database) -> None:
    run_migration(pre_v069_db, migrate)
    ordered = [
        r[0]
        for r in pre_v069_db.execute(
            "SELECT column_name FROM duckdb_columns() "
            "WHERE schema_name = 'app' AND table_name = 'account_settings' "
            "ORDER BY column_index"
        ).fetchall()
    ]
    assert ordered[-2:] == list(_NEW_COLUMNS)
    assert ordered.index("archived_at") < ordered.index("investment_source_type")
    for column in _NEW_COLUMNS:
        assert _column_comment(pre_v069_db, column)


def test_v069_is_a_no_op_on_replay(pre_v069_db: Database) -> None:
    run_migration(pre_v069_db, migrate)
    run_migration(pre_v069_db, migrate)
    assert pre_v069_db.execute(
        "SELECT COUNT(*) FROM app.account_settings"
    ).fetchone() == (3,)


@pytest.mark.fresh_db
def test_v069_upgrade_column_order_matches_fresh_schema(db: Database) -> None:
    """An upgraded table has the same ordered schema as a fresh install."""
    fresh_schema = [
        (row[1], row[2])
        for row in db.execute("PRAGMA table_info('app.account_settings')").fetchall()
    ]
    for column in _NEW_COLUMNS:
        db.execute(
            f"ALTER TABLE app.account_settings DROP COLUMN {column}"
        )  # test fixture, hardcoded column names

    run_migration(db, migrate)

    upgraded_schema = [
        (row[1], row[2])
        for row in db.execute("PRAGMA table_info('app.account_settings')").fetchall()
    ]
    assert upgraded_schema == fresh_schema
