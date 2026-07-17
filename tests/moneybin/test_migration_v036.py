"""Tests for V036: rename app.account_settings.iso_currency_code -> currency_code."""

from __future__ import annotations

import pytest

from moneybin.database import Database
from moneybin.sql.migrations.V036__rename_iso_currency_code_to_currency_code import (
    migrate,
)
from tests.moneybin.migration_helpers import column_exists, insert_rows, run_migration

pytestmark = pytest.mark.fresh_db

_ROWS: list[tuple[str, str | None]] = [
    ("acct_checking01", "USD"),
    ("acct_savings002", "EUR"),
    ("acct_creditcrd3", None),
]


def _reset_to_pre_v036_state(db: Database) -> None:
    """Reverse the V036 end-state so the migration has work to do."""
    db.execute(
        "ALTER TABLE app.account_settings RENAME COLUMN currency_code TO iso_currency_code"
    )


def _populate(db: Database) -> None:
    insert_rows(
        db, "app", "account_settings", ("account_id", "iso_currency_code"), _ROWS
    )


class TestV036Migration:
    """V036 renames app.account_settings.iso_currency_code, idempotently."""

    def test_v036_renames_column(self, db: Database) -> None:
        _reset_to_pre_v036_state(db)
        _populate(db)
        assert column_exists(db, "app", "account_settings", "iso_currency_code")
        assert not column_exists(db, "app", "account_settings", "currency_code")

        run_migration(db, migrate)

        assert not column_exists(db, "app", "account_settings", "iso_currency_code")
        assert column_exists(db, "app", "account_settings", "currency_code")

    def test_v036_preserves_data(self, db: Database) -> None:
        _reset_to_pre_v036_state(db)
        _populate(db)

        run_migration(db, migrate)

        rows = db.execute(
            "SELECT account_id, currency_code FROM app.account_settings "
            "ORDER BY account_id"
        ).fetchall()
        assert rows == sorted(_ROWS)

    def test_v036_idempotent_on_second_run(self, db: Database) -> None:
        _reset_to_pre_v036_state(db)
        _populate(db)
        run_migration(db, migrate)
        run_migration(db, migrate)
        assert column_exists(db, "app", "account_settings", "currency_code")

    def test_v036_idempotent_on_fresh_install(self, db: Database) -> None:
        # No reset — db comes from init_schemas with the final shape already.
        run_migration(db, migrate)
        assert column_exists(db, "app", "account_settings", "currency_code")
