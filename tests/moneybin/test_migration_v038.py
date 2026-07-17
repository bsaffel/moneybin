"""Tests for V038: add {iso_currency_code,unofficial_currency_code} to raw.plaid_balances.

Mirrors the pattern already captured for Plaid securities/investment-
transactions/holdings but never wired into the balances path. Fresh installs
get both columns from the schema DDL; existing installs get them via this
migration. Pure additive (`ADD COLUMN IF NOT EXISTS ... NULL`), so it is
idempotent and needs no backfill.
"""

from __future__ import annotations

import pytest

from moneybin.database import Database
from moneybin.sql.migrations.V038__add_iso_currency_code_to_plaid_balances import (
    migrate,
)

pytestmark = pytest.mark.fresh_db

_NEW_COLUMNS = ["iso_currency_code", "unofficial_currency_code"]

# The pre-V038 shape of raw.plaid_balances (neither currency column).
_PRE_V038_DDL = """
    CREATE TABLE raw.plaid_balances (
        account_id VARCHAR NOT NULL,
        balance_date DATE NOT NULL,
        current_balance DECIMAL(18, 2),
        available_balance DECIMAL(18, 2),
        source_file VARCHAR NOT NULL,
        source_type VARCHAR NOT NULL DEFAULT 'plaid',
        source_origin VARCHAR NOT NULL,
        extracted_at TIMESTAMP,
        loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (account_id, balance_date, source_origin)
    )
"""


def _plaid_balances_columns(db: Database) -> set[str]:
    rows = db.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = 'raw' AND table_name = 'plaid_balances'"
    ).fetchall()
    return {row[0] for row in rows}


def _recreate_pre_v038_table(db: Database) -> None:
    """Reverse the V038 end-state: rebuild raw.plaid_balances without the new columns."""
    db.execute("DROP TABLE IF EXISTS raw.plaid_balances")
    db.execute(_PRE_V038_DDL)
    db.execute(
        "INSERT INTO raw.plaid_balances "
        "(account_id, balance_date, current_balance, available_balance, "
        "source_file, source_type, source_origin) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        [
            "acc_pre_001",
            "2026-06-01",
            "1000.00",
            "950.00",
            "sync_old",
            "plaid",
            "item_abc",
        ],
    )


def test_v038_adds_currency_columns(db: Database) -> None:
    _recreate_pre_v038_table(db)
    before = _plaid_balances_columns(db)
    assert not (before & set(_NEW_COLUMNS)), "setup failed: new columns already present"

    migrate(db._conn)  # pyright: ignore[reportPrivateUsage]

    after = _plaid_balances_columns(db)
    for col in _NEW_COLUMNS:
        assert col in after, f"V038 did not add {col}"

    # The pre-existing row gets NULL for both new columns (no backfill).
    row = db.execute(
        "SELECT iso_currency_code, unofficial_currency_code FROM raw.plaid_balances "
        "WHERE account_id = 'acc_pre_001'"
    ).fetchone()
    assert row == (None, None)


def test_v038_is_idempotent(db: Database) -> None:
    """ADD COLUMN IF NOT EXISTS — a second run (columns already present) is a no-op."""
    _recreate_pre_v038_table(db)

    migrate(db._conn)  # pyright: ignore[reportPrivateUsage]
    migrate(db._conn)  # pyright: ignore[reportPrivateUsage]

    after = _plaid_balances_columns(db)
    for col in _NEW_COLUMNS:
        assert col in after
