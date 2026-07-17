"""Tests for V037: add currency_code to raw.ofx_transactions/raw.ofx_balances.

Fresh installs get the column from the schema DDL; existing installs get it
via this migration. Pure additive (`ADD COLUMN IF NOT EXISTS ... NULL`), so
it is idempotent and needs no backfill.
"""

from __future__ import annotations

import pytest

from moneybin.database import Database
from moneybin.sql.migrations.V037__add_currency_code_to_ofx_tables import migrate

pytestmark = pytest.mark.fresh_db

# The pre-V037 shape of raw.ofx_transactions/raw.ofx_balances (no currency_code).
_PRE_V037_TRANSACTIONS_DDL = """
    CREATE TABLE raw.ofx_transactions (
        source_transaction_id VARCHAR,
        account_id VARCHAR,
        transaction_type VARCHAR,
        date_posted TIMESTAMP,
        amount DECIMAL(18, 2),
        payee VARCHAR,
        memo VARCHAR,
        check_number VARCHAR,
        source_file VARCHAR,
        extracted_at TIMESTAMP,
        loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        import_id VARCHAR,
        source_type VARCHAR DEFAULT 'ofx',
        source_origin VARCHAR,
        PRIMARY KEY (source_transaction_id, account_id, source_file)
    )
"""

_PRE_V037_BALANCES_DDL = """
    CREATE TABLE raw.ofx_balances (
        account_id VARCHAR,
        statement_start_date TIMESTAMP,
        statement_end_date TIMESTAMP,
        ledger_balance DECIMAL(18, 2),
        ledger_balance_date TIMESTAMP,
        available_balance DECIMAL(18, 2),
        source_file VARCHAR,
        extracted_at TIMESTAMP,
        loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        import_id VARCHAR,
        source_type VARCHAR DEFAULT 'ofx',
        source_origin VARCHAR,
        PRIMARY KEY (account_id, statement_end_date, source_file)
    )
"""


def _column_exists(db: Database, table: str) -> bool:
    row = db.execute(
        "SELECT 1 FROM duckdb_columns() "
        "WHERE schema_name = 'raw' AND table_name = ? AND column_name = 'currency_code'",
        [table],
    ).fetchone()
    return row is not None


def _recreate_pre_v037_tables(db: Database) -> None:
    """Reverse the V037 end-state: rebuild both tables without currency_code."""
    db.execute("DROP TABLE IF EXISTS raw.ofx_transactions")
    db.execute(_PRE_V037_TRANSACTIONS_DDL)
    db.execute(
        "INSERT INTO raw.ofx_transactions "
        "(source_transaction_id, account_id, transaction_type, date_posted, "
        "amount, payee, source_file, source_type, source_origin) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            "fitid_pre_001",
            "acc_001",
            "DEBIT",
            "2026-06-01 00:00:00",
            "12.34",
            "OLD ROW",
            "chase_2026_06.ofx",
            "ofx",
            "chase",
        ],
    )

    db.execute("DROP TABLE IF EXISTS raw.ofx_balances")
    db.execute(_PRE_V037_BALANCES_DDL)
    db.execute(
        "INSERT INTO raw.ofx_balances "
        "(account_id, statement_end_date, ledger_balance, source_file, "
        "source_type, source_origin) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        [
            "acc_001",
            "2026-06-30 00:00:00",
            "500.00",
            "chase_2026_06.ofx",
            "ofx",
            "chase",
        ],
    )


def test_v037_adds_currency_code_to_both_tables(db: Database) -> None:
    _recreate_pre_v037_tables(db)
    assert not _column_exists(db, "ofx_transactions"), (
        "setup failed: column already present"
    )
    assert not _column_exists(db, "ofx_balances"), (
        "setup failed: column already present"
    )

    migrate(db._conn)  # pyright: ignore[reportPrivateUsage]

    assert _column_exists(db, "ofx_transactions")
    assert _column_exists(db, "ofx_balances")

    # Pre-existing rows get NULL for the new column (no backfill).
    txn_row = db.execute(
        "SELECT currency_code FROM raw.ofx_transactions "
        "WHERE source_transaction_id = 'fitid_pre_001'"
    ).fetchone()
    assert txn_row == (None,)
    bal_row = db.execute(
        "SELECT currency_code FROM raw.ofx_balances WHERE account_id = 'acc_001'"
    ).fetchone()
    assert bal_row == (None,)


def test_v037_is_idempotent(db: Database) -> None:
    """ADD COLUMN IF NOT EXISTS — a second run (column already present) is a no-op."""
    _recreate_pre_v037_tables(db)

    migrate(db._conn)  # pyright: ignore[reportPrivateUsage]
    migrate(db._conn)  # pyright: ignore[reportPrivateUsage]

    assert _column_exists(db, "ofx_transactions")
    assert _column_exists(db, "ofx_balances")
