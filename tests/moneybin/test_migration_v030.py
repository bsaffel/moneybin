"""Tests for V030: add Plaid's extended transaction columns.

V030 adds the default-returned Plaid fields the broker previously discarded to
`raw.plaid_transactions`. Fresh installs get them from the schema DDL; existing
installs get them via this migration. Pure additive (`ADD COLUMN IF NOT EXISTS
... NULL`), so it is idempotent and needs no backfill.
"""

from __future__ import annotations

import pytest

from moneybin.database import Database
from moneybin.sql.migrations.V030__add_plaid_transaction_fields import migrate

pytestmark = pytest.mark.fresh_db

_NEW_COLUMNS = [
    "original_description",
    "iso_currency_code",
    "authorized_date",
    "pending_transaction_id",
    "payment_channel",
    "check_number",
    "merchant_entity_id",
    "location_address",
    "location_city",
    "location_region",
    "location_postal_code",
    "location_country",
    "location_latitude",
    "location_longitude",
    "category_detailed",
    "category_confidence",
]

# The raw.plaid_transactions shape BEFORE V030 (original columns + PK). DuckDB
# forbids dropping a column positioned before the PK-indexed source_origin, so we
# recreate the pre-migration table rather than ALTER ... DROP COLUMN (the
# _recreate_* pattern from test_migration_v012).
_PRE_V030_DDL = """
    CREATE TABLE raw.plaid_transactions (
        transaction_id VARCHAR NOT NULL,
        account_id VARCHAR NOT NULL,
        transaction_date DATE NOT NULL,
        amount DECIMAL(18, 2) NOT NULL,
        description VARCHAR,
        merchant_name VARCHAR,
        category VARCHAR,
        pending BOOLEAN DEFAULT FALSE,
        source_file VARCHAR NOT NULL,
        source_type VARCHAR NOT NULL DEFAULT 'plaid',
        source_origin VARCHAR NOT NULL,
        extracted_at TIMESTAMP,
        loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (transaction_id, source_origin)
    )
"""


def _plaid_txn_columns(db: Database) -> set[str]:
    rows = db.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = 'raw' AND table_name = 'plaid_transactions'"
    ).fetchall()
    return {row[0] for row in rows}


def _recreate_pre_v030_table(db: Database) -> None:
    """Reverse the V030 end-state: rebuild raw.plaid_transactions without the new columns."""
    db.execute("DROP TABLE IF EXISTS raw.plaid_transactions")
    db.execute(_PRE_V030_DDL)
    # One realistic pre-migration row, so we also prove additive-on-populated.
    db.execute(
        "INSERT INTO raw.plaid_transactions "
        "(transaction_id, account_id, transaction_date, amount, description, "
        "pending, source_file, source_type, source_origin) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            "txn_pre_001",
            "acc_001",
            "2026-06-01",
            "12.34",
            "OLD ROW",
            False,
            "sync_old",
            "plaid",
            "item_abc",
        ],
    )


def test_v030_adds_extended_plaid_columns(db: Database) -> None:
    _recreate_pre_v030_table(db)
    before = _plaid_txn_columns(db)
    assert not (before & set(_NEW_COLUMNS)), "setup failed: new columns already present"

    migrate(db._conn)  # pyright: ignore[reportPrivateUsage]

    after = _plaid_txn_columns(db)
    for col in _NEW_COLUMNS:
        assert col in after, f"V030 did not add {col}"

    # The pre-existing row gets NULL for every new column (no backfill).
    row = db.execute(
        "SELECT original_description, iso_currency_code, location_city "
        "FROM raw.plaid_transactions WHERE transaction_id = 'txn_pre_001'"
    ).fetchone()
    assert row == (None, None, None)


def test_v030_is_idempotent(db: Database) -> None:
    """ADD COLUMN IF NOT EXISTS — a second run (columns already present) is a no-op."""
    _recreate_pre_v030_table(db)

    migrate(db._conn)  # pyright: ignore[reportPrivateUsage]
    migrate(db._conn)  # pyright: ignore[reportPrivateUsage]

    after = _plaid_txn_columns(db)
    for col in _NEW_COLUMNS:
        assert col in after
