"""V053: drop the ``'USD'`` DDL default on manual investment events.

The load-bearing claims are about rows that predate the change: the default
cannot be told apart from a user who typed ``USD``, so the migration must leave
every existing value exactly where it is and only stop the *next* omitted
currency from being fabricated. Populated fixtures are what prove the "no
backfill, no rewrite" half.
"""

from __future__ import annotations

import pytest

from moneybin.database import Database
from moneybin.sql.migrations.V053__drop_manual_investment_currency_default import (
    migrate,
)
from tests.moneybin.migration_helpers import run_migration

_PRE_V053_DDL = """
CREATE TABLE raw.manual_investment_transactions (
    source_transaction_id VARCHAR PRIMARY KEY,
    source_type VARCHAR NOT NULL DEFAULT 'manual',
    source_origin VARCHAR NOT NULL DEFAULT 'user',
    import_id VARCHAR NOT NULL,
    account_id VARCHAR NOT NULL,
    security_id VARCHAR,
    security_ref VARCHAR,
    type VARCHAR NOT NULL,
    subtype VARCHAR,
    event_group_id VARCHAR,
    trade_date DATE NOT NULL,
    settlement_date DATE,
    original_acquisition_date DATE,
    quantity DECIMAL(28, 10),
    price DECIMAL(28, 10),
    amount DECIMAL(18, 2),
    fees DECIMAL(18, 2),
    currency_code VARCHAR DEFAULT 'USD',
    description VARCHAR,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR NOT NULL,
    investment_transaction_id VARCHAR
)
"""

_SEED = """
INSERT INTO raw.manual_investment_transactions
    (source_transaction_id, import_id, account_id, security_id, type,
     trade_date, quantity, amount, currency_code, created_by,
     investment_transaction_id)
VALUES
    ('manual_a1b2c3d4e5f6', 'import_a1b2c3d4', 'acct_a1b2c3d4', 'sec_a1b2c3d4',
     'buy', '2026-01-15'::DATE, 10::DECIMAL(28,10), -1504.95::DECIMAL(18,2),
     'USD', 'cli', 'inv_a1b2c3d4e5f6'),
    ('manual_b2c3d4e5f6a1', 'import_a1b2c3d4', 'acct_b2c3d4e5', 'sec_b2c3d4e5',
     'buy', '2026-02-20'::DATE, 4::DECIMAL(28,10), -812.40::DECIMAL(18,2),
     'EUR', 'mcp', 'inv_b2c3d4e5f6a1'),
    ('manual_c3d4e5f6a1b2', 'import_c3d4e5f6', 'acct_b2c3d4e5', NULL,
     'dividend', '2026-03-31'::DATE, NULL, 27.10::DECIMAL(18,2),
     NULL, 'cli', 'inv_c3d4e5f6a1b2')
"""


def _column_default(db: Database) -> str | None:
    row = db.execute(
        """
        SELECT column_default FROM duckdb_columns()
        WHERE schema_name = 'raw'
          AND table_name = 'manual_investment_transactions'
          AND column_name = 'currency_code'
        """
    ).fetchone()
    assert row is not None, "raw.manual_investment_transactions.currency_code not found"
    return row[0]


@pytest.fixture
def pre_v053_db(db: Database) -> Database:
    """A populated table still carrying the ``DEFAULT 'USD'`` the migration drops.

    Rebuilt from the pre-V053 DDL rather than ALTERed, so the fixture is the
    table shape an existing database actually has.
    """
    db.execute("DROP TABLE raw.manual_investment_transactions")
    db.execute(_PRE_V053_DDL)
    db.execute(_SEED)
    assert _column_default(db) == "'USD'", "fixture must start with the default"
    return db


@pytest.mark.unit
def test_migrate_drops_the_currency_default(pre_v053_db: Database) -> None:
    """After V053 the column has no default at all."""
    run_migration(pre_v053_db, migrate)

    assert _column_default(pre_v053_db) is None


@pytest.mark.unit
def test_migrate_leaves_every_existing_currency_untouched(
    pre_v053_db: Database,
) -> None:
    """No backfill: a stored ``'USD'`` is indistinguishable from a typed one."""
    run_migration(pre_v053_db, migrate)

    rows = pre_v053_db.execute(
        "SELECT source_transaction_id, currency_code "
        "FROM raw.manual_investment_transactions ORDER BY source_transaction_id"
    ).fetchall()
    assert rows == [
        ("manual_a1b2c3d4e5f6", "USD"),
        ("manual_b2c3d4e5f6a1", "EUR"),
        ("manual_c3d4e5f6a1b2", None),
    ]


@pytest.mark.unit
def test_after_migrate_an_omitted_currency_stays_null(pre_v053_db: Database) -> None:
    """The point of the migration: the next omitted currency is not fabricated."""
    run_migration(pre_v053_db, migrate)

    pre_v053_db.execute(
        """
        INSERT INTO raw.manual_investment_transactions
            (source_transaction_id, import_id, account_id, type, trade_date,
             created_by)
        VALUES ('manual_d4e5f6a1b2c3', 'import_d4e5f6a1', 'acct_b2c3d4e5',
                'deposit', '2026-04-02'::DATE, 'cli')
        """
    )
    row = pre_v053_db.execute(
        "SELECT currency_code FROM raw.manual_investment_transactions "
        "WHERE source_transaction_id = 'manual_d4e5f6a1b2c3'"
    ).fetchone()
    assert row is not None
    assert row[0] is None


@pytest.mark.unit
def test_migrate_is_idempotent(pre_v053_db: Database) -> None:
    """Re-running against an already-migrated table is a no-op, not an error."""
    run_migration(pre_v053_db, migrate)
    run_migration(pre_v053_db, migrate)

    assert _column_default(pre_v053_db) is None
