"""V035: created_by rebuild preserves rows and enforces the CHECK.

V035 rebuilds ``app.securities`` to add the ``created_by`` provenance column
(DuckDB cannot ``ADD COLUMN`` with a CHECK constraint). Per
``.claude/rules/database.md`` "Migration test data realism", the fixture
seeds >=3 rows with non-trivial values across the nullable columns the
rebuild's INSERT...SELECT must preserve, and the mutation tests drive
``migrate()`` through ``run_migration()`` to reproduce the runner's
enclosing BEGIN/COMMIT transaction (the V034 idiom for this same rebuild
pattern).
"""

from __future__ import annotations

from datetime import datetime

import duckdb
import pytest

from moneybin.database import Database
from moneybin.sql.migrations.V035__add_securities_created_by import migrate
from tests.moneybin.migration_helpers import run_migration

pytestmark = pytest.mark.fresh_db

_OLD_SHAPE = """
CREATE TABLE app.securities (
    security_id VARCHAR NOT NULL PRIMARY KEY,
    name VARCHAR NOT NULL,
    security_type VARCHAR NOT NULL,
    ticker VARCHAR, exchange VARCHAR, cusip VARCHAR, isin VARCHAR, figi VARCHAR,
    coingecko_id VARCHAR, is_cash_equivalent BOOLEAN, cost_basis_method VARCHAR,
    currency_code VARCHAR NOT NULL DEFAULT 'USD',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
)
"""


@pytest.fixture
def old_shape_db(db: Database) -> Database:
    """The pre-V035 app.securities shape, populated with >=3 realistic rows."""
    db.execute("DROP TABLE app.securities")
    db.execute(_OLD_SHAPE)
    db.execute(
        """
        INSERT INTO app.securities (
            security_id, name, security_type, ticker, exchange, cusip, isin,
            figi, coingecko_id, is_cash_equivalent, cost_basis_method,
            currency_code, created_at, updated_at
        ) VALUES
        ('abc123def456', 'Apple Inc.', 'equity', 'AAPL', 'NASDAQ',
         '037833100', 'US0378331005', 'BBG000B9XRY4', NULL, FALSE, 'fifo',
         'USD', TIMESTAMP '2024-01-02 03:04:05', TIMESTAMP '2024-01-02 03:04:05'),
        ('bitcoin000001', 'Bitcoin', 'crypto', NULL, NULL, NULL, NULL, NULL,
         'bitcoin', FALSE, NULL, 'USD',
         TIMESTAMP '2024-02-03 04:05:06', TIMESTAMP '2024-02-03 04:05:06'),
        ('moneymkt000001', 'Fidelity Government MM', 'cash', NULL, NULL, NULL,
         NULL, NULL, NULL, TRUE, NULL, 'USD',
         TIMESTAMP '2024-03-04 05:06:07', TIMESTAMP '2024-03-04 05:06:07')
        """
    )
    return db


def test_v035_backfills_user_and_preserves_rows(old_shape_db: Database) -> None:
    run_migration(old_shape_db, migrate)
    rows = old_shape_db.execute(
        "SELECT security_id, created_by FROM app.securities ORDER BY security_id"
    ).fetchall()
    assert rows == [
        ("abc123def456", "user"),
        ("bitcoin000001", "user"),
        ("moneymkt000001", "user"),
    ]


def test_v035_preserves_all_existing_columns(old_shape_db: Database) -> None:
    """The rebuild's INSERT...SELECT column list must not silently drop a column."""
    run_migration(old_shape_db, migrate)

    row = old_shape_db.execute(
        """
        SELECT name, security_type, ticker, exchange, cusip, isin, figi,
               coingecko_id, is_cash_equivalent, cost_basis_method,
               currency_code, created_at, updated_at
          FROM app.securities
         WHERE security_id = 'abc123def456'
        """
    ).fetchone()
    assert row == (
        "Apple Inc.",
        "equity",
        "AAPL",
        "NASDAQ",
        "037833100",
        "US0378331005",
        "BBG000B9XRY4",
        None,
        False,
        "fifo",
        "USD",
        datetime(2024, 1, 2, 3, 4, 5),
        datetime(2024, 1, 2, 3, 4, 5),
    )


def test_v035_is_idempotent(old_shape_db: Database) -> None:
    run_migration(old_shape_db, migrate)
    run_migration(old_shape_db, migrate)
    row = old_shape_db.execute("SELECT COUNT(*) FROM app.securities").fetchone()
    assert row is not None and row[0] == 3


def test_v035_check_rejects_unknown_provenance(old_shape_db: Database) -> None:
    run_migration(old_shape_db, migrate)
    with pytest.raises(duckdb.ConstraintException):
        old_shape_db.execute(
            "INSERT INTO app.securities (security_id, name, security_type, created_by) "
            "VALUES ('x1y2z3a4b5c6', 'X Corp', 'equity', 'ofx')"
        )


def test_fresh_schema_has_security_link_tables(db: Database) -> None:
    for table in ("app.security_links", "app.security_link_decisions"):
        row = db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()  # noqa: S608  # fixed table list
        assert row is not None and row[0] == 0
