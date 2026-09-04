"""V054: reserve the received leg for single-row currency conversions."""

from __future__ import annotations

from decimal import Decimal

import pytest

from moneybin.database import Database
from moneybin.sql.migrations.V054__add_currency_conversion_shape import migrate
from tests.moneybin.migration_helpers import run_migration

_TABLES = (
    "raw.ofx_transactions",
    "raw.tabular_transactions",
    "raw.plaid_transactions",
    "raw.manual_transactions",
)


@pytest.fixture
def pre_v054_db(db: Database) -> Database:
    """Four populated pre-V054 tables without the received-leg columns."""
    for table in _TABLES:
        db.execute(f"DROP TABLE {table}")  # noqa: S608  # closed internal table set
        db.execute(  # noqa: S608  # closed internal table set
            f"CREATE TABLE {table} (source_transaction_id VARCHAR, amount DECIMAL(18, 2))"
        )
        db.execute(
            f"INSERT INTO {table} VALUES ('txn-before-v054', 125.50)"  # noqa: S608  # closed internal table set
        )
    return db


@pytest.mark.parametrize("table", _TABLES)
def test_v054_adds_nullable_received_leg_without_rewriting_rows(
    pre_v054_db: Database, table: str
) -> None:
    run_migration(pre_v054_db, migrate)
    run_migration(pre_v054_db, migrate)

    columns = {
        row[1]
        for row in pre_v054_db.execute(
            f"PRAGMA table_info('{table}')"  # noqa: S608  # closed internal table set
        ).fetchall()
    }
    assert {"to_amount", "to_currency"} <= columns
    assert pre_v054_db.execute(
        f"SELECT source_transaction_id, amount, to_amount, to_currency FROM {table}"  # noqa: S608  # closed internal table set
    ).fetchall() == [("txn-before-v054", Decimal("125.50"), None, None)]


@pytest.mark.fresh_db
@pytest.mark.parametrize("table", _TABLES)
def test_v054_upgrade_column_order_matches_fresh_schema(
    db: Database, table: str
) -> None:
    """An upgraded table has the same ordered schema as a fresh install."""
    fresh_schema = [
        (row[1], row[2])
        for row in db.execute(
            f"PRAGMA table_info('{table}')"  # noqa: S608  # closed internal table set
        ).fetchall()
    ]
    schema, table_name = table.split(".")
    pre_v054_table = f"{schema}._pre_v054_{table_name}"
    db.execute(
        f"CREATE TABLE {pre_v054_table} AS "  # noqa: S608  # closed internal table set
        f"SELECT * EXCLUDE (to_amount, to_currency) FROM {table} LIMIT 0"
    )
    db.execute(
        f"DROP TABLE {table} CASCADE"  # noqa: S608  # isolated test database
    )
    db.execute(
        f"ALTER TABLE {pre_v054_table} RENAME TO {table_name}"  # noqa: S608  # closed internal table set
    )

    run_migration(db, migrate)

    upgraded_schema = [
        (row[1], row[2])
        for row in db.execute(
            f"PRAGMA table_info('{table}')"  # noqa: S608  # closed internal table set
        ).fetchall()
    ]
    assert upgraded_schema == fresh_schema
