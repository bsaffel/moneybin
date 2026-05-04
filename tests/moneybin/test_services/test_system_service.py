"""Tests for SystemService — data inventory and review queue counts."""

from __future__ import annotations

from collections.abc import Generator
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock

import pytest

import moneybin.database as db_module
from moneybin.database import Database
from moneybin.services.system_service import SystemService, SystemStatus
from tests.moneybin.db_helpers import create_core_tables_raw

_INSERT_TRANSACTIONS = """
    INSERT INTO core.fct_transactions (
        transaction_id, account_id, transaction_date, amount,
        amount_absolute, transaction_direction, description,
        transaction_type, is_pending, currency_code, source_type,
        source_extracted_at, loaded_at,
        transaction_year, transaction_month, transaction_day,
        transaction_day_of_week, transaction_year_month, transaction_year_quarter
    ) VALUES
    ('T1', 'ACC001', '2026-03-01', -50.00, 50.00, 'expense', 'Coffee Shop',
     'DEBIT', false, 'USD', 'ofx', '2026-03-01', CURRENT_TIMESTAMP,
     2026, 3, 1, 0, '2026-03', '2026-Q1'),
    ('T2', 'ACC001', '2026-04-15', 5000.00, 5000.00, 'income', 'Employer',
     'CREDIT', false, 'USD', 'ofx', '2026-04-15', CURRENT_TIMESTAMP,
     2026, 4, 15, 1, '2026-04', '2026-Q2')
"""  # noqa: S608  # test input, not executing SQL


@pytest.fixture()
def system_db(tmp_path: Path) -> Generator[Database, None, None]:
    """Database with core + raw tables for SystemService tests."""
    mock_store = MagicMock()
    mock_store.get_key.return_value = "test-encryption-key-256bit-placeholder"
    database = Database(
        tmp_path / "test.duckdb",
        secret_store=mock_store,
        no_auto_upgrade=True,
    )
    conn = database.conn
    create_core_tables_raw(conn)

    conn.execute("""
        INSERT INTO core.dim_accounts VALUES
        ('ACC001', '111000025', 'CHECKING', 'Test Bank', '1234', 'ofx',
         'test.qfx', '2025-01-01', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP,
         'Test Bank CHECKING ...0001', NULL, NULL, NULL, NULL, 'USD', NULL, FALSE, TRUE),
        ('ACC002', '222000050', 'SAVINGS', 'Other Bank', '5678', 'ofx',
         'other.qfx', '2025-01-01', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP,
         'Other Bank SAVINGS ...0002', NULL, NULL, NULL, NULL, 'USD', NULL, FALSE, TRUE)
    """)  # noqa: S608  # test input, not executing SQL

    conn.execute(_INSERT_TRANSACTIONS)

    db_module._database_instance = database  # type: ignore[attr-defined]

    yield database

    db_module._database_instance = None  # type: ignore[attr-defined]
    database.close()


@pytest.mark.unit
def test_status_returns_system_status_type(system_db: Database) -> None:
    """status() returns a SystemStatus dataclass."""
    svc = SystemService(db=system_db)
    result = svc.status()
    assert isinstance(result, SystemStatus)


@pytest.mark.unit
def test_status_has_required_fields(system_db: Database) -> None:
    """SystemStatus has all required fields."""
    svc = SystemService(db=system_db)
    result = svc.status()
    assert hasattr(result, "accounts_count")
    assert hasattr(result, "transactions_count")
    assert hasattr(result, "transactions_date_range")
    assert hasattr(result, "last_import_at")
    assert hasattr(result, "matches_pending")
    assert hasattr(result, "categorize_pending")


@pytest.mark.unit
def test_status_counts_accounts(system_db: Database) -> None:
    """accounts_count reflects the number of rows in dim_accounts."""
    svc = SystemService(db=system_db)
    result = svc.status()
    assert result.accounts_count == 2


@pytest.mark.unit
def test_status_counts_transactions(system_db: Database) -> None:
    """transactions_count reflects the number of rows in fct_transactions."""
    svc = SystemService(db=system_db)
    result = svc.status()
    assert result.transactions_count == 2


@pytest.mark.unit
def test_status_date_range(system_db: Database) -> None:
    """transactions_date_range reflects the actual min/max transaction dates."""
    svc = SystemService(db=system_db)
    result = svc.status()
    min_date, max_date = result.transactions_date_range
    assert min_date == date(2026, 3, 1)
    assert max_date == date(2026, 4, 15)


@pytest.mark.unit
def test_status_date_range_empty_db(tmp_path: Path) -> None:
    """transactions_date_range is (None, None) when no transactions exist."""
    mock_store = MagicMock()
    mock_store.get_key.return_value = "test-encryption-key-256bit-placeholder"
    database = Database(
        tmp_path / "empty.duckdb",
        secret_store=mock_store,
        no_auto_upgrade=True,
    )
    create_core_tables_raw(database.conn)
    try:
        svc = SystemService(db=database)
        result = svc.status()
        assert result.accounts_count == 0
        assert result.transactions_count == 0
        assert result.transactions_date_range == (None, None)
        assert result.last_import_at is None
    finally:
        database.close()


@pytest.mark.unit
def test_status_last_import_at_none_when_table_missing(system_db: Database) -> None:
    """last_import_at is None when import_log table doesn't exist or is empty."""
    svc = SystemService(db=system_db)
    result = svc.status()
    # import_log table exists (created by create_core_tables_raw) but is empty
    assert result.last_import_at is None


@pytest.mark.unit
def test_status_last_import_at_populated(system_db: Database) -> None:
    """last_import_at returns the most recent completed_at from import_log."""
    system_db.conn.execute("""
        INSERT INTO raw.import_log (
            import_id, source_file, source_type, source_origin, account_names,
            status, started_at, completed_at
        ) VALUES
        ('id1', 'a.csv', 'csv', 'test', '[]', 'complete',
         '2026-04-01', '2026-04-01 12:00:00'),
        ('id2', 'b.csv', 'csv', 'test', '[]', 'complete',
         '2026-04-10', '2026-04-10 09:00:00')
    """)  # noqa: S608  # test input, not executing SQL
    svc = SystemService(db=system_db)
    result = svc.status()
    assert result.last_import_at == date(2026, 4, 10)


@pytest.mark.unit
def test_status_queue_counts_integer_types(system_db: Database) -> None:
    """matches_pending and categorize_pending are integers."""
    svc = SystemService(db=system_db)
    result = svc.status()
    assert isinstance(result.matches_pending, int)
    assert isinstance(result.categorize_pending, int)
    # matches_pending is 0 because match_decisions table is empty
    assert result.matches_pending == 0
    # categorize_pending reflects the 2 uncategorized transactions in the fixture
    assert result.categorize_pending == 2
