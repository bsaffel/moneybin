# tests/moneybin/test_services/test_transaction_service.py
"""Tests for TransactionService."""

from __future__ import annotations

from collections.abc import Generator
from pathlib import Path
from unittest.mock import MagicMock

import pytest

import moneybin.database as db_module
from moneybin.database import Database
from moneybin.services.transaction_service import (
    RecurringResult,
    RecurringTransaction,
    Transaction,
    TransactionSearchResult,
    TransactionService,
)
from tests.moneybin.db_helpers import create_core_tables_raw


@pytest.fixture()
def transaction_db(tmp_path: Path) -> Generator[Database, None, None]:
    """Yield a Database with core + app tables and test transactions."""
    mock_store = MagicMock()
    mock_store.get_key.return_value = "test-encryption-key-256bit-placeholder"
    database = Database(
        tmp_path / "test.duckdb",
        secret_store=mock_store,
        no_auto_upgrade=True,
    )
    conn = database.conn
    create_core_tables_raw(conn)

    # Insert test transactions: 3 "Coffee Shop" across months for recurring
    conn.execute("""
        INSERT INTO core.fct_transactions (
            transaction_id, account_id, transaction_date, amount,
            amount_absolute, transaction_direction, description,
            transaction_type, is_pending, currency_code, source_type,
            source_extracted_at, loaded_at,
            transaction_year, transaction_month, transaction_day,
            transaction_day_of_week, transaction_year_month,
            transaction_year_quarter
        ) VALUES
        ('T1', 'A1', '2026-04-10', -50.00, 50.00, 'expense',
         'Coffee Shop', 'DEBIT', false, 'USD', 'ofx',
         '2026-04-10', CURRENT_TIMESTAMP,
         2026, 4, 10, 3, '2026-04', '2026-Q2'),
        ('T2', 'A1', '2026-04-15', 5000.00, 5000.00, 'income',
         'Employer Inc', 'CREDIT', false, 'USD', 'ofx',
         '2026-04-15', CURRENT_TIMESTAMP,
         2026, 4, 15, 1, '2026-04', '2026-Q2'),
        ('T3', 'A1', '2026-03-10', -50.00, 50.00, 'expense',
         'Coffee Shop', 'DEBIT', false, 'USD', 'ofx',
         '2026-03-10', CURRENT_TIMESTAMP,
         2026, 3, 10, 1, '2026-03', '2026-Q1'),
        ('T4', 'A1', '2026-02-10', -50.00, 50.00, 'expense',
         'Coffee Shop', 'DEBIT', false, 'USD', 'ofx',
         '2026-02-10', CURRENT_TIMESTAMP,
         2026, 2, 10, 1, '2026-02', '2026-Q1')
    """)  # noqa: S608  # test input, not executing SQL

    # Categorize one transaction
    conn.execute("""
        INSERT INTO app.transaction_categories
            (transaction_id, category, subcategory, categorized_at,
             categorized_by)
        VALUES
        ('T1', 'Food & Drink', 'Coffee Shops', CURRENT_TIMESTAMP, 'user')
    """)  # noqa: S608  # test input, not executing SQL

    db_module._database_instance = database  # type: ignore[attr-defined]
    yield database
    db_module._database_instance = None  # type: ignore[attr-defined]
    database.close()


class TestTransactionSearch:
    """Tests for TransactionService.search()."""

    @pytest.mark.unit
    def test_returns_search_result(self, transaction_db: Database) -> None:
        service = TransactionService(transaction_db)
        result = service.search()
        assert isinstance(result, TransactionSearchResult)
        assert result.total_count == 4
        assert len(result.transactions) == 4

    @pytest.mark.unit
    def test_transaction_fields(self, transaction_db: Database) -> None:
        service = TransactionService(transaction_db)
        result = service.search()
        txn = next(t for t in result.transactions if t.transaction_id == "T1")
        assert isinstance(txn, Transaction)
        assert txn.account_id == "A1"
        assert txn.amount == -50.00
        assert txn.description == "Coffee Shop"
        assert txn.category == "Food & Drink"

    @pytest.mark.unit
    def test_filter_by_description(self, transaction_db: Database) -> None:
        service = TransactionService(transaction_db)
        result = service.search(description="coffee")
        assert result.total_count == 3
        for txn in result.transactions:
            assert "Coffee" in txn.description

    @pytest.mark.unit
    def test_filter_by_date_range(self, transaction_db: Database) -> None:
        service = TransactionService(transaction_db)
        result = service.search(start_date="2026-04-01", end_date="2026-04-30")
        assert result.total_count == 2

    @pytest.mark.unit
    def test_filter_uncategorized_only(self, transaction_db: Database) -> None:
        service = TransactionService(transaction_db)
        result = service.search(uncategorized_only=True)
        # T1 is categorized, T2/T3/T4 are not
        assert result.total_count == 3
        for txn in result.transactions:
            assert txn.category is None

    @pytest.mark.unit
    def test_limit_and_offset(self, transaction_db: Database) -> None:
        service = TransactionService(transaction_db)
        result = service.search(limit=2, offset=0)
        assert len(result.transactions) == 2
        assert result.total_count == 4

    @pytest.mark.unit
    def test_to_envelope_sensitivity_medium(self, transaction_db: Database) -> None:
        service = TransactionService(transaction_db)
        result = service.search()
        envelope = result.to_envelope()
        d = envelope.to_dict()
        assert d["summary"]["sensitivity"] == "medium"
        assert d["summary"]["total_count"] == 4
        assert isinstance(d["data"], list)


class TestRecurring:
    """Tests for TransactionService.recurring()."""

    @pytest.mark.unit
    def test_returns_recurring_result(self, transaction_db: Database) -> None:
        service = TransactionService(transaction_db)
        result = service.recurring(min_occurrences=3)
        assert isinstance(result, RecurringResult)
        assert len(result.transactions) == 1

    @pytest.mark.unit
    def test_recurring_fields(self, transaction_db: Database) -> None:
        service = TransactionService(transaction_db)
        result = service.recurring(min_occurrences=3)
        rec = result.transactions[0]
        assert isinstance(rec, RecurringTransaction)
        assert rec.description == "Coffee Shop"
        assert rec.occurrence_count == 3
        assert rec.avg_amount == -50.00

    @pytest.mark.unit
    def test_min_occurrences_filter(self, transaction_db: Database) -> None:
        service = TransactionService(transaction_db)
        # With min_occurrences=4, Coffee Shop (3 occurrences) excluded
        result = service.recurring(min_occurrences=4)
        assert len(result.transactions) == 0

    @pytest.mark.unit
    def test_to_envelope_sensitivity_medium(self, transaction_db: Database) -> None:
        service = TransactionService(transaction_db)
        result = service.recurring(min_occurrences=3)
        envelope = result.to_envelope()
        d = envelope.to_dict()
        assert d["summary"]["sensitivity"] == "medium"
