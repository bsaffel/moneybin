"""Tests for TransactionService.get()."""

from __future__ import annotations

from collections.abc import Generator
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock

import pytest

import moneybin.database as db_module
from moneybin.database import Database
from moneybin.services.transaction_service import Transaction, TransactionGetResult, TransactionService
from tests.moneybin.db_helpers import create_core_tables_raw


@pytest.fixture()
def txn_db(tmp_path: Path) -> Generator[Database, None, None]:
    """Database with core+app tables and 4 test transactions."""
    mock_store = MagicMock()
    mock_store.get_key.return_value = "test-encryption-key-256bit-placeholder"
    database = Database(tmp_path / "test.duckdb", secret_store=mock_store, no_auto_upgrade=True)
    conn = database.conn
    create_core_tables_raw(conn)

    conn.execute("""
        INSERT INTO core.dim_accounts
            (account_id, routing_number, account_type, institution_name, institution_fid,
             source_type, source_file, extracted_at, loaded_at, updated_at)
        VALUES
        ('A1', '111000025', 'CHECKING', 'Test Bank', '1234', 'ofx',
         'test.qfx', '2026-01-01', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
        ('A2', '222000050', 'SAVINGS', 'Other Bank', '5678', 'ofx',
         'other.qfx', '2026-01-01', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
    """)  # noqa: S608  # test input, not executing SQL

    conn.execute("""
        INSERT INTO core.fct_transactions (
            transaction_id, account_id, transaction_date, amount,
            amount_absolute, transaction_direction, description,
            transaction_type, is_pending, currency_code, source_type,
            source_extracted_at, loaded_at,
            transaction_year, transaction_month, transaction_day,
            transaction_day_of_week, transaction_year_month,
            transaction_year_quarter,
            category, notes, tags, splits
        ) VALUES
        ('T1', 'A1', '2026-04-10', -50.00, 50.00, 'expense',
         'Coffee Shop', 'DEBIT', false, 'USD', 'ofx',
         '2026-04-10', CURRENT_TIMESTAMP,
         2026, 4, 10, 3, '2026-04', '2026-Q2',
         'Food & Drink', NULL, ['work', 'lunch'], NULL),
        ('T2', 'A1', '2026-04-15', 5000.00, 5000.00, 'income',
         'Employer Inc', 'CREDIT', false, 'USD', 'ofx',
         '2026-04-15', CURRENT_TIMESTAMP,
         2026, 4, 15, 1, '2026-04', '2026-Q2',
         NULL, NULL, NULL, NULL),
        ('T3', 'A2', '2026-03-10', -50.00, 50.00, 'expense',
         'Coffee Shop', 'DEBIT', false, 'USD', 'ofx',
         '2026-03-10', CURRENT_TIMESTAMP,
         2026, 3, 10, 1, '2026-03', '2026-Q1',
         'Food & Drink', NULL, NULL, NULL),
        ('T4', 'A1', '2026-02-10', -200.00, 200.00, 'expense',
         'Rent Payment', 'DEBIT', false, 'USD', 'ofx',
         '2026-02-10', CURRENT_TIMESTAMP,
         2026, 2, 10, 1, '2026-02', '2026-Q1',
         NULL, NULL, NULL, NULL)
    """)  # noqa: S608  # test input, not executing SQL

    db_module._database_instance = database  # type: ignore[attr-defined]
    yield database
    db_module._database_instance = None  # type: ignore[attr-defined]
    database.close()


class TestTransactionGet:
    """Tests for TransactionService.get()."""

    @pytest.mark.unit
    def test_returns_get_result(self, txn_db: Database) -> None:
        result = TransactionService(txn_db).get()
        assert isinstance(result, TransactionGetResult)
        assert len(result.transactions) == 4

    @pytest.mark.unit
    def test_transaction_fields(self, txn_db: Database) -> None:
        result = TransactionService(txn_db).get()
        t = next(x for x in result.transactions if x.transaction_id == "T1")
        assert isinstance(t, Transaction)
        assert t.account_id == "A1"
        assert t.amount == Decimal("-50.00")
        assert t.description == "Coffee Shop"
        assert t.category == "Food & Drink"

    @pytest.mark.unit
    def test_curation_fields_populated(self, txn_db: Database) -> None:
        result = TransactionService(txn_db).get()
        t = next(x for x in result.transactions if x.transaction_id == "T1")
        assert t.tags == ["work", "lunch"]
        assert t.notes is None
        assert t.splits is None

    @pytest.mark.unit
    def test_curation_fields_null_when_absent(self, txn_db: Database) -> None:
        result = TransactionService(txn_db).get()
        t = next(x for x in result.transactions if x.transaction_id == "T2")
        assert t.tags is None
        assert t.notes is None
        assert t.splits is None

    @pytest.mark.unit
    def test_filter_by_date_from(self, txn_db: Database) -> None:
        result = TransactionService(txn_db).get(date_from="2026-04-01")
        assert len(result.transactions) == 2
        for t in result.transactions:
            assert t.transaction_date >= "2026-04-01"

    @pytest.mark.unit
    def test_filter_by_date_to(self, txn_db: Database) -> None:
        result = TransactionService(txn_db).get(date_to="2026-03-31")
        assert len(result.transactions) == 2
        for t in result.transactions:
            assert t.transaction_date <= "2026-03-31"

    @pytest.mark.unit
    def test_filter_by_amount_min(self, txn_db: Database) -> None:
        # income only (positive)
        result = TransactionService(txn_db).get(amount_min=Decimal("0"))
        assert len(result.transactions) == 1
        assert result.transactions[0].transaction_id == "T2"

    @pytest.mark.unit
    def test_filter_by_categories(self, txn_db: Database) -> None:
        result = TransactionService(txn_db).get(categories=["Food & Drink"])
        assert len(result.transactions) == 2
        for t in result.transactions:
            assert t.category == "Food & Drink"

    @pytest.mark.unit
    def test_filter_uncategorized_only(self, txn_db: Database) -> None:
        result = TransactionService(txn_db).get(uncategorized_only=True)
        assert len(result.transactions) == 2
        for t in result.transactions:
            assert t.category is None

    @pytest.mark.unit
    def test_filter_by_description(self, txn_db: Database) -> None:
        result = TransactionService(txn_db).get(description="coffee")
        assert len(result.transactions) == 2
        for t in result.transactions:
            assert "Coffee" in t.description

    @pytest.mark.unit
    def test_filter_by_exact_account_id(self, txn_db: Database) -> None:
        result = TransactionService(txn_db).get(accounts=["A2"])
        assert len(result.transactions) == 1
        assert result.transactions[0].transaction_id == "T3"

    @pytest.mark.unit
    def test_filter_by_display_name(self, txn_db: Database) -> None:
        result = TransactionService(txn_db).get(accounts=["Test Bank"])
        assert len(result.transactions) == 3
        for t in result.transactions:
            assert t.account_id == "A1"

    @pytest.mark.unit
    def test_multi_account_filter(self, txn_db: Database) -> None:
        result = TransactionService(txn_db).get(accounts=["A1", "A2"])
        assert len(result.transactions) == 4

    @pytest.mark.unit
    def test_unresolvable_account_silently_skipped(self, txn_db: Database) -> None:
        result = TransactionService(txn_db).get(accounts=["DOES_NOT_EXIST_XYZ"])
        assert len(result.transactions) == 0

    @pytest.mark.unit
    def test_cursor_pagination(self, txn_db: Database) -> None:
        first = TransactionService(txn_db).get(limit=2)
        assert len(first.transactions) == 2
        assert first.next_cursor is not None

        second = TransactionService(txn_db).get(limit=2, cursor=first.next_cursor)
        assert len(second.transactions) == 2
        assert second.next_cursor is None

        all_ids = {t.transaction_id for t in first.transactions} | {t.transaction_id for t in second.transactions}
        assert all_ids == {"T1", "T2", "T3", "T4"}

    @pytest.mark.unit
    def test_no_next_cursor_when_fits_in_one_page(self, txn_db: Database) -> None:
        result = TransactionService(txn_db).get(limit=10)
        assert result.next_cursor is None

    @pytest.mark.unit
    def test_empty_result(self, txn_db: Database) -> None:
        result = TransactionService(txn_db).get(date_from="2030-01-01")
        assert result.transactions == []
        assert result.next_cursor is None

    @pytest.mark.unit
    def test_to_envelope_sensitivity_medium(self, txn_db: Database) -> None:
        result = TransactionService(txn_db).get()
        d = result.to_envelope().to_dict()
        assert d["summary"]["sensitivity"] == "medium"
        assert isinstance(d["data"], list)

    @pytest.mark.unit
    def test_to_envelope_next_cursor_propagated(self, txn_db: Database) -> None:
        result = TransactionService(txn_db).get(limit=2)
        d = result.to_envelope().to_dict()
        assert "next_cursor" in d
        assert d["next_cursor"] == result.next_cursor
