"""Tests for CSV loader."""

from pathlib import Path
from unittest.mock import MagicMock

import polars as pl
import pytest

from moneybin.database import Database
from moneybin.loaders.csv_loader import CSVLoader


@pytest.fixture()
def test_db(tmp_path: Path) -> Database:
    """A temporary Database instance for testing."""
    mock_store = MagicMock()
    mock_store.get_key.return_value = "test-key"
    return Database(
        tmp_path / "test.duckdb", secret_store=mock_store, no_auto_upgrade=True
    )


@pytest.fixture()
def sample_accounts() -> pl.DataFrame:
    """A sample accounts DataFrame."""
    return pl.DataFrame([
        {
            "account_id": "chase-7022",
            "account_type": None,
            "institution_name": "Chase",
            "source_file": "/test/chase.csv",
            "extracted_at": "2025-12-16T10:00:00",
        }
    ])


@pytest.fixture()
def sample_transactions() -> pl.DataFrame:
    """A sample transactions DataFrame."""
    return pl.DataFrame([
        {
            "transaction_id": "csv_abc123",
            "account_id": "chase-7022",
            "transaction_date": "2025-12-16",
            "post_date": "2025-12-17",
            "amount": -41.67,
            "description": "TARGET.COM",
            "memo": None,
            "category": "Shopping",
            "subcategory": None,
            "transaction_type": "Sale",
            "transaction_status": None,
            "check_number": None,
            "reference_number": None,
            "balance": None,
            "member_name": None,
            "source_file": "/test/chase.csv",
            "extracted_at": "2025-12-16T10:00:00",
        },
        {
            "transaction_id": "csv_def456",
            "account_id": "chase-7022",
            "transaction_date": "2025-12-15",
            "post_date": "2025-12-16",
            "amount": -5.75,
            "description": "STARBUCKS",
            "memo": None,
            "category": "Food & Drink",
            "subcategory": None,
            "transaction_type": "Sale",
            "transaction_status": None,
            "check_number": None,
            "reference_number": None,
            "balance": None,
            "member_name": None,
            "source_file": "/test/chase.csv",
            "extracted_at": "2025-12-16T10:00:00",
        },
    ])


class TestCSVLoaderTableCreation:
    """Test raw table creation."""

    def test_creates_tables(self, test_db: Database) -> None:
        loader = CSVLoader(test_db)
        loader.create_raw_tables()

        tables = test_db.conn.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'raw'
            ORDER BY table_name
        """).fetchall()
        table_names = [t[0] for t in tables]

        assert "csv_accounts" in table_names
        assert "csv_transactions" in table_names


class TestCSVLoaderData:
    """Test loading data into raw tables."""

    def test_load_data(
        self,
        test_db: Database,
        sample_accounts: pl.DataFrame,
        sample_transactions: pl.DataFrame,
    ) -> None:
        loader = CSVLoader(test_db)
        row_counts = loader.load_data({
            "accounts": sample_accounts,
            "transactions": sample_transactions,
        })

        assert row_counts["accounts"] == 1
        assert row_counts["transactions"] == 2

    def test_idempotent_reload(
        self,
        test_db: Database,
        sample_accounts: pl.DataFrame,
        sample_transactions: pl.DataFrame,
    ) -> None:
        """Loading the same data twice should not create duplicates."""
        loader = CSVLoader(test_db)
        data = {"accounts": sample_accounts, "transactions": sample_transactions}

        loader.load_data(data)
        loader.load_data(data)

        count = test_db.conn.execute(
            "SELECT COUNT(*) FROM raw.csv_transactions"
        ).fetchone()

        assert count is not None
        assert count[0] == 2  # Not 4

    def test_query_loaded_data(
        self,
        test_db: Database,
        sample_accounts: pl.DataFrame,
        sample_transactions: pl.DataFrame,
    ) -> None:
        loader = CSVLoader(test_db)
        loader.load_data({
            "accounts": sample_accounts,
            "transactions": sample_transactions,
        })

        conn = test_db.conn

        # Check accounts
        acct = conn.execute("SELECT institution_name FROM raw.csv_accounts").fetchone()
        assert acct is not None
        assert acct[0] == "Chase"

        # Check transactions
        txns = conn.execute(
            "SELECT description, amount FROM raw.csv_transactions ORDER BY amount"
        ).fetchall()
        assert len(txns) == 2
        assert txns[0][0] == "TARGET.COM"

    def test_empty_data(self, test_db: Database) -> None:
        loader = CSVLoader(test_db)
        row_counts = loader.load_data({
            "accounts": pl.DataFrame(),
            "transactions": pl.DataFrame(),
        })
        assert row_counts == {}
