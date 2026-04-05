"""Tests for CSV loader."""

from pathlib import Path

import duckdb
import polars as pl
import pytest

from moneybin.loaders.csv_loader import CSVLoader


@pytest.fixture()
def db_path(tmp_path: Path) -> Path:
    """Path to a temporary DuckDB database."""
    return tmp_path / "test.duckdb"


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

    def test_creates_tables(self, db_path: Path) -> None:
        loader = CSVLoader(db_path)
        loader.create_raw_tables()

        conn = duckdb.connect(str(db_path))
        tables = conn.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'raw'
            ORDER BY table_name
        """).fetchall()
        table_names = [t[0] for t in tables]
        conn.close()

        assert "csv_accounts" in table_names
        assert "csv_transactions" in table_names


class TestCSVLoaderData:
    """Test loading data into raw tables."""

    def test_load_data(
        self,
        db_path: Path,
        sample_accounts: pl.DataFrame,
        sample_transactions: pl.DataFrame,
    ) -> None:
        loader = CSVLoader(db_path)
        row_counts = loader.load_data({
            "accounts": sample_accounts,
            "transactions": sample_transactions,
        })

        assert row_counts["accounts"] == 1
        assert row_counts["transactions"] == 2

    def test_idempotent_reload(
        self,
        db_path: Path,
        sample_accounts: pl.DataFrame,
        sample_transactions: pl.DataFrame,
    ) -> None:
        """Loading the same data twice should not create duplicates."""
        loader = CSVLoader(db_path)
        data = {"accounts": sample_accounts, "transactions": sample_transactions}

        loader.load_data(data)
        loader.load_data(data)

        conn = duckdb.connect(str(db_path), read_only=True)
        count = conn.execute("SELECT COUNT(*) FROM raw.csv_transactions").fetchone()
        conn.close()

        assert count is not None
        assert count[0] == 2  # Not 4

    def test_query_loaded_data(
        self,
        db_path: Path,
        sample_accounts: pl.DataFrame,
        sample_transactions: pl.DataFrame,
    ) -> None:
        loader = CSVLoader(db_path)
        loader.load_data({
            "accounts": sample_accounts,
            "transactions": sample_transactions,
        })

        conn = duckdb.connect(str(db_path), read_only=True)

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

        conn.close()

    def test_empty_data(self, db_path: Path) -> None:
        loader = CSVLoader(db_path)
        row_counts = loader.load_data({
            "accounts": pl.DataFrame(),
            "transactions": pl.DataFrame(),
        })
        assert row_counts == {}
