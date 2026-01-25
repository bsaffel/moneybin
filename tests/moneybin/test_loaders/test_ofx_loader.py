"""Tests for OFX raw data loader to DuckDB."""

from pathlib import Path

import duckdb
import polars as pl
import pytest

from moneybin.loaders.ofx_loader import OFXLoader


@pytest.fixture
def test_database(tmp_path: Path) -> Path:
    """Create a temporary test database."""
    db_path = tmp_path / "test.duckdb"
    return db_path


@pytest.fixture
def sample_ofx_data() -> dict[str, pl.DataFrame]:
    """Create sample OFX data for testing."""
    return {
        "institutions": pl.DataFrame({
            "organization": ["Test Bank"],
            "fid": ["12345"],
            "source_file": ["test.qfx"],
            "extracted_at": ["2025-01-24T12:00:00"],
        }),
        "accounts": pl.DataFrame({
            "account_id": ["ACC001"],
            "routing_number": ["123456789"],
            "account_type": ["CHECKING"],
            "institution_org": ["Test Bank"],
            "institution_fid": ["12345"],
            "source_file": ["test.qfx"],
            "extracted_at": ["2025-01-24T12:00:00"],
        }),
        "transactions": pl.DataFrame({
            "transaction_id": ["TXN001", "TXN002"],
            "account_id": ["ACC001", "ACC001"],
            "transaction_type": ["DEBIT", "CREDIT"],
            "date_posted": ["2025-01-15T12:00:00", "2025-01-20T12:00:00"],
            "amount": [-50.00, 100.00],
            "payee": ["Coffee Shop", "Payroll"],
            "memo": ["Morning coffee", "Salary"],
            "check_number": [None, None],
            "source_file": ["test.qfx", "test.qfx"],
            "extracted_at": ["2025-01-24T12:00:00", "2025-01-24T12:00:00"],
        }),
        "balances": pl.DataFrame({
            "account_id": ["ACC001"],
            "statement_start_date": ["2025-01-01T00:00:00"],
            "statement_end_date": ["2025-01-31T23:59:59"],
            "ledger_balance": [5000.00],
            "ledger_balance_date": ["2025-01-31T23:59:59"],
            "available_balance": [4800.00],
            "source_file": ["test.qfx"],
            "extracted_at": ["2025-01-24T12:00:00"],
        }),
    }


@pytest.mark.unit
def test_loader_initialization(test_database: Path) -> None:
    """Test that loader initializes correctly."""
    loader = OFXLoader(test_database)
    assert loader.database_path == test_database


@pytest.mark.unit
def test_create_raw_tables(test_database: Path) -> None:
    """Test that raw tables are created in DuckDB."""
    loader = OFXLoader(test_database)
    loader.create_raw_tables()

    # Verify tables were created
    conn = duckdb.connect(str(test_database))

    # Check schema exists
    schemas = conn.execute(
        "SELECT schema_name FROM information_schema.schemata"
    ).fetchall()
    assert ("raw",) in schemas

    # Check all tables exist
    tables = conn.execute(
        """
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'raw' AND table_name LIKE 'ofx_%'
        """
    ).fetchall()

    table_names = [t[0] for t in tables]
    assert "ofx_institutions" in table_names
    assert "ofx_accounts" in table_names
    assert "ofx_transactions" in table_names
    assert "ofx_balances" in table_names

    conn.close()


@pytest.mark.unit
def test_load_data(
    test_database: Path, sample_ofx_data: dict[str, pl.DataFrame]
) -> None:
    """Test that data is loaded into raw tables."""
    loader = OFXLoader(test_database)
    row_counts = loader.load_data(sample_ofx_data)

    # Verify row counts returned
    assert row_counts["institutions"] == 1
    assert row_counts["accounts"] == 1
    assert row_counts["transactions"] == 2
    assert row_counts["balances"] == 1

    # Verify data in database
    conn = duckdb.connect(str(test_database))

    # Check transactions table
    result = conn.execute("SELECT COUNT(*) FROM raw.ofx_transactions").fetchone()
    assert result is not None
    assert result[0] == 2

    # Check transaction data
    tx_df = conn.execute(
        """
        SELECT transaction_id, amount, payee
        FROM raw.ofx_transactions
        ORDER BY date_posted
        """
    ).pl()

    assert len(tx_df) == 2
    assert tx_df["transaction_id"][0] == "TXN001"
    assert float(tx_df["amount"][0]) == -50.00
    assert tx_df["payee"][0] == "Coffee Shop"

    conn.close()


@pytest.mark.unit
def test_load_data_idempotent(
    test_database: Path, sample_ofx_data: dict[str, pl.DataFrame]
) -> None:
    """Test that loading the same data multiple times is idempotent."""
    loader = OFXLoader(test_database)

    # Load data twice
    loader.load_data(sample_ofx_data)
    row_counts = loader.load_data(sample_ofx_data)

    # Should still have same row counts (INSERT OR REPLACE)
    assert row_counts["transactions"] == 2

    # Verify database has correct count
    conn = duckdb.connect(str(test_database))
    result = conn.execute("SELECT COUNT(*) FROM raw.ofx_transactions").fetchone()
    assert result is not None
    assert result[0] == 2  # Not 4, because of INSERT OR REPLACE
    conn.close()


@pytest.mark.unit
def test_query_raw_data(
    test_database: Path, sample_ofx_data: dict[str, pl.DataFrame]
) -> None:
    """Test that raw data can be queried from database."""
    loader = OFXLoader(test_database)
    loader.load_data(sample_ofx_data)

    # Query transactions
    df = loader.query_raw_data("transactions")

    assert isinstance(df, pl.DataFrame)
    assert len(df) == 2
    assert "transaction_id" in df.columns
    assert "amount" in df.columns


@pytest.mark.unit
def test_query_raw_data_with_limit(
    test_database: Path, sample_ofx_data: dict[str, pl.DataFrame]
) -> None:
    """Test that query limit works correctly."""
    loader = OFXLoader(test_database)
    loader.load_data(sample_ofx_data)

    # Query with limit
    df = loader.query_raw_data("transactions", limit=1)

    assert len(df) == 1


@pytest.mark.unit
def test_load_empty_dataframes(test_database: Path) -> None:
    """Test that loading empty DataFrames doesn't cause errors."""
    empty_data = {
        "institutions": pl.DataFrame(),
        "accounts": pl.DataFrame(),
        "transactions": pl.DataFrame(),
        "balances": pl.DataFrame(),
    }

    loader = OFXLoader(test_database)
    row_counts = loader.load_data(empty_data)

    # Should return empty counts
    assert len(row_counts) == 0

    # Verify tables exist but are empty
    conn = duckdb.connect(str(test_database))
    result = conn.execute("SELECT COUNT(*) FROM raw.ofx_transactions").fetchone()
    assert result is not None
    assert result[0] == 0
    conn.close()
