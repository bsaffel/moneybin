"""Tests for OFX/QFX file extractor.

This module tests the OFX extractor with sample QFX data to ensure proper
parsing, validation, and data extraction into raw table structures.
"""

from datetime import datetime
from decimal import Decimal
from pathlib import Path

import polars as pl
import pytest

from moneybin.extractors.ofx_extractor import (
    OFXExtractionConfig,
    OFXExtractor,
    OFXTransactionSchema,
    extract_ofx_file,
)

# Path to test fixtures directory
FIXTURES_DIR = Path(__file__).parent.parent.parent / "fixtures"


@pytest.fixture
def sample_ofx_file() -> Path:
    """Path to sample OFX fixture file for testing."""
    fixture_path = FIXTURES_DIR / "sample_statement.qfx"
    if not fixture_path.exists():
        raise FileNotFoundError(
            f"Test fixture not found: {fixture_path}\n"
            f"Expected location: tests/fixtures/sample_statement.qfx"
        )
    return fixture_path


@pytest.fixture
def extractor_config(tmp_path: Path) -> OFXExtractionConfig:
    """Create test extraction configuration."""
    return OFXExtractionConfig(
        save_raw_data=True,
        raw_data_path=tmp_path / "raw_ofx",
        preserve_source_files=True,
        validate_balances=True,
    )


@pytest.mark.unit
def test_ofx_transaction_schema_validation() -> None:
    """Test that OFX transaction schema validates and converts data correctly."""
    # Test valid transaction
    tx = OFXTransactionSchema(
        id="TXN001",
        type="DEBIT",
        date=datetime(2025, 1, 15),
        amount=Decimal("-50.00"),
        payee="Test Merchant",
        memo="Test purchase",
        checknum=None,
    )

    assert tx.id == "TXN001"
    assert tx.type == "DEBIT"
    assert tx.amount == Decimal("-50.00")
    assert tx.payee == "Test Merchant"

    # Test amount conversion from float
    tx2 = OFXTransactionSchema(
        id="TXN002",
        type="CREDIT",
        date=datetime(2025, 1, 15),
        amount=100.50,  # type: ignore[arg-type]
        payee="Test Payer",
        memo=None,
        checknum=None,
    )

    assert tx2.amount == Decimal("100.50")


@pytest.mark.unit
def test_extractor_initialization(extractor_config: OFXExtractionConfig) -> None:
    """Test that OFX extractor initializes correctly."""
    extractor = OFXExtractor(extractor_config)

    assert extractor.config == extractor_config
    assert extractor.config.raw_data_path.exists()


@pytest.mark.unit
def test_extract_from_file_creates_dataframes(
    sample_ofx_file: Path, extractor_config: OFXExtractionConfig
) -> None:
    """Test that extraction creates all expected DataFrames."""
    extractor = OFXExtractor(extractor_config)
    results = extractor.extract_from_file(sample_ofx_file)

    # Check all expected tables are present
    assert "institutions" in results
    assert "accounts" in results
    assert "transactions" in results
    assert "balances" in results

    # Check that results are DataFrames
    assert isinstance(results["institutions"], pl.DataFrame)
    assert isinstance(results["accounts"], pl.DataFrame)
    assert isinstance(results["transactions"], pl.DataFrame)
    assert isinstance(results["balances"], pl.DataFrame)


@pytest.mark.unit
def test_extract_institutions_data(
    sample_ofx_file: Path, extractor_config: OFXExtractionConfig
) -> None:
    """Test that institution data is extracted correctly."""
    extractor = OFXExtractor(extractor_config)
    results = extractor.extract_from_file(sample_ofx_file)

    institutions = results["institutions"]

    # Should have at least one institution
    assert len(institutions) >= 1

    # Check expected columns
    assert "organization" in institutions.columns
    assert "fid" in institutions.columns
    assert "source_file" in institutions.columns
    assert "extracted_at" in institutions.columns

    # Check values
    first_row = institutions.row(0, named=True)
    assert first_row["organization"] == "Test Bank"
    assert first_row["fid"] == "12345"


@pytest.mark.unit
def test_extract_accounts_data(
    sample_ofx_file: Path, extractor_config: OFXExtractionConfig
) -> None:
    """Test that account data is extracted correctly."""
    extractor = OFXExtractor(extractor_config)
    results = extractor.extract_from_file(sample_ofx_file)

    accounts = results["accounts"]

    # Should have at least one account
    assert len(accounts) >= 1

    # Check expected columns
    assert "account_id" in accounts.columns
    assert "routing_number" in accounts.columns
    assert "account_type" in accounts.columns
    assert "institution_org" in accounts.columns

    # Check values
    first_row = accounts.row(0, named=True)
    assert first_row["account_id"] == "9876543210"
    assert first_row["routing_number"] == "123456789"
    assert first_row["account_type"] == "CHECKING"


@pytest.mark.unit
def test_extract_transactions_data(
    sample_ofx_file: Path, extractor_config: OFXExtractionConfig
) -> None:
    """Test that transaction data is extracted correctly."""
    extractor = OFXExtractor(extractor_config)
    results = extractor.extract_from_file(sample_ofx_file)

    transactions = results["transactions"]

    # Should have 3 transactions from sample data
    assert len(transactions) == 3

    # Check expected columns
    expected_cols = [
        "transaction_id",
        "account_id",
        "transaction_type",
        "date_posted",
        "amount",
        "payee",
        "memo",
    ]
    for col in expected_cols:
        assert col in transactions.columns

    # Check first transaction (debit)
    tx1 = transactions.row(0, named=True)
    assert tx1["transaction_id"] == "TXN001"
    assert tx1["transaction_type"].upper() == "DEBIT"
    assert tx1["amount"] == -50.00
    assert tx1["payee"] == "Coffee Shop"

    # Check second transaction (credit)
    tx2 = transactions.row(1, named=True)
    assert tx2["transaction_id"] == "TXN002"
    assert tx2["transaction_type"].upper() == "CREDIT"
    assert tx2["amount"] == 1000.00
    assert tx2["payee"] == "Payroll Deposit"


@pytest.mark.unit
def test_extract_balances_data(
    sample_ofx_file: Path, extractor_config: OFXExtractionConfig
) -> None:
    """Test that balance data is extracted correctly."""
    extractor = OFXExtractor(extractor_config)
    results = extractor.extract_from_file(sample_ofx_file)

    balances = results["balances"]

    # Should have at least one balance record
    assert len(balances) >= 1

    # Check expected columns
    assert "account_id" in balances.columns
    assert "ledger_balance" in balances.columns
    assert "available_balance" in balances.columns

    # Check values
    first_row = balances.row(0, named=True)
    assert first_row["account_id"] == "9876543210"
    assert first_row["ledger_balance"] == 5000.00
    assert first_row["available_balance"] == 4800.00


@pytest.mark.unit
def test_extract_saves_raw_parquet_files(
    sample_ofx_file: Path, extractor_config: OFXExtractionConfig
) -> None:
    """Test that raw parquet files are saved in organized directory structure."""
    extractor = OFXExtractor(extractor_config)
    extractor.extract_from_file(sample_ofx_file)

    # Check that extraction directory was created
    extraction_dir = extractor_config.raw_data_path / "extracted" / sample_ofx_file.stem
    assert extraction_dir.exists()
    assert extraction_dir.is_dir()

    # Check that all expected parquet files exist
    expected_files = [
        "institutions.parquet",
        "accounts.parquet",
        "transactions.parquet",
        "balances.parquet",
    ]
    for filename in expected_files:
        file_path = extraction_dir / filename
        assert file_path.exists(), f"Expected file not found: {filename}"

    # Verify we can read the transaction parquet file
    transactions_path = extraction_dir / "transactions.parquet"
    df = pl.read_parquet(transactions_path)
    assert len(df) == 3


@pytest.mark.unit
def test_extract_with_institution_name_override(
    sample_ofx_file: Path, extractor_config: OFXExtractionConfig
) -> None:
    """Test that institution name can be overridden."""
    extractor = OFXExtractor(extractor_config)
    results = extractor.extract_from_file(sample_ofx_file, institution_name="My Bank")

    institutions = results["institutions"]
    first_row = institutions.row(0, named=True)

    # Institution name should be overridden
    assert first_row["organization"] == "My Bank"


@pytest.mark.unit
def test_extract_nonexistent_file_raises_error(
    extractor_config: OFXExtractionConfig,
) -> None:
    """Test that extracting non-existent file raises FileNotFoundError."""
    extractor = OFXExtractor(extractor_config)

    with pytest.raises(FileNotFoundError):
        extractor.extract_from_file(Path("/nonexistent/file.qfx"))


@pytest.mark.unit
def test_extract_invalid_ofx_raises_error(
    tmp_path: Path, extractor_config: OFXExtractionConfig
) -> None:
    """Test that invalid OFX content raises ValueError."""
    # Create file with invalid OFX content
    invalid_file = tmp_path / "invalid.qfx"
    invalid_file.write_text("This is not valid OFX content")

    extractor = OFXExtractor(extractor_config)

    with pytest.raises(ValueError, match="Invalid OFX file format"):
        extractor.extract_from_file(invalid_file)


@pytest.mark.unit
def test_convenience_function(sample_ofx_file: Path) -> None:
    """Test the convenience function for OFX extraction."""
    results = extract_ofx_file(sample_ofx_file)

    # Check all expected tables are present
    assert "institutions" in results
    assert "accounts" in results
    assert "transactions" in results
    assert "balances" in results

    # Check transactions were extracted
    assert len(results["transactions"]) == 3


@pytest.mark.unit
def test_extract_preserves_metadata(
    sample_ofx_file: Path, extractor_config: OFXExtractionConfig
) -> None:
    """Test that extraction preserves metadata like source file and extraction time."""
    extractor = OFXExtractor(extractor_config)
    results = extractor.extract_from_file(sample_ofx_file)

    # Check transactions have metadata
    transactions = results["transactions"]
    first_tx = transactions.row(0, named=True)

    assert "source_file" in first_tx
    assert "extracted_at" in first_tx
    assert str(sample_ofx_file) in first_tx["source_file"]

    # Verify extraction timestamp is recent (within last minute)
    extracted_at = datetime.fromisoformat(first_tx["extracted_at"])
    time_diff = datetime.now() - extracted_at
    assert time_diff.total_seconds() < 60
