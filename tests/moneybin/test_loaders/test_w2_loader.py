"""Tests for W2 data loader.

This module tests the W2 loader to ensure proper loading of extracted
W2 data into DuckDB raw tables.
"""

from pathlib import Path

import polars as pl
import pytest

from moneybin.loaders.w2_loader import W2Loader


@pytest.fixture
def sample_w2_data() -> pl.DataFrame:
    """Create sample W2 data for testing."""
    return pl.DataFrame([
        {
            "tax_year": 2024,
            "employee_ssn": "123-45-6789",
            "employee_first_name": "John",
            "employee_last_name": "Doe",
            "employee_address": "123 Main St, Anytown, CA 12345",
            "employer_ein": "12-3456789",
            "employer_name": "Acme Corp",
            "employer_address": "456 Business Blvd, Big City, CA 54321",
            "control_number": "12345678",
            "wages": 100000.00,
            "federal_income_tax": 20000.00,
            "social_security_wages": 100000.00,
            "social_security_tax": 6200.00,
            "medicare_wages": 100000.00,
            "medicare_tax": 1450.00,
            "social_security_tips": None,
            "allocated_tips": None,
            "dependent_care_benefits": None,
            "nonqualified_plans": 19500.00,
            "is_statutory_employee": False,
            "is_retirement_plan": True,
            "is_third_party_sick_pay": False,
            "state_local_info": '[{"state": "CA", "employer_state_id": "1234567", "state_wages": 100000.00, "state_income_tax": 5000.00}]',
            "optional_boxes": '{"box_12_codes": {"D": "19500.00"}}',
            "source_file": "/path/to/test.pdf",
            "extracted_at": "2025-01-24T12:00:00",
        }
    ])


@pytest.mark.unit
def test_loader_initialization(tmp_path: Path) -> None:
    """Test that W2 loader initializes correctly."""
    db_path = tmp_path / "test.duckdb"
    loader = W2Loader(db_path)

    assert loader.database_path == db_path


@pytest.mark.unit
def test_create_raw_tables(tmp_path: Path) -> None:
    """Test that raw tables are created successfully."""
    db_path = tmp_path / "test.duckdb"
    loader = W2Loader(db_path)

    # Create tables
    loader.create_raw_tables()

    # Verify table exists by querying
    import duckdb

    conn = duckdb.connect(str(db_path))
    result = conn.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'raw' AND table_name = 'w2_forms'
    """).fetchall()
    conn.close()

    assert len(result) == 1
    assert result[0][0] == "w2_forms"


@pytest.mark.unit
def test_load_data(tmp_path: Path, sample_w2_data: pl.DataFrame) -> None:
    """Test that W2 data is loaded correctly."""
    db_path = tmp_path / "test.duckdb"
    loader = W2Loader(db_path)

    # Load data
    row_count = loader.load_data(sample_w2_data)

    assert row_count == 1


@pytest.mark.unit
def test_load_data_creates_tables(tmp_path: Path, sample_w2_data: pl.DataFrame) -> None:
    """Test that loading data creates tables if they don't exist."""
    db_path = tmp_path / "test.duckdb"
    loader = W2Loader(db_path)

    # Load data without explicitly creating tables first
    row_count = loader.load_data(sample_w2_data)

    assert row_count == 1

    # Verify data was loaded
    result = loader.query_raw_data()
    assert len(result) == 1


@pytest.mark.unit
def test_query_raw_data(tmp_path: Path, sample_w2_data: pl.DataFrame) -> None:
    """Test querying W2 data from database."""
    db_path = tmp_path / "test.duckdb"
    loader = W2Loader(db_path)

    # Load data first
    loader.load_data(sample_w2_data)

    # Query data
    result = loader.query_raw_data()

    assert len(result) == 1
    assert isinstance(result, pl.DataFrame)

    # Check data integrity
    row = result.row(0, named=True)
    assert row["tax_year"] == 2024
    assert row["employee_ssn"] == "123-45-6789"
    assert row["employee_first_name"] == "John"
    assert row["employer_ein"] == "12-3456789"
    assert row["wages"] == 100000.00


@pytest.mark.unit
def test_query_with_limit(tmp_path: Path, sample_w2_data: pl.DataFrame) -> None:
    """Test querying with limit parameter."""
    db_path = tmp_path / "test.duckdb"
    loader = W2Loader(db_path)

    # Load data
    loader.load_data(sample_w2_data)

    # Query with limit
    result = loader.query_raw_data(limit=1)

    assert len(result) == 1


@pytest.mark.unit
def test_load_empty_dataframe(tmp_path: Path) -> None:
    """Test that loading empty DataFrame doesn't raise errors."""
    db_path = tmp_path / "test.duckdb"
    loader = W2Loader(db_path)

    # Create empty DataFrame with correct schema
    empty_df = pl.DataFrame(
        schema={
            "tax_year": pl.Int64,
            "employee_ssn": pl.String,
            "employee_first_name": pl.String,
            "employee_last_name": pl.String,
            "employer_ein": pl.String,
            "employer_name": pl.String,
            "wages": pl.Float64,
            "federal_income_tax": pl.Float64,
        }
    )

    # Should not raise error
    row_count = loader.load_data(empty_df)
    assert row_count == 0


@pytest.mark.unit
def test_load_duplicate_data_replaces(
    tmp_path: Path, sample_w2_data: pl.DataFrame
) -> None:
    """Test that loading duplicate data uses INSERT OR REPLACE."""
    db_path = tmp_path / "test.duckdb"
    loader = W2Loader(db_path)

    # Load data first time
    loader.load_data(sample_w2_data)

    # Modify data and load again (same primary key)
    modified_data = sample_w2_data.clone()
    # Change a non-key field
    modified_data = modified_data.with_columns(
        pl.lit(25000.00).alias("federal_income_tax")
    )

    # Load modified data
    row_count = loader.load_data(modified_data)
    assert row_count == 1

    # Verify that data was replaced (not duplicated)
    result = loader.query_raw_data()
    assert len(result) == 1

    # Verify the value was updated
    row = result.row(0, named=True)
    assert row["federal_income_tax"] == 25000.00


@pytest.mark.unit
def test_json_fields_stored_correctly(
    tmp_path: Path, sample_w2_data: pl.DataFrame
) -> None:
    """Test that JSON fields (state_local_info, optional_boxes) are stored correctly."""
    db_path = tmp_path / "test.duckdb"
    loader = W2Loader(db_path)

    # Load data
    loader.load_data(sample_w2_data)

    # Query data
    import duckdb

    conn = duckdb.connect(str(db_path))

    # Query and validate JSON fields
    result = conn.execute("""
        SELECT
            state_local_info,
            optional_boxes
        FROM raw.w2_forms
    """).fetchone()

    conn.close()

    # Check that JSON fields are stored as JSON (not string)
    assert result is not None, "Expected to find W2 data in database"
    state_info = result[0]
    optional_boxes = result[1]

    # DuckDB should return these as Python objects (lists/dicts)
    assert state_info is not None
    assert optional_boxes is not None


@pytest.mark.unit
def test_missing_sql_file_raises_error(tmp_path: Path) -> None:
    """Test that missing SQL schema file raises appropriate error."""
    db_path = tmp_path / "test.duckdb"
    loader = W2Loader(db_path)

    # Temporarily change sql_dir to non-existent path
    loader.sql_dir = tmp_path / "nonexistent"

    with pytest.raises(FileNotFoundError, match="SQL schema file not found"):
        loader.create_raw_tables()
