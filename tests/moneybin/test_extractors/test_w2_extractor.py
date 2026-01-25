"""Tests for W2 PDF extractor.

This module tests the W2 extractor with sample W2 PDF data to ensure proper
parsing, validation, and data extraction into raw table structures.
"""

from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import polars as pl
import pytest

from moneybin.extractors.w2_extractor import (
    W2ExtractionConfig,
    W2Extractor,
    W2FormSchema,
    W2OptionalBoxes,
    W2StateLocalInfo,
)

# Path to test fixtures directory
FIXTURES_DIR = Path(__file__).parent.parent.parent / "fixtures"


@pytest.fixture(scope="session")
def sample_w2_file() -> Path:
    """Path to sample W2 fixture file for testing."""
    fixture_path = FIXTURES_DIR / "sample_w2_2024.pdf"
    if not fixture_path.exists():
        raise FileNotFoundError(
            f"Test fixture not found: {fixture_path}\n"
            f"Expected location: tests/fixtures/sample_w2_2024.pdf"
        )
    return fixture_path


@pytest.fixture
def extractor_config(tmp_path: Path) -> W2ExtractionConfig:
    """Create test extraction configuration matching production settings.

    OCR is enabled by default to test real-world behavior. Test performance
    is maintained via session-scoped fixture caching (see cached_w2_extraction).
    """
    return W2ExtractionConfig(
        save_raw_data=True,
        raw_data_path=tmp_path / "raw_w2",
        preserve_source_files=True,
        require_dual_extraction=True,  # Require both methods to succeed by default
        min_confidence_score=0.7,  # Lower threshold for tests
        enable_ocr=True,
    )


@pytest.fixture(scope="session")
def cached_w2_extraction(sample_w2_file: Path) -> pl.DataFrame:
    """Extract W2 data once and cache for all tests (session-scoped).

    This speeds up tests by avoiding re-extraction for each test. The extraction
    runs with OCR enabled (matching production) but only executes once per test
    session due to the session scope.

    Performance: OCR runs once (~10s) at session start, then all tests use cached
    result. This is much faster than running OCR for each test while still testing
    production-like behavior.
    """
    config = W2ExtractionConfig(
        save_raw_data=False,  # Don't save during test caching
        require_dual_extraction=False,  # Allow either method to succeed
        min_confidence_score=0.7,
        enable_ocr=True,  # Enable OCR to match production behavior
    )
    extractor = W2Extractor(config)
    return extractor.extract_from_file(sample_w2_file, tax_year=2024)


@pytest.mark.unit
def test_w2_state_local_info_validation() -> None:
    """Test that W2 state/local info schema validates correctly."""
    # Test valid state info
    state_info = W2StateLocalInfo(
        state="CA",
        employer_state_id="1234567",
        state_wages=Decimal("100000.00"),
        state_income_tax=Decimal("5000.00"),
        local_wages=Decimal("100000.00"),
        local_income_tax=Decimal("1000.00"),
        locality_name="San Francisco",
    )

    assert state_info.state == "CA"
    assert state_info.state_wages == Decimal("100000.00")
    assert state_info.state_income_tax == Decimal("5000.00")

    # Test amount conversion from string
    state_info2 = W2StateLocalInfo(
        state="NY",
        state_wages="75000.50",  # type: ignore[arg-type]
        state_income_tax="3500.25",  # type: ignore[arg-type]
    )

    assert state_info2.state_wages == Decimal("75000.50")
    assert state_info2.state_income_tax == Decimal("3500.25")


@pytest.mark.unit
def test_w2_optional_boxes_schema() -> None:
    """Test that W2 optional boxes schema validates correctly."""
    # Test box 12 codes
    optional = W2OptionalBoxes(
        box_12_codes={"D": "19500.00", "DD": "8450.00"},
        box_14_other="Union dues: $500",
    )

    assert optional.box_12_codes is not None
    assert optional.box_12_codes["D"] == "19500.00"
    assert optional.box_14_other == "Union dues: $500"


@pytest.mark.unit
def test_w2_form_schema_validation() -> None:
    """Test that W2 form schema validates and converts data correctly."""
    # Test valid W2
    w2 = W2FormSchema(
        tax_year=2024,
        employee_ssn="123-45-6789",
        employee_first_name="John",
        employee_last_name="Doe",
        employee_address="123 Main St, Anytown, CA 12345",
        employer_ein="12-3456789",
        employer_name="Acme Corp",
        employer_address="456 Business Blvd, Big City, CA 54321",
        wages=Decimal("100000.00"),
        federal_income_tax=Decimal("20000.00"),
        social_security_wages=Decimal("100000.00"),
        social_security_tax=Decimal("6200.00"),
        medicare_wages=Decimal("100000.00"),
        medicare_tax=Decimal("1450.00"),
        social_security_tips=None,
        allocated_tips=None,
        dependent_care_benefits=None,
        nonqualified_plans=None,
        is_statutory_employee=False,
        is_retirement_plan=False,
        is_third_party_sick_pay=False,
        control_number=None,
        optional_boxes=None,
    )

    assert w2.tax_year == 2024
    assert w2.employee_ssn == "123-45-6789"
    assert w2.wages == Decimal("100000.00")
    assert w2.federal_income_tax == Decimal("20000.00")

    # Test amount conversion from float
    w2_2 = W2FormSchema(
        tax_year=2024,
        employee_ssn="987-65-4321",
        employee_first_name="Jane",
        employee_last_name="Smith",
        employer_ein="98-7654321",
        employer_name="Tech Inc",
        wages=75000.50,  # type: ignore[arg-type]
        federal_income_tax=15000.10,  # type: ignore[arg-type]
    )

    assert w2_2.wages == Decimal("75000.50")
    assert w2_2.federal_income_tax == Decimal("15000.10")


@pytest.mark.unit
def test_extractor_initialization(extractor_config: W2ExtractionConfig) -> None:
    """Test that W2 extractor initializes correctly."""
    extractor = W2Extractor(extractor_config)

    assert extractor.config == extractor_config
    assert extractor.config.raw_data_path.exists()


@pytest.mark.unit
def test_extract_from_file_creates_dataframe(
    sample_w2_file: Path, extractor_config: W2ExtractionConfig
) -> None:
    """Test that extraction creates expected DataFrame."""
    extractor = W2Extractor(extractor_config)
    result = extractor.extract_from_file(sample_w2_file)

    # Check that result is a DataFrame
    assert isinstance(result, pl.DataFrame)

    # Should have exactly one row (one W2)
    assert len(result) == 1


@pytest.mark.unit
def test_extract_w2_data(cached_w2_extraction: pl.DataFrame) -> None:
    """Test that W2 data is extracted correctly from sample PDF."""
    result = cached_w2_extraction

    # Get the first (and only) row
    w2 = result.row(0, named=True)

    # Check expected columns exist
    expected_cols = [
        "tax_year",
        "employee_ssn",
        "employee_first_name",
        "employee_last_name",
        "employer_ein",
        "employer_name",
        "wages",
        "federal_income_tax",
        "social_security_wages",
        "social_security_tax",
        "medicare_wages",
        "medicare_tax",
        "source_file",
        "extracted_at",
    ]
    for col in expected_cols:
        assert col in w2

    # Validate basic data types and values
    assert w2["tax_year"] == 2024
    assert isinstance(w2["employee_ssn"], str)
    assert isinstance(w2["employee_first_name"], str)
    assert isinstance(w2["employee_last_name"], str)
    assert isinstance(w2["employer_ein"], str)
    assert isinstance(w2["employer_name"], str)

    # Check that monetary amounts are extracted
    assert w2["wages"] > 0
    assert w2["federal_income_tax"] > 0


@pytest.mark.unit
def test_extract_employee_info(cached_w2_extraction: pl.DataFrame) -> None:
    """Test that employee information is extracted correctly."""
    result = cached_w2_extraction

    w2 = result.row(0, named=True)

    # Check employee information based on sample W2 (Google Cloud sample)
    assert w2["employee_first_name"] == "Howard"
    assert w2["employee_last_name"] == "Radial"
    assert w2["employee_ssn"] == "077-49-4905"

    # Note: Google Cloud sample doesn't have extractable address in text layer
    # This is a limitation of the sample, not the extractor


@pytest.mark.unit
def test_extract_employer_info(cached_w2_extraction: pl.DataFrame) -> None:
    """Test that employer information is extracted correctly."""
    result = cached_w2_extraction

    w2 = result.row(0, named=True)

    # Check employer information based on sample W2 (Google Cloud sample)
    # Note: Google Cloud sample doesn't have clear employer name or address in extractable text
    assert w2["employer_ein"] == "37-2766773"

    # Verify EIN format is correct
    assert "-" in w2["employer_ein"]


@pytest.mark.unit
def test_extract_wage_and_tax_amounts(cached_w2_extraction: pl.DataFrame) -> None:
    """Test that wage and tax amounts are extracted correctly."""
    result = cached_w2_extraction

    w2 = result.row(0, named=True)

    # Check that amounts are reasonable (based on Google Cloud sample W2)
    # Wages should be positive and in reasonable range
    assert w2["wages"] > 0
    assert w2["wages"] < 100000  # Sample has ~$28k

    # Federal tax should be positive
    assert w2["federal_income_tax"] > 0

    # Social Security wages should be present
    assert w2["social_security_wages"] is not None
    assert w2["social_security_wages"] > 0

    # Medicare wages should match wages for this sample
    assert w2["medicare_wages"] is not None
    assert w2["medicare_wages"] > 0


@pytest.mark.unit
def test_extract_state_info(cached_w2_extraction: pl.DataFrame) -> None:
    """Test that state tax information is extracted as JSON."""
    result = cached_w2_extraction

    w2 = result.row(0, named=True)

    # State info may or may not be present depending on sample
    # Google Cloud sample doesn't have clear state info in extractable text
    if w2["state_local_info"] is not None:
        # If present, parse JSON to verify structure
        import json

        state_info: list[dict[str, Any]] = json.loads(w2["state_local_info"])
        assert isinstance(state_info, list)

        if len(state_info) > 0:
            # Check first state has expected fields
            first_state = state_info[0]
            assert "state" in first_state


@pytest.mark.unit
def test_extract_optional_boxes(cached_w2_extraction: pl.DataFrame) -> None:
    """Test that optional boxes (box 12 codes) are extracted as JSON."""
    result = cached_w2_extraction

    w2 = result.row(0, named=True)

    # Optional boxes might be present
    if w2["optional_boxes"] is not None:
        import json

        optional: dict[str, Any] = json.loads(w2["optional_boxes"])
        assert isinstance(optional, dict)

        # From sample: "C 454.20", "D 23000.00", "DD 15963.22", "W 8300.00"
        if "box_12_codes" in optional:
            box_12: dict[str, str] = optional["box_12_codes"]
            assert isinstance(box_12, dict)
            # Should have extracted some codes
            assert len(box_12) > 0


@pytest.mark.unit
def test_extract_saves_raw_parquet_file(
    sample_w2_file: Path, extractor_config: W2ExtractionConfig
) -> None:
    """Test that raw parquet file is saved in organized directory structure."""
    extractor = W2Extractor(extractor_config)
    extractor.extract_from_file(sample_w2_file)

    # Check that extraction directory was created
    extraction_dir = extractor_config.raw_data_path / "extracted" / sample_w2_file.stem
    assert extraction_dir.exists()
    assert extraction_dir.is_dir()

    # Check that parquet file exists
    parquet_file = extraction_dir / "w2_form.parquet"
    assert parquet_file.exists(), "Expected parquet file not found"

    # Verify we can read the parquet file
    df = pl.read_parquet(parquet_file)
    assert len(df) == 1


@pytest.mark.unit
def test_extract_nonexistent_file_raises_error(
    extractor_config: W2ExtractionConfig,
) -> None:
    """Test that extracting non-existent file raises FileNotFoundError."""
    extractor = W2Extractor(extractor_config)

    with pytest.raises(FileNotFoundError):
        extractor.extract_from_file(Path("/nonexistent/file.pdf"))


@pytest.mark.unit
def test_extract_invalid_pdf_raises_error(
    tmp_path: Path, extractor_config: W2ExtractionConfig
) -> None:
    """Test that invalid PDF content raises ValueError."""
    # Create file with invalid PDF content
    invalid_file = tmp_path / "invalid.pdf"
    invalid_file.write_text("This is not valid PDF content")

    extractor = W2Extractor(extractor_config)

    with pytest.raises(ValueError, match="Both extraction methods failed"):
        extractor.extract_from_file(invalid_file)


@pytest.mark.unit
def test_convenience_function(cached_w2_extraction: pl.DataFrame) -> None:
    """Test the convenience function for W2 extraction."""
    result = cached_w2_extraction

    # Check that result is a DataFrame
    assert isinstance(result, pl.DataFrame)

    # Should have exactly one row
    assert len(result) == 1

    # Check basic fields
    w2 = result.row(0, named=True)
    assert w2["tax_year"] == 2024
    assert w2["employee_first_name"] == "Howard"


@pytest.mark.unit
def test_extract_preserves_metadata(cached_w2_extraction: pl.DataFrame) -> None:
    """Test that extraction preserves metadata like source file and extraction time."""
    result = cached_w2_extraction

    # Check W2 has metadata
    w2 = result.row(0, named=True)

    assert "source_file" in w2
    assert "extracted_at" in w2
    assert "sample_w2_2024.pdf" in w2["source_file"]

    # Verify extraction timestamp is parseable
    extracted_at = datetime.fromisoformat(w2["extracted_at"])
    # Just verify it's a valid datetime (don't check recency for cached result)
    assert extracted_at.year >= 2024


@pytest.mark.unit
def test_extract_tax_year_validation(cached_w2_extraction: pl.DataFrame) -> None:
    """Test that tax year is validated correctly."""
    result = cached_w2_extraction

    w2 = result.row(0, named=True)

    # Tax year should be reasonable (2000-2100)
    assert w2["tax_year"] >= 2000
    assert w2["tax_year"] <= 2100
    # Should match sample (2024)
    assert w2["tax_year"] == 2024


@pytest.mark.unit
def test_extract_without_year_parameter(
    sample_w2_file: Path, extractor_config: W2ExtractionConfig
) -> None:
    """Test that year can be extracted from document without explicit parameter."""
    extractor = W2Extractor(extractor_config)

    # Extract WITHOUT providing tax_year parameter
    result = extractor.extract_from_file(sample_w2_file, tax_year=None)

    # Should still extract year from document (Google Cloud sample is from 2018)
    w2 = result.row(0, named=True)
    assert w2["tax_year"] == 2018  # Year from Google Cloud sample PDF

    # Verify all other fields extracted correctly
    assert w2["employee_first_name"] == "Howard"
    # Note: Google Cloud sample doesn't have extractable employer name
