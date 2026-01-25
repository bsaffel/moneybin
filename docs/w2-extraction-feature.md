# W2 Form Extraction Feature

## Overview

Complete implementation of W2 tax form extraction and loading functionality following the OFX import pattern. The feature uses a **dual extraction strategy** (text + OCR) to ensure high confidence in data extraction from PDF W2 forms.

> **📖 For detailed technical documentation**, see [W-2 Extraction Architecture](./w2-extraction-architecture.md) which covers the complete extraction pipeline, parsing logic, confidence scoring, and error handling strategies.

## Key Features

### 1. Dual Extraction Strategy

- **Text Extraction**: Fast extraction using pdfplumber (~1 second)
- **OCR Extraction**: Robust extraction using pytesseract + pdf2image (~10 seconds)
- **Confidence Scoring**: Automatic confidence calculation based on data completeness
- **Result Comparison**: Validates agreement between methods (requires 80% field agreement)
- **Fallback**: Uses text-only if OCR fails but confidence is high enough

### 2. Thoughtful Data Model

The data model balances structure with flexibility:

**Core Fields (Typed Columns)**:
- Employee info: SSN, name, address
- Employer info: EIN, name, address
- Wages and federal/state/medicare taxes
- Control number, checkboxes (box 13)

**Flexible Fields (JSON)**:
- `state_local_info`: Array supporting 0-2 states per W2
- `optional_boxes`: Object for sparse data (box 12 codes, box 14)

This design avoids both sparse wide tables (50+ mostly-null columns) and over-normalization (10+ tables), while maintaining fidelity with the raw source.

### 3. Comprehensive Testing

- 18 tests covering extraction, loading, validation, and error handling
- **Smart test caching**: Session-scoped fixtures cache OCR results
- **First run**: ~11 seconds (includes OCR extraction)
- **Subsequent runs**: ~1 second (uses cached results)
- Tests use production-like configuration (OCR enabled)

## Components

### Extractor

**Location**: `src/moneybin/extractors/w2_extractor.py`

**Key Classes**:
- `W2Extractor`: Main extractor with dual extraction strategy
- `W2FormSchema`: Pydantic model for validation
- `W2StateLocalInfo`: State/local tax information model
- `W2OptionalBoxes`: Optional box data model
- `ExtractionResult`: Result container for each extraction method

**Configuration**:
```python
W2ExtractionConfig(
    save_raw_data=True,
    raw_data_path=Path("data/raw/w2"),
    require_dual_extraction=True,  # Require both methods to agree
    min_confidence_score=0.8,      # Minimum confidence threshold
    enable_ocr=True,                # Enable/disable OCR (for testing)
)
```

### Loader

**Location**: `src/moneybin/loaders/w2_loader.py`

**Features**:
- Creates DuckDB raw tables following Fivetran naming convention
- Uses `INSERT OR REPLACE` for idempotent loading
- Handles JSON fields (state_local_info, optional_boxes)
- Query support with optional limits

### SQL Schema

**Location**: `src/moneybin/sql/schema/raw_w2_forms.sql`

**Table**: `raw.w2_forms`

**Key Design Decisions**:
- Primary key: `(tax_year, employee_ssn, employer_ein, source_file)`
- Indexes on tax_year, employee_ssn, and employer_ein
- JSON columns for state/local info and optional boxes
- DECIMAL(18,2) for monetary amounts

### CLI Commands

**Location**: `src/moneybin/cli/commands/extract.py`

**Command**: `moneybin extract w2`

**Usage**:
```bash
# Extract and load with explicit year
moneybin extract w2 ~/Downloads/W2.pdf --year 2024

# Extract without loading to database
moneybin extract w2 W2.pdf --no-load

# Extract without copying source file
moneybin extract w2 W2.pdf --no-copy
```

**Features**:
- Optional tax year parameter (auto-derives from PDF creation date if not provided)
- Copies source file to `data/raw/w2/` directory
- Extracts to parquet format
- Loads to DuckDB raw tables
- Detailed logging with extraction summary

## Tax Year Extraction Strategy

Since tax year may not appear in extracted text (especially in image-based PDFs), the system uses a multi-tier fallback strategy:

1. **Explicit parameter**: Use `--year` CLI option (preferred for production)
2. **PDF text extraction**: Search for 4-digit year (2020-2030)
3. **Filename parsing**: Extract year from filename
4. **PDF metadata**: Derive from creation date (assumes W2 created in year after tax year)

## Dependencies

### New Dependencies Added

- `pytesseract==0.3.13`: Python wrapper for Tesseract OCR
- `pdf2image==1.17.0`: Convert PDF pages to images for OCR

### System Dependencies

- **poppler**: PDF rendering library (installed via Homebrew)
- **tesseract**: OCR engine (installed via Homebrew)

Installation:
```bash
brew install poppler tesseract
uv add pytesseract pdf2image
```

## Performance

### Production Usage

- Text extraction: ~1 second
- OCR extraction: ~10 seconds
- Total per W2: ~11 seconds (with dual extraction)
- Confidence validation ensures accuracy

### Test Performance

- **First test run**: ~11 seconds (includes one OCR extraction, cached for session)
- **Subsequent runs**: ~1 second (uses cached extraction result)
- **Strategy**:
  - Session-scoped fixtures cache extraction across all tests
  - OCR enabled to match production behavior
  - OCR runs once per test session, not per test
  - 180× faster than running OCR for each test individually

## Data Flow

```
W2 PDF → Text Extraction → Parse → Validate
    ↓
    OCR Extraction → Parse → Validate
    ↓
Compare Results → Select Best → Pydantic Validation
    ↓
Save to Parquet → Load to DuckDB (raw.w2_forms)
    ↓
dbt Transformations (future)
```

## Example Data

**Input**: IRS Form W-2 PDF

**Output**:
```json
{
  "tax_year": 2024,
  "employee_ssn": "409-53-0099",
  "employee_first_name": "Brandon",
  "employee_last_name": "Saffel",
  "employer_ein": "26-4175727",
  "employer_name": "Okta, Inc",
  "wages": 319075.95,
  "federal_income_tax": 63880.90,
  "social_security_wages": 168600.00,
  "social_security_tax": 10453.20,
  "medicare_wages": 342075.95,
  "medicare_tax": 6238.78,
  "state_local_info": [
    {
      "state": "GA",
      "employer_state_id": "3087320BQ",
      "state_wages": "319075.95",
      "state_income_tax": "16681.47"
    }
  ],
  "optional_boxes": {
    "box_12_codes": {
      "C": "454.20",
      "D": "23000.00",
      "W": "8300.00",
      "DD": "15963.22"
    }
  }
}
```

## Testing

Run tests:
```bash
# W2 extractor tests (0.66s)
uv run pytest tests/moneybin/test_extractors/test_w2_extractor.py -v

# W2 loader tests
uv run pytest tests/moneybin/test_loaders/test_w2_loader.py -v

# All W2 tests (~1s)
uv run pytest tests/moneybin/test_extractors/test_w2_extractor.py tests/moneybin/test_loaders/test_w2_loader.py -v
```

## Future Enhancements

1. **OCR Optimization**: Cache OCR results to avoid re-processing
2. **Multi-page Support**: Handle W2s with multiple copies on different pages
3. **Batch Processing**: Process multiple W2s in parallel
4. **dbt Models**: Create analytical models for W2 data
5. **W2 Validation**: Cross-check amounts against IRS guidelines
6. **Import from Images**: Support JPG/PNG W2 images directly

## Files Modified/Created

### New Files
- `src/moneybin/extractors/w2_extractor.py` (901 lines)
- `src/moneybin/loaders/w2_loader.py` (126 lines)
- `src/moneybin/sql/schema/raw_w2_forms.sql` (74 lines)
- `tests/moneybin/test_extractors/test_w2_extractor.py` (394 lines)
- `tests/moneybin/test_loaders/test_w2_loader.py` (164 lines)
- `tests/fixtures/sample_w2_2024.pdf` (test fixture)

### Modified Files
- `src/moneybin/cli/commands/extract.py` (added `extract_w2` command)
- `src/moneybin/loaders/__init__.py` (exported `W2Loader`)
- `pyproject.toml` (added pytesseract, pdf2image dependencies)

## References

- [pdfplumber Documentation](https://pdfplumber.readthedocs.io/)
- [pytesseract Documentation](https://github.com/madmaze/pytesseract)
- [IRS W-2 Form Specifications](https://www.irs.gov/forms-pubs/about-form-w-2)
- [Pydantic Documentation](https://docs.pydantic.dev/)
