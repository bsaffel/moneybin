# W-2 Extraction Architecture

## Overview

The W-2 extraction system implements a **dual extraction strategy** combining text extraction and OCR to reliably extract IRS Form W-2 data from PDF files. The system is designed for accuracy, handling various PDF formats (text-based and image-based) and validating results through cross-method comparison.

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Extraction Pipeline](#extraction-pipeline)
- [PDF Layout Handling](#pdf-layout-handling)
- [Text Parsing Logic](#text-parsing-logic)
- [Confidence Scoring](#confidence-scoring)
- [Data Model](#data-model)
- [Error Handling](#error-handling)
- [Performance](#performance)
- [Testing Strategy](#testing-strategy)

## Architecture Overview

```
┌─────────────┐
│  W-2 PDF    │
│  (Input)    │
└──────┬──────┘
       │
       ├──────────────────────┬──────────────────────┐
       │                      │                      │
       ▼                      ▼                      ▼
┌──────────────┐      ┌──────────────┐      ┌──────────────┐
│ PDF Cropping │      │ PDF Cropping │      │   Metadata   │
│ (top-left ¼) │      │ (top-left ¼) │      │  Extraction  │
└──────┬───────┘      └──────┬───────┘      └──────┬───────┘
       │                      │                      │
       ▼                      ▼                      │
┌──────────────┐      ┌──────────────┐              │
│   pdfplumber │      │  pytesseract │              │
│ Text Extract │      │  OCR Extract │              │
└──────┬───────┘      └──────┬───────┘              │
       │                      │                      │
       ▼                      ▼                      ▼
┌────────────────────────────────────────────────────┐
│           Regex-Based Text Parsing                 │
│     (Tax Year, SSN, EIN, Names, Amounts)           │
└────────────────────┬───────────────────────────────┘
                     │
                     ▼
┌────────────────────────────────────────────────────┐
│         Pydantic Schema Validation                 │
│           (W2FormSchema)                           │
└────────────────────┬───────────────────────────────┘
                     │
                     ▼
┌────────────────────────────────────────────────────┐
│         Confidence Calculation                     │
│    (Based on field completeness)                   │
└────────────────────┬───────────────────────────────┘
                     │
                     ▼
┌────────────────────────────────────────────────────┐
│         Method Agreement Check                     │
│   (Compare text vs OCR results)                    │
└────────────────────┬───────────────────────────────┘
                     │
                     ▼
┌────────────────────────────────────────────────────┐
│         Select Best Result                         │
│   (Highest confidence above threshold)             │
└────────────────────┬───────────────────────────────┘
                     │
                     ▼
┌────────────────────────────────────────────────────┐
│         Polars DataFrame                           │
│    → Parquet → DuckDB (raw.w2_forms)               │
└────────────────────────────────────────────────────┘
```

## Extraction Pipeline

### 1. Dual Extraction Strategy

The system attempts extraction using two methods:

**Text Extraction (pdfplumber)**
- Fast (~1 second per W-2)
- Works best with text-based PDFs
- Direct text extraction without image processing
- Preferred when confidence is high

**OCR Extraction (pytesseract)**
- Slower (~10 seconds per W-2)
- Works with image-based or scanned PDFs
- Converts PDF pages to images (300 DPI)
- Fallback for low-quality PDFs

### 2. Cropping Strategy

**Problem**: Many W-2 PDFs contain 4 copies in a 2×2 grid layout (employee copy, employer copy, duplicate for each), causing pdfplumber and pytesseract to extract duplicate data like:
```
"Okta, Inc Okta, Inc"
"100 First Street 100 First Street"
```

**Solution**: Crop to top-left quadrant before extraction
- **pdfplumber**: Crop page using `page.crop((0, 0, width/2, height/2))`
- **pytesseract**: Crop PIL image using `image.crop((0, 0, width//2, height//2))`

**Benefits**:
- Eliminates duplicate data at the source
- Reduces processing time (1/4 of page data)
- Works consistently for both methods
- Simpler than post-processing deduplication

### 3. Method Comparison & Validation

After both extractions complete:
1. Calculate confidence score for each result (0.0 to 1.0)
2. Compare key fields between methods (SSN, EIN, wages, federal tax)
3. Check agreement ratio (must be ≥80% for high confidence)
4. Select result with highest confidence above threshold (default: 0.7)

**Configuration Options**:
```python
W2ExtractionConfig(
    require_dual_extraction=True,   # Both methods must succeed
    min_confidence_score=0.8,       # Minimum acceptable confidence
    enable_ocr=True,                 # Enable OCR (disable for testing)
)
```

## PDF Layout Handling

### Typical W-2 PDF Layouts

**Layout 1: Single W-2 (Standard)**
```
┌─────────────────────┐
│                     │
│    W-2 Form         │
│  (Employee Copy)    │
│                     │
└─────────────────────┘
```

**Layout 2: Side-by-Side Copies**
```
┌─────────────┬─────────────┐
│   W-2       │   W-2       │
│ (Employee)  │ (Employer)  │
└─────────────┴─────────────┘
```

**Layout 3: 2×2 Grid (Most Common)**
```
┌─────────────┬─────────────┐
│   W-2       │   W-2       │
│ (Employee)  │ (Employee)  │
├─────────────┼─────────────┤
│   W-2       │   W-2       │
│ (Employer)  │ (Employer)  │
└─────────────┴─────────────┘
```

**Our Approach**: Always crop to top-left quadrant (works for all layouts)

## Text Parsing Logic

The `_parse_w2_text()` method uses regex patterns and heuristics to extract W-2 fields. See the method's docstring for detailed implementation notes.

### Parsing Order

1. **Text Cleanup**: Normalize whitespace (`" ".join(text.split())`)
2. **Tax Year**: Multi-tier fallback strategy
3. **SSN**: Pattern `\d{3}[-\s]?\d{2}[-\s]?\d{4}`
4. **EIN**: Pattern `\d{2}[-\s]?\d{7}`
5. **Monetary Amounts**: All `\d{1,7}\.\d{2}` amounts in order
6. **Employer Name**: Company name with suffix (Inc, LLC, Corp, etc.)
7. **Addresses**: Street address patterns with state and ZIP
8. **Employee Name**: Capitalized words near SSN with filtering
9. **State Info**: State code + employer ID + wages + tax
10. **Box 12 Codes**: Letter codes + amounts (retirement, etc.)

### Tax Year Extraction Strategy

Multi-tier fallback approach to handle various PDF types:

```
1. Explicit Parameter (--year CLI flag)
   ↓ (if not provided)
2. Text Pattern Match (20[2-3]\d in extracted text)
   ↓ (if not found)
3. OCR Error Correction ([eEoO0]0[2-3]\d → 20[2-3]\d)
   ↓ (if not found)
4. Filename Pattern (year in PDF filename)
   ↓ (if not found)
5. PDF Metadata (creation year - 1)
   ↓ (if not found)
FAIL: ValueError
```

**Rationale**: Many W-2 PDFs don't contain the tax year in extractable text (especially image-based PDFs). The fallback strategy ensures robust year detection.

### Employee Name Extraction

**Challenge**: Distinguishing employee names from employer names, addresses, and city names in unstructured text.

**Strategy**:
1. Find SSN position in text
2. Search 500 characters after SSN for capitalized word pairs
3. Filter out false positives:
   - Company indicators (Inc, LLC, Corp, Company)
   - Address components (Street, Avenue, Floor, Suite)
   - City prefixes (San, Los, New, Fort)
   - Ordinals/directions (First, Second, North, South)
4. Use first remaining match as employee name

**Example**:
```
Text: "26-4175727 Okta, Inc 100 First Street San Francisco, CA 94105 409-53-0099 Brandon Saffel..."
                                                                                    ^^^^^^^ ^^^^^^^
                                                                                    Matched!
```

### Monetary Amount Mapping

W-2 forms contain amounts in a predictable order. The parser finds all decimal amounts and maps them sequentially:

```python
amounts = re.findall(r"\b\d{1,7}\.\d{2}\b", text)

# Map to W-2 boxes in order:
data["wages"] = amounts[0]                    # Box 1
data["federal_income_tax"] = amounts[1]       # Box 2
data["social_security_wages"] = amounts[2]    # Box 3
data["social_security_tax"] = amounts[3]      # Box 4
data["medicare_wages"] = amounts[4]           # Box 5
data["medicare_tax"] = amounts[5]             # Box 6
```

**Fallback**: If fewer amounts found, reuse wages for SS/Medicare wages.

## Confidence Scoring

The `_calculate_confidence()` method scores each extraction result based on field completeness.

### Confidence Formula

```
confidence = (0.7 × required_completeness) + (0.3 × important_completeness)

where:
  required_completeness = (required_fields_present) / (total_required_fields)
  important_completeness = (important_fields_present) / (total_important_fields)
```

### Field Classification

**Required Fields** (70% weight):
- `tax_year`, `employee_ssn`, `employee_first_name`, `employee_last_name`
- `employer_ein`, `employer_name`
- `wages`, `federal_income_tax`

**Important Fields** (30% weight):
- `social_security_wages`, `social_security_tax`
- `medicare_wages`, `medicare_tax`
- `employee_address`, `employer_address`

### Confidence Thresholds

- **≥0.8**: High confidence, accept result
- **0.7-0.8**: Medium confidence, accept if no better option
- **<0.7**: Low confidence, reject (by default)

### Agreement Checking

The `_check_agreement()` method compares key fields between text and OCR results:

**Comparison Logic**:
- **Numeric fields**: Exact match (1.0) or within 5% (0.5 partial credit)
- **String fields**: Exact match only (1.0)

**Agreement Threshold**: ≥80% for high confidence

**Example**:
```python
# Text extraction result
text_data = {"wages": 319075.95, "ssn": "409-53-0099", ...}

# OCR extraction result
ocr_data = {"wages": 319076.00, "ssn": "409-53-0099", ...}

# Agreement check:
# - wages: 319075.95 vs 319076.00 → within 5% → 0.5 credit
# - ssn: exact match → 1.0 credit
# - ... (check all fields)
# Agreement ratio: 0.95 (95%) → HIGH AGREEMENT
```

## Data Model

### Schema Design Philosophy

The W-2 data model balances **structure** (queryability) with **flexibility** (handling sparse/variable data):

**Typed Columns** → Core fields used in most queries
**JSON Columns** → Variable/sparse data that would create many NULL columns

### W2FormSchema (Pydantic Model)

See `src/moneybin/extractors/w2_extractor.py` for full schema definition.

**Core Fields**:
- Employee: SSN, name, address
- Employer: EIN, name, address
- Wages/taxes: All major W-2 boxes (1-11, 13)

**JSON Fields**:
- `state_local_info`: Array of state/local tax records (0-2 states typical)
- `optional_boxes`: Object with box 12 codes and box 14 other

**Example JSON Structure**:
```json
{
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
      "C": "454.20",       // Group-term life insurance
      "D": "23000.00",     // 401(k) contributions
      "W": "8300.00",      // Employer HSA contributions
      "DD": "15963.22"     // Employer health coverage
    }
  }
}
```

### Database Schema

**Table**: `raw.w2_forms`
**Primary Key**: `(tax_year, employee_ssn, employer_ein, source_file)`

**Design Rationale**:
- Composite key prevents duplicates while allowing re-processing
- No indexes needed (table typically has <50 rows)
- JSON columns for sparse data (state_local_info, optional_boxes)
- DECIMAL(18,2) for monetary values (precision)
- VARCHAR for identifiers (SSN, EIN)

See `src/moneybin/sql/schema/raw_w2_forms.sql` for complete schema.

## Error Handling

### Extraction Failures

**Both Methods Fail**:
```
ValueError: Both extraction methods failed for {file_path}:
  Text: {text_error}
  OCR: {ocr_error}
```

**Low Confidence**:
```
ValueError: Text extraction confidence too low: 0.65 < 0.70
```

**Method Disagreement** (with `require_dual_extraction=True`):
```
ValueError: Extraction methods disagree (agreement: 0.45).
Cannot proceed with low confidence.
```

### Required Field Failures

The parser raises `ValueError` if required fields cannot be extracted:
- Tax year (all fallback strategies exhausted)
- Employee SSN
- Employer EIN
- Employee name (after filtering heuristics)
- Minimum wage amounts (at least 2 amounts required)

### OCR Error Handling

Common OCR errors and corrections:

| OCR Output | Correction | Pattern |
|------------|-----------|---------|
| `e024` | `2024` | Tax year |
| `o024` | `2024` | Tax year |
| `0024` | `2024` | Tax year |
| `409 53 0099` | `409-53-0099` | SSN formatting |
| `26 4175727` | `26-4175727` | EIN formatting |

## Performance

### Extraction Time

| Method | Time | Use Case |
|--------|------|----------|
| Text only | ~1s | Text-based PDFs |
| OCR only | ~10s | Image PDFs |
| Dual (text + OCR) | ~11s | High confidence validation |

### Test Performance

- **18 tests** in **~11 seconds** (first run with OCR) or **~1 second** (cached runs)
- **Strategy**: Session-scoped fixtures cache extraction, OCR enabled but runs once
- **Speedup**: 180× faster than re-extracting for each test (after initial OCR)

### Optimization Techniques

1. **Crop before processing**: Reduces data to 25% of original
2. **Session-scoped fixtures**: Cache extraction results across tests (OCR runs once)
3. **Dual extraction enabled**: Tests run with production-like configuration
4. **Minimal DPI**: Use 300 DPI for OCR (balance quality vs speed)

## Testing Strategy

### Test Coverage

See `tests/moneybin/test_extractors/test_w2_extractor.py` for full test suite.

**Unit Tests**:
- Text extraction
- OCR extraction (mocked)
- Confidence calculation
- Agreement checking
- Field parsing
- Error handling

**Integration Tests**:
- End-to-end extraction
- Database loading
- CLI commands

### Test Fixtures

**Session-scoped fixture** (`cached_w2_extraction`):
```python
@pytest.fixture(scope="session")
def cached_w2_extraction(sample_w2_file: Path) -> pl.DataFrame:
    """Extract W2 once and reuse across all tests.

    OCR is enabled to match production behavior, but session scope
    means it only runs once per test session (~10s initial cost,
    then cached for all subsequent tests).
    """
    config = W2ExtractionConfig(
        enable_ocr=True,  # Match production
        require_dual_extraction=False,
        min_confidence_score=0.7
    )
    extractor = W2Extractor(config)
    return extractor.extract_from_file(sample_w2_file)
```

**Benefits**:
- Extracts PDF once per test session (with OCR)
- All tests use cached DataFrame
- First test run: ~11s (includes OCR)
- Subsequent runs: ~1s (cached)
- Tests match production behavior (OCR enabled)

### Mock Strategies

**Configuration Testing**: Test with various config combinations:
- `require_dual_extraction=True/False`
- `min_confidence_score=0.7/0.8/0.9`
- `enable_ocr=True` (default for production-like testing)

## Best Practices

### For Production Use

1. **Always provide tax year explicitly**: Use `--year` flag to avoid ambiguity
2. **Enable dual extraction**: Set `require_dual_extraction=True` for high-confidence validation
3. **Monitor confidence scores**: Log and review low-confidence extractions
4. **Validate results**: Spot-check extracted data against original PDFs
5. **Keep source files**: Use `preserve_source_files=True` for audit trail

### For Development

1. **Use session fixtures**: Cache expensive OCR operations across tests
2. **Test with varied PDFs**: Include text-based and image-based samples
3. **First test run includes OCR**: Expect ~11s on first run, then cached
4. **Match production config**: Tests run with OCR enabled by default

### For Debugging

1. **Check extraction logs**: Review method comparison and confidence scores
2. **Inspect intermediate text**: Log raw extracted text to diagnose parsing issues
3. **Test cropping**: Verify that PDF cropping is working correctly
4. **Compare methods**: Run both text and OCR to identify format issues

## Future Enhancements

1. **Machine Learning**: Train model to extract W-2 fields from images directly
2. **Layout Detection**: Automatically detect W-2 layout (single, side-by-side, grid)
3. **Multi-page Support**: Handle W-2s spanning multiple pages
4. **Batch Processing**: Process multiple W-2s in parallel
5. **Interactive Review**: Web UI for reviewing/correcting low-confidence extractions
6. **Historical Comparison**: Flag unexpected year-over-year changes
7. **IRS Validation**: Cross-check amounts against IRS guidelines (e.g., SS wage cap)

## References

### Documentation
- [IRS Form W-2 Specifications](https://www.irs.gov/forms-pubs/about-form-w-2)
- [pdfplumber Documentation](https://pdfplumber.readthedocs.io/)
- [pytesseract Documentation](https://github.com/madmaze/pytesseract)
- [Pydantic Documentation](https://docs.pydantic.dev/)

### Implementation Files
- `src/moneybin/extractors/w2_extractor.py` - Main extraction logic
- `src/moneybin/loaders/w2_loader.py` - Database loading
- `src/moneybin/sql/schema/raw_w2_forms.sql` - Database schema
- `src/moneybin/cli/commands/extract.py` - CLI command
- `tests/moneybin/test_extractors/test_w2_extractor.py` - Tests

### Related Documentation
- [W-2 Extraction Feature Guide](./w2-extraction-feature.md) - Feature overview, usage examples, and CLI commands
- [OFX Import Guide](./ofx-import-guide.md) - OFX extraction (similar pattern)
- [Testing Strategy](./.cursor/rules/python-testing.mdc) - Testing standards

---

**Quick Links**:
- **Getting Started**: See [W-2 Extraction Feature Guide](./w2-extraction-feature.md) for usage examples
- **Code Reference**: See `src/moneybin/extractors/w2_extractor.py` for implementation
- **API Reference**: See method docstrings in `W2Extractor` class for detailed parameter descriptions
