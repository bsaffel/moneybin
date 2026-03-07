# Feature: W-2 PDF Extraction

## Status
implemented

## Goal
Extract IRS Form W-2 data from PDF files using dual extraction (text + OCR) for high-confidence results, and load into DuckDB for tax analysis.

## Background
- [ADR-001: Medallion Data Layers](../../architecture/001-medallion-data-layers.md)
- [Data Model](../../reference/data-model.md) -- `raw.w2_forms` table

## Requirements

1. Extract all standard W-2 fields: employee/employer info, wages (Box 1), federal/state/FICA taxes.
2. Support text-based and image-based PDFs via dual extraction strategy.
3. Handle common W-2 layouts: single form, side-by-side, 2x2 grid.
4. Calculate confidence score for each extraction (threshold: 0.7 minimum).
5. Compare text and OCR results when both succeed (require 80% agreement).
6. Store state/local info and optional boxes as JSON for flexibility.
7. Idempotent loading with composite primary key.
8. Multi-tier tax year fallback (explicit param > text > OCR correction > filename > metadata).

## Data Model

### Raw table

`raw.w2_forms` -- PK: `(tax_year, employee_ssn, employer_ein, source_file)`

**Typed columns**: Employee SSN/name/address, employer EIN/name/address, wages, federal income tax, SS wages/tax, Medicare wages/tax, control number, Box 13 flags.

**JSON columns**:
- `state_local_info` -- Array of `{state, employer_state_id, state_wages, state_income_tax}` (0-2 entries)
- `optional_boxes` -- Object with `box_12_codes` (letter code -> amount) and `box_14_other`

All monetary values use `DECIMAL(18,2)`.

## Implementation Plan

### Files created
- `src/moneybin/extractors/w2_extractor.py` (901 lines) -- Dual extraction, parsing, confidence scoring
- `src/moneybin/loaders/w2_loader.py` (126 lines) -- DuckDB loading
- `src/moneybin/sql/schema/raw_w2_forms.sql` (74 lines) -- DDL
- `tests/moneybin/test_extractors/test_w2_extractor.py` (394 lines)
- `tests/moneybin/test_loaders/test_w2_loader.py` (164 lines)

### Key decisions

**Dual extraction**: Text extraction (~1s) via pdfplumber as primary, OCR (~10s) via pytesseract as validation/fallback. Both methods crop to top-left quadrant first to handle 2x2 grid layouts.

**Confidence scoring**: `0.7 * required_field_completeness + 0.3 * important_field_completeness`. Required fields: tax_year, SSN, EIN, employer name, wages, federal tax. Important: SS/Medicare wages and taxes, addresses.

**JSON for variable data**: W-2 state/local info varies (0-2 states). Box 12 codes are sparse. JSON columns avoid 50+ mostly-NULL columns while maintaining queryability for core fields.

**Amount mapping**: W-2 forms list amounts in predictable order. Parser finds all decimal amounts and maps sequentially to boxes 1-6.

## CLI Interface

```bash
# Extract and load
moneybin extract w2 ~/Downloads/W2.pdf --year 2024

# Extract without loading
moneybin extract w2 W2.pdf --no-load

# Skip source file copy
moneybin extract w2 W2.pdf --no-copy
```

## MCP Interface

- `tax.w2_summary` -- Summarizes W-2 data by year
- `moneybin://w2/{tax_year}` -- Resource for W-2 data

## Testing Strategy

- **Session-scoped fixtures**: Cache OCR extraction across all tests (first run ~11s, subsequent ~1s).
- **18 tests** covering: text extraction, OCR extraction, confidence calculation, agreement checking, field parsing, error handling, end-to-end extraction, database loading.
- Tests run with OCR enabled to match production behavior.

### Error handling tested
- Both methods fail
- Low confidence results
- Method disagreement with `require_dual_extraction=True`
- Missing required fields (SSN, EIN, minimum amounts)
- OCR error correction (e.g., `e024` -> `2024` for tax year)

## Dependencies

- `pdfplumber` -- Text extraction from PDFs
- `pytesseract` -- OCR engine wrapper
- `pdf2image` -- PDF to image conversion for OCR
- `polars` -- DataFrame operations
- System: `poppler` and `tesseract` (via Homebrew)

## Out of Scope

- Form 1040 extraction
- 1099 form extraction
- Multi-page W-2 support
- Machine learning-based field extraction
