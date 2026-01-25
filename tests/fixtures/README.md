# Test Fixtures

This directory contains sample data files used across test suites.

## W-2 Tax Form Fixtures

### `sample_w2_2024.pdf`

A public domain sample W-2 from Google Cloud Document AI samples containing non-PII test data:

- **Source**: https://storage.googleapis.com/cloud-samples-data/documentai/LendingDocAI/W2Parser/W2_XL_input_clean_1000.pdf
- **Employee**: Howard Radial (SSN: 077-49-4905 - sample/fake data)
- **Employer**: EIN 37-2766773
- **Tax Year**: 2010/2018 (test overrides to 2024)
- **Wages**: $28,287.19
- **Federal Tax**: $1,608.75
- **Layout**: 2×2 grid (4 copies of W-2 on single page)

Used by:

- `test_w2_extractor.py` - Testing W-2 PDF extraction and parsing
- `test_w2_loader.py` - Testing W-2 data loading to DuckDB

**Note**: This is a publicly available sample document with no real PII, safe for committing to source control.

## OFX/QFX Fixtures

### `sample_statement.qfx`

A minimal but complete OFX file containing:

- **Institution**: Test Bank (FID: 12345)
- **Account**: Checking account 9876543210 at routing 123456789
- **Transactions**: 3 sample transactions (2 debits, 1 credit)
- **Balances**: Ledger balance of $5,000.00, available balance of $4,800.00

Used by:

- `test_ofx_extractor.py` - Testing OFX file parsing and extraction
- `test_ofx_loader.py` - Testing OFX data loading to DuckDB

## Usage

```python
from pathlib import Path

# Access fixture files
FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"
sample_qfx = FIXTURES_DIR / "sample_statement.qfx"
```

## Adding New Fixtures

When adding new test data files:

1. Place the file in this directory
2. Document it in this README
3. Use descriptive filenames (e.g., `sample_credit_card.qfx`, `invalid_format.qfx`)
4. Keep files minimal but realistic
