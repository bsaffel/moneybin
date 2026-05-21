# Test Fixtures

This directory contains sample data files used across test suites.

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
