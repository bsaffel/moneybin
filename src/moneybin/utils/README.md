# Utility Modules

This directory contains shared utility functions used across MoneyBin.

## Modules

### `file.py`

Generic file utilities for copying source files into the raw data directory.

**Key function:**
```python
from moneybin.utils.file import copy_to_raw

# Copy any file to raw data directory
copy_to_raw(
    source_file="~/Downloads/bank.qfx",
    file_type="ofx",
    base_data_path="data/raw"
)
```

**Features:**
- **Idempotent**: Uses SHA-256 hash to detect identical files
- **Generic**: Works with any file type (OFX, CSV, PDF, etc.)
- **Type-based organization**: Files organized by type in subdirectories

**Examples:**
```python
# OFX/QFX files go to data/raw/ofx/
copy_to_raw("statement.qfx", "ofx")
copy_to_raw("statement.ofx", "ofx")  # Same directory

# CSV files go to data/raw/csv/
copy_to_raw("transactions.csv", "csv")

# PDF files go to data/raw/pdf/
copy_to_raw("statement.pdf", "pdf")
```

### `secrets_manager.py`

Secure credential management (future implementation).

## Design Principles

1. **Simple functions over classes** - Prefer standalone functions unless state is needed
2. **Generic and reusable** - Utilities should work across different data sources
3. **Idempotent by default** - Operations should be safe to run multiple times
4. **Type hints required** - All functions must have complete type annotations
5. **Path objects** - Always use `pathlib.Path` over string paths
