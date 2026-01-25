# OFX/QFX Import Guide

## Overview

MoneyBin supports importing OFX (Open Financial Exchange) and QFX (Quicken Web Connect) files, providing full control over your financial data with an idempotent, archive-friendly workflow.

## Quick Start

```bash
# Basic import - copies source, extracts, and loads to DuckDB
moneybin extract ofx ~/Downloads/WellsFargo_2025.qfx

# With custom institution name
moneybin extract ofx file.qfx --institution "Wells Fargo"

# Extract only (skip database load)
moneybin extract ofx file.qfx --no-load

# Skip source file copy (use file in place)
moneybin extract ofx file.qfx --no-copy
```

## Directory Structure

MoneyBin organizes OFX data like extracting a zip archive - clean, idempotent, and portable:

```
data/raw/ofx/
├── WellsFargo_Incoming_2025.qfx              # Source file
├── Chase_Checking_2025.qfx                   # Another source file
└── extracted/                                # Extracted data
    ├── WellsFargo_Incoming_2025/             # Named after source
    │   ├── institutions.parquet
    │   ├── accounts.parquet
    │   ├── transactions.parquet
    │   └── balances.parquet
    └── Chase_Checking_2025/
        ├── institutions.parquet
        ├── accounts.parquet
        ├── transactions.parquet
        └── balances.parquet
```

## Idempotent Behavior

Running the same import multiple times is safe and clean:

1. **Source file copying**:
   - If identical file exists → skips copy
   - If different content → overwrites
   - No `_1`, `_2`, `_3` suffixes!

2. **Parquet extraction**:
   - Overwrites previous extraction
   - Same filenames each time
   - Clean directory structure

3. **DuckDB loading**:
   - Uses `INSERT OR REPLACE` for idempotency
   - Primary keys prevent duplicates
   - Safe to reload same data

## Data Flow

```
Source File (QFX)
    ↓
Copy to data/raw/ofx/ (idempotent, hash-based)
    ↓
Extract to Parquet (data/raw/ofx/extracted/<name>/)
    ↓
Load to DuckDB (raw.ofx_* tables)
```

The copy step uses `moneybin.utils.file.copy_to_raw()` which:

- Checks file hash to avoid redundant copies
- Preserves original filename
- Overwrites only if content differs

## Why Parquet First?

MoneyBin saves to Parquet before DuckDB for data ownership:

✅ **Portable Archive**: Universal format readable by any tool
✅ **Disaster Recovery**: Rebuild database from Parquet anytime
✅ **Data Lineage**: Clear separation of raw vs transformed data
✅ **Multi-Database**: Use same Parquet with different databases
✅ **Backup Strategy**: Parquet files ARE your backup

Think of it like this:

- **Parquet files** = Permanent financial archive (like paper statements)
- **DuckDB** = Working database (like a spreadsheet)

## Raw Tables

Data loads into these DuckDB tables:

- `raw.ofx_institutions` - Financial institution info
- `raw.ofx_accounts` - Account details (account_id, type, routing)
- `raw.ofx_transactions` - Transaction records with amounts, dates, payees
- `raw.ofx_balances` - Account balance snapshots

## Supported File Formats

- **.qfx** - Quicken Web Connect (most common)
- **.ofx** - Open Financial Exchange

Both formats are OFX under the hood - QFX is just Quicken's branding.

## File Format Support

MoneyBin handles both OFX format styles:

### SGML Format (most common)

```
OFXHEADER:100DATA:OFXSGMLVERSION:102...
<OFX><SIGNONMSGSRSV1>...
```

Used by: Wells Fargo, Chase, Bank of America, most banks

### XML Format

```xml
<?xml version="1.0" encoding="UTF-8"?>
<OFX>
  <SIGNONMSGSRSV1>...
```

Used by: Some credit unions, smaller institutions

MoneyBin automatically detects and handles both formats.

## Example: Wells Fargo Import

```bash
$ moneybin extract ofx ~/Downloads/WellsFargo_Incoming_2025.qfx --institution "Wells Fargo"

👤 Using profile: default
📁 Copying file to data/raw/ofx/...
✅ Copied to: data/raw/ofx/WellsFargo_Incoming_2025.qfx
📊 Extracting OFX data from: data/raw/ofx/WellsFargo_Incoming_2025.qfx
✅ Extraction complete:
   Institutions: 1 rows
   Accounts: 1 rows
   Transactions: 186 rows
   Balances: 1 rows
💾 Loading data to DuckDB raw tables...
✅ Data loaded to DuckDB:
   raw.ofx_institutions: 1 rows
   raw.ofx_accounts: 1 rows
   raw.ofx_transactions: 186 rows
   raw.ofx_balances: 1 rows
🎉 OFX import complete!
```

Results:

- Source file: 93 KB
- Extracted Parquet: 17 KB total (8.4 KB for transactions)
- Compression ratio: ~5.5x

## Querying Imported Data

```sql
-- View all transactions
SELECT * FROM raw.ofx_transactions;

-- Check account balances
SELECT account_id, ledger_balance, available_balance
FROM raw.ofx_balances;

-- Transaction summary
SELECT
    COUNT(*) as total_transactions,
    SUM(CASE WHEN amount < 0 THEN 1 ELSE 0 END) as debits,
    SUM(CASE WHEN amount > 0 THEN 1 ELSE 0 END) as credits,
    SUM(amount) as net_amount
FROM raw.ofx_transactions;
```

## Data Transformation

After loading to raw tables, use dbt to transform:

```bash
# Transform OFX raw data into analytics models
moneybin transform run
```

See `dbt/models/core/` for transformation models.

## Troubleshooting

### File Not Found

```bash
Error: File not found: /path/to/file.qfx
```

**Solution**: Check the file path and ensure the file exists.

### Invalid OFX Format

```bash
Error: Invalid OFX file format: ...
```

**Solution**: Verify the file is a valid OFX/QFX file. Try opening in a text editor to check the format.

### Database Lock Error

```bash
Error: Could not set lock on file "moneybin.duckdb"
```

**Solution**: Close any other DuckDB connections (duckdb CLI, other processes).

## Best Practices

1. **Keep source files**: The `--copy` (default) preserves original files
2. **Regular imports**: Import monthly statements as you receive them
3. **Backup Parquet**: Your `data/raw/ofx/extracted/` directory is your archive
4. **Profile separation**: Use profiles for different people/accounts

   ```bash
   moneybin --profile=alice extract ofx alice_transactions.qfx
   moneybin --profile=bob extract ofx bob_transactions.qfx
   ```

## Technical Details

- **Parser**: Uses `ofxparse` library (well-maintained, 14+ years)
- **Validation**: Pydantic schemas validate all extracted data
- **Decimal precision**: Uses Python Decimal for exact financial amounts
- **Timestamps**: ISO 8601 format with timezone support
- **Idempotency**: Hash-based duplicate detection for source files
