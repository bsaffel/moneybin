# Data Import

MoneyBin imports financial data from local files. Each import auto-detects the file type, loads data into the raw schema, and rebuilds core analytical tables.

```bash
moneybin import file <path>
```

## OFX / QFX Bank Statements

Import OFX and QFX files from any bank or credit card provider. MoneyBin extracts accounts, transactions, and balances from the standard OFX format.

```bash
# Import a checking account statement
moneybin import file ~/Downloads/checking.qfx

# Tag with institution name for organization
moneybin import file ~/Downloads/savings.ofx --institution "Wells Fargo"
```

**What gets extracted:**
- Accounts (name, type, institution, account ID)
- Transactions (date, amount, description, type, FITID)
- Balances (ledger and available, as-of date)

OFX files carry their own account identifiers, so no additional metadata is needed. Re-importing the same file is safe — the FITID (Financial Transaction ID) prevents duplicates.

## Smart Tabular Import

The universal tabular importer handles CSV, TSV, Excel (.xlsx), Parquet, and Feather files from any institution. It uses a five-stage pipeline to automatically detect file structure, map columns, and normalize data.

```bash
# Auto-detect everything — works for most bank exports
moneybin import file ~/Downloads/chase_activity.csv --account-name "Chase Checking"

# Use a built-in or saved format
moneybin import file ~/Downloads/transactions.csv --format chase_credit

# Override specific column mappings when auto-detection misses
moneybin import file export.csv --override date=TransDate --override amount=Amt

# Import Excel with a specific sheet
moneybin import file ~/Downloads/report.xlsx --sheet "Transactions"

# Import Parquet from a data warehouse export
moneybin import file ~/Downloads/transactions.parquet --account-name "Main Account"
```

### Five-Stage Pipeline

1. **Format detection** — identifies encoding, delimiter, file type, preamble rows
2. **File reading** — finds the header row, strips trailing summary rows
3. **Column mapping** — matches headers to standard fields using 100+ aliases and content validation
4. **Transform** — parses dates, normalizes amounts (handles sign conventions, debit/credit splits, international number formats), generates content-hash IDs
5. **Load** — writes to raw tables with import batch tracking

### Supported File Formats

| Format | Extensions | Notes |
|--------|-----------|-------|
| CSV | `.csv` | Auto-detects delimiter (comma, semicolon, pipe) |
| TSV | `.tsv`, `.tab` | Tab-delimited |
| Excel | `.xlsx` | Auto-selects the largest sheet, or specify with `--sheet` |
| Parquet | `.parquet` | Zero-copy Arrow ingestion |
| Feather | `.feather` | Zero-copy Arrow ingestion |

### Built-In Institution Formats

| Format | Institution | Notes |
|--------|-----------|-------|
| `chase_credit` | Chase | Credit card exports |
| `citi_credit` | Citi | Credit card exports |
| `mint` | Mint | Migration from Mint exports |
| `tiller` | Tiller | Migration from Tiller Money spreadsheets |
| `ynab` | YNAB | Migration from You Need A Budget exports |
| `maybe` | Maybe | Migration from Maybe Finance exports |

### Column Detection

The column mapper uses 100+ header aliases to identify standard fields:
- Date columns: "Transaction Date", "Post Date", "Fecha", "Datum", etc.
- Amount columns: "Amount", "Debit", "Credit", "Betrag", etc.
- Description columns: "Description", "Memo", "Payee", "Narrative", etc.

Each detected mapping is validated against the actual data content (e.g., a column mapped as "date" is checked for date-parseable values).

### Sign Conventions

Different banks use different conventions for representing expenses and income:

| Convention | Meaning | Example banks |
|-----------|---------|---------------|
| `negative_is_expense` | Negative amounts are expenses (most common) | Chase, Wells Fargo |
| `negative_is_income` | Negative amounts are income (inverted) | Some credit cards |
| `split_debit_credit` | Separate debit and credit columns | Citi, some European banks |

Specify with `--sign` if auto-detection picks the wrong convention.

### Number Formats

| Format | Example | Regions |
|--------|---------|---------|
| `us` | `1,234.56` | US, UK, most English-speaking |
| `european` | `1.234,56` | Germany, France, most EU |
| `swiss_french` | `1'234.56` | Switzerland |
| `zero_decimal` | `123456` (amounts in cents) | Some payment processors |

### Import Options Reference

| Option | Short | Description |
|--------|-------|-------------|
| `--institution` | `-i` | Institution name (OFX) or format name |
| `--account-id` | `-a` | Account identifier (bypasses name matching) |
| `--account-name` | `-n` | Account name for single-account tabular files |
| `--format` | `-f` | Use a specific named format (bypass detection) |
| `--override` | | Field-to-column override, repeatable |
| `--sign` | | Sign convention override |
| `--date-format` | | Date format (strptime format string) |
| `--number-format` | | Number format: us, european, swiss_french, zero_decimal |
| `--sheet` | | Excel sheet name |
| `--delimiter` | | Explicit delimiter for text formats |
| `--encoding` | | File encoding (e.g., utf-8, latin-1) |
| `--skip-transform` | | Skip rebuilding core tables after import |
| `--no-save-format` | | Don't save detected format for future use |
| `--no-row-limit` | | Override row count limit |
| `--no-size-limit` | | Override file size limit |

## W-2 PDF Extraction

Extract wage and tax data from IRS Form W-2 PDFs.

```bash
moneybin import file ~/Downloads/2024_W2.pdf
```

**What gets extracted:**
- Employer information (name, EIN, address)
- Wages and compensation (Box 1)
- Federal and state tax withholding (Boxes 2, 17)
- Social Security and Medicare wages/taxes (Boxes 3-6)
- State-specific information
- Tax year

The extractor uses dual parsing strategies (structured field extraction and text pattern matching) for robust extraction across different PDF generators.

## Import Management

Track, inspect, and manage your imports.

```bash
# Show a summary of all imported data
moneybin import status

# List recent imports with batch details
moneybin import history

# Preview a file's structure without importing
moneybin import preview ~/Downloads/transactions.csv

# Revert an import (deletes all rows from that batch)
moneybin import revert <import-id>
```

Each import creates a batch record with:
- Unique import ID
- Source file path
- Row counts (accounts, transactions)
- Detection confidence score
- Format used (built-in, saved, or auto-detected)
- Timestamp

## Format System

Formats define how to read a specific institution's tabular exports. They specify column mapping, date format, sign convention, delimiter, and other parsing details.

```bash
# List all formats (built-in and user-saved)
moneybin import formats list

# Show details for a format
moneybin import formats show chase_credit

# Delete a user-saved format
moneybin import formats delete my_bank
```

When you import a file and auto-detection succeeds, the mapping is saved as a user format. Future imports from the same institution use the saved format directly, skipping detection.
