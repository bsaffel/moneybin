# Feature: CSV Import

## Status
ready

## Goal
Import bank transaction data from CSV files into MoneyBin, supporting common bank formats with a fallback generic parser for unknown formats.

## Background
- [ADR-001: Medallion Data Layers](../architecture/001-medallion-data-layers.md)
- [OFX Import](implemented/ofx-import.md) -- Pattern to follow
- [Data Sources](../reference/data-sources.md) -- Priority 3 source

## Requirements

1. Parse CSV files from common banks (Chase, Wells Fargo, Capital One, Fidelity).
2. Provide a generic parser with column mapping for unknown bank formats.
3. Support `--bank` flag for explicit bank selection; auto-detect when possible.
4. Follow the OFX import pattern: copy source, extract to Parquet, load to DuckDB.
5. Idempotent loading via composite primary keys.
6. Generate synthetic transaction IDs (hash of date + amount + description + account) when source has no unique ID.
7. Normalize amounts to the MoneyBin convention (negative = expense, positive = income).
8. Handle bank-specific quirks (sign conventions, date formats, header variations).

## Data Model

### Raw tables

```sql
CREATE TABLE IF NOT EXISTS raw.csv_accounts (
    account_id VARCHAR NOT NULL,
    account_type VARCHAR,
    institution_name VARCHAR NOT NULL,
    source_file VARCHAR NOT NULL,
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (account_id, source_file)
);

CREATE TABLE IF NOT EXISTS raw.csv_transactions (
    transaction_id VARCHAR NOT NULL,
    account_id VARCHAR NOT NULL,
    transaction_date DATE NOT NULL,
    amount DECIMAL(18, 2) NOT NULL,
    description VARCHAR,
    category VARCHAR,
    memo VARCHAR,
    check_number VARCHAR,
    source_file VARCHAR NOT NULL,
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (transaction_id, account_id, source_file)
);
```

### Staging views (SQLMesh)

- `prep.stg_csv__accounts` -- Standardize column names to match OFX staging
- `prep.stg_csv__transactions` -- Normalize amounts, standardize dates

### Core integration

Add CTE + `UNION ALL` in `dim_accounts.sql` and `fct_transactions.sql` with `source_system = 'csv'`.

## Implementation Plan

### Files to create
- `src/moneybin/extractors/csv_extractor.py` -- CSV parsing with bank-specific profiles
- `src/moneybin/loaders/csv_loader.py` -- DuckDB loading
- `src/moneybin/sql/schema/raw_csv_transactions.sql` -- DDL
- `src/moneybin/sql/schema/raw_csv_accounts.sql` -- DDL
- `sqlmesh/models/csv/stg_csv__accounts.sql` -- Staging view
- `sqlmesh/models/csv/stg_csv__transactions.sql` -- Staging view
- `sqlmesh/models/csv/schema.yml` -- SQLMesh audits
- `tests/moneybin/test_extractors/test_csv_extractor.py`
- `tests/moneybin/test_loaders/test_csv_loader.py`

### Files to modify
- `src/moneybin/cli/commands/extract.py` -- Add `extract csv` command
- `sqlmesh/models/core/dim_accounts.sql` -- Add CSV CTE + UNION ALL
- `sqlmesh/models/core/fct_transactions.sql` -- Add CSV CTE + UNION ALL

### Key decisions

- **Bank profiles**: Each bank gets a profile defining column mapping, date format, amount sign convention, and header detection patterns. Profiles are data (dict/dataclass), not separate parser classes.
- **Synthetic transaction IDs**: Hash of `(date, amount, description, source_file)` since most bank CSVs lack unique IDs. Include `source_file` to avoid cross-file collisions.
- **Account ID from CLI**: CSVs don't contain account IDs. User provides via `--account-id` flag, or system generates from bank + filename.
- **Auto-detect**: Check first few lines against known bank header patterns. Fall back to generic parser.

## CLI Interface

```bash
# Import with explicit bank
moneybin data extract csv transactions.csv --bank=chase --account-id=chase-checking

# Auto-detect bank format
moneybin data extract csv transactions.csv --account-id=wf-savings

# Generic import with column mapping
moneybin data extract csv data.csv --date-col=Date --amount-col=Amount --desc-col=Description

# Skip database load
moneybin data extract csv data.csv --bank=chase --no-load
```

## MCP Interface

No new tools needed. CSV data flows through existing core tables and is accessible via all existing MCP read tools.

Write tool `import_file` should be extended to detect `.csv` extension and route to CSV extractor.

## Testing Strategy

- Unit tests for each bank profile (Chase, Wells Fargo, Capital One, Fidelity)
- Test generic parser with column mapping
- Test auto-detection of bank format
- Test synthetic transaction ID generation (deterministic, collision-resistant)
- Test amount normalization (different sign conventions)
- Test idempotent loading (import same file twice)
- Test integration with SQLMesh staging and core models

## Dependencies

- `polars` -- CSV reading and DataFrame operations
- No new packages required

## Out of Scope

- PDF bank statement processing
- Investment account CSV (Fidelity positions/trades) -- separate spec
- Automatic column mapping via LLM
