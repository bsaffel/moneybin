# Feature: OFX/QFX Import

## Status
implemented

## Goal
Import financial data from OFX/QFX bank files into MoneyBin with an idempotent, archive-friendly workflow.

## Background
- [ADR-001: Medallion Data Layers](../../decisions/001-medallion-data-layers.md)
- [Data Model](../../reference/data-model.md) -- `raw.ofx_*` tables

## Requirements

1. Parse both SGML and XML OFX formats automatically.
2. Extract four entity types: institutions, accounts, transactions, balances.
3. Copy source file to `data/raw/ofx/` with hash-based duplicate detection.
4. Extract to Parquet files in `data/raw/ofx/extracted/<filename>/`.
5. Load to DuckDB raw tables with `INSERT OR REPLACE` for idempotency.
6. Re-importing the same file must be safe (no duplicates, no data loss).
7. Support `--no-load` (extract only) and `--no-copy` (skip source copy) flags.

## Data Model

### Raw tables created

- `raw.ofx_institutions` -- PK: `(organization, fid)`
- `raw.ofx_accounts` -- PK: `(account_id, source_file, extracted_at)`
- `raw.ofx_transactions` -- PK: `(transaction_id, account_id, source_file)`
- `raw.ofx_balances` -- PK: `(account_id, statement_end_date, source_file)`

All tables include `source_file`, `extracted_at`, and `loaded_at` metadata columns.

### Staging views (SQLMesh)

- `prep.stg_ofx__institutions` -- Renames `organization` to `institution_name`
- `prep.stg_ofx__accounts` -- Standardizes column names
- `prep.stg_ofx__transactions` -- Casts `date_posted` to DATE, trims strings
- `prep.stg_ofx__balances` -- Casts timestamps to DATE

## Implementation Plan

### Files created
- `src/moneybin/extractors/ofx_extractor.py` -- OFX/QFX parsing with ofxparse
- `src/moneybin/loaders/ofx_loader.py` -- DuckDB loading with INSERT OR REPLACE
- `src/moneybin/sql/schema/raw_ofx_*.sql` -- DDL for raw tables
- `sqlmesh/models/ofx/stg_ofx__*.sql` -- Staging views
- `sqlmesh/models/core/dim_accounts.sql` -- Account dimension (UNION ALL from staging)
- `sqlmesh/models/core/fct_transactions.sql` -- Transaction fact table

### Key decisions
- **Parquet first**: Save to Parquet before DuckDB. Parquet files are the permanent archive; DuckDB is the working database. Enables disaster recovery and format portability.
- **Hash-based copy**: `moneybin.utils.file.copy_to_raw()` checks file hash to avoid redundant copies.
- **Composite PKs**: Include `source_file` in primary keys so re-importing is idempotent while preserving multi-import history.

## CLI Interface

```bash
# Basic import
moneybin data extract ofx ~/Downloads/WellsFargo_2025.qfx

# With custom institution name
moneybin data extract ofx file.qfx --institution "Wells Fargo"

# Extract only (skip database load)
moneybin data extract ofx file.qfx --no-load

# Skip source file copy
moneybin data extract ofx file.qfx --no-copy
```

## MCP Interface

Read tools query OFX data through core tables:
- `accounts.list` -- Lists accounts from `core.dim_accounts`
- `accounts.balances` -- Latest balances from `raw.ofx_balances`
- `transactions.search` -- Queries `core.fct_transactions`
- `institutions.list` -- Lists institutions from `raw.ofx_institutions`

Write tool for import:
- `import_file` -- Triggers OFX import from within AI conversation

## Testing Strategy

- Unit tests for OFX parsing (SGML and XML formats)
- Loader tests with in-memory DuckDB
- CLI tests verifying argument parsing and exit codes
- Integration tests for end-to-end import flow
- Idempotency tests (import same file twice)

## Dependencies

- `ofxparse` -- OFX/QFX file parsing (well-maintained, 14+ years)
- `polars` -- DataFrame operations for Parquet output
- `pydantic` -- Schema validation

## Out of Scope

- CSV import (separate spec)
- Plaid import (separate spec)
- Transaction categorization (separate spec)
