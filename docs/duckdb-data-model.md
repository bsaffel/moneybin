# DuckDB Data Model

## Overview

MoneyBin stores all financial data in a local [DuckDB](https://duckdb.org/) database organized into three layers:

- **Raw**: Source data preserved exactly as extracted, one set of tables per source type
- **Staging**: Light transformations via dbt views (renaming, type casting, trimming)
- **Core**: Canonical fact and dimension tables that unify all sources

For a visual representation, see the [Entity Relationship Diagram](duckdb-er-diagram.md).

## Schema Source of Truth

Column-level definitions live in code, not in documentation:

- **Raw table DDL**: `src/moneybin/sql/schema/` -- one file per table
- **Staging views**: `dbt/models/ofx/` -- dbt SQL models
- **Core tables**: `dbt/models/core/` -- dbt SQL models with contract tests
- **Schema tests**: `dbt/models/` YAML files -- constraints, relationships, accepted values

To inspect the live schema, use `moneybin db shell` or the MCP `schema.describe` tool.

## Design Principles

1. **Local-first**: All data stored in a user-controlled DuckDB file
2. **Multi-source**: Raw tables are source-specific; core tables unify them
3. **Idempotent**: Composite primary keys prevent duplicate imports
4. **Analytical**: Optimized for SQL queries, aggregations, and time-series analysis
5. **Auditable**: Every record tracks its source file and extraction timestamp

## Schemas

| Schema | Purpose | Materialization |
|--------|---------|-----------------|
| `raw` | Source-specific tables, preserved as-is | Tables (created by loaders) |
| `prep` | Staging transformations | Views (created by dbt) |
| `core` | Canonical analytical tables | Tables (created by dbt) |

---

## Raw Layer

Raw tables preserve source data exactly as extracted. Each source type gets its own set of tables. All raw tables include `source_file`, `extracted_at`, and `loaded_at` metadata columns for auditability.

### `raw.ofx_institutions`

Financial institution information extracted from OFX/QFX files. One row per institution, keyed by `(organization, fid)`.

### `raw.ofx_accounts`

Account details extracted from OFX/QFX files. Stores the account identifier, routing number, account type (checking, savings, credit card, etc.), and the associated institution. Keyed by `(account_id, source_file, extracted_at)` to preserve history across imports.

### `raw.ofx_transactions`

Transaction records from OFX/QFX files. Each transaction has a unique FITID from the bank, along with the posted date, amount (using OFX sign convention), payee, memo, and optional check number. Keyed by `(transaction_id, account_id, source_file)` to prevent duplicate imports.

### `raw.ofx_balances`

Account balance snapshots from OFX/QFX statement files. Captures both ledger and available balances along with the statement period. Keyed by `(account_id, statement_end_date, source_file)`.

### `raw.w2_forms`

IRS Form W-2 Wage and Tax Statement data extracted from PDFs. Stores all standard W-2 boxes: employee/employer identification, core wages and tax amounts (Boxes 1-6), additional compensation (Boxes 7-8), benefits (Boxes 10-11), and checkbox flags (Box 13). State/local tax info (Boxes 15-20) and variable boxes (12, 14) are stored as JSON for flexibility. Keyed by `(tax_year, employee_ssn, employer_ein, source_file)`.

---

## Staging Layer

Staging models are dbt views in the `prep` schema that perform light transformations. They are not persisted tables.

| View | Source | Key Transformations |
|------|--------|---------------------|
| `prep.stg_ofx__institutions` | `raw.ofx_institutions` | Renames `organization` to `institution_name` |
| `prep.stg_ofx__accounts` | `raw.ofx_accounts` | Standardizes column names |
| `prep.stg_ofx__transactions` | `raw.ofx_transactions` | Casts `date_posted` to DATE, trims strings |
| `prep.stg_ofx__balances` | `raw.ofx_balances` | Casts timestamps to DATE |

---

## Core Layer

### `core.dim_accounts`

Canonical deduplicated account dimension from all data sources. One row per unique account, with the most recent extraction winning when duplicates exist. Includes the account type, institution name, and a `source_system` column identifying where the record originated (ofx, plaid, manual).

**Current sources**: OFX (`stg_ofx__accounts`)

**Planned sources**: Plaid (`stg_plaid__accounts`), manual CSV (`stg_manual__accounts`)

### `core.fct_transactions`

Canonical fact table for all transactions across all sources. One row per transaction per source system. Standardizes amounts, adds derived time dimensions (year, month, quarter), and includes a `transaction_direction` label (expense, income, zero).

Key behaviors:

- **Amount standardization**: Negative = expense, positive = income, regardless of source convention. OFX amounts are used as-is; Plaid amounts are sign-flipped.
- **Derived columns**: `amount_absolute`, `transaction_year_month`, `transaction_year_quarter`, and day-of-week are computed for efficient analytical queries.
- **Multi-source readiness**: Plaid-specific columns (merchant name, category, location, pending status) are included and NULL for OFX-sourced records.

**Current sources**: OFX (`stg_ofx__transactions`)

**Planned sources**: Plaid (`stg_plaid__transactions`), manual CSV (`stg_manual__transactions`)

---

## Data Flow

```text
Source Files          Extractors         Raw Tables           dbt Staging         Core Tables
─────────────        ──────────         ──────────           ───────────         ───────────
OFX/QFX files  ───→  ofx_extractor ───→ raw.ofx_*     ───→  prep.stg_ofx__* ─┐
                                                                               ├─→ core.dim_accounts
PDF W-2 forms  ───→  w2_extractor  ───→ raw.w2_forms        (no staging)      │   core.fct_transactions
                                                                               │
CSV files      ───→  (planned)     ───→ raw.csv_*     ───→  prep.stg_csv__* ──┘
Plaid API      ───→  (planned)     ───→ raw.plaid_*   ───→  prep.stg_plaid__*─┘
```

---

## Planned Source Tables

### Plaid API (Encrypted Sync tier)

These tables will be created when Plaid integration is implemented:

- `raw.plaid_accounts` -- Account details
- `raw.plaid_transactions` -- Transaction data
- `raw.plaid_balances` -- Balance snapshots
- `raw.plaid_securities` -- Investment security definitions
- `raw.plaid_holdings` -- Investment positions
- `raw.plaid_investment_transactions` -- Investment trades
- `raw.plaid_liabilities` -- Debt and loan details

### Manual CSV Import

- `raw.csv_transactions` -- User-provided transaction data
- `raw.csv_accounts` -- User-provided account data

All planned sources will flow through source-specific staging views into the same `core.dim_accounts` and `core.fct_transactions` tables via UNION ALL in the dbt models.

---

## Key Design Decisions

### Composite Primary Keys

Raw tables use composite primary keys to prevent duplicate imports. Keys typically include the natural identifier plus `source_file` (and sometimes `extracted_at`) so that re-importing the same file is idempotent while preserving data from multiple import sessions.

### Amount Convention

`core.fct_transactions` standardizes amounts across all sources:

- **Negative** = money leaving your account (expenses, payments, transfers out)
- **Positive** = money entering your account (income, deposits, refunds)
- `amount_absolute` provides the unsigned value for aggregations
- `transaction_direction` provides a human-readable label

### Source System Tracking

The `source_system` column in core tables identifies where each record originated, enabling source-specific filtering and debugging.

### JSON for Variable Data

W-2 forms use JSON columns for data that varies in structure:

- `state_local_info`: Supports 0-2 state entries per W-2
- `optional_boxes`: Sparse data from boxes 12 and 14

This balances queryability (typed columns for core fields) with flexibility (JSON for variable fields).

---

## Example Queries

### Monthly Spending Summary

```sql
SELECT
    transaction_year_month,
    SUM(CASE WHEN transaction_direction = 'expense' THEN amount_absolute ELSE 0 END) AS total_expenses,
    SUM(CASE WHEN transaction_direction = 'income' THEN amount ELSE 0 END) AS total_income,
    SUM(amount) AS net_cashflow
FROM core.fct_transactions
GROUP BY transaction_year_month
ORDER BY transaction_year_month DESC;
```

### Account Balances with Institution Info

```sql
SELECT
    a.account_id,
    a.institution_name,
    a.account_type,
    b.ledger_balance,
    b.available_balance,
    b.ledger_balance_date
FROM core.dim_accounts a
LEFT JOIN (
    SELECT account_id, ledger_balance, available_balance, ledger_balance_date,
           ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY ledger_balance_date DESC) AS rn
    FROM raw.ofx_balances
) b ON a.account_id = b.account_id AND b.rn = 1
ORDER BY a.institution_name, a.account_type;
```

### W-2 Tax Summary

```sql
SELECT
    tax_year,
    employer_name,
    wages,
    federal_income_tax,
    social_security_tax,
    medicare_tax,
    federal_income_tax + COALESCE(social_security_tax, 0) + COALESCE(medicare_tax, 0) AS total_federal_tax
FROM raw.w2_forms
ORDER BY tax_year DESC, employer_name;
```
