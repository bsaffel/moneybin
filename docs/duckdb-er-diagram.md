# DuckDB Data Model - Entity Relationship Diagram

This document contains the Entity Relationship (ER) diagram for the MoneyBin DuckDB data model. Source-specific raw tables are transformed through dbt staging views into canonical analytical tables in the core layer.

For full column definitions, see the DDL files in `src/moneybin/sql/schema/` and dbt models in `dbt/models/`. The diagram below shows primary keys, foreign keys, and key business columns only.

## Entity Relationship Diagram

```mermaid
erDiagram
    %% ================================================================
    %% RAW LAYER -- Source-specific tables, preserved as-is
    %% ================================================================

    raw_ofx_institutions {
        varchar organization PK
        varchar fid PK
    }

    raw_ofx_accounts {
        varchar account_id PK
        varchar account_type "CHECKING, SAVINGS, CREDITCARD, etc."
        varchar institution_org FK
        varchar institution_fid FK
        varchar source_file PK
        timestamp extracted_at PK
    }

    raw_ofx_transactions {
        varchar transaction_id PK "Unique FITID"
        varchar account_id PK_FK
        decimal amount "OFX sign convention"
        timestamp date_posted
        varchar payee
        varchar source_file PK
    }

    raw_ofx_balances {
        varchar account_id PK_FK
        timestamp statement_end_date PK
        decimal ledger_balance
        decimal available_balance
        varchar source_file PK
    }

    raw_w2_forms {
        integer tax_year PK
        varchar employee_ssn PK
        varchar employer_ein PK
        varchar employer_name
        decimal wages "Box 1"
        decimal federal_income_tax "Box 2"
        varchar source_file PK
    }

    %% ================================================================
    %% CORE LAYER -- Canonical tables built by dbt
    %% ================================================================

    core_dim_accounts {
        varchar account_id PK
        varchar account_type
        varchar institution_name
        varchar source_system "ofx, plaid, manual"
    }

    core_fct_transactions {
        varchar transaction_id
        varchar account_id FK
        date transaction_date
        decimal amount "Negative is expense"
        varchar transaction_direction "expense, income, zero"
        varchar description
        varchar source_system "ofx, plaid, manual"
        varchar transaction_year_month "YYYY-MM"
    }

    %% ================================================================
    %% RAW LAYER RELATIONSHIPS
    %% ================================================================

    raw_ofx_institutions ||--o{ raw_ofx_accounts : "has"
    raw_ofx_accounts ||--o{ raw_ofx_transactions : "has"
    raw_ofx_accounts ||--o{ raw_ofx_balances : "tracks"

    %% ================================================================
    %% CORE LAYER RELATIONSHIPS
    %% ================================================================

    core_dim_accounts ||--o{ core_fct_transactions : "has"

    %% ================================================================
    %% DATA LINEAGE (raw feeds core via dbt staging views)
    %% ================================================================

    raw_ofx_accounts }o--|| core_dim_accounts : "feeds_via_dbt"
    raw_ofx_transactions }o--|| core_fct_transactions : "feeds_via_dbt"
```

## Staging Layer (not shown in ERD)

Between the raw and core layers, dbt creates **staging views** in the `prep` schema that perform light transformations (renaming, type casting, trimming). These are ephemeral views, not persisted tables:

- `prep.stg_ofx__institutions` -- Renames `organization` to `institution_name`
- `prep.stg_ofx__accounts` -- Standardizes column names
- `prep.stg_ofx__transactions` -- Casts `date_posted` to DATE, trims strings
- `prep.stg_ofx__balances` -- Casts timestamps to DATE

## Data Flow

```text
OFX/QFX Files ──→ Extractors ──→ raw.ofx_* tables
PDF W-2 Forms ──→ Extractors ──→ raw.w2_forms

raw.ofx_* ──→ prep.stg_ofx__* (views) ──→ core.dim_accounts
                                        ──→ core.fct_transactions
```

## Planned Source Tables

The core layer is designed to accept data from multiple sources via UNION ALL in the dbt models. Planned raw tables for future data sources:

### Plaid API (Encrypted Sync tier)

- `raw.plaid_accounts` -- Account details from Plaid
- `raw.plaid_transactions` -- Transaction data from Plaid
- `raw.plaid_balances` -- Balance snapshots from Plaid
- `raw.plaid_securities` -- Investment security definitions
- `raw.plaid_holdings` -- Investment positions
- `raw.plaid_investment_transactions` -- Investment trades
- `raw.plaid_liabilities` -- Debt and loan details

### Manual CSV Import

- `raw.csv_transactions` -- User-provided CSV transaction data
- `raw.csv_accounts` -- User-provided CSV account data

All planned sources will flow through source-specific staging views into the same `core.dim_accounts` and `core.fct_transactions` tables.

## Key Design Features

- **Multi-source support**: The `source_system` column in core tables identifies the origin of each record (ofx, plaid, manual)
- **Idempotent loading**: Composite primary keys in raw tables prevent duplicate imports
- **Amount standardization**: `fct_transactions` normalizes amounts (negative = expense, positive = income) regardless of source convention
- **Time dimensions**: Derived year/month/quarter columns enable efficient time-based analysis
- **Deduplication**: `dim_accounts` keeps only the most recent record per account when multiple imports exist

## Entity Cardinalities

- One Institution can have many Accounts (1:N)
- One Account can have many Transactions (1:N)
- One Account can have many Balance snapshots (1:N)
- W-2 forms are independent (one per employee per employer per year)
