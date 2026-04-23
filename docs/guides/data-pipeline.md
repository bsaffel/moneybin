# Data Pipeline

MoneyBin uses a three-layer medallion architecture powered by [SQLMesh](https://sqlmesh.com) and [DuckDB](https://duckdb.org). Every transformation is a SQL model you can read, audit, and modify.

## Three-Layer Architecture

```
Raw (raw.*)          Staging (prep.*)         Core (core.*)
-----------          ---------------          -------------
Untouched data  -->  Light cleaning,     -->  Canonical, deduplicated,
from extractors      type casting (views)     multi-source (tables)
```

### Raw Layer (`raw.*`)

Exact data from file extractors and loaders. Never modified after import.

| Table | Source |
|-------|--------|
| `raw.ofx_transactions` | OFX/QFX bank statements |
| `raw.ofx_accounts` | OFX account metadata |
| `raw.ofx_balances` | OFX balance snapshots |
| `raw.tabular_transactions` | CSV, TSV, Excel, Parquet, Feather imports |
| `raw.tabular_accounts` | Tabular account metadata |
| `raw.w2_forms` | W-2 PDF tax data |

### Staging Layer (`prep.*`)

Light cleaning and type casting. One view per raw source. These are DuckDB views, not materialized tables — they compute on the fly from raw data.

| View | Purpose |
|------|---------|
| `prep.stg_ofx__transactions` | Clean OFX transactions |
| `prep.stg_ofx__accounts` | Clean OFX accounts |
| `prep.stg_ofx__balances` | Clean OFX balances |
| `prep.stg_ofx__institutions` | OFX institution metadata |
| `prep.stg_tabular__transactions` | Clean tabular transactions |
| `prep.stg_tabular__accounts` | Clean tabular accounts |

### Core Layer (`core.*`)

Canonical, deduplicated, multi-source tables. All consumers (MCP server, CLI, direct SQL) read from core.

| Table | Purpose |
|-------|---------|
| `core.dim_accounts` | All accounts from all sources, deduplicated |
| `core.fct_transactions` | All transactions from all sources, deduplicated, with categorization |

## Key Design Decisions

- **One canonical table per entity.** Consumers never query raw or staging directly.
- **Multi-source union.** Core models `UNION ALL` from every staging source with a `source_type` column tracking origin (`ofx`, `csv`, `tsv`, `excel`, `parquet`, etc.).
- **Dedup in core.** `ROW_NUMBER()` windows handle within-source duplicates.
- **Adding a data source** means writing staging views and adding a CTE to the relevant core model. No consumer changes needed.
- **Accounting sign convention.** Negative = expense, positive = income. `DECIMAL(18,2)` for amounts, `DATE` for dates.

## SQLMesh Transforms

SQLMesh manages the transformation pipeline. The pipeline runs automatically after each `moneybin import file` command unless `--skip-transform` is specified.

```bash
# Apply all pending changes (rebuild core tables from raw data)
moneybin transform apply

# Preview what will change before applying
moneybin transform plan

# Check current model state
moneybin transform status

# Validate model SQL without running
moneybin transform validate

# Run data quality audits
moneybin transform audit

# Force recompute a model for a date range
moneybin transform restate
```

### Model Files

SQLMesh models live under `sqlmesh/models/`:

```
sqlmesh/models/
├── prep/           # Staging views (1:1 with raw sources)
│   ├── stg_ofx__transactions.sql
│   ├── stg_ofx__accounts.sql
│   ├── stg_ofx__balances.sql
│   ├── stg_ofx__institutions.sql
│   ├── stg_tabular__transactions.sql
│   └── stg_tabular__accounts.sql
└── core/           # Canonical tables (multi-source, deduplicated)
    ├── dim_accounts.sql
    └── fct_transactions.sql
```

Each model is a plain SQL file with a `MODEL()` header that declares dependencies, materialization strategy, and scheduling.
