# MoneyBin dbt Data Models

## Architecture

MoneyBin uses a **medallion architecture** (Raw → Staging → Core) inspired by
the Inmon common-tables approach. The core layer serves as the single source
of truth for all downstream consumers.

```
┌──────────────────────────────────────────────────────────┐
│  Raw Layer (schema: raw)                                 │
│  Python loaders → DuckDB tables                          │
│  ofx_accounts, ofx_transactions, w2_forms, ...           │
└──────────────┬───────────────────────────────────────────┘
               │
               ▼
┌──────────────────────────────────────────────────────────┐
│  Staging Layer (schema: prep)                            │
│  dbt views — light cleaning, type casting                │
│  stg_ofx__accounts, stg_ofx__transactions, ...           │
└──────────────┬───────────────────────────────────────────┘
               │
               ▼
┌──────────────────────────────────────────────────────────┐
│  Core Layer (schema: core)                               │
│  dbt tables — canonical, deduplicated, multi-source      │
│  dim_accounts, fct_transactions                          │
│  ↑ MCP server, CLI, and reports read from here           │
└──────────────────────────────────────────────────────────┘
```

## Model Inventory

### Core (Gold) — `models/core/`

| Model              | Type      | Description                                      |
|--------------------|-----------|--------------------------------------------------|
| `dim_accounts`     | Dimension | Canonical accounts from all sources, deduplicated |
| `fct_transactions` | Fact      | Canonical transactions from all sources           |

### Staging (Silver) — `models/ofx/`

| Model                  | Source | Description                              |
|------------------------|--------|------------------------------------------|
| `stg_ofx__accounts`    | OFX    | Account details, standardized columns    |
| `stg_ofx__transactions`| OFX    | Transactions with DATE types, trimmed    |
| `stg_ofx__balances`    | OFX    | Balance snapshots with DATE types        |
| `stg_ofx__institutions`| OFX    | Institution names and IDs                |

## Key Design Decisions

1. **One canonical table per entity** — All consumers read from `dim_accounts`
   and `fct_transactions`, never from raw or staging tables directly.

2. **Source-agnostic consumers** — The MCP server references core tables via
   `TableRef` constants. Adding a new data source (e.g. Plaid) requires only
   dbt model changes — no MCP server code changes.

3. **Amount convention** — All amounts in `fct_transactions` use accounting
   sign convention: negative = expense, positive = income. Source-specific
   conventions are normalized in the core model.

4. **Deduplication** — Core models use `ROW_NUMBER()` windows partitioned by
   natural keys, keeping the most recently extracted record.

## Running

```bash
# Build all models
uv run dbt run

# Run tests
uv run dbt test

# Build a specific model
uv run dbt run --select dim_accounts
```

## Adding a New Data Source

1. Create staging models in `models/<source>/` (e.g. `models/plaid/`)
2. Add a CTE in the relevant core model and `UNION ALL` it
3. No changes needed to MCP server or other downstream consumers
