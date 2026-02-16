# MoneyBin Implementation Summary

## Current Status

MoneyBin is an open-source, local-first personal financial analysis platform with an MCP server as the primary interface and a full data toolkit (DuckDB, dbt, Jupyter, Streamlit) for hands-on analysis.

### What's Built

#### MCP Server

The MCP server is the primary interface, giving AI assistants secure, read-only access to local financial data. See [`mcp-server-design.md`](mcp-server-design.md) for the complete specification.

- **8 live tools**: `schema.list_tables`, `schema.describe`, `accounts.list`, `accounts.balances`, `transactions.search`, `tax.w2_summary`, `institutions.list`, `sql.query`
- **10 stub tools**: Return helpful not-implemented messages with instructions to enable
- **5 live resources**: Schema info, account summaries, recent transactions, W-2 data
- **5 live prompts**: Spending analysis, anomaly detection, tax preparation, account overview, transaction search
- **Privacy controls**: Read-only mode, result size limits, table allowlist, query validation
- **Transports**: stdio (default), SSE, streamable-http

#### Data Pipeline

| Component | Status | Details |
|-----------|--------|---------|
| OFX/QFX import | Implemented | Extract from bank files via `moneybin extract ofx` |
| W-2 PDF extraction | Implemented | Dual extraction (text + OCR) via `moneybin extract w2` |
| Parquet loading | Implemented | Load extracted data into DuckDB |
| dbt staging models | Implemented | 4 OFX staging views in `prep` schema |
| dbt core models | Implemented | `dim_accounts` and `fct_transactions` tables |
| dbt tests | Implemented | Schema tests, relationship tests, contract tests |

#### CLI

Full Typer-based CLI with commands for extraction, loading, transformation, database exploration, MCP server, credentials, and configuration management.

#### Profile System

Complete profile-based data isolation:

- Separate DuckDB database per profile
- Isolated raw/processed data directories
- Profile-specific logging
- First-run interactive setup
- Resolution priority: CLI flag > env var > saved default > interactive prompt

#### Testing

- MCP server tests (tools, resources, prompts, privacy, server lifecycle)
- Schema contract tests (dbt YAML vs DDL sync)
- Extractor tests (OFX, W-2)
- Loader tests
- CLI command tests
- Profile system tests
- Logging configuration tests

---

### MCP Tool Implementation Status

#### Live (8 tools)

| Tool | Description |
|------|-------------|
| `schema.list_tables` | List all tables and views in the database |
| `schema.describe` | Show columns, types, and row count for a table |
| `accounts.list` | List all financial accounts with type and institution |
| `accounts.balances` | Get the most recent balance for each account |
| `transactions.search` | Search transactions with date/amount/payee/account filters |
| `tax.w2_summary` | W-2 tax form data by year |
| `institutions.list` | List all connected financial institutions |
| `sql.query` | Execute arbitrary read-only SQL queries |

#### Implement Now -- Tier 1 (11 tools)

Can be built with existing data model (`fct_transactions`, `dim_accounts`, `ofx_balances`):

| Tool | Description | Implementation Notes |
|------|-------------|---------------------|
| `overview.data_status` | What data is loaded, date ranges, freshness | Query `information_schema` + source tables |
| `spending.monthly_summary` | Income vs expenses by month | GROUP BY `transaction_year_month` |
| `spending.by_category` | Spending by category/type | GROUP BY `transaction_type` |
| `spending.compare_periods` | Compare two time periods | Two aggregation queries + deltas |
| `spending.top_merchants` | Top N payees by spend | GROUP BY `description` ORDER BY SUM |
| `cashflow.summary` | Net inflows vs outflows | Income/expense aggregation by period |
| `cashflow.income_sources` | Identify income streams | Filter income, group by description |
| `accounts.activity` | Per-account transaction summary | Aggregate from `fct_transactions` |
| `accounts.balance_history` | Balance trends over time | Time series from `ofx_balances` |
| `transactions.find_recurring` | Identify subscriptions | Group by description+amount, count |
| `transactions.find_large` | Find outlier transactions | Filter by threshold or percentile |

#### Implement Partial -- Tier 2 (2 tools)

Can be partially implemented; full version needs additional data sources:

| Tool | Description | Limitation |
|------|-------------|-----------|
| `overview.net_worth` | Assets minus liabilities | Full version needs investments + liability data |
| `tax.summary` | Comprehensive tax summary | Full version needs 1099 forms |

#### Keep as Stub -- Tier 3 (4 tools)

Require new data sources (Plaid integration or budget definitions):

| Tool | Description | Blocker |
|------|-------------|---------|
| `investments.holdings` | Investment positions | Needs Plaid securities data |
| `investments.performance` | Portfolio returns | Needs time-series investment data |
| `liabilities.summary` | Outstanding debts | Needs Plaid liability data |
| `budget.status` | Budget vs actual | Needs budget definition mechanism |

---

### Data Model

| Layer | Tables | Status |
|-------|--------|--------|
| `raw.ofx_institutions` | Institution info from OFX files | Implemented |
| `raw.ofx_accounts` | Account details from OFX files | Implemented |
| `raw.ofx_transactions` | Transactions from OFX files | Implemented |
| `raw.ofx_balances` | Balance snapshots from OFX files | Implemented |
| `raw.w2_forms` | W-2 tax form data from PDFs | Implemented |
| `prep.stg_ofx__*` | 4 staging views | Implemented |
| `core.dim_accounts` | Canonical account dimension | Implemented |
| `core.fct_transactions` | Canonical transaction fact table | Implemented |
| `raw.csv_*` | CSV import tables | Planned |
| `raw.plaid_*` | Plaid API tables | Planned (Encrypted Sync) |

See [`duckdb-er-diagram.md`](duckdb-er-diagram.md) for the ER diagram and [`duckdb-data-model.md`](duckdb-data-model.md) for full schema definitions.

---

## Roadmap

### Near-Term

1. **Implement Tier 1 MCP tools** (11 tools) -- all implementable with existing data
2. **Migrate to namespaced tool naming** per MCP SEP-986
3. **Add 3 new prompts** (financial health check, subscription audit, year in review)
4. **Add `moneybin://status` resource** for data freshness
5. **CSV import** -- manual CSV transaction import for broader bank coverage

### Medium-Term

1. **Implement Tier 2 MCP tools** (net worth, tax summary)
2. **Transaction categorization** -- automated category assignment
3. **Streamlit dashboards** -- interactive financial dashboards
4. **Write tool architecture** -- opt-in write tools behind config flag

### Long-Term

1. **Encrypted Sync tier** -- Plaid integration with E2E encryption
2. **Investment tracking** -- securities, holdings, performance
3. **Liability tracking** -- debts, loans, mortgages
4. **Budget management** -- budget definitions and tracking
5. **MCP elicitation** -- interactive data collection mid-workflow

---

## Profile System

### Architecture

**Core files**: `src/moneybin/config.py`, `src/moneybin/utils/user_config.py`

Each profile maintains completely isolated:

- **Database**: `data/{profile}/moneybin.duckdb`
- **Data**: `data/{profile}/raw/`, `data/{profile}/processed/`
- **Logs**: `logs/{profile}/moneybin.log`

### Profile Resolution Priority

1. CLI flag: `--profile=alice` (highest priority)
2. Environment variable: `MONEYBIN_PROFILE=alice`
3. Saved default: `~/.moneybin/config.yaml`
4. Interactive prompt (first run only)

### Usage

```bash
# Different users
moneybin --profile=alice extract ofx bank-files/*.qfx
moneybin --profile=bob extract ofx other-files/*.qfx

# Set a default
moneybin config set-default-profile alice

# Environment variable
export MONEYBIN_PROFILE=alice
moneybin transform run
```

---

## Documentation Index

| Document | Description |
|----------|-------------|
| [`mcp-server-design.md`](mcp-server-design.md) | Complete MCP server specification (tools, resources, prompts) |
| [`privacy-tiers-architecture.md`](privacy-tiers-architecture.md) | Three-tier data custody model |
| [`duckdb-er-diagram.md`](duckdb-er-diagram.md) | Entity relationship diagram |
| [`duckdb-data-model.md`](duckdb-data-model.md) | Complete schema definitions |
| [`application-architecture.md`](application-architecture.md) | System architecture |
| [`ofx-import-guide.md`](ofx-import-guide.md) | OFX/QFX import guide |
| [`w2-extraction-architecture.md`](w2-extraction-architecture.md) | W-2 extraction technical design |
| [`w2-extraction-feature.md`](w2-extraction-feature.md) | W-2 extraction feature guide |
| [`data-sources-strategy.md`](data-sources-strategy.md) | Data source priorities and roadmap |
| [`architecture/e2e-encryption.md`](architecture/e2e-encryption.md) | E2E encryption design |
| [`architecture/security-tradeoffs.md`](architecture/security-tradeoffs.md) | Security tradeoff analysis |
