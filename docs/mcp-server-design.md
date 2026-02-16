# MCP Server Design

MoneyBin's primary interface is a [Model Context Protocol (MCP)](https://modelcontextprotocol.io/) server that gives AI assistants secure, read-only access to your local financial data. All data stays on your machine -- nothing is sent to any external service.

This document defines the complete MCP tool surface, naming conventions, resources, prompts, privacy controls, and the roadmap for write-tool support.

---

## Naming Convention

All tools use **dot-separated namespaces** per [MCP SEP-986](https://modelcontextprotocol.io/community/seps/986-specify-format-for-tool-names):

```
namespace.action
```

Namespaces group tools by financial domain. This makes tool discovery intuitive for both AI assistants and client UIs (Cursor, Claude Desktop, etc.).

---

## Tools (25 total)

### `schema.*` -- Database Discovery

| Tool | Description | Status |
|------|-------------|--------|
| `schema.list_tables` | List all tables and views with schema, type, and column count | LIVE |
| `schema.describe` | Show columns, data types, and row count for a specific table | LIVE |

### `accounts.*` -- Account Management

| Tool | Description | Status |
|------|-------------|--------|
| `accounts.list` | List all financial accounts with type and institution | LIVE |
| `accounts.balances` | Get the most recent balance for each account | LIVE |
| `accounts.activity` | Per-account summary: transaction count, total in/out, average, date range | NEW |
| `accounts.balance_history` | Balance trends over time from OFX balance snapshots | IMPLEMENT |

### `transactions.*` -- Transaction Queries

| Tool | Description | Status |
|------|-------------|--------|
| `transactions.search` | Search transactions with date, amount, payee, and account filters | LIVE |
| `transactions.find_recurring` | Identify subscriptions and recurring charges by pattern | IMPLEMENT |
| `transactions.find_large` | Find outlier transactions above a threshold or statistical percentile | NEW |

### `spending.*` -- Spending Analysis

| Tool | Description | Status |
|------|-------------|--------|
| `spending.by_category` | Spending breakdown by category or transaction type for a period | IMPLEMENT |
| `spending.monthly_summary` | Income vs expenses summary by month | IMPLEMENT |
| `spending.compare_periods` | Compare spending between two time periods with deltas | NEW |
| `spending.top_merchants` | Top N payees/merchants by total spend in a period | NEW |

### `cashflow.*` -- Cash Flow

| Tool | Description | Status |
|------|-------------|--------|
| `cashflow.summary` | Net inflows vs outflows by period (week/month/quarter) | NEW |
| `cashflow.income_sources` | Identify and summarize income streams by source | NEW |

### `investments.*` -- Investment Data

| Tool | Description | Status |
|------|-------------|--------|
| `investments.holdings` | Current investment positions and values | STUB |
| `investments.performance` | Portfolio returns over a time period | STUB |

> Requires Plaid investment data via Encrypted Sync tier.

### `liabilities.*` -- Debt Data

| Tool | Description | Status |
|------|-------------|--------|
| `liabilities.summary` | Outstanding debts, interest rates, and payment info | STUB |

> Requires Plaid liability data via Encrypted Sync tier.

### `tax.*` -- Tax Information

| Tool | Description | Status |
|------|-------------|--------|
| `tax.w2_summary` | W-2 tax form data including wages, taxes, and employer info | LIVE |
| `tax.summary` | Comprehensive tax summary across all data sources for a year | PARTIAL |

### `budget.*` -- Budget Tracking

| Tool | Description | Status |
|------|-------------|--------|
| `budget.status` | Budget vs actual spending comparison | STUB |

> Requires a budget definition mechanism (not yet designed).

### `overview.*` -- High-Level Summaries

| Tool | Description | Status |
|------|-------------|--------|
| `overview.net_worth` | Calculate total net worth (assets minus liabilities) | PARTIAL |
| `overview.data_status` | What data is loaded, date ranges, freshness, row counts per source | NEW |

### `institutions.*` -- Institution Info

| Tool | Description | Status |
|------|-------------|--------|
| `institutions.list` | List all connected financial institutions | LIVE |

### `sql.*` -- Power User

| Tool | Description | Status |
|------|-------------|--------|
| `sql.query` | Execute an arbitrary read-only SQL query against DuckDB | LIVE |

### Status Legend

- **LIVE**: Fully implemented, backed by real DuckDB queries
- **IMPLEMENT**: Stub exists, can be implemented now with existing data model
- **NEW**: New tool, needs to be created and implemented
- **PARTIAL**: Can be partially implemented; full version needs additional data sources
- **STUB**: Returns a not-implemented message; requires new data sources or features

### Status Summary

- **LIVE (8)**: `schema.list_tables`, `schema.describe`, `accounts.list`, `accounts.balances`, `transactions.search`, `tax.w2_summary`, `institutions.list`, `sql.query`
- **IMPLEMENT NOW (4)**: `accounts.balance_history`, `transactions.find_recurring`, `spending.by_category`, `spending.monthly_summary`
- **NEW (7)**: `overview.data_status`, `accounts.activity`, `transactions.find_large`, `spending.compare_periods`, `spending.top_merchants`, `cashflow.summary`, `cashflow.income_sources`
- **PARTIAL (2)**: `overview.net_worth`, `tax.summary`
- **STUB (4)**: `investments.holdings`, `investments.performance`, `liabilities.summary`, `budget.status`

---

## Resources (8 total)

Resources provide read-only data endpoints that AI assistants can access directly for context. They represent relatively static data that clients may cache.

| URI | Description | Status |
|-----|-------------|--------|
| `moneybin://schema/tables` | All tables in the database with schema and type | LIVE |
| `moneybin://schema/{table_name}` | Column definitions for a specific table | LIVE |
| `moneybin://accounts/summary` | Account listing with latest balances | LIVE |
| `moneybin://transactions/recent` | Last 30 days of transactions | LIVE |
| `moneybin://w2/{tax_year}` | W-2 data for a specific tax year | LIVE |
| `moneybin://status` | Data freshness, loaded sources, date ranges, profile info | NEW |
| `moneybin://investments/holdings` | Current investment positions | STUB |
| `moneybin://spending/categories` | Spending breakdown by category | IMPLEMENT |

---

## Prompts (8 total)

Prompts are pre-built workflow templates that guide AI assistants through common financial analysis tasks.

| Prompt | Description | Status |
|--------|-------------|--------|
| `analyze_spending` | Analyze spending patterns and identify top categories for a period | LIVE |
| `find_anomalies` | Look for unusual or suspicious transactions | LIVE |
| `tax_preparation` | Gather tax-related information for a specific year | LIVE |
| `account_overview` | Comprehensive overview of all accounts and balances | LIVE |
| `transaction_search` | Help find specific transactions matching a description | LIVE |
| `financial_health_check` | Comprehensive review: balances, cash flow, recurring charges, trends | NEW |
| `subscription_audit` | Find all recurring charges, calculate annual cost, identify savings | NEW |
| `year_in_review` | Annual summary: total income, spending by category, net savings, tax data | NEW |

---

## Privacy & Security Controls

All privacy controls are implemented in [`src/moneybin/mcp/privacy.py`](../src/moneybin/mcp/privacy.py).

### Read-Only by Default

- DuckDB is opened in **read-only mode** (`duckdb.connect(path, read_only=True)`)
- SQL queries are validated to reject write operations (INSERT, UPDATE, DELETE, DROP, CREATE, ALTER, etc.)
- Only SELECT, WITH, DESCRIBE, SHOW, PRAGMA, and EXPLAIN statements are allowed

### Result Size Limits

- **MAX_ROWS**: 1000 (configurable via `MONEYBIN_MCP_MAX_ROWS`)
- **MAX_CHARS**: 50,000 (configurable via `MONEYBIN_MCP_MAX_CHARS`)
- Results exceeding limits are truncated with a notice

### Table Allowlist (Optional)

- Set `MONEYBIN_MCP_ALLOWED_TABLES` to a comma-separated list of schema-qualified table names
- When set, only those tables can be queried via `schema.describe` and `sql.query`
- When unset (default), all tables are accessible

### Transport

- **stdio** (default): Local, per-user integration with strong process isolation
- **sse**: Server-Sent Events for network-accessible deployments
- **streamable-http**: Full HTTP transport for remote access

---

## Write Tools Architecture (Planned)

Future write tools will enable AI assistants to help with data management, not just querying. They will be **opt-in** and gated behind a configuration flag.

### Activation

```bash
# Environment variable
MONEYBIN_MCP_ALLOW_WRITES=true

# Or via CLI flag
moneybin mcp serve --allow-writes
```

### Planned Write Tools

| Tool | Description |
|------|-------------|
| `data.import_ofx` | Trigger OFX/QFX file import from within the AI conversation |
| `data.import_csv` | Trigger CSV file import |
| `data.run_transforms` | Trigger dbt run to rebuild core tables |
| `transactions.categorize` | Tag a transaction with a user-defined category |
| `transactions.set_merchant` | Map a raw payee name to a clean merchant name |

### Implementation Notes

- `server.py` will accept a `read_write: bool` parameter in `init_db()`
- Privacy module will validate write-tool access against the config flag
- Write tools will be registered in a separate `write_tools.py` module, only imported when enabled
- All write operations will be logged for auditability

---

## Advanced MCP Features (Future)

### Elicitation

The MCP spec (Nov 2025) supports [elicitation](https://modelcontextprotocol.io/specification/2025-11-25/client/elicitation) -- the server can ask the user structured questions mid-workflow. Use cases:

- "I found 15 uncategorized transactions. Would you like me to group them by merchant?"
- "Which time period should I compare against?"

### Sampling

[Sampling](https://modelcontextprotocol.io/specification/2025-11-25/server/sampling) allows the server to request LLM completions. Use cases:

- Auto-categorization of transactions based on payee names
- Natural language to SQL translation for complex queries
- Anomaly explanation

### Tool Icons

The spec supports [icons](https://modelcontextprotocol.io/specification/2025-11-25/changelog) on tools, resources, and prompts for better UI in client applications.

---

## Implementation Tiers

### Tier 1 -- Implement Now (existing data model)

These tools can be fully implemented using `core.fct_transactions`, `core.dim_accounts`, and `raw.ofx_balances`:

- `overview.data_status` -- Query `information_schema` + source tables for counts and date ranges
- `spending.monthly_summary` -- GROUP BY `transaction_year_month` on `fct_transactions`
- `spending.by_category` -- GROUP BY `transaction_type` (and `category` when available)
- `spending.compare_periods` -- Two aggregation queries with delta calculation
- `spending.top_merchants` -- GROUP BY `description` ORDER BY SUM(amount)
- `cashflow.summary` -- Income vs expense aggregation by period
- `cashflow.income_sources` -- Filter `transaction_direction = 'income'`, group by description
- `accounts.activity` -- Per-account aggregation from `fct_transactions`
- `accounts.balance_history` -- Time series from `raw.ofx_balances`
- `transactions.find_recurring` -- Group by description + amount, filter by occurrence count
- `transactions.find_large` -- Filter by absolute amount threshold or statistical percentile

### Tier 2 -- Partial Implementation

- `overview.net_worth` -- Sum of latest balances; full version needs investment + liability data
- `tax.summary` -- Combine W2 data + tax-flagged transactions; full version needs 1099 forms

### Tier 3 -- Requires New Data Sources

- `investments.holdings` -- Needs Plaid securities/holdings data
- `investments.performance` -- Needs time-series investment data
- `liabilities.summary` -- Needs Plaid liability data
- `budget.status` -- Needs a budget definition mechanism

### Tier 4 -- Write Tools (documented, not implemented)

- `data.import_ofx`, `data.import_csv`, `data.run_transforms`
- `transactions.categorize`, `transactions.set_merchant`
