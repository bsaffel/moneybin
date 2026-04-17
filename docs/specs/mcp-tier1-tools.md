# Feature: MCP Tier 1 Analytical Tools

## Status
ready

## Goal
Implement 11 analytical MCP tools that can be built now with the existing data model (`core.fct_transactions`, `core.dim_accounts`, `raw.ofx_balances`).

## Background
- [MCP Read Tools](implemented/mcp-read-tools.md) -- Existing tool patterns
- [MCP Write Tools](implemented/mcp-write-tools.md) -- Some overlap (monthly_summary, recurring already implemented here)
- [ADR-003: MCP Primary Interface](../decisions/003-mcp-primary-interface.md)
- Source: `src/moneybin/mcp/tools.py` (read tools), `src/moneybin/mcp/write_tools.py` (write tools)

## Requirements

1. Each tool returns JSON-formatted results.
2. All tools use parameterized queries (no string interpolation).
3. All tools check `table_exists()` before querying and return helpful messages.
4. All tools respect `MAX_ROWS` and `MAX_CHARS` limits.
5. Tools use dot-separated namespace convention per MCP SEP-986.
6. Tools read from core/raw schemas via `TableRef` constants.

## Tools to Implement

### 1. `overview.data_status`
What data is loaded, date ranges, freshness, row counts per source.

**Query approach**: Query `information_schema` for table row counts, query source tables for `MIN/MAX` dates and `COUNT(*)`.

```sql
SELECT 'ofx_transactions' AS source,
    COUNT(*) AS row_count,
    MIN(transaction_date) AS earliest,
    MAX(transaction_date) AS latest
FROM core.fct_transactions
WHERE source_system = 'ofx'
UNION ALL ...
```

### 2. `spending.monthly_summary`
Income vs expenses summary by month.

**Note**: Already partially implemented as `get_monthly_summary` in `write_tools.py`. Migrate to read tools with namespace naming.

```sql
SELECT transaction_year_month,
    SUM(CASE WHEN amount > 0 THEN amount ELSE 0 END) AS income,
    SUM(CASE WHEN amount < 0 THEN ABS(amount) ELSE 0 END) AS expenses,
    SUM(amount) AS net,
    COUNT(*) AS transaction_count
FROM core.fct_transactions
GROUP BY transaction_year_month
ORDER BY transaction_year_month DESC
LIMIT ?
```

### 3. `spending.by_category`
Spending breakdown by category or transaction type for a period.

**Note**: Already partially implemented as `get_spending_by_category` in `write_tools.py`. This version should work without categories (fall back to transaction type).

**Parameters**: `month` (YYYY-MM, optional), `start_date`, `end_date`

### 4. `spending.compare_periods`
Compare spending between two time periods with deltas.

**Parameters**: `period1_start`, `period1_end`, `period2_start`, `period2_end`

**Query approach**: Two aggregation CTEs, then compute deltas (absolute and percentage).

### 5. `spending.top_merchants`
Top N payees/merchants by total spend in a period.

**Parameters**: `months` (default 3), `limit` (default 20)

```sql
SELECT description, COUNT(*) AS txn_count,
    SUM(ABS(amount)) AS total_spent, ROUND(AVG(amount), 2) AS avg_amount
FROM core.fct_transactions
WHERE amount < 0
    AND transaction_date >= ?
GROUP BY description
ORDER BY total_spent DESC
LIMIT ?
```

### 6. `cashflow.summary`
Net inflows vs outflows by period (week/month/quarter).

**Parameters**: `period` (week/month/quarter, default month), `months` (default 12)

### 7. `cashflow.income_sources`
Identify and summarize income streams by source.

**Parameters**: `months` (default 12)

```sql
SELECT description, COUNT(*) AS occurrences,
    SUM(amount) AS total, ROUND(AVG(amount), 2) AS avg_amount,
    MIN(transaction_date) AS first_seen, MAX(transaction_date) AS last_seen
FROM core.fct_transactions
WHERE amount > 0 AND transaction_date >= ?
GROUP BY description
ORDER BY total DESC
```

### 8. `accounts.activity`
Per-account summary: transaction count, total in/out, average, date range.

```sql
SELECT account_id, COUNT(*) AS txn_count,
    SUM(CASE WHEN amount > 0 THEN amount ELSE 0 END) AS total_in,
    SUM(CASE WHEN amount < 0 THEN ABS(amount) ELSE 0 END) AS total_out,
    ROUND(AVG(amount), 2) AS avg_txn,
    MIN(transaction_date) AS first_txn, MAX(transaction_date) AS last_txn
FROM core.fct_transactions
GROUP BY account_id
```

### 9. `accounts.balance_history`
Balance trends over time from OFX balance snapshots.

**Parameters**: `account_id` (optional), `months` (default 12)

```sql
SELECT account_id, ledger_balance_date, ledger_balance, available_balance
FROM raw.ofx_balances
WHERE ledger_balance_date >= ?
ORDER BY account_id, ledger_balance_date
```

### 10. `transactions.find_recurring`
Identify subscriptions and recurring charges by pattern.

**Note**: Already implemented as `find_recurring_transactions` in `write_tools.py`. Migrate to read tools with namespace naming.

### 11. `transactions.find_large`
Find outlier transactions above a threshold or statistical percentile.

**Parameters**: `threshold` (dollar amount, optional), `percentile` (0-100, default 95), `months` (default 6)

```sql
WITH stats AS (
    SELECT PERCENTILE_CONT(?) WITHIN GROUP (ORDER BY ABS(amount)) AS threshold
    FROM core.fct_transactions
    WHERE transaction_date >= ?
)
SELECT t.transaction_id, t.transaction_date, t.amount, t.description, t.account_id
FROM core.fct_transactions t, stats
WHERE ABS(t.amount) >= stats.threshold AND t.transaction_date >= ?
ORDER BY ABS(t.amount) DESC
```

## Implementation Plan

### Files to create
- None -- tools added to existing files

### Files to modify
- `src/moneybin/mcp/tools.py` -- Add new read tools (data_status, balance_history, find_large, compare_periods, top_merchants, cashflow, income_sources, activity)
- `src/moneybin/mcp/write_tools.py` -- Remove tools migrated to read tools (or keep as aliases)
- `tests/moneybin/test_mcp/test_tools.py` -- Add tests for each new tool

### Key decisions

- **Migration path**: `monthly_summary`, `spending_by_category`, `find_recurring` exist in `write_tools.py`. Migrate to `tools.py` with proper namespacing. Keep backward compatibility temporarily if needed.
- **Namespace convention**: Use `spending.monthly_summary` not `get_monthly_summary`. Registration with FastMCP may need name parameter.
- **Period parameters**: Standardize on `months` (integer, lookback from today) as primary period parameter, with optional `start_date`/`end_date` override.

## Testing Strategy

- One test per tool with sample data in in-memory DuckDB
- Test with empty tables (graceful "no data" messages)
- Test parameter validation (invalid dates, negative months)
- Test result truncation at MAX_ROWS

## Dependencies

- No new packages required
- Existing data model sufficient

## Out of Scope

- Tier 2 tools (net_worth, tax.summary) -- require additional data sources
- Tier 3 tools (investments, liabilities) -- require Plaid data
- Write tools (categorization, budgets) -- already implemented
