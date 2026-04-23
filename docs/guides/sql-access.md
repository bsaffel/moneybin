# Direct SQL Access

Your financial data lives in DuckDB — a fast, embedded analytical database. Query it with standard SQL from MoneyBin's built-in tools or any DuckDB-compatible client.

## Built-In Tools

```bash
# Interactive SQL shell
moneybin db shell

# One-off query
moneybin db query "SELECT category, SUM(amount) as total
  FROM core.fct_transactions
  WHERE date >= '2026-01-01'
  GROUP BY category
  ORDER BY total"

# Browser-based UI
moneybin db ui
```

The shell and UI connect to the encrypted database transparently — no manual key management needed.

## MCP SQL Access

The `run_read_query` MCP tool lets AI assistants execute arbitrary read-only SQL:

```sql
-- Only SELECT, WITH, DESCRIBE, SHOW, PRAGMA, and EXPLAIN are allowed
-- Write operations are rejected
SELECT * FROM core.fct_transactions WHERE amount < -500 ORDER BY date DESC LIMIT 10
```

## External Tools

Connect from any DuckDB-compatible tool (Python, R, DBeaver, etc.) using:

- **Database file:** `~/.moneybin/profiles/<name>/moneybin.duckdb`
- **Encryption key:** Get it with `moneybin db key`

```python
import duckdb

# You'll need the encryption key from `moneybin db key`
conn = duckdb.connect(
    "~/.moneybin/profiles/default/moneybin.duckdb",
    config={"encryption_key": "your-key-here"},
)
df = conn.execute("SELECT * FROM core.fct_transactions").fetchdf()
```

## Key Tables

### Core (analytical queries go here)

| Table | Description | Key columns |
|-------|-------------|-------------|
| `core.fct_transactions` | All transactions, all sources, deduplicated | `transaction_id`, `date`, `amount`, `description`, `category`, `subcategory`, `account_id`, `source_type` |
| `core.dim_accounts` | All accounts, all sources, deduplicated | `account_id`, `account_name`, `account_type`, `institution_name` |

### Raw (source data, untouched)

| Table | Description |
|-------|-------------|
| `raw.ofx_transactions` | OFX/QFX transaction data |
| `raw.ofx_accounts` | OFX account metadata |
| `raw.ofx_balances` | OFX balance snapshots |
| `raw.tabular_transactions` | CSV/TSV/Excel/Parquet/Feather transaction data |
| `raw.tabular_accounts` | Tabular account metadata |
| `raw.w2_forms` | W-2 tax form data |

### App (user-created state)

| Table | Description |
|-------|-------------|
| `app.categorization_rules` | Active auto-categorization rules |
| `app.merchant_mappings` | Merchant name normalization mappings |
| `app.categories` | Category taxonomy |
| `app.budgets` | Monthly budget targets |
| `app.import_log` | Import batch tracking |

## Example Queries

**Monthly spending by category:**
```sql
SELECT
    strftime(date, '%Y-%m') AS month,
    category,
    SUM(amount) AS total
FROM core.fct_transactions
WHERE amount < 0
GROUP BY month, category
ORDER BY month DESC, total
```

**Top merchants by spend:**
```sql
SELECT
    description,
    COUNT(*) AS txn_count,
    SUM(amount) AS total_spent
FROM core.fct_transactions
WHERE amount < 0
GROUP BY description
ORDER BY total_spent
LIMIT 20
```

**Income vs expenses by month:**
```sql
SELECT
    strftime(date, '%Y-%m') AS month,
    SUM(CASE WHEN amount > 0 THEN amount ELSE 0 END) AS income,
    SUM(CASE WHEN amount < 0 THEN amount ELSE 0 END) AS expenses,
    SUM(amount) AS net
FROM core.fct_transactions
GROUP BY month
ORDER BY month DESC
```

**Uncategorized transactions:**
```sql
SELECT date, amount, description, account_id
FROM core.fct_transactions
WHERE category IS NULL
ORDER BY date DESC
LIMIT 50
```

**Account summary:**
```sql
SELECT
    a.account_name,
    a.account_type,
    a.institution_name,
    COUNT(t.transaction_id) AS txn_count,
    MIN(t.date) AS first_txn,
    MAX(t.date) AS last_txn
FROM core.dim_accounts a
LEFT JOIN core.fct_transactions t ON a.account_id = t.account_id
GROUP BY a.account_name, a.account_type, a.institution_name
ORDER BY a.institution_name, a.account_name
```
