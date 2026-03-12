---
globs: ["**/*.sql", "sqlmesh/models/**", "src/moneybin/sql/**"]
---

# DuckDB & SQL Standards

## SQL Formatting

All SQL formatted with `sqlmesh format` (uses sqlglot, understands `MODEL()` blocks natively).

```bash
uv run sqlmesh format
```

## File Types

- **Raw schema** (`src/moneybin/sql/schema/*.sql`): Plain SQL DDL.
- **SQLMesh models** (`sqlmesh/models/**/*.sql`): Plain SQL with `MODEL()` block header.

## DuckDB Function Reference

### Date/Time
- Parse: `strptime(date_string, '%Y-%m-%d')`
- Format: `strftime(date_col, '%Y-%m')`
- Extract: `YEAR(date_col)`, `MONTH(date_col)`, `DAY(date_col)`
- Truncate: `date_trunc('month', date_col)`

### Aggregation & Windows
- Running total: `SUM(amount) OVER (ORDER BY date ROWS UNBOUNDED PRECEDING)`
- Previous period: `LAG(amount, 1) OVER (PARTITION BY account ORDER BY date)`
- Round currency: `ROUND(amount, 2)`

### File I/O
- Read: `read_csv('file.csv', auto_detect=true)`, `read_parquet('data/*.parquet')`
- Write: `COPY (SELECT ...) TO 'output.csv' (HEADER, DELIMITER ',')`

### String
- Pattern: `regexp_matches(description, 'PATTERN')`
- Match: `description LIKE '%MERCHANT%'`
- Case: `UPPER()`, `LOWER()`

## Anti-Patterns

- No MySQL/PostgreSQL-specific syntax (this is DuckDB).
- No `LIMIT` without `ORDER BY` (non-deterministic).
- No `FLOAT` for currency (use `DECIMAL(18,2)`).
- No string concatenation for queries (use parameterized).

## Financial Query Patterns

```sql
-- Annual spending by category
SELECT strftime('%Y', date) as year, category, SUM(ABS(amount)) as total
FROM fct_transactions
WHERE amount < 0
GROUP BY year, category ORDER BY year DESC, total DESC;

-- Monthly trends
SELECT strftime('%Y-%m', date) as month, SUM(ABS(amount)) as spending
FROM fct_transactions WHERE amount < 0
GROUP BY month ORDER BY month;
```

- Validate date ranges. Handle edge cases (refunds, transfers, corrections).
- Use indexes for common query patterns. Filter early (WHERE before JOINs).
