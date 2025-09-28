# SQL Formatting Guide for MoneyBin

This guide outlines best practices for formatting SQL code within DuckDB statements in the MoneyBin project.

## Tools and Configuration

### Manual Formatting (Recommended)

Due to compatibility issues with automated SQL formatters and Python f-strings, we use manual formatting following consistent standards.

### Optional: sqlparse for Basic Formatting

For basic SQL formatting assistance, we include `sqlparse`:

```bash
# Install sqlparse (already included in dev dependencies)
uv add --dev sqlparse

# Format SQL string programmatically
echo "select * from table" | uv run python -c "import sqlparse; import sys; print(sqlparse.format(sys.stdin.read(), keyword_case='upper', identifier_case='lower', reindent=True))"
```

## Formatting Standards

### 1. **Keyword Capitalization**

- SQL keywords: `UPPERCASE`
- Functions: `lowercase`
- Identifiers (tables, columns): `snake_case`

### 2. **Indentation and Spacing**

- Use 4 spaces for indentation
- Align clauses vertically
- Put each major clause on its own line

### 3. **Comma Placement**

- Trailing commas (at end of line)
- Space after commas

### 4. **Line Breaks**

- Break long statements into multiple lines
- Each SELECT column on its own line for complex queries
- Subqueries indented properly

## Examples

### ✅ Good Formatting

```python
# Simple query
result = conn.sql(f"""
    SELECT COUNT(*)
    FROM information_schema.tables
    WHERE table_name = '{table_name}'
""").fetchone()

# Complex query with subquery
conn.sql(f"""
    INSERT INTO {table_name}
    SELECT *
    FROM read_parquet({file_pattern})
    WHERE transaction_id NOT IN (
        SELECT DISTINCT transaction_id
        FROM {table_name}
    )
""")

# Multi-column SELECT
conn.sql(f"""
    SELECT
        account_id,
        account_name,
        account_type,
        balance_current,
        balance_available
    FROM {table_name}
    WHERE account_type IN ('depository', 'credit')
        AND balance_current > 0
    ORDER BY balance_current DESC
""")
```

### ❌ Poor Formatting

```python
# Avoid: All on one line
conn.sql(f"SELECT * FROM {table_name} WHERE account_id NOT IN (SELECT DISTINCT account_id FROM {table_name})")

# Avoid: Inconsistent indentation
conn.sql(f"""
SELECT *
  FROM read_parquet({file_pattern})
WHERE transaction_id NOT IN (
SELECT DISTINCT transaction_id
    FROM {table_name}
)
""")

# Avoid: Leading commas
conn.sql(f"""
    SELECT account_id
         , account_name
         , balance_current
    FROM {table_name}
""")
```

## Python f-string Best Practices

### 1. **Triple Quotes for Multiline**

Always use triple quotes for multiline SQL:

```python
# Good
conn.sql(f"""
    SELECT *
    FROM {table_name}
    WHERE created_date >= '{start_date}'
""")

# Avoid single quotes for multiline
conn.sql(f'SELECT * FROM {table_name} WHERE created_date >= \'{start_date}\'')
```

### 2. **Variable Validation**

Always validate variables before using in SQL:

```python
# Validate table name is safe SQL identifier
if not table_name.isidentifier():
    raise ValueError(f"Invalid table name: {table_name}")

conn.sql(f"SELECT COUNT(*) FROM {table_name}")  # noqa: S608
```

### 3. **Security Comments**

Add security comments for dynamic SQL:

```python
conn.sql(f"""
    SELECT COUNT(*)
    FROM {table_name}
""")  # noqa: S608  # table_name validated as safe identifier
```

## Integration with Development Workflow

### 1. **Cursor/VS Code Integration**

Install SQL extensions for better syntax highlighting:

- "SQL Formatter" extension
- "DuckDB SQL" extension (if available)

Configure Cursor settings for SQL:

```json
{
  "sql.format.keywordCase": "upper",
  "sql.format.identifierCase": "lower",
  "sql.format.functionCase": "lower"
}
```

### 2. **Code Review Standards**

- Review SQL formatting during code reviews
- Ensure consistency with the patterns shown in this guide
- Focus on readability and maintainability

### 3. **Optional Automated Formatting**

For individual SQL strings, you can use sqlparse:

```python
import sqlparse

sql = "select count(*) from table where id > 100"
formatted = sqlparse.format(
    sql,
    keyword_case='upper',
    identifier_case='lower',
    reindent=True
)
print(formatted)
# Output:
# SELECT count(*)
# FROM table
# WHERE id > 100
```

## DuckDB-Specific Considerations

### 1. **Function Names**

Use DuckDB-specific function names correctly:

```sql
-- Good: DuckDB functions
SELECT
    strftime(transaction_date, '%Y-%m') as month,
    count(*) as transaction_count
FROM transactions
GROUP BY strftime(transaction_date, '%Y-%m')

-- Avoid: Generic SQL that might not work in DuckDB
SELECT
    DATE_FORMAT(transaction_date, '%Y-%m') as month,
    count(*) as transaction_count
FROM transactions
GROUP BY DATE_FORMAT(transaction_date, '%Y-%m')
```

### 2. **Parquet Functions**

Format DuckDB's Parquet functions clearly:

```sql
-- Good: Clear Parquet reading
CREATE TABLE raw_transactions AS
SELECT *
FROM read_parquet('data/raw/plaid/transactions_*.parquet')

-- Good: With options
CREATE TABLE raw_transactions AS
SELECT *
FROM read_parquet(
    'data/raw/plaid/transactions_*.parquet',
    union_by_name = true,
    filename = true
)
```

## Common Patterns in MoneyBin

### 1. **Incremental Loading Pattern**

```python
def _load_table_incrementally(self, conn, table_name: str, file_pattern: str) -> int:
    """Load data incrementally into DuckDB table."""
    # Check if table exists
    result = conn.sql(f"""
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = '{table_name}'
    """).fetchone()
    table_exists = (result[0] if result else 0) > 0

    if table_exists:
        # Incremental insert
        conn.sql(f"""
            INSERT INTO {table_name}
            SELECT *
            FROM read_parquet({file_pattern})
            WHERE id NOT IN (
                SELECT DISTINCT id
                FROM {table_name}
            )
        """)
    else:
        # Create new table
        conn.sql(f"""
            CREATE TABLE {table_name} AS
            SELECT *
            FROM read_parquet({file_pattern})
        """)
```

### 2. **Status Query Pattern**

```python
def get_table_status(self, conn, table_name: str) -> dict:
    """Get table status information."""
    return conn.sql(f"""
        SELECT
            COUNT(*) as row_count,
            MIN(created_date) as earliest_record,
            MAX(created_date) as latest_record
        FROM {table_name}
    """).fetchone()
```

This formatting approach ensures consistency, readability, and maintainability across the MoneyBin codebase while leveraging modern SQL formatting tools.
