# SQL Schema Files

This directory contains SQL schema definitions for MoneyBin's DuckDB database.

## Structure

```text
sql/
├── schema/                   # Table schema definitions
│   ├── raw_schema.sql        # Raw schema creation
│   ├── raw_ofx_*.sql         # OFX raw tables
│   └── raw_plaid_*.sql       # Plaid raw tables (future)
└── README.md
```

## Benefits of Separate SQL Files

1. **Full SQL syntax highlighting** in IDEs
2. **SQL linting with SQLFluff** (configured in `pyproject.toml`)
3. **Version control friendly** - clear diffs for schema changes
4. **Reusable** - Can execute directly with DuckDB CLI
5. **Aligns with dbt** - Consistent with transformation SQL files
6. **Editor support** - Works with SQL formatter extensions

## Schema Naming Convention

### Raw Tables

- **Prefix**: `raw_<source>_<entity>.sql`
- **Schema**: `raw.<source>_<entity>`
- **Examples**:
  - `raw_ofx_transactions.sql` → `raw.ofx_transactions`
  - `raw_plaid_transactions.sql` → `raw.plaid_transactions`

### Core Tables (Future)

- **Prefix**: `core_<entity>.sql`
- **Schema**: `core.<entity>`
- **Examples**: `core_transactions.sql` → `core.transactions`

## Usage

SQL files are automatically loaded by loader classes:

```python
from moneybin.loaders.ofx_loader import OfxRawLoader

loader = OFXLoader("data/duckdb/moneybin.duckdb")
loader.create_raw_tables()  # Executes all OFX schema files
```

## Manual Execution

You can also run SQL files directly:

```bash
# Execute single file
duckdb moneybin.duckdb < src/moneybin/sql/schema/raw_ofx_transactions.sql

# Execute all schema files
cat src/moneybin/sql/schema/*.sql | duckdb moneybin.duckdb
```

## Linting

All SQL files are linted with SQLFluff:

```bash
# Check all SQL files
uv run sqlfluff lint src/moneybin/sql/

# Fix auto-fixable issues
uv run sqlfluff fix src/moneybin/sql/

# Check specific file
uv run sqlfluff lint src/moneybin/sql/schema/raw_ofx_transactions.sql
```

SQLFluff configuration is in `pyproject.toml`:

- Dialect: DuckDB
- Templater: Jinja (for dbt compatibility)
- Max line length: 88 characters

## Adding New Tables

1. Create SQL file in `schema/` directory
2. Follow naming convention: `raw_<source>_<entity>.sql`
3. Add comments describing the table purpose
4. Run SQLFluff to check formatting
5. Update loader class to execute the new schema file

Example template:

```sql
-- Raw <source> <entity> table
-- Purpose: Brief description of what this table stores
CREATE TABLE IF NOT EXISTS raw.<source>_<entity> (
    id VARCHAR PRIMARY KEY,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- Add your columns here
);
```

## Table Design Guidelines

1. **Use `CREATE TABLE IF NOT EXISTS`** - Makes loading idempotent
2. **Add comments** - Describe table purpose and column meanings
3. **Include PRIMARY KEY** - Ensures uniqueness and improves query performance
4. **Use `loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP`** - Track when data was loaded
5. **Use `VARCHAR` over `TEXT`** - DuckDB convention for strings
6. **Use `DECIMAL(18, 2)` for money** - Exact precision for financial data

## Related Documentation

- [DuckDB Data Types](https://duckdb.org/docs/sql/data_types/overview)
- [DuckDB CREATE TABLE](https://duckdb.org/docs/sql/statements/create_table)
- [SQLFluff Documentation](https://docs.sqlfluff.com/)
- [MoneyBin dbt Models](../../dbt/models/)
