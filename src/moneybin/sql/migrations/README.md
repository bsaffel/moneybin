# Database Migrations

Migration files that apply schema changes to MoneyBin's DuckDB database.

## Naming Convention

```
V<NNN>__<snake_case_description>.<sql|py>
```

- **V prefix** + **3+ digit version** (monotonic integer, zero-padded)
- **Double underscore** separator (`__`)
- **Snake case** description
- **Extension**: `.sql` (default) or `.py` (escape hatch)

Examples:
- `V001__create_tax_lots.sql`
- `V002__backfill_account_types.py`
- `V010__add_currency_column.sql`

## SQL Migrations (default, ~80% of cases)

Plain SQL DDL/DML. Executed as a single transaction.

```sql
ALTER TABLE app.categories ADD COLUMN color VARCHAR;
```

## Python Migrations (escape hatch)

Export a single `migrate(conn)` function. The runner calls it within a transaction.

```python
def migrate(conn):
    """Backfill account types from institution metadata."""
    rows = conn.execute("SELECT ...").fetchall()
    for row in rows:
        conn.execute("UPDATE ... WHERE id = ?", [row[0]])
```

## Scope

Migrations may alter:
- `raw.*` — loader-owned tables
- `app.*` — application state tables

Migrations must NOT alter (owned by SQLMesh):
- `core.*`, `prep.*`, `analytics.*`

## When to Use SQL vs Python

| SQL | Python |
|-----|--------|
| Adding/altering/dropping tables or columns | Backfilling data based on runtime state |
| Creating/dropping indexes | Importing seed data from external files |
| Simple INSERT/UPDATE with literal values | Conditional logic (if column exists, skip) |
| Any DDL that is purely structural | Reading Python package versions or metadata |
