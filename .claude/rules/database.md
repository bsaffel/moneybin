---
description: "Database standards: DuckDB patterns, SQL formatting, schema conventions, model naming, column comments"
globs: ["**/*.sql", "sqlmesh/**", "src/moneybin/sql/**", "src/moneybin/database.py", "src/moneybin/schema.py", "src/moneybin/loaders/**"]
---

# Database Standards

## Connection Management

**Never call `duckdb.connect()` directly.** Use the `Database` class (`src/moneybin/database.py`) via `get_database()` for all database access. The `Database` class handles encryption key retrieval, encrypted file attachment, extension loading, schema initialization, and migrations. One long-lived read-write connection per process.

```python
from moneybin.database import get_database

db = get_database()
result = db.execute(
    "SELECT * FROM core.fct_transactions WHERE account_id = ?", [acct_id]
)
```

See [`privacy-data-protection.md`](../../docs/specs/privacy-data-protection.md) for the full design.

## Bulk Data Loading

Use `Database.ingest_dataframe()` for loading Polars DataFrames into DuckDB. This method converts the DataFrame to Arrow (`df.to_arrow()` — zero-copy) and writes via the encrypted connection. All loaders should use this method rather than constructing INSERT statements manually.

```python
db = get_database()
db.ingest_dataframe("raw.tabular_transactions", df, on_conflict="replace")
```

See [`smart-import-tabular.md`](../../docs/specs/smart-import-tabular.md) for the full design.

## Column Name Consistency Across Layers

A column name — especially identifiers — must contain the same logical values in every table and view where it appears. Any column named `X` should be joinable to any other column named `X` across raw, prep, core, and app schemas. When a new layer introduces a new concept (e.g., a synthetic key), give it a new name. Never reuse an existing column name with different semantics.

**One concept, one column name.** If two columns across layers carry the same semantic (same values, same meaning), they must share the same name. Don't introduce a layer-specific alias (e.g., `source_format` in raw vs `source_type` in core) — use one name throughout. The canonical provenance column is `source_type` (values: `csv`, `tsv`, `excel`, `parquet`, `ofx`, `plaid`, etc.) — neutral enough for both file formats and API/sync sources. If a trivial mapping is needed (e.g., `xlsx` → `excel`), resolve it at write time so downstream layers never see the raw variant — if both values coexist, JOIN predicates between layers silently produce empty results.

## Model Naming Conventions

`stg_`, `int_`, `dim_`, `fct_`, `bridge_`, `agg_`, `seed_` — see CLAUDE.md "Architecture: Data Layers" for the full prefix/schema/purpose table.

`stg_` models use double-underscore to separate source system from entity: `stg_ofx__transactions`. `int_` models use it to separate domain from transformation: `int_transactions__merged`.

## SQLMesh Invocation

**Always use the Python API** (`sqlmesh.Context`), never subprocess. Subprocess calls break profile isolation — child processes read `~/.moneybin/config.yaml` and connect to the wrong database. Since SQLMesh's DuckDB config doesn't support `ENCRYPTION_KEY`, inject a pre-connected adapter into `BaseDuckDBConnectionConfig._data_file_to_adapter` keyed by db path — SQLMesh will reuse it instead of opening its own connection. See `run_transforms()` in `import_service.py` and `_run_sqlmesh_migrate()` in `database.py`.

## SQL Formatting

```bash
uv run sqlmesh -p sqlmesh format
```

## File Types

- **Raw schema** (`src/moneybin/sql/schema/*.sql`): Plain SQL DDL.
- **SQLMesh models** (`sqlmesh/models/**/*.sql`): Plain SQL with `MODEL()` block header.

## Table and Column Comments

Every column should have a comment. Use existing schema files as examples for style and content.

### Comment Placement

Both SQLMesh models and schema DDL use the same pattern: `/* description */` block comment on the line immediately before `MODEL()` or `CREATE TABLE` for table comments, and inline `-- comment` on columns. `prep.*` staging views get no comments (internal layer).

### How Comments Reach DuckDB's Catalog

**SQLMesh models:** `register_comments` (enabled by default) auto-detects the `/* */` block before `MODEL()` as the table description and inline comments on outermost SELECT columns as column descriptions. Both are applied as `COMMENT ON TABLE`/`COLUMN` on every `sqlmesh run`. Important: if a `column_descriptions` block is present in MODEL(), auto-detection of inline comments is disabled — use one or the other, not both.

**Schema DDL files:** `schema.py:_apply_comments()` uses sqlglot to parse each file — sqlglot attaches `/* */` block comments to adjacent `Create` expressions and trailing `--` comments to `ColumnDef` expressions. Applied on every app startup via `init_schemas`.

### Gotchas

- Column comments go on the **final SELECT only** in SQLMesh models, not CTEs.
- `sqlmesh format` converts `--` to `/* */` — both styles work.
- **Do not use** the `columns` block with `COMMENT` keyword — SQLMesh silently swallows it without writing to DuckDB's catalog. Use inline comments instead.

## DuckDB vs. PostgreSQL

Claude defaults to PostgreSQL syntax. Use DuckDB equivalents:

| Task | DuckDB (correct) | PostgreSQL (wrong) |
|------|-------------------|--------------------|
| Parse date string | `strptime(s, '%Y-%m-%d')` | `TO_DATE(s, 'YYYY-MM-DD')` |
| Format date | `strftime(d, '%Y-%m')` | `TO_CHAR(d, 'YYYY-MM')` |
| Extract year | `YEAR(d)`, `MONTH(d)`, `DAY(d)` | `EXTRACT(YEAR FROM d)` |
| Date difference (days) | `ABS(a.date - b.date)` or `DATEDIFF('day', a.date, b.date)` | `a.date - b.date` (same, but see note) |
| Regex match | `regexp_matches(s, 'PAT')` | `s ~ 'PAT'` |
| Read file | `read_csv('f.csv')`, `read_parquet('*.parquet')` | N/A |
| Write file | `COPY (...) TO 'f.csv' (HEADER, DELIMITER ',')` | `\copy` or `COPY` with different options |

**Date arithmetic in SQLMesh models:** In raw SQL or Python-executed queries, both `DATE - DATE` and `DATEDIFF('day', ...)` work. However, **in SQLMesh model files**, use `DATE - DATE` subtraction — sqlglot's transpiler misparses `DATEDIFF('day', a, b)` as `DATE_DIFF(a, b, CAST('day' AS DATE))`, producing a runtime `BinderError`. Direct date subtraction survives transpilation correctly.

## Anti-Patterns

- No MySQL/PostgreSQL-specific syntax (this is DuckDB).
- No `LIMIT` without `ORDER BY` (non-deterministic).
- No `FLOAT` for financial quantities. Floating-point arithmetic produces rounding errors that compound across records. Use `decimal.Decimal` in Python, `pl.Decimal` in Polars, and `DECIMAL` in SQL. Use `Decimal` string literals everywhere including tests: `Decimal("52.30")`, never `Decimal(52.30)` (which captures the float imprecision). Non-financial floats (confidence scores, weights, percentages, returns) are fine as `float`.

  | Data type | SQL | Polars | Examples |
  |---|---|---|---|
  | Money amounts | `DECIMAL(18,2)` | `pl.Decimal(18, 2)` | Balances, transaction amounts, wages, taxes, budget targets, gains/losses, filter thresholds on money |
  | Quantities | `DECIMAL(18,8)` | `pl.Decimal(18, 8)` | Share counts (fractional shares), units held |
  | Unit prices | `DECIMAL(18,8)` | `pl.Decimal(18, 8)` | Stock/crypto prices, NAV, cost basis per share |
  | Exchange rates | `DECIMAL(18,8)` | `pl.Decimal(18, 8)` | Currency conversion rates |
- No string concatenation for queries (use parameterized).

### Authoritative References

- SQLMesh model configuration: https://sqlmesh.readthedocs.io/en/latest/reference/model_configuration/#general-model-properties
- DuckDB `COMMENT ON` syntax: https://duckdb.org/docs/stable/sql/statements/comment_on
