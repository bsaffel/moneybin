---
globs: ["**/*.sql", "sqlmesh/models/**", "src/moneybin/sql/**", "src/moneybin/**/*.py"]
---

# Database Standards

## Connection Management

**Never call `duckdb.connect()` directly.** Use the `Database` class (`src/moneybin/database.py`) via `get_database()` for all database access. The `Database` class handles encryption key retrieval, encrypted file attachment, extension loading, schema initialization, and migrations. One long-lived read-write connection per process.

```python
from moneybin.database import get_database

db = get_database()
result = db.execute("SELECT * FROM core.fct_transactions WHERE account_id = ?", [acct_id])
```

See [`data-protection.md`](../../docs/specs/data-protection.md) for the full design.

## Column Name Consistency Across Layers

A column name — especially identifiers — must contain the same logical values in every table and view where it appears. Any column named `X` should be joinable to any other column named `X` across raw, prep, core, and app schemas. When a new layer introduces a new concept (e.g., a synthetic key), give it a new name. Never reuse an existing column name with different semantics.

## Model Naming Conventions

| Prefix | Schema | Purpose |
|---|---|---|
| `stg_` | `prep` | 1:1 with a source table; light cleaning, type casting, within-source dedup |
| `int_` | `prep` | Intermediate transformations; not for direct consumption |
| `dim_` | `core` | Dimension: descriptive entity |
| `fct_` | `core` | Fact: event/transaction with measures |
| `bridge_` | `core` | Many-to-many link between facts or dimensions |
| `agg_` | `core` | Pre-aggregated summary |
| `seed_` | `prep` | Static reference data loaded from files |

`stg_` models use double-underscore to separate source system from entity: `stg_ofx__transactions`. `int_` models use it to separate domain from transformation: `int_transactions__merged`.

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

### Authoritative References

- SQLMesh model configuration: https://sqlmesh.readthedocs.io/en/latest/reference/model_configuration/#general-model-properties
- DuckDB `COMMENT ON` syntax: https://duckdb.org/docs/stable/sql/statements/comment_on
