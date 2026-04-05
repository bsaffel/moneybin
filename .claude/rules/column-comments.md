# Column Comments

## Approach by Table Type

| Table type | Mechanism | Where |
|---|---|---|
| `core.*` SQLMesh models | Inline `-- comment` on final SELECT columns | `sqlmesh/models/core/*.sql` |
| `app.*`, `raw.*` schema DDL | Inline `-- comment` on CREATE TABLE columns | `src/moneybin/sql/schema/*.sql` |
| `prep.*` staging views | None | Internal transformation layer; not for direct consumption |

## How Comments Reach DuckDB's Catalog

**SQLMesh models:** SQLMesh's `register_comments` setting (enabled by default) reads inline SQL comments on SELECT columns and issues `COMMENT ON COLUMN` to DuckDB automatically on every `sqlmesh run`. Important: if a `column_descriptions` block is present in the MODEL(), auto-detection of inline comments is disabled — use one or the other, not both.

**Schema DDL files:** `schema.py:_apply_inline_column_comments()` parses inline `-- comments` from `CREATE TABLE` column definitions and runs `COMMENT ON COLUMN` after each file executes during `init_schemas`. This runs on every app startup, so comments are always current.

## SQLMesh Model Pattern

Add inline comments to the final SELECT only — not to CTEs. SQLMesh reads from the outermost SELECT:

```sql
MODEL (
  name core.dim_accounts,
  kind FULL,
  grain account_id
);

WITH ... (CTEs)
SELECT
  account_id, -- Unique account identifier; stable across imports; foreign key in fct_transactions
  routing_number, -- ABA bank routing number; NULL when not provided by source
  amount -- Transaction amount; negative = expense, positive = income
FROM ...
```

`sqlmesh format` converts `--` to `/* */` block comments — both styles are detected by SQLMesh.

**Do not use** the `columns` block with `COMMENT` keyword — SQLMesh silently swallows it without writing to DuckDB's catalog or its own `column_descriptions`. Use inline comments or the `column_descriptions (col = 'text')` block instead.

## Schema DDL Pattern

Add `-- comment` at the end of each column definition line:

```sql
CREATE TABLE IF NOT EXISTS raw.ofx_transactions (
    transaction_id VARCHAR, -- Unique transaction identifier from OFX <FITID> element; part of primary key
    amount DECIMAL(18, 2), -- OFX TRNAMT element; negative = expense, positive = income
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- Timestamp when this record was inserted into the database
    PRIMARY KEY (transaction_id, account_id, source_file)
);
```

The `-- comment` is the single source of truth. Do **not** add a separate `COMMENT ON COLUMN` block — `schema.py` extracts and applies the inline comments automatically.

## What to Comment

**Every column should have a comment.** Even self-evident columns benefit from a brief description that eliminates ambiguity. Particularly important:

- **Sign conventions** — `amount`: negative = expense, positive = income
- **Source field mapping** — which OFX/CSV field this column came from; how it maps to core
- **Synthetic or caller-supplied values** — not present in the source file
- **Enum semantics** — `match_type`: contains, exact, or regex
- **JSON schema** — document the shape of JSON columns
- **Nullability** — always NULL for certain source systems, or conditionally NULL
- **Primary key participation** — note when a column is part of the primary key
- **Timestamp semantics** — distinguish extracted_at (parsing), loaded_at (DB insert), updated_at (last modified)

## Authoritative References

Always check these before making changes to the comment strategy:

- SQLMesh model configuration: https://sqlmesh.readthedocs.io/en/latest/reference/model_configuration/#general-model-properties
- DuckDB `COMMENT ON` syntax: https://duckdb.org/docs/stable/sql/statements/comment_on
