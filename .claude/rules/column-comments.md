# Table and Column Comments

## Approach by Table Type

| Table type | Table comment | Column comments | Where |
|---|---|---|---|
| `core.*` SQLMesh models | `/* description */` on the line before `MODEL()` | Inline `-- comment` on final SELECT columns | `sqlmesh/models/core/*.sql` |
| `app.*`, `raw.*` schema DDL | `/* description */` on the line before `CREATE TABLE` | Inline `-- comment` on column definitions | `src/moneybin/sql/schema/*.sql` |
| `prep.*` staging views | None | None | Internal transformation layer; not for direct consumption |

Both file types use the same `/* description */` placement pattern — immediately before the main DDL statement. `sqlmesh format` produces this same style from `--` comment lines in model files.

## How Comments Reach DuckDB's Catalog

**SQLMesh models:** SQLMesh's `register_comments` (enabled by default) auto-detects the `/* */` block comment before `MODEL()` as the table description and applies it as `COMMENT ON TABLE` on every `sqlmesh run`. Inline SQL comments on outermost SELECT columns are applied as `COMMENT ON COLUMN` the same way. Important: if a `column_descriptions` block is present in the MODEL(), auto-detection of inline comments is disabled — use one or the other, not both.

**Schema DDL files:** `schema.py:_apply_comments()` uses sqlglot to parse each file and reads comments directly from the AST — the same mechanism SQLMesh uses internally. sqlglot attaches `/* */` block comments to adjacent `Create` expressions and trailing `--` comments to `ColumnDef` expressions. `COMMENT ON TABLE` and `COMMENT ON COLUMN` are then applied on every app startup via `init_schemas`.

## SQLMesh Model Pattern

**Table comment:** add a `/* description */` block comment on the line immediately before `MODEL()` (this is also the style `sqlmesh format` produces):

```sql
/* Canonical accounts dimension; deduplicated accounts from all sources */
MODEL (
  name core.dim_accounts,
  kind FULL,
  grain account_id
);
```

**Column comments:** add inline comments to the final SELECT only — not to CTEs. SQLMesh reads from the outermost SELECT:

```sql
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

**Table comment:** add a `/* description */` block comment on the line immediately before `CREATE TABLE`. `schema.py` extracts and applies it as `COMMENT ON TABLE`:

```sql
/* Transaction records extracted from OFX/QFX files; one record per transaction per account per source file */
CREATE TABLE IF NOT EXISTS raw.ofx_transactions (
    transaction_id VARCHAR, -- Unique transaction identifier from OFX <FITID> element; part of primary key
    amount DECIMAL(18, 2), -- OFX TRNAMT element; negative = expense, positive = income
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- Timestamp when this record was inserted into the database
    PRIMARY KEY (transaction_id, account_id, source_file)
);
```

**Column comments:** add `-- comment` at the end of each column definition line. Do **not** add separate `COMMENT ON COLUMN` blocks — `schema.py` extracts and applies the inline comments automatically.

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
