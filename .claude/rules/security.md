---
description: "Security: SQL injection prevention, input validation, XSS, PII in logs, exception wrapping"
paths: ["src/moneybin/**/*.py", "**/*.sql"]
---

# Security

## SQL Injection Prevention

- **Always use parameterized queries** with `?` placeholders for any user-supplied or variable value. Never use f-strings, `.format()`, or `%` string interpolation to build SQL in production code.
- **Table/column names cannot be parameterized** — when dynamic identifiers are needed, validate against an allowlist. Three valid approaches, in order of preference:
  1. **`TableRef` constants** (compile-time) — preferred when the set of valid tables is known at design time.
  2. **`duckdb_tables()` / `duckdb_columns()`** (runtime) — query DuckDB's catalog to validate that a table or column actually exists before interpolating. Use this when the valid set is dynamic or when you want to validate against the live schema.
  3. **`sqlglot` quoting** — use `sqlglot.exp.to_identifier(name, quoted=True).sql("duckdb")` for programmatic identifier quoting when building SQL programmatically. Already a project dependency.
  ```python
  # CORRECT — validate against catalog, then quote with sqlglot
  from sqlglot import exp

  valid_tables = db.execute(
      "SELECT schema_name || '.' || table_name FROM duckdb_tables()"
  ).fetchall()
  qualified = f"{schema}.{table}"
  if (qualified,) not in valid_tables:
      raise ValueError(f"Unknown table: {qualified}")
  safe_schema = exp.to_identifier(schema, quoted=True).sql("duckdb")
  safe_table = exp.to_identifier(table, quoted=True).sql("duckdb")
  db.execute(f"SELECT * FROM {safe_schema}.{safe_table} WHERE id = ?", [record_id])

  # CORRECT — compile-time allowlist
  from moneybin.tables import INTERFACE_TABLES

  if table_name not in {table.full_name for table in INTERFACE_TABLES}:
      raise ValueError(f"Unknown table: {table_name}")
  ```
  Never use bare f-string interpolation for identifiers, even after validation — always double-quote as defense in depth.
- **DuckDB `read_*` paths**: Validate file paths before passing to `read_csv()`, `read_parquet()`, etc. — these are SQL injection vectors since DuckDB can read remote URLs and glob patterns.
- **Test exception**: Test cases may use string-built SQL to construct fixture data or verify edge cases. Annotate with `# noqa: S608  # building test input string, not executing SQL` and keep the constructed SQL within the test — never in a helper that production code could import.

```python
# CORRECT — parameterized value
conn.execute("SELECT * FROM fct_transactions WHERE account_id = ?", [account_id])

# CORRECT — allowlisted identifier + parameterized value
VALID_COLUMNS = {"date", "amount", "description", "category"}
if sort_col not in VALID_COLUMNS:
    raise ValueError(f"Invalid column: {sort_col}")
conn.execute(
    f"SELECT * FROM fct_transactions WHERE amount > ? ORDER BY {sort_col}", [min_amount]
)

# WRONG — string interpolation
conn.execute(f"SELECT * FROM fct_transactions WHERE account_id = '{account_id}'")
```

## Output Encoding (XSS Prevention)

- **Web UI**: All user-derived content must be escaped before rendering. Use templating engines with auto-escaping enabled (e.g., Jinja2 `autoescape=True`). Never construct HTML with string concatenation or f-strings.
- **MCP tool responses**: Return structured data (dicts/lists), not pre-formatted HTML. The MCP host is responsible for rendering. Never embed user-derived content in HTML or Markdown links without escaping.
- **Markdown injection**: When including user-supplied values in Markdown output (descriptions, notes), escape `[]()` link syntax and backticks to prevent content injection in MCP host rendering.
- **CLI output**: Use `logging` or `typer.echo()` — not raw `print()` with unescaped user data. This prevents log injection and terminal escape sequence attacks.

## Input Validation

- **Validate at system boundaries** — CLI arguments, MCP tool inputs, API responses, file content. Internal function-to-function calls can trust typed parameters.
- **Path traversal**: Reject paths containing `..` segments or absolute paths when a relative path within a known directory is expected. Use `Path.resolve()` and verify the result is under the intended root.
- **Numeric ranges**: Clamp or reject financial values outside reasonable bounds (e.g., transaction amounts, date ranges). Prevent negative counts, zero denominators, and dates far in the future or past.
- **String lengths**: Enforce maximum lengths on user-supplied strings (descriptions, notes, account names) before they reach the database. DuckDB `VARCHAR` is unbounded by default — the application must set limits.
- **Enum/set membership**: When a parameter must be one of a known set (account types, sort directions, export formats), validate against the set explicitly. Prefer `Literal` types or `Enum` classes over bare strings.
- **Pydantic for structured input**: Use Pydantic models with `Field(...)` constraints (`min_length`, `max_length`, `ge`, `le`, `pattern`) for any structured input from external sources. Let validation failures raise before business logic runs.
- **File content**: Treat uploaded/imported file content (CSV, OFX, PDF) as untrusted. Validate structure and field types after parsing — do not assume files conform to expected schemas.

## External Library Exception Wrapping

When catching exceptions from external libraries (keyring, duckdb, argon2, base64):

1. **Read the library docs** for the exact exception type raised — don't guess.
2. **Wrap at the boundary module**, not in callers. E.g., `SecretStore.delete_key()` catches `keyring.errors.PasswordDeleteError` and raises `SecretNotFoundError`.
3. **Comment untyped exceptions**: DuckDB raises generic errors on bad encryption keys. Use `# noqa: BLE001 — duckdb raises untyped errors on bad ENCRYPTION_KEY` to document why a broad catch is needed.
4. **Test the wrapping**: mock the real library exception type, verify the project exception is raised.

## General Practices

- `SecretStr` for passwords/API keys in Pydantic Settings.
- Subprocess commands as lists (`["cmd", "arg"]`), never `shell=True` with user input.
- Log detailed errors internally; return generic messages to users.

## PII in Logs and Errors

- **Never log** account numbers, routing numbers, SSNs, transaction amounts, balances, full descriptions, or merchant names. Log record counts, entity IDs, status codes, and masked values only.
- A `SanitizedLogFormatter` (`src/moneybin/log_sanitizer.py`) provides runtime detection and masking of PII patterns (SSNs, account numbers, dollar amounts) as a safety net. It masks and warns — it never suppresses log entries.
- **Error messages** returned to users (CLI, MCP) must be generic. Catch specific exceptions and return clean messages — never let stack traces with financial data in local variables propagate to output.
- See [`privacy-data-protection.md`](../../docs/specs/privacy-data-protection.md) for the full list of allowed vs prohibited log content.

## Operations metadata is judged by disclosure, not by column class

Two different surfaces get judged two different ways, and conflating them
costs review rounds without buying privacy.

- **Data plane** — report outputs, `sql_query` results, exported table rows.
  Governed by declared `DataClass` and its CI-verified derivation
  (`reports.md`). A column's class decides its masking. This is where the
  machinery earns its keep, and none of it is negotiable.
- **Operations metadata** — audit context (`app.audit_log.context_json`),
  receipts, metric labels, log lines. These describe an operation the user
  performed on their own machine. The test is **not** "what class would this
  column be" but: *does this value cross a boundary carrying something the
  other side cannot already see?* The boundaries that matter are the model
  provider (anything an MCP tool returns) and any artifact that outlives the
  session.

Applying data-plane reasoning to operations metadata produces two failure
modes, both observed:

- **Over-restriction** — withholding a value the provider already receives
  through a normal read, which costs information and protects nothing.
- **Under-restriction by proxy** — persisting a *derivative* (a hash, a
  truncated digest, a bucketed count) on the theory that it is not the value.
  A digest over a low-entropy input is a verifier for guessing that input, and
  a digest over an already-masked input collapses distinct values. Neither is
  a masking transform. **Never persist a hash of a sensitive value as a
  privacy measure** — either the value is safe to record, or the field does
  not belong in the row.

Prefer an identifier the system already minted (`export_id`, `operation_id`)
or a content checksum over anything derived from user input: both discriminate
at least as well and disclose nothing. Origin: PR #374, where a receipt's
parameter fingerprint was added, found to leak on one path and collapse on the
other, and removed — the export id and checksums already did the job.

**A declared class is evidence, not proof.** Before reasoning from one, check
what the field actually holds: `balance_drift`'s `account` parameter is
declared `ACCOUNT_IDENTIFIER` (CRITICAL) but accepts an opaque `account_id` or
a display name. Over-declaration passes CI by design (`reports.md`), so a class
read in isolation can overstate a field's sensitivity by two tiers.
