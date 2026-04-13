# Security

## SQL Injection Prevention

- **Always use parameterized queries** with `?` placeholders for any user-supplied or variable value. Never use f-strings, `.format()`, or `%` string interpolation to build SQL in production code.
- **Table/column names cannot be parameterized** — when dynamic identifiers are needed, validate against an allowlist of known names (e.g., `TableRef` constants or a schema dict). Never pass user input directly into identifier positions.
- **DuckDB `read_*` paths**: Validate file paths before passing to `read_csv()`, `read_parquet()`, etc. — these are SQL injection vectors since DuckDB can read remote URLs and glob patterns.
- **Test exception**: Test cases may use string-built SQL to construct fixture data or verify edge cases. Annotate with `# noqa: S608  # building test input string, not executing SQL` and keep the constructed SQL within the test — never in a helper that production code could import.

```python
# CORRECT — parameterized value
conn.execute("SELECT * FROM fct_transactions WHERE account_id = ?", [account_id])

# CORRECT — allowlisted identifier + parameterized value
VALID_COLUMNS = {"date", "amount", "description", "category"}
if sort_col not in VALID_COLUMNS:
    raise ValueError(f"Invalid column: {sort_col}")
conn.execute(f"SELECT * FROM fct_transactions ORDER BY {sort_col} WHERE amount > ?", [min_amount])

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
