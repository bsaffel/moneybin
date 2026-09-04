<!-- Last reviewed: 2026-09-02 -->
# Direct SQL Access

MoneyBin stores your finances in an encrypted DuckDB file. You can query it from your own scripts and clients with the same SQL you'd write against any DuckDB. This guide covers the read-only surface, how to connect from external tools, and the patterns that hold up across releases.

The schema reference lives in [`docs/reference/data-model.md`](../reference/data-model.md) — table grains, column types, sign conventions, join recipes, and the canonical queries that demonstrate them. This guide is the *how*: which schemas to read, which tools to use, and how to attach the encrypted file from a non-MoneyBin client.

## The read surface

| Schema | Purpose | Read from your SQL? |
|---|---|---|
| `core.*` | Canonical analytical tables — `fct_*`, `dim_*`, `bridge_*`. | **Yes.** |
| `reports.*` | Curated presentation views, one per CLI/MCP report. | **Yes.** |
| `app.*` | User state — notes, tags, splits, categorizations, budgets, account settings. | Yes for reads. **Writes go through MoneyBin commands or the MCP write tools, not raw SQL.** |
| `meta.*` | Cross-source provenance and SQLMesh freshness. | Yes, for lineage debugging. |
| `seeds.*` | Reference data shipped with MoneyBin (categories). | Yes, but you usually want `core.dim_categories` instead. |
| `prep.*` | Internal staging views. | **No.** Column shapes change without notice. |
| `raw.*` | Untouched per-source loader output. | Internal. Read only when you specifically need to inspect what an importer produced. |

`core.*` and `reports.*` are stable consumer surfaces. `app.*` is readable as a debugging aid, but every consumer-relevant column already surfaces through `core.fct_transactions` (notes, tags, splits as nested `LIST(STRUCT(...))` columns) or `core.dim_accounts` (account settings joined in). Reach down into `app.*` only when you need raw history that the dim/fact resolution discards.

This table describes what's reachable through `db query`/`db shell`/`db ui` and external clients, which have no schema restriction of their own — DuckDB's own permissions are the only gate. The agent-safe paths (`sql_query` and `moneybin sql query`) admit five of the seven: `core`, `app`, `reports`, `raw`, and `prep`. `meta` and `seeds` are refused. Rows from `raw` and `prep` are masked by a different mechanism than the other three — 34 columns across 17 tables carry an explicit CRITICAL declaration, and every other column is masked by value shape rather than by declaration. See [`sql_query` rules](#sql_query-rules-mcp-tool-and-moneybin-sql-query-cli) below.

See [`docs/reference/data-model.md`](../reference/data-model.md) for column-level documentation of every table above.

## Four paths to query

All four built-in paths attach the database under the alias **`moneybin`** and pre-issue `USE moneybin;`. The schema-qualified names below (`core.fct_transactions`, `reports.spending_trend`) work in every path without any extra setup. All four assume the database is **unlocked** — the encryption key is in the OS keychain. If you see `Database is locked — run 'moneybin db unlock' first`, that's a passphrase-mode profile that hasn't been unlocked this boot. Auto-key profiles unlock automatically on first use. Details in [`database-security.md`](database-security.md).

### `moneybin sql query "<sql>"` — privacy-safe SQL from the CLI

The agent-mediated counterpart to `db query` below, and the CLI twin of the MCP `sql_query` tool: both call the same `execute_sql_query` primitive, so they share the read-only gate, the five-schema restriction, sqlglot column lineage, and CRITICAL masking:

```bash
moneybin sql query "SELECT year_month, total_spend
                    FROM reports.spending_trend
                    WHERE category = 'Food & Drink'
                    ORDER BY year_month DESC LIMIT 12"
```

Output is governed by `-o, --output {text,json}` (only two formats — not `db query`'s five, since results go through the same envelope as MCP tools) and `--json-fields` to project a subset of columns. Full rule set — allowed statements, blocked functions, multi-statement handling, masking — in [`sql_query` rules](#sql_query-rules-mcp-tool-and-moneybin-sql-query-cli) below.

### `moneybin db query "<sql>"` — one-shot from the CLI

For scripts, one-liners, and anything you'd pipe into `jq` or `csvq`. Output is governed by `-o, --output`:

```bash
moneybin db query "SELECT year_month, total_spend
                   FROM reports.spending_trend
                   WHERE category = 'Food & Drink'
                   ORDER BY year_month DESC LIMIT 12" \
                  --output csv
```

Available formats: `text` (DuckDB's `-table` boxed ASCII, the default), `json`, `csv`, `markdown`, `box`. Output goes straight to stdout; informational messages go to stderr.

**JSON shape.** `--output json` invokes DuckDB CLI's native `-json` formatter, which emits a top-level **array of objects** — one object per row, keyed by column name verbatim:

```json
[
  {"year_month":"2026-04","total_spend":"1284.50"},
  {"year_month":"2026-03","total_spend":"1102.18"}
]
```

`DECIMAL` columns — MoneyBin's money type, `DECIMAL(18,2)` on every amount — serialize as JSON **strings**, not numbers: DuckDB's `-json` formatter preserves exact decimal precision rather than risk a double-precision float rounding a cent away. Plain `INTEGER`/`DOUBLE` columns serialize as ordinary JSON numbers. `jq` consumers need `tonumber` before arithmetic on a money column. Dates serialize as `"2026-04-15"`; timestamps serialize space-separated, not `T`-separated (`"2026-04-15 10:23:00"`, not ISO 8601's `"2026-04-15T10:23:00"`). SQL `NULL` serializes as JSON `null` with the key still present. The whole result is buffered before any byte hits stdout — large result sets allocate memory on both DuckDB's side and yours; add an explicit `LIMIT` or stream via `COPY ... TO '/tmp/out.parquet'` from `db shell` for big extracts.

This is **not** the same envelope as MoneyBin's higher-level CLI commands or MCP tools. Those return `{"status", "summary", "data", "actions"}`; `db query --output json` is raw rows. For envelope parity with MCP, use the higher-level read commands listed in [`cli-reference.md`](cli-reference.md).

**Parameter binding.** `db query` has no `--param` flag and no stdin JSON input — the SQL is taken as a single positional argument and forwarded to DuckDB's `-c` flag. **Do not interpolate untrusted values into the SQL string from the shell** — `moneybin db query "SELECT ... WHERE id = '$id'"` is a SQL-injection footgun if `$id` came from a user, a file, or an LLM. For parameterized read queries from agent loops, prefer `moneybin sql query` or the MCP `sql_query` tool (also un-parameterized, but the read-only parser blocks the dangerous shapes — see [`sql_query` rules](#sql_query-rules-mcp-tool-and-moneybin-sql-query-cli) below) or attach from Python and use `conn.execute(sql, [params])` directly.

**Exit codes.** `0` on success, `1` on runtime failure (SQL syntax error, missing table, lock contention exhausted, locked database), `2` on usage error (missing flag value). On runtime failure DuckDB's own error message goes to stderr; nothing is written to stdout. There is no JSON error envelope from this command — script consumers should check the exit code, not parse stdout, to detect failure.

### `moneybin db shell` — interactive DuckDB shell

For exploration, schema browsing, ad-hoc SELECTs. Opens the system DuckDB CLI with the encrypted database pre-attached as `moneybin`:

```bash
moneybin db shell
```

You'll be inside DuckDB with `USE moneybin;` already issued. `.tables`, `.schema`, `.help`, `.quit` all work. Requires the DuckDB CLI binary on `PATH` — install from [duckdb.org/docs/installation](https://duckdb.org/docs/installation/).

### `moneybin db ui` — DuckDB Web UI in the browser

For visual exploration with a query editor and tabular results that copy-paste cleanly:

```bash
moneybin db ui
```

Same DuckDB CLI under the hood; just launched with the `-ui` flag. Press Ctrl+C to stop the local server.

### External clients

DBeaver, your own Python `duckdb` client, anything that speaks DuckDB. Requires the encryption key — see the next section.

## Connecting an external client

### Get the encryption key

```bash
moneybin db key show
```

Prints the 64-character hex key to **stdout**. The same command also writes a security warning to stderr — that's intentional and unconditional. Treat the key with the same care as a password manager export: anyone with the key and the file has full access to your data.

In a script, capture stdout only: `KEY=$(moneybin db key show -q)`.

### Get the database path

Default location: `<base>/profiles/<profile>/moneybin.duckdb`. `<base>` resolves to `--home` or `$MONEYBIN_HOME` if either is given; in development mode, it is `<repo-root>/.moneybin` inside a MoneyBin checkout or `<cwd>/.moneybin` outside one; otherwise a MoneyBin repo checkout uses `<cwd>/.moneybin`, and the default is `~/.moneybin`. A typical install puts the file at `~/.moneybin/profiles/default/moneybin.duckdb`. See [`database-security.md`](database-security.md) for the full resolution order.

### DuckDB version compatibility

The encrypted DuckDB file format is versioned with DuckDB itself; the version that wrote it must be compatible with the version reading it. MoneyBin currently bundles **DuckDB 1.5.4** (pinned in `uv.lock`; `pyproject.toml` only sets a floor, `duckdb>=1.3.2`). Your external client should use a DuckDB release with the same major.minor or one with documented forward-compatibility. A version mismatch typically surfaces as an opaque `IO Error` or `Serialization Error` on ATTACH. `moneybin db info` prints the DuckDB version *currently running* (it runs `SELECT version()`), which is the number to match your external client against. It is not a record of what wrote the file — DuckDB persists no writer-version metadata, so after a MoneyBin upgrade `db info` reports the new runtime even for a file last written by an older release.

### DuckDB CLI

```bash
KEY=$(moneybin db key show -q)
duckdb
```

```sql
ATTACH '/Users/you/.moneybin/profiles/default/moneybin.duckdb'
    AS moneybin (TYPE DUCKDB, ENCRYPTION_KEY '<paste-key-here>', READ_ONLY);
USE moneybin;
SELECT COUNT(*) FROM core.fct_transactions;
```

`TYPE DUCKDB` is required for encrypted attaches; `ENCRYPTION_KEY` takes the hex string MoneyBin generated; `READ_ONLY` blocks any accidental write from your session (see [Read-only invariant](#read-only-invariant) below).

### Python

```python
import duckdb
import subprocess

KEY = subprocess.run(
    ["moneybin", "db", "key", "show", "-q"],
    capture_output=True,
    text=True,
    check=True,
).stdout.strip()
DB = "/Users/you/.moneybin/profiles/default/moneybin.duckdb"

conn = duckdb.connect()  # in-memory connection
conn.execute(
    f"ATTACH '{DB}' AS moneybin (TYPE DUCKDB, ENCRYPTION_KEY '{KEY}', READ_ONLY)"
)
conn.execute("USE moneybin")

df = conn.sql(
    "SELECT * FROM core.fct_transactions WHERE account_id = ? LIMIT 100",
    params=["acc_abc123"],
).df()
```

DuckDB's Python bindings don't accept `ENCRYPTION_KEY` as a `connect()` config option — encrypted databases are opened via `ATTACH`. Open an in-memory `connect()` first, then `ATTACH` the encrypted file. Once attached, `conn.execute(sql, [params])` supports proper `?` placeholder binding — use it for any value that didn't come from your own source code.

### DBeaver

1. Install the DuckDB driver from DBeaver's driver manager (or [duckdb.org/docs/clients/dbeaver](https://duckdb.org/docs/clients/dbeaver)).
2. New Connection → DuckDB → point at `moneybin.duckdb`. The JDBC URL is `jdbc:duckdb:/Users/you/.moneybin/profiles/default/moneybin.duckdb`.
3. In the connection's *Driver properties*, add `encryption_key` with the hex value from `moneybin db key show`.
4. Optional but recommended: set `access_mode=read_only` in the same panel.

DBeaver hands these properties to the JDBC driver, which then issues an encrypted ATTACH on connect.

### Other tools (Datasette, Metabase, etc.)

Tools that don't speak encrypted-DuckDB natively need an unencrypted source. The portable pattern is to export the views you want into Parquet from `db shell`:

```sql
COPY (SELECT * FROM reports.cash_flow) TO '/tmp/cash_flow.parquet';
```

Then point the downstream tool at the Parquet file. **Never share the live encrypted file** — its protection is the encryption key, and you'd have to ship that alongside.

## Errors and error semantics

| Path | Error surface |
|---|---|
| `sql query` | Rejected, unparseable, out-of-scope, unknown-table, or failed queries raise a classified error (`error_codes.SQL_*`): a structured JSON error envelope on stdout with `--output json`, a ❌-prefixed message otherwise. Exit `1` either way. |
| `db query` | DuckDB error to stderr, exit `1`. No JSON envelope on error path. |
| `db shell` / `db ui` | DuckDB error printed inline in the shell; subprocess exit `1` on hard failure. |
| External `duckdb` Python | Raises `duckdb.Error` (or specific subclasses like `duckdb.IOException`, `duckdb.InvalidInputException`, `duckdb.CatalogException`, `duckdb.BinderException`). A wrong `ENCRYPTION_KEY` surfaces as a generic decryption / IO error — DuckDB doesn't distinguish bad-key from corrupt-file. |
| MCP `sql_query` | Standard response envelope with `status: "error"` and `error: {code, message, hint?}`. Validation rejections (writes, file access, URL literals) return `status: "ok"` with `data.error` set — the tool itself succeeded, the query was just not allowed. |

## Lock contention and retry

DuckDB is single-writer, multi-reader. Multiple read-only connections coexist with each other; they coexist with a MoneyBin writer only between its write operations — a read-only open *through the `Database` helper* that lands during an active write retries on the same backoff as writers (start 50 ms, ×1.5, cap 500 ms, 10 s budget) before raising a lock error. Which paths get that helper is the distinction the bullets below draw.

- **`db query` and `db shell` do not retry either.** They shell out to the DuckDB CLI with a generated init script that attaches the encrypted file, so the project's `Database` helper — and its backoff — is never in the path. If the subprocess hits a lock error, the command converts it straight to exit 1. Treat these two like the external attaches below, not like other `moneybin` commands: on a race, retry in your own script.
- **Other `moneybin` commands do retry.** Everything that goes through the `Database` helper backs off on lock contention (start 50 ms, ×1.5, cap 500 ms) until the write-lock wait budget expires (10 s, fixed at build time). Read paths don't contend with each other; only a concurrent active write makes a read retry.
- **External read-only attaches do not retry.** If your client races a moneybin write and DuckDB returns a lock error, your client sees it immediately. Wait a few seconds and retry — moneybin's exclusive lock windows are short (typically sub-second per imported batch).
- **`moneybin db ps`** shows which processes have the file open; **`moneybin db kill`** sends SIGTERM. Use these if a stale process is blocking a write.

For headless or multi-machine deployments, [`database-security.md`](database-security.md) covers active-passive and snapshot-and-copy patterns.

## Refresh concurrency and snapshot consistency

DuckDB does **not** provide snapshot isolation across separate attaches by default. While `moneybin refresh` is running, your external read-only connection sees a moving target — a query started during the refresh window may observe a mix of pre- and post-refresh rows for models that get rebuilt. For one-off ad-hoc queries this is rarely an issue; for multi-query analyses where every result must be consistent, take a backup first and attach to that:

```bash
moneybin db backup --output ~/snapshots/moneybin-$(date +%F).duckdb
```

Then ATTACH the backup file with `READ_ONLY` from your client. The backup is a frozen point-in-time copy.

## File lifecycle

- Multiple `READ_ONLY` attaches across processes coexist with each other and with a running moneybin process — query freely from your scripts while `moneybin reports` runs in another terminal.
- External attaches don't need to be explicitly `DETACH`ed before the moneybin process exits — the underlying file is independent. Closing your client's connection (or letting the process exit) is sufficient.
- If the moneybin process dies mid-write, DuckDB replays its WAL on the next attach. External attaches that were holding the file open during the crash may see a stale view; close and reopen.

## Read-only invariant

`db query` and `db shell` open the database with default permissions for the unlocked profile — **you can technically `INSERT`/`UPDATE`/`DELETE` from them**. MoneyBin's service/repository write paths are the enforcement boundary for application mutations; raw SQL is outside that boundary. Don't write to `core.*` or `reports.*` from raw SQL; you'll bypass audit logging and SQLMesh's view contracts will undo your changes on the next `moneybin refresh`.

The safe paths:

- **External clients:** attach with `READ_ONLY` as shown above. DuckDB then rejects writes at the engine level.
- **AI agents:** use the MCP `sql_query` tool or the CLI `moneybin sql query` command. The read-only parser rejects anything that isn't `SELECT`/`WITH`/`DESCRIBE`/`SHOW`.
- **Your own writes to `app.*`:** go through the CLI (`moneybin transactions notes add`, `moneybin transactions tags add`, etc.) or the MCP write tools. They emit `app.audit_log` rows and the right metric counters.

## `sql_query` rules (MCP tool and `moneybin sql query` CLI)

The MCP `sql_query` tool and the `moneybin sql query` CLI command are the agent-safe SQL paths — both call the same `execute_sql_query` primitive, so every rule below applies identically to both surfaces unless noted. Keyword gates are **regex-based**, with a parse-backed statement check on top — design your queries accordingly:

- **Allowed top-level statements:** `SELECT`, `WITH`, `DESCRIBE`, `SHOW`. Match is case-insensitive against the leading non-whitespace token.
- **`PRAGMA` and `EXPLAIN` are not allowed** — use `DESCRIBE <table>` or `SHOW ALL TABLES` to inspect schema. The rule behind the allowed list is that a statement is executable only if the schema gate can resolve every table it names, and these two reference tables while hiding them: a pragma's target is a string literal rather than a table reference, and an `EXPLAIN`'s entire payload is left unparsed. The gate finds no tables in either and passes without having checked anything. Both were live holes — `PRAGMA storage_info` returns per-segment min/max statistics that are a *cleartext prefix of the stored values* (a CRITICAL routing number's first eight digits, unmasked, where a `SELECT` on the same column returns `*****`), and `EXPLAIN ANALYZE` **executes** its inner query, reporting row counts from a path meant to return schema text. For query plans, use `moneybin db query` (raw operator access, no privacy middleware).
- **Allowed schemas:** `core`, `app`, `reports`, `raw`, and `prep`. The limit binds **every statement that names a table** — `DESCRIBE meta.model_freshness` is refused exactly as `SELECT ... FROM meta.model_freshness` is. `meta` and `seeds` remain readable through `db query`/`db shell`/external clients (see [the read surface](#the-read-surface) above); it is the agent-facing path that refuses them.
- **How `raw` and `prep` are masked:** by value shape, not by column declaration. `core`, `app`, and `reports` declare a `DataClass` for every deployed column and CI verifies the coverage. `raw` and `prep` cannot be covered that way — column names track each source's export format, and `raw.gsheet_<alias>` / `raw.pdf_<alias>` views are minted at connect time from your own spreadsheet headers and document fields. So 34 columns across 17 tables carry an explicit CRITICAL declaration, ten names and no others: `account_id`, `account_name`, `account_number`, `account_number_masked`, `source_account_key`, `routing_number`, the Plaid `mask`, the account-prefixed composite `match_group_id`, and the opaque `source_bytes` / `account_names` payloads. Every other column is re-scanned per value at execution: an SSN-shaped value (`NNN-NN-NNNN`) comes back `***-**-****`, and an unbroken run of 8 or more digits keeps only its last four (`12345678` → `****...5678`).

  **What that scan does not catch:** an account number of 4 to 7 digits, one written with separators (`1234-5678`), or one stored as `DECIMAL` or `FLOAT` — the scan covers strings and integers. In a `raw.gsheet_<alias>` or `raw.pdf_<alias>` view it is the only masking there is. Keep a spreadsheet out of MoneyBin if you don't want an agent reading its cells, or drop an existing one with `moneybin gsheet disconnect <connection-id> --purge`, which removes the view and its raw rows.
- **What the schema gate does not cover:** `SHOW ALL TABLES` names no table, so there is nothing for the gate to resolve, and DuckDB's catalog listing includes a `column_names` and `column_types` array for every table it lists. The *shape* of `meta` and `seeds` — table names, column names, types — is therefore still reachable, even though `DESCRIBE` on those same tables is refused. This is a structure disclosure, not a data one: no statement returns their row values. Treat it as the current boundary rather than a guarantee that fenced schemas are invisible.
- **Blocked anywhere in the query body:** `INSERT`, `UPDATE`, `DELETE`, `DROP`, `CREATE`, `ALTER`, `TRUNCATE`, `REPLACE`, `MERGE`, `COPY`, `ATTACH`, `DETACH`, `EXPORT`, `IMPORT`. A `WITH writes AS (INSERT ...) SELECT ...` is rejected because the body matches `INSERT`. The check is conservative — a `SELECT` with the literal word `INSERT` in a string would also be rejected; quote or alias around it.
- **Blocked function names:** the table-valued readers `read_csv`, `read_parquet`, `read_json`, `read_ndjson`, `read_text`, `read_blob`, `read_delta`, `read_iceberg`, their `scan_*` aliases, the legacy `parquet_scan`, and `glob(...)`. These would let an agent exfiltrate local files even though they look like SELECTs.
- **Blocked literals:** URL schemes (`http://`, `https://`, `s3://`, `az://`, `gcs://`) and any `FROM '...'`/`JOIN '...'` quoted-path replacement scan.
- **Multi-statement input:** rejected. Every statement in `SELECT 1; SELECT routing_number FROM core.dim_accounts` is individually a legal read, so the keyword gates above all pass — but DuckDB returns the last statement's rows while classification reads the first, so the query is refused before execution. The statement count is taken from your query text exactly as DuckDB receives it, so a second statement cannot hide behind a `--` comment. A trailing `; -- comment` or a doubled `;;` is still one statement and runs normally.
- **Row cap:** `mcp.max_rows` from `MoneyBinSettings` (default **1000**), shared by both surfaces. Results are buffered, not streamed.
- **Time cap:** `mcp.tool_timeout_seconds` (default **30 s**), applied by the MCP tool decorator only — `moneybin sql query` has no equivalent wall-clock cap. On MCP timeout the active DuckDB statement is interrupted.
- **Sensitivity tier:** derived per call from the columns your query returns (the max class among them). Every MCP call, and every CLI call made with `--output json`, is recorded to the per-call privacy log (`privacy.log.jsonl`) with the tool name, tier, returned data classes, and row count — **not** the query text and **not** row content. `moneybin sql query` under its default `--output text` writes no privacy event: the text branch returns before the audit write. Pass `--output json` when the query needs to land in the log. CRITICAL columns (account/routing numbers) are masked identically on both surfaces: account identifiers keep the last four digits (`****1234`), routing numbers are masked in full (`*****`, no digits retained). An output column the classifier can't resolve fails closed to the most-sensitive treatment. An output column drawing from more than one source — a `UNION` branch, a `COALESCE`, a `CASE` arm — takes whichever of its inputs masks hardest, so mixing a `prep` column into a `core` projection keeps the value-shape scan on the result. There is no consent-grant requirement today (the consent ledger records but does not gate). See [What the AI Provider Sees](what-the-ai-sees.md).

For schema-aware composition without burning tokens on the full catalog, call `sql_schema(table=None)` first (compact catalog) and then `sql_schema(table='core.fct_transactions')` for the table you need.

The compact catalog covers only the curated tables — the interface tables, plus the `raw.gsheet_<alias>` / `raw.pdf_<alias>` seed views minted by an active connection — which is a strict subset of what `sql_query` will read. Every other `raw` and `prep` model and the internal `app` tables are queryable but uncurated. To see those, call `sql_schema(table='<schema>.*')` (e.g. `sql_schema(table='raw.*')`). It lists the live relations in one schema with their `kind` (`table` or `view`) and a `curated` flag, and is bounded by the same five schemas `sql_query` reads — so it never names a relation you cannot then query, and never reaches `meta` or `seeds`. A relation with `curated: false` has no purpose text or example queries; read its columns with `sql_query(query='DESCRIBE <schema.name>')`.

## Anti-patterns

[`data-model.md`'s Anti-patterns section](../reference/data-model.md#anti-patterns) is the authoritative list. The load-bearing ones for SQL access:

- Don't `SUM(amount) FROM core.fct_transactions` without filtering `NOT is_transfer` — transfers double-count within account slices.
- Don't aggregate `core.fct_transactions.amount` and `core.fct_transaction_lines.line_amount` in the same query — pick one grain, or you get 2× the answer.
- Don't read from `prep.*` — shape is unstable; column comments aren't emitted.
- Don't mix `reports.*` sign conventions in one expression — `outflow` is negative, `total_spend` is positive.
- Don't `SUM(amount)` across mixed currencies until FX conversion ships — filter by `currency_code`.

## Stability promise

MoneyBin is pre-v1. Column names and view shapes in `core.*` and `reports.*` may rename or restructure before launch — but every change lands in [`CHANGELOG.md`](../../CHANGELOG.md). Post-launch, the surface locks: additive changes only, with deprecation windows for anything that has to move.

Practical implication for scripts: pin to MoneyBin versions in your tooling and re-read the changelog when bumping. `meta.model_freshness` reports the SQLMesh model versions in effect (`last_changed_at`, `last_applied_at`, `last_executed_at` per model) — useful for schema-drift checks; `moneybin db info` reports the running DuckDB version — useful for the client-compatibility check in [DuckDB version compatibility](#duckdb-version-compatibility) above. The two are unrelated version axes; neither substitutes for the other.

## See also

- [`docs/reference/data-model.md`](../reference/data-model.md) — table grains, column types, join recipes, canonical queries, anti-patterns.
- [`docs/guides/data-pipeline.md`](data-pipeline.md) — how rows reach `core.*`; what's read-only versus write-restricted.
- [`docs/guides/cli-reference.md`](cli-reference.md) — every `db` subcommand, `--output` shapes, envelope details.
- [`docs/guides/database-security.md`](database-security.md) — encryption model, unlock flow, base-directory resolution, multi-machine patterns.
- [`docs/guides/mcp-server.md`](mcp-server.md) — the `sql_query` MCP tool and the agent-safe path (CLI twin: `moneybin sql query`).
