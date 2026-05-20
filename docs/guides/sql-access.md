<!-- Last reviewed: 2026-05-17 -->
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

See [`docs/reference/data-model.md`](../reference/data-model.md) for column-level documentation of every table above.

## Three paths to query

All three built-in paths attach the database under the alias **`moneybin`** and pre-issue `USE moneybin;`. The schema-qualified names below (`core.fct_transactions`, `reports.spending_trend`) work in every path without any extra setup. All three assume the database is **unlocked** — the encryption key is in the OS keychain. If you see `Database is locked — run 'moneybin db unlock' first`, that's a passphrase-mode profile that hasn't been unlocked this boot. Auto-key profiles unlock automatically on first use. Details in [`database-security.md`](database-security.md).

### `moneybin db query "<sql>"` — one-shot from the CLI

For scripts, one-liners, and anything you'd pipe into `jq` or `csvq`. Output is governed by `-o, --output`:

```bash
moneybin db query "SELECT year_month, total_spend
                   FROM reports.spending_trend
                   WHERE category = 'Food and Drink'
                   ORDER BY year_month DESC LIMIT 12" \
                  --output csv
```

Available formats: `text` (DuckDB's `-table` boxed ASCII, the default), `json`, `csv`, `markdown`, `box`. Output goes straight to stdout; informational messages go to stderr.

**JSON shape.** `--output json` invokes DuckDB CLI's native `-json` formatter, which emits a top-level **array of objects** — one object per row, keyed by column name verbatim:

```json
[
  {"year_month":"2026-04","total_spend":1284.50},
  {"year_month":"2026-03","total_spend":1102.18}
]
```

Decimal/numeric columns serialize as JSON numbers. Dates and timestamps serialize as ISO 8601 strings (`"2026-04-15"`, `"2026-04-15T10:23:00"`). SQL `NULL` serializes as JSON `null` with the key still present. The whole result is buffered before any byte hits stdout — large result sets allocate memory on both DuckDB's side and yours; add an explicit `LIMIT` or stream via `COPY ... TO '/tmp/out.parquet'` from `db shell` for big extracts.

This is **not** the same envelope as MoneyBin's higher-level CLI commands or MCP tools. Those return `{"status", "summary", "data", "actions"}`; `db query --output json` is raw rows. For envelope parity with MCP, use the higher-level read commands listed in [`cli-reference.md`](cli-reference.md).

**Parameter binding.** `db query` has no `--param` flag and no stdin JSON input — the SQL is taken as a single positional argument and forwarded to DuckDB's `-c` flag. **Do not interpolate untrusted values into the SQL string from the shell** — `moneybin db query "SELECT ... WHERE id = '$id'"` is a SQL-injection footgun if `$id` came from a user, a file, or an LLM. For parameterized read queries from agent loops, prefer the MCP `sql_query` tool (also un-parameterized, but the read-only parser blocks the dangerous shapes — see [MCP `sql_query` rules](#mcp-sql_query-rules) below) or attach from Python and use `conn.execute(sql, [params])` directly.

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

Default location: `<base>/profiles/<profile>/moneybin.duckdb`. `<base>` resolves to `$MONEYBIN_BASE_DIR`, then `$XDG_DATA_HOME/moneybin`, then `~/.moneybin`. A typical install puts the file at `~/.moneybin/profiles/default/moneybin.duckdb`. See [`database-security.md`](database-security.md) for the full resolution order.

### DuckDB version compatibility

The encrypted DuckDB file format is versioned with DuckDB itself; the version that wrote it must be compatible with the version reading it. MoneyBin currently bundles **DuckDB 1.5.2** (see `pyproject.toml`). Your external client should use a DuckDB release with the same major.minor or one with documented forward-compatibility. A version mismatch typically surfaces as an opaque `IO Error` or `Serialization Error` on ATTACH. `moneybin db info` prints the DuckDB version the file was last opened with.

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
| `db query` | DuckDB error to stderr, exit `1`. No JSON envelope on error path. |
| `db shell` / `db ui` | DuckDB error printed inline in the shell; subprocess exit `1` on hard failure. |
| External `duckdb` Python | Raises `duckdb.Error` (or specific subclasses like `duckdb.IOException`, `duckdb.InvalidInputException`, `duckdb.CatalogException`, `duckdb.BinderException`). A wrong `ENCRYPTION_KEY` surfaces as a generic decryption / IO error — DuckDB doesn't distinguish bad-key from corrupt-file. |
| MCP `sql_query` | Standard response envelope with `status: "error"` and `error: {code, message, hint?}`. Validation rejections (writes, file access, URL literals) return `status: "ok"` with `data.error` set — the tool itself succeeded, the query was just not allowed. |

## Lock contention and retry

DuckDB is single-writer, multi-reader. Multiple read-only connections coexist with each other and with the moneybin writer.

- **`db query` and `db shell`** open the database the same way other moneybin commands do — through the project's `Database` connection helper. On a write path, that helper retries on lock contention with exponential backoff (start 50 ms, ×1.5, cap 500 ms) until the configured wait budget expires (default 5 s). Read paths are read-only and do not contend with each other.
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

`db query` and `db shell` open the database with default permissions for the unlocked profile — **you can technically `INSERT`/`UPDATE`/`DELETE` from them**. The managed-write middleware that protects `core.*` and `reports.*` is enforced in the MoneyBin write path, not at the SQL layer. Don't write to those schemas from raw SQL; you'll bypass audit logging and SQLMesh's view contracts will undo your changes on the next `moneybin refresh`.

The safe paths:

- **External clients:** attach with `READ_ONLY` as shown above. DuckDB then rejects writes at the engine level.
- **AI agents:** use the MCP `sql_query` tool. The read-only parser rejects anything that isn't `SELECT`/`WITH`/`DESCRIBE`/`SHOW`/`PRAGMA`/`EXPLAIN`.
- **Your own writes to `app.*`:** go through the CLI (`moneybin transactions note add`, `moneybin transactions tag`, etc.) or the MCP write tools. They emit `app.audit_log` rows and the right metric counters.

## MCP `sql_query` rules

The MCP `sql_query` tool is the agent-safe SQL path. Its allowed-statement check is **regex-based**, not a full SQL parser — design your queries accordingly:

- **Allowed top-level statements:** `SELECT`, `WITH`, `DESCRIBE`, `SHOW`, `PRAGMA`, `EXPLAIN`. Match is case-insensitive against the leading non-whitespace token.
- **Blocked anywhere in the query body:** `INSERT`, `UPDATE`, `DELETE`, `DROP`, `CREATE`, `ALTER`, `TRUNCATE`, `REPLACE`, `MERGE`, `COPY`, `ATTACH`, `DETACH`, `EXPORT`, `IMPORT`. A `WITH writes AS (INSERT ...) SELECT ...` is rejected because the body matches `INSERT`. The check is conservative — a `SELECT` with the literal word `INSERT` in a string would also be rejected; quote or alias around it.
- **Blocked function names:** the table-valued readers `read_csv`, `read_parquet`, `read_json`, `read_ndjson`, `read_text`, `read_blob`, `read_delta`, `read_iceberg`, their `scan_*` aliases, the legacy `parquet_scan`, and `glob(...)`. These would let an agent exfiltrate local files even though they look like SELECTs.
- **Blocked literals:** URL schemes (`http://`, `https://`, `s3://`, `az://`, `gcs://`) and any `FROM '...'`/`JOIN '...'` quoted-path replacement scan.
- **Multi-statement input:** not split — the whole string is checked as one statement. A semicolon followed by a write keyword is rejected by the body scan.
- **Row cap:** `mcp.max_rows` from `MoneyBinSettings` (default **1000**). Results are buffered, not streamed.
- **Time cap:** `mcp.tool_timeout_seconds` (default **30 s**) applied by the MCP decorator. On timeout the active DuckDB statement is interrupted.
- **Sensitivity tier:** `medium`. The tool requires the `mcp-data-sharing` consent grant; calls and their queries are logged to `app.audit_log` per the privacy middleware. Row content is not logged.

For schema-aware composition without burning tokens on the full catalog, call `sql_schema(table=None)` first (compact catalog) and then `sql_schema(table='core.fct_transactions')` for the table you need.

## Anti-patterns

[`data-model.md`'s Anti-patterns section](../reference/data-model.md#anti-patterns) is the authoritative list. The load-bearing ones for SQL access:

- Don't `SUM(amount) FROM core.fct_transactions` without filtering `NOT is_transfer` — transfers double-count within account slices.
- Don't aggregate `core.fct_transactions.amount` and `core.fct_transaction_lines.line_amount` in the same query — pick one grain, or you get 2× the answer.
- Don't read from `prep.*` — shape is unstable; column comments aren't emitted.
- Don't mix `reports.*` sign conventions in one expression — `outflow` is negative, `total_spend` is positive.
- Don't `SUM(amount)` across mixed currencies until FX conversion ships — filter by `currency_code`.

## Stability promise

MoneyBin is pre-v1. Column names and view shapes in `core.*` and `reports.*` may rename or restructure before launch — but every change lands in [`CHANGELOG.md`](../../CHANGELOG.md). Post-launch, the surface locks: additive changes only, with deprecation windows for anything that has to move.

Practical implication for scripts: pin to MoneyBin versions in your tooling and re-read the changelog when bumping. `meta.model_freshness` and `moneybin db info` both report the SQLMesh model versions in effect, useful for compatibility checks.

## See also

- [`docs/reference/data-model.md`](../reference/data-model.md) — table grains, column types, join recipes, canonical queries, anti-patterns.
- [`docs/guides/data-pipeline.md`](data-pipeline.md) — how rows reach `core.*`; what's read-only versus write-restricted.
- [`docs/guides/cli-reference.md`](cli-reference.md) — every `db` subcommand, `--output` shapes, envelope details.
- [`docs/guides/database-security.md`](database-security.md) — encryption model, unlock flow, base-directory resolution, multi-machine patterns.
- [`docs/guides/mcp-server.md`](mcp-server.md) — the `sql_query` tool and the agent-safe path.
