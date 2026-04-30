# ADR-000: DuckDB as the Embedded Analytical Store

## Status
accepted (retroactive)

## Context

MoneyBin is a single-user, local-first personal finance application. The storage layer needs to support:

- **Analytical workloads** -- aggregations, window functions, multi-table joins across transactions, accounts, balances, and categorization rules. SQLMesh-driven medallion transforms (`raw` → `prep` → `core`) push thousands of rows through `UNION ALL` + `ROW_NUMBER()` dedup logic on every import.
- **Embedded deployment** -- the database lives next to the app. No separate server to install, secure, or keep running. The MCP server and CLI both open the same file.
- **Encryption at rest** -- AES-256-GCM on the database file (see [ADR-004](004-e2e-encryption.md)). Per-user, no shared infrastructure.
- **Direct file/dataframe interop** -- read CSV, OFX-derived parquet, and Polars/Pandas dataframes without an ETL hop.
- **Modern SQL** -- CTEs, window functions, `QUALIFY`, struct/list types, and SQLMesh's column-level lineage all assume a recent SQL dialect.

The choice is foundational and predates ADR-001. This ADR is written retroactively to capture the reasoning so future contributors understand why DuckDB -- not SQLite or Postgres -- is the storage substrate.

## Decision

Use **DuckDB** as the sole embedded analytical store for the MoneyBin client. All layers (`raw`, `prep`, `core`) live in a single encrypted DuckDB file. SQLMesh targets DuckDB via `sqlmesh[duckdb]`. Python code accesses it through the `Database` abstraction (`get_database()`); direct `duckdb.connect()` calls are forbidden (see AGENTS.md).

## Alternatives Considered

### 1. SQLite

Closest comparison: also embedded, also a single file, ubiquitous tooling.

**Rejected because:**

- **OLTP, not OLAP.** Row-oriented storage means full-table scans for analytical queries (`SUM(amount) GROUP BY merchant_id, month`). On a multi-year transaction history, this is the hot path.
- **Limited SQL surface.** No native `QUALIFY`, weak window-function ergonomics, no `STRUCT`/`LIST` types, no array functions. SQLMesh's audit + lineage features assume a richer dialect.
- **No first-class dataframe / file interop.** `duckdb.read_csv`, `duckdb.from_df`, and direct Parquet scans are central to the import pipeline. Replicating these on SQLite means hand-written ETL.
- **Type system.** SQLite's dynamic typing requires extra validation to keep `DECIMAL(18,2)` amounts and `DATE` columns honest. DuckDB enforces them at the engine level.

SQLite would have been viable for *just* the raw layer, but using two engines for one app is not worth the complexity.

### 2. Postgres (local install or Docker)

The default "real database."

**Rejected because:**

- **Operational burden on the user.** A personal finance app cannot require installing and running Postgres. The product is local-first and zero-config; "run `brew install postgresql` and start the service" is a non-starter.
- **Encryption story is harder.** File-level AES-GCM on a single file (DuckDB) is straightforward. Encrypting Postgres at rest means filesystem-level encryption, TDE extensions, or pgcrypto on every column -- all heavier and less portable.
- **Analytical performance.** Postgres is row-oriented; analytical queries need extensions (Citus columnar, cstore_fdw) that don't ship by default. DuckDB is columnar out of the box and routinely beats Postgres on OLAP benchmarks at this data scale.
- **Concurrency we don't need.** MoneyBin has one writer (the import pipeline) and a small number of local readers (CLI, MCP). Postgres's MVCC and connection pooling solve problems we don't have.

### 3. In-memory only (Polars / Pandas + Parquet files)

Skip the database; load Parquet into dataframes per query.

**Rejected because:**

- **No SQL.** SQLMesh, the MCP server's read tools, and the CLI all depend on a SQL surface. Reimplementing transforms in Polars expression syntax is a large rewrite and loses lineage.
- **No transactions.** Imports need atomic upserts across raw + core tables. Parquet rewrites are not transactional.
- **No persistent indexes / constraints.** Every query reloads and re-derives state.

### 4. Cloud OLAP (BigQuery, Snowflake, ClickHouse Cloud)

**Rejected because:**

- Violates the local-first design philosophy. User financial data must not leave the user's machine without explicit sync (see ADR-002).
- Cost model (per-query or per-warehouse-hour) is wrong for a personal app.
- Cold-start latency makes the CLI feel sluggish.

### 5. ClickHouse / chDB embedded

Columnar, fast, embeddable via chDB.

**Rejected because:**

- Smaller Python ecosystem and tooling than DuckDB at the time of decision.
- SQLMesh DuckDB adapter is more mature than the ClickHouse adapter for embedded use cases.
- DuckDB's Pandas/Polars/Arrow zero-copy story is stronger.

### 6. Apache DataFusion (embedded via `datafusion-python`)

The closest peer to DuckDB: Rust-backed, columnar, modern SQL, embeddable in Python.

**Rejected because:**

- **No SQLMesh adapter.** SQLMesh has first-class DuckDB support; DataFusion would require building and maintaining an adapter, or giving up SQLMesh entirely.
- **Smaller Python / tooling ecosystem.** Fewer examples, fewer third-party integrations, less mature client API. DuckDB ships polished Python bindings with zero-copy Arrow/Polars/Pandas interop; DataFusion's bindings are usable but lag.
- **Persistence story is thinner.** DataFusion is primarily a query engine over Parquet/Arrow rather than a self-contained on-disk database. We'd still need to design the storage format, encryption wrapper, and migration story ourselves.
- **No clear SQL-feature advantage.** Both engines cover the dialect we need; there's no compelling reason to take on the ecosystem cost.

DataFusion is a credible alternative we'd revisit if SQLMesh added a first-class adapter or if DuckDB's roadmap stalled.

## Consequences

### Positive

- **Single-file deployment.** The whole app's state is one encrypted file the user can back up, sync, or delete.
- **Analytical performance by default.** Aggregations over years of transactions run in milliseconds; categorization rule matching is fast even on large rule sets.
- **Rich SQL.** SQLMesh models use `QUALIFY`, window dedup, and struct columns directly.
- **Dataframe interop.** Importers and exports flow through DuckDB ↔ Polars ↔ Arrow with zero copies, keeping the rule "DuckDB > Polars > Pandas" cheap to follow.
- **Single connection abstraction.** `get_database()` is the only entry point; `duckdb.connect()` is banned by AGENTS.md.

### Negative / accepted tradeoffs

- **Single-writer.** DuckDB does not support concurrent writers to the same file. Acceptable: the import pipeline is the sole writer. Read-only connections from the MCP server are safe.
- **Younger ecosystem.** Smaller community than Postgres/SQLite, fewer third-party tools, occasional rough edges in the Python client. Mitigated by DuckDB's rapid release cadence and Motherduck-funded development.
- **Format stability.** DuckDB's storage format has changed across major versions; database files require migration on upgrade. Mitigated by pinning the `duckdb` version and exercising upgrade paths in CI.
- **Encryption is app-level.** DuckDB does not natively encrypt the storage file; we wrap it via AES-256-GCM at the file layer. See [ADR-004](004-e2e-encryption.md) and [ADR-013](013-encryption-key-management.md).
- **No row-level concurrency for an eventual multi-user mode.** If MoneyBin ever grows beyond a single-user local app, the storage layer would need to be revisited. The `Database` abstraction keeps this swap localized.

## References

- [ADR-001: Medallion Data Layers](001-medallion-data-layers.md)
- [ADR-004: End-to-End Encryption](004-e2e-encryption.md)
- [ADR-006: SQLMesh Replaces dbt](006-sqlmesh-replaces-dbt.md)
- [ADR-013: Encryption Key Management](013-encryption-key-management.md)
- [DuckDB vs SQLite (DuckDB docs)](https://duckdb.org/why_duckdb)
