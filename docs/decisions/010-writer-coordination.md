# ADR-010: Writer Coordination for Concurrent Database Access

## Status
accepted

## Context

MoneyBin's storage layer is a single encrypted DuckDB file opened via
`get_database()`, which today returns a module-level singleton holding a
read-write connection for the lifetime of the process. This works while there
is one writer (the import pipeline or MCP server) and no concurrent out-of-process
writers. Two concrete deliverables break that assumption:

- **`web-ui-prototype.md`** — a FastAPI process that must accept categorisation
  approval mutations while the MCP server is running.
- **`sync-plaid.md`** — a Plaid sync process that writes transaction batches
  independently of the MCP server.

### Empirically confirmed DuckDB behaviour

DuckDB's cross-process connection model is stricter than its documentation
implies. Tested against DuckDB 1.5.2 with `ATTACH (TYPE DUCKDB, ENCRYPTION_KEY
...)`:

| Scenario | Result |
|---|---|
| Process A holds write connection → Process B opens write | ❌ `IOException: Conflicting lock is held` |
| Process A holds write connection → Process B opens read-only | ❌ `IOException: Conflicting lock is held` |
| Process A holds read-only → Process B opens write | ❌ `CatalogException: different configuration` |
| Process A holds read-only → Process B opens read-only | ✅ succeeds |

Note: the `CatalogException: different configuration` in row 3 is a **lock-contention signal** — DuckDB rejects opening a write connection when a read-only connection is already attached with a different configuration. It is not a missing-tables error. The implementation must classify this exception as `DatabaseLockError` (at ATTACH time), not `DatabaseNotInitializedError`.

The constraint is: **at most one process holds a connection of any kind, unless
all open connections are read-only.** A long-lived read-write singleton (the
current model) permanently blocks every other process from connecting.

### Measured connection overhead

Timed against a bootstrapped encrypted database:

| Operation | Median | Notes |
|---|---|---|
| Raw `connect + ATTACH + USE` | 14 ms | bare DuckDB, no Python init |
| `Database(no_auto_upgrade=True)` | 79 ms | includes `init_schemas` + `refresh_views` |
| `refresh_views()` alone | 1.7 ms | the view rebuild is not the bottleneck |

The 65 ms gap between raw ATTACH and the full `Database` init is `init_schemas()`
executing idempotent DDL on every open. Skipping it for read-only connections
brings the cost down to ~14 ms.

### The single-human-user property

MoneyBin is a single-user, local-first personal finance application. Write
operations are human-paced: one categorisation approval at a time, one daily
Plaid sync, one CLI import command. Sustained multi-writer contention is
effectively impossible at this scale. The worst realistic case is a Plaid
sync batch (1–3 seconds) overlapping with a user-triggered Web UI mutation.

## Decision

Replace the long-lived read-write singleton with **short-lived, purpose-declared
connections** across all consumers. Every caller acquires a connection, does its
work, and releases immediately. Lock contention is handled by an exponential
backoff retry (up to 5 seconds) rather than assumed-absent.

### Connection modes

`Database` gains a `read_only: bool = False` parameter:

- **`read_only=True`**: emits `ATTACH ... (READ_ONLY)`, skips `init_schemas()`
  and `refresh_views()`. Cost: ~14 ms. Multiple such connections can coexist
  across processes simultaneously.
- **`read_only=False`** (default): current behaviour, full init sequence. Cost:
  ~79 ms. Exclusive — no other process may hold any connection while this is
  open.

### `get_database()` contract

The singleton is removed. `get_database()` creates a new `Database` per call
and retries on lock contention:

```python
def get_database(
    read_only: bool = False,
    max_wait: float = 5.0,
) -> Database:
    deadline = time.monotonic() + max_wait
    delay = 0.05
    while True:
        try:
            return Database(db_path, read_only=read_only, no_auto_upgrade=True)
        except DatabaseLockError:
            if time.monotonic() >= deadline:
                raise
            time.sleep(delay)
            delay = min(delay * 1.5, 0.5)
```

All callers use the existing context-manager protocol — `Database` already
implements `__enter__` / `__exit__` — to ensure the connection is released
immediately after use:

```python
# read path
with get_database(read_only=True) as db:
    rows = db.execute("SELECT ...").fetchall()

# write path
with get_database() as db:
    db.execute("INSERT INTO ...")
```

### Encryption-key caching

`SecretStore.get_key()` hits the OS keychain on every call. Under short-lived
connections this cost is paid per operation. The retrieved key is cached in
process memory after the first successful retrieval — the key is stable for
the lifetime of a process and does not need to be re-fetched.

### Uninitialized-database detection

Read-only connections skip the DDL that normally creates tables and views. If
the database has never been initialised (`db init` has not been run), a
read-only open must fail cleanly before attempting any query. `Database.__init__`
checks `db_path.exists()` when `read_only=True` and raises
`DatabaseNotInitializedError` immediately:

```
Database not found at <path>.
Run 'moneybin db init' to initialize it first.
```

If the file exists but is incomplete (a crashed `db init` left a partial file),
subsequent queries will raise `duckdb.CatalogException` on missing tables or
views. This is caught at the CLI boundary and re-raised as
`DatabaseNotInitializedError` with the same message. CLI error handlers that
already catch `DatabaseKeyError` are extended to catch
`DatabaseNotInitializedError` with the same one-line, no-traceback display
pattern.

### SQLMesh operations

`sqlmesh_context()` and `transform apply` require the raw `duckdb.DuckDBPyConnection`
to inject into `BaseDuckDBConnectionConfig._data_file_to_adapter`. This is
incompatible with a read-only connection. These operations
open a write connection and hold it for the full duration of the SQLMesh
run — potentially tens of seconds. During this window, all other write
attempts retry up to their `max_wait` limit.

For CLI invocations: if `transform apply` or `import inbox sync` is running,
other write operations queue behind the retry. This is acceptable given the
single-user constraint. If a 5-second retry is exhausted (SQLMesh is still
running), the caller receives a `DatabaseLockError` with a message indicating
an active operation is in progress.

### Consumer access patterns

| Consumer | Mode | Notes |
|---|---|---|
| MCP read tools | `read_only=True` | per tool-call; ~14 ms overhead |
| MCP write tools | `read_only=False` | per tool-call; ~79 ms overhead |
| Web UI reads | `read_only=True` | per request |
| Web UI mutations | `read_only=False` | per mutation; retries if sync is active |
| Plaid sync (batch) | `read_only=False` | holds write lock for batch duration |
| CLI read commands | `read_only=True` | ~14 ms; no init overhead |
| CLI write commands | `read_only=False` | ~79 ms |
| `transform apply` | `read_only=False` | exclusive; held for full SQLMesh run |

### `interrupt_and_reset_database()`

The MCP timeout path calls `interrupt_and_reset_database()` to drop the write
lock when a tool call is cancelled. Under short-lived connections the connection
is already scoped to the tool-call context manager and will close on exit.
`interrupt_and_reset_database()` retains its current behaviour — interrupt the
active statement, force-close the connection — to handle the case where a
long-running query is mid-execution when the timeout fires and the thread
survivor keeps running past task cancellation.

## Alternatives Considered

### 1. IPC socket server — first process becomes the DB owner

The first process to open the database also starts a socket server thread.
Subsequent processes connect as socket clients via a Unix socket and proxy
all `Database` method calls, including `ingest_dataframe()` (Arrow data
serialized as Parquet written to a temp path, path sent over socket).

**Rejected for MVP because:**

- Requires building a socket protocol, a proxy object implementing the full
  `Database` interface, Arrow/Parquet serialization for bulk ingestion,
  PID-file lifecycle management, and stale-socket detection with re-election.
- The owner process takes on socket-server responsibilities it wasn't designed
  for. If MCP crashes, the Web UI inherits the socket server — correct in
  theory, operationally surprising.
- `interrupt_and_reset_database()` on the owner must interrupt only the active
  query without closing the socket, which requires a more surgical intervention
  than the current close-and-reset.

Named as the natural upgrade path if MoneyBin gains multiple persistent
always-on services with high write frequency that makes retry-based contention
unacceptable.

### 2. ATTACH journal for out-of-process writers

Secondary writers (Plaid sync) write to a sidecar `.duckdb` journal file they
own exclusively. The primary DB owner (MCP server) merges journals into the
main database on demand via `ATTACH`.

**Rejected because:**

- Solves only the background-batch case (Plaid sync). Interactive writers (Web
  UI) still need a different write path.
- Introduces a consistency window: journal entries are invisible to the MCP
  server until the merge fires. For personal finance this is surprising ("did
  my sync run?" — MCP says no, journal not yet merged).
- Merge logic must handle schema evolution, partial failures, conflict
  resolution, and journal accumulation on crash. Non-trivial implementation
  surface for a problem that the retry model handles at zero complexity cost.

Could pair with the socket-IPC model for background sync if the retry window
for Plaid's multi-second batches becomes operationally noisy. Noted as an
option, not the primary path.

### 3. MotherDuck or Postgres as canonical store

Resolve the single-writer constraint by replacing DuckDB with a server-mode
database.

**Rejected because:**

- Violates the local-first design philosophy (see ADR-000, ADR-002).
- Abandons the OLAP performance, DataFrame interop, and SQLMesh integration
  that motivated choosing DuckDB.
- Appropriate if MoneyBin becomes multi-tenant or multi-user; premature at
  single-user MVP scale.

### 4. Co-locate all services in one process

FastAPI, MCP server, and Plaid sync share a single process and the existing
`_database_instance` singleton.

**Rejected because:**

- Couples deployment lifecycle of three independent services. A Web UI deploy
  restarts MCP; a Plaid sync crash restarts the Web UI.
- Contradicts the architecture's separation of MCP server, Web UI, and sync
  as independently operated components.

## Consequences

### Positive

- No IPC layer, no socket protocol, no proxy objects. Each process is
  self-contained and independently deployable.
- Read-only connections are genuinely lightweight (~14 ms) and can run
  concurrently across any number of processes without coordination.
- Write contention is transparent: callers retry automatically and surface a
  clear `DatabaseLockError` only after the patience window is exhausted.
- `get_database()` callers are unchanged in interface; the context-manager
  protocol is already implemented.
- Natural failover: if any process exits, its connection is released and the
  next caller acquires the lock without ceremony.

### Negative / accepted tradeoffs

- **Per-operation connection overhead.** Every tool call, HTTP request, or CLI
  command pays 14–79 ms to open and close a connection. Acceptable at personal
  finance query rates; would be a bottleneck under high-frequency programmatic
  access.
- **SQLMesh operations hold the write lock for their full duration.** A
  `transform apply` that takes 30 seconds blocks all write attempts for 30
  seconds. Callers that exhaust `max_wait` receive a `DatabaseLockError`.
  Acceptable for single-user; would require the IPC socket server for
  concurrent multi-user use.
- **`init_schemas()` and `refresh_views()` must be gated on `read_only=False`.**
  Read-only connections depend on schemas and views that were created by a
  prior write-mode open. `db init` (always write-mode) is the prerequisite for
  any read-only access.
- **Encryption-key caching is required.** Without it, keychain access on every
  short-lived connection open adds 10–100 ms per operation on macOS. The cache
  is process-scoped and in-memory only — the key is never written to disk.

## References

- [ADR-000: DuckDB as the Embedded Analytical Store](000-duckdb-as-embedded-store.md)
- [ADR-004: End-to-End Encryption](004-e2e-encryption.md)
- [ADR-009: Encryption Key Management](009-encryption-key-management.md)
- [`architecture-shared-primitives.md`](../specs/architecture-shared-primitives.md) §(a)
- `web-ui-prototype.md` — first consumer of the write coordination contract (spec forthcoming)
- [`sync-plaid.md`](../specs/sync-plaid.md) — out-of-process writer that triggered this ADR
- [DuckDB concurrency documentation](https://duckdb.org/docs/stable/connect/concurrency)
