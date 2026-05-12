# Feature: Database Writer Coordination

## Status
draft

## Goal

Implement [ADR-010](../decisions/010-writer-coordination.md): replace the long-lived read-write singleton in `database.py` with short-lived, purpose-declared connections. Every caller acquires a connection, does its work, and releases it. Read-only connections skip `init_schemas()` and `refresh_views()` (~14 ms overhead) and coexist across processes. Write connections are exclusive (~79 ms) and retry on lock contention up to 5 seconds.

## Background

- [ADR-010: Writer Coordination](../decisions/010-writer-coordination.md) — the design decision; read this first
- [ADR-000: DuckDB as Embedded Store](../decisions/000-duckdb-as-embedded-store.md) — why DuckDB; single-writer constraint
- `src/moneybin/database.py` — all changes land here
- `docs/specs/architecture-shared-primitives.md` §(a) — settled in ADR-010

## Requirements

1. `Database(read_only=True)` skips `init_schemas()`, `refresh_views()`, and all migrations; opens DuckDB with `READ_ONLY` flag.
2. `Database(read_only=True)` raises `DatabaseNotInitializedError` before any DuckDB operation if `db_path` does not exist.
3. `Database(read_only=False)` (default) behaves identically to the current `Database.__init__()` sequence.
4. `get_database(read_only, max_wait)` creates a new `Database` per call; no singleton.
5. `get_database()` retries on `DatabaseLockError` with exponential backoff (start 50 ms, ×1.5, cap 500 ms) until `max_wait` is exhausted, then re-raises `DatabaseLockError` with the message: `"Could not acquire write lock after {max_wait}s. Another moneybin process may be writing. Check with 'moneybin db ps'."`
6. The encryption key retrieved from `SecretStore` is cached in process memory after the first successful retrieval; never re-fetched within the process lifetime.
7. All `get_database()` callers use the context-manager protocol to ensure connections are released immediately after use.
8. MCP tool bodies declare `read_only=True` for pure-read tools and `read_only=False` for any tool that writes to `app.*` or `raw.*`.
9. CLI commands that only read data declare `read_only=True`; commands that write (import, categorize, curation) declare `read_only=False`.
10. `interrupt_and_reset_database()` interrupts and closes the currently-active write connection, if any.
11. `sqlmesh_context()` accepts an explicit `db: Database` parameter (the caller's write connection) instead of reading the module-level singleton.
12. The atexit metrics flush opens a fresh connection only if the database was accessed during the session.
13. `DatabaseNotInitializedError` is caught at all CLI error handlers alongside `DatabaseKeyError`, displayed as a one-line message with no traceback.
14. The `.claude/rules/database.md` connection-management section is updated to reflect the new per-operation model.

## Implementation Plan

### Phase 1 — `database.py` core

**New exceptions**

```python
class DatabaseLockError(Exception):
    """DuckDB file lock held by another process; caller may retry."""


class DatabaseNotInitializedError(Exception):
    """Database file missing or incomplete; run 'moneybin db init'."""
```

**Encryption key cache**

```python
_cached_encryption_key: str | None = None
```

In `Database.__init__()`, before calling `store.get_key()`:
```python
global _cached_encryption_key
if _cached_encryption_key is not None:
    encryption_key = _cached_encryption_key
else:
    encryption_key = store.get_key(_KEY_NAME)
    _cached_encryption_key = encryption_key
```

**`build_attach_sql()` — `read_only` flag**

Add `read_only: bool = False` parameter. When `True`, append `READ_ONLY` to the options list:
```sql
ATTACH '...' AS "moneybin" (TYPE DUCKDB, ENCRYPTION_KEY '...', READ_ONLY)
```

**`Database.__init__()` — `read_only` parameter**

```python
def __init__(
    self,
    db_path: Path,
    *,
    read_only: bool = False,
    secret_store: SecretStore | None = None,
    no_auto_upgrade: bool | None = None,
) -> None:
```

When `read_only=True`: check `db_path.exists()` first — raise `DatabaseNotInitializedError` if missing:
```
Database not found at <path>.
Run 'moneybin db init' to initialize it first.
```
If the file exists, skip the entire write-mode bootstrap: no `mkdir`, no permission set/check, no `init_schemas()`, no migrations, no `refresh_views()`. Use `build_attach_sql(db_path, key, read_only=True)`.

When `read_only=False`: current behaviour unchanged (full init sequence).

**Exception classification — two distinct `CatalogException` cases:**

`duckdb.CatalogException` is raised in two unrelated situations that need different wrappers:

1. **ATTACH-time config mismatch** (`CatalogException: different configuration`) — signals a lock-contention condition: another process holds an incompatible (e.g. read-only) connection. Classify as `DatabaseLockError` by catching in `Database.__init__` at the `ATTACH` call and pattern-matching on `"different configuration"` in the message.

2. **Query-time missing table** (`CatalogException: ... does not exist`) — signals a partially initialised database (crashed `db init`). Classify as `DatabaseNotInitializedError` by catching at the CLI/service boundary when a query fails, pattern-matching on `"does not exist"` in the message.

Wrap DuckDB's `IOException` containing `"Conflicting lock"` in `DatabaseLockError`. (The observed message from DuckDB 1.5.2 is `"Conflicting lock is held"` — match on `"Conflicting lock"`; do not add `"already open"` until confirmed against DuckDB 1.5.2.)

**Active write connection slot**

```python
_active_write_conn: Database | None = None
_active_write_lock: threading.Lock = threading.Lock()
```

In `get_database(read_only=False)` after successful open, register the returned instance:
```python
with _active_write_lock:
    global _active_write_conn
    _active_write_conn = db
```

In `Database.close()` (and `interrupt_and_reset()`), de-register:
```python
with _active_write_lock:
    global _active_write_conn
    if _active_write_conn is self:
        _active_write_conn = None
```

**`get_database()` — new signature**

Migrations run on the **first write-mode open** of the process, then skipped for all subsequent opens (read or write). This preserves correct upgrade behaviour without re-running the migration check on every short-lived write connection.

```python
_migration_check_done: bool = False


def get_database(
    read_only: bool = False,
    max_wait: float = 5.0,
) -> Database:
    global _database_accessed, _migration_check_done
    deadline = time.monotonic() + max_wait
    delay = 0.05
    skip_upgrade = read_only or _migration_check_done
    while True:
        try:
            db = Database(
                get_settings().database.path,
                read_only=read_only,
                no_auto_upgrade=skip_upgrade,
            )
            if not read_only:
                _migration_check_done = True
            _database_accessed = True
            if not read_only:
                with _active_write_lock:
                    global _active_write_conn
                    _active_write_conn = db
            return db
        except DatabaseLockError:
            if time.monotonic() >= deadline:
                raise
            time.sleep(delay)
            delay = min(delay * 1.5, 0.5)
```

The `_active_write_conn` registration must appear after the successful `Database(...)` construction and before `return`, gated on `read_only=False` — this is what `interrupt_and_reset_database()` reads.

**`interrupt_and_reset_database()`**

```python
def interrupt_and_reset_database() -> None:
    with _active_write_lock:
        conn = _active_write_conn
    if conn is not None:
        conn.interrupt_and_reset()
```

**Database-accessed flag**

```python
_database_accessed: bool = False


def database_was_accessed() -> bool:
    return _database_accessed
```

Set to `True` in `get_database()` after the first successful open.

**Functions to remove**

- `_database_instance` singleton and all references
- `close_database()`
- `get_database_if_initialized()` — replaced by `database_was_accessed()`
- `_temporary_singleton()` — `sqlmesh_context()` will take an explicit `db` parameter

**`sqlmesh_context()` — new signature**

```python
@contextmanager
def sqlmesh_context(
    db: Database,
    sqlmesh_root: Path | None = None,
) -> Generator[Any, None, None]:
```

Replace the singleton reads (`_database_instance._conn`, `_database_instance._db_path`) with `db._conn` and `db._db_path`. The caller is responsible for holding `db` open (inside a `with get_database() as db:` block) for the full duration of the SQLMesh run.

`init_db()` (the `db init` entry point) currently wraps `sqlmesh_context()` inside `_temporary_singleton()`. After this change, it calls `sqlmesh_context(db)` directly using the locally-opened `db`:
```python
with Database(db_path, secret_store=store, no_auto_upgrade=False) as db:
    materialize_seeds(db)
    if needs_sqlmesh_migrate:
        with sqlmesh_context(db) as ctx:
            ctx.migrate()
```

### Phase 2 — MCP tool migration

**Classification rule**

| Mode | When |
|---|---|
| `read_only=True` | Tool only SELECTs from `core.*`, `reports.*`, `app.*` |
| `read_only=False` | Tool INSERTs/UPDATEs/DELETEs to `app.*` or `raw.*`, or runs import |

**Read-only MCP tools** (representative; full list in `mcp/tools/`)

- All `reports.py` tools (networth, spending, cashflow, recurring, merchant_activity, etc.)
- `accounts.py`: list, get, summary, balance history/list, resolve
- `categories.py`: get_all, list_rules, stats, list_uncategorized
- `transactions_categorize.py`: list_rules, stats, list_uncategorized, auto-rule review/stats
- `transactions_categorize_assist.py`: all (redacted read-only)
- `merchants.py`: list_merchants
- `system.py`: status
- `tax.py`: all read operations
- `curation.py`: list_events (audit log read)
- `mcp/resources.py`: all three resources

**Write MCP tools** (representative)

- `accounts.py`: rename, set_include_in_net_worth, archive, unarchive, settings_update, balance_reconcile, balance_assert, balance_delete_assertion
- `categories.py`: create_category, toggle_category
- `merchants.py`: update merchant settings/rules
- `transactions_categorize.py`: categorize_items, create_rules, deactivate_rule, accept
- `curation.py`: add/edit/delete note, set_tags, rename_tag, set_splits, set_labels
- `import_tools.py`: all
- `import_inbox.py`: sync
- `budget.py`: write operations
- `sql.py`: always write mode (conservative; the tool accepts arbitrary SQL)
- `transactions.py`: transactions_create is write; list/search are read

**Caller pattern**

Before:
```python
service = SomeService(get_database())
return service.read_operation()
```

After (read):
```python
with get_database(read_only=True) as db:
    return SomeService(db).read_operation()
```

After (write):
```python
with get_database() as db:
    return SomeService(db).write_operation()
```

**`mcp/server.py` changes**

- Remove `get_db()` (no persistent connection to expose).
- `table_exists()`: open its own `read_only=True` connection:
  ```python
  def table_exists(table: TableRef) -> bool:
      with get_database(read_only=True) as db:
          ...
  ```
- `init_db()`: remove `get_database()` warm-up call; just call `register_core_tools()`.
- `close_db()`: remove `close_database()` call; flush metrics if accessed:
  ```python
  def close_db() -> None:
      from moneybin.database import database_was_accessed

      if database_was_accessed():
          flush_metrics()
  ```

**`mcp/decorator.py`**

`interrupt_and_reset_database()` call is unchanged. It now operates via `_active_write_conn` instead of the singleton — no code change needed in the decorator itself.

### Phase 3 — CLI migration

**`cli/utils.py` `handle_cli_errors()`**

Remove the `read_only` parameter and stop yielding `Database`. `handle_cli_errors()` becomes a pure error-handler — it wraps error classification and exit-code logic, nothing more:

```python
@contextmanager
def handle_cli_errors() -> Generator[None, None, None]:
    try:
        yield
    except typer.Exit:
        raise
    except Exception as e:
        user_error = classify_user_error(e)
        if user_error is None:
            raise
        logger.error(f"❌ {user_error.message}")
        if user_error.hint:
            logger.info(user_error.hint)
        raise typer.Exit(1) from e
```

Add `DatabaseNotInitializedError` to `classify_user_error()` (in `moneybin/errors.py`), producing a one-line message:
```
❌ Database not found. Run 'moneybin db init' to initialize it first.
```

**CLI command pattern**

Each command explicitly acquires its own connection with the mode it needs, nested inside `handle_cli_errors()`:

```python
# read-only command
with handle_cli_errors():
    with get_database(read_only=True) as db:
        result = SomeService(db).read_operation()

# write command
with handle_cli_errors():
    with get_database() as db:
        SomeService(db).write_operation()
```

Read-only commands: `reports *`, `accounts list/show/balance history/balance list`, `transactions list/search`, `categories list`, `merchants list`, `system status`, `db ps`, `db query`.

Write commands: all imports, categorize, curation, transform, `db init/lock/unlock/migrate`.

**`sqlmesh_command()` in `cli/utils.py`**

`sqlmesh_command()` still yields a `Database` (callers pass it to `sqlmesh_context(db)`), but it now opens the write connection itself rather than inheriting one from `handle_cli_errors()`. It also takes over both error-classification paths — classified user errors and SQLMesh's broad exceptions — since it no longer wraps `handle_cli_errors()`:

```python
@contextmanager
def sqlmesh_command(
    operation: str, *, success: str | None = None
) -> Generator[Database, None, None]:
    logger.info(f"⚙️  {operation}...")
    try:
        with get_database() as db:
            yield db
        logger.info(f"✅ {success or f'{operation} completed'}")
    except typer.Exit:
        raise
    except Exception as e:
        user_error = classify_user_error(e)
        if user_error is not None:
            logger.error(f"❌ {user_error.message}")
            if user_error.hint:
                logger.info(user_error.hint)
        else:
            logger.error(f"❌ {operation} failed: {e}")  # noqa: BLE001 — SQLMesh raises broad exceptions
        raise typer.Exit(1) from e
```

Call site for `transform apply` is unchanged:
```python
with sqlmesh_command("SQLMesh transform") as db:
    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)
```

### Phase 4 — Observability

**`observability.py` `flush_metrics()`**

Replace `get_database_if_initialized()` with `database_was_accessed()`:

```python
def flush_metrics() -> None:
    try:
        from moneybin.database import database_was_accessed
        from moneybin.metrics.persistence import flush_to_duckdb

        if not database_was_accessed():
            return
        # Write connection needed — metrics are written to app schema
        with get_database(max_wait=2.0) as db:
            flush_to_duckdb(db)
    except Exception:  # noqa: BLE001
        logger.debug("Metrics flush on exit failed", exc_info=True)
```

`max_wait=2.0` bounds shutdown time. If a write connection is held by a slow operation at atexit, the flush is skipped (metrics lost for this session, accumulated next run).

The periodic flush (MCP stream, every 5 minutes) calls `flush_metrics()` unchanged.

### Files to Modify

| File | Change summary |
|---|---|
| `src/moneybin/database.py` | All Phase 1 changes |
| `src/moneybin/mcp/server.py` | Remove `get_db()`, update `init_db()`/`close_db()`/`table_exists()` |
| `src/moneybin/mcp/decorator.py` | No change (uses `interrupt_and_reset_database()` which still exists) |
| `src/moneybin/mcp/resources.py` | All three resources: `read_only=True` |
| `src/moneybin/mcp/tools/accounts.py` | Mixed: classify per tool |
| `src/moneybin/mcp/tools/categories.py` | Mixed |
| `src/moneybin/mcp/tools/curation.py` | Mixed (reads: list_events; writes: all others) |
| `src/moneybin/mcp/tools/import_inbox.py` | Write |
| `src/moneybin/mcp/tools/import_tools.py` | Write |
| `src/moneybin/mcp/tools/merchants.py` | Mixed |
| `src/moneybin/mcp/tools/reports.py` | Read-only |
| `src/moneybin/mcp/tools/sql.py` | Write (conservative) |
| `src/moneybin/mcp/tools/system.py` | Read-only |
| `src/moneybin/mcp/tools/tax.py` | Read-only |
| `src/moneybin/mcp/tools/transactions.py` | Mixed |
| `src/moneybin/mcp/tools/transactions_categorize.py` | Mixed |
| `src/moneybin/mcp/tools/transactions_categorize_assist.py` | Read-only |
| `src/moneybin/mcp/tools/budget.py` | Write |
| `src/moneybin/cli/utils.py` | `handle_cli_errors()` becomes error-only (no `db` yield); `sqlmesh_command()` opens its own write connection |
| `src/moneybin/cli/commands/import_cmd.py` | Add explicit `get_database()` calls inside `handle_cli_errors()` blocks |
| `src/moneybin/observability.py` | Phase 4 changes |
| `src/moneybin/services/inbox_service.py` | `InboxService.create()` class method |
| `src/moneybin/services/schema_catalog.py` | Read-only |
| `src/moneybin/errors.py` | Add `DatabaseNotInitializedError` classification |
| `.claude/rules/database.md` | Update connection-management section |

### Key Decisions

**Why `_migration_check_done` instead of hardcoding `no_auto_upgrade=True`**: Migrations must run when a new version of MoneyBin is installed. Hardcoding `no_auto_upgrade=True` in `get_database()` would skip them entirely, regressing upgrade behaviour. The flag ensures migrations run exactly once per process — on the first write-mode open — then are skipped for all subsequent opens (read or write), avoiding the overhead on every short-lived write connection.

**Why the `_active_write_conn` slot instead of removing `interrupt_and_reset_database()`**: The MCP decorator's timeout path (`mcp/decorator.py`) needs to interrupt a mid-flight DuckDB query and release the write lock before another tool can proceed. Without a global reference to the current write connection, the interrupt can't fire. The slot gives `interrupt_and_reset_database()` a target without re-introducing a singleton.

**`sql.py` always uses write mode**: `execute_sql` accepts arbitrary SQL. Parsing statement type to pick the mode adds fragile complexity. Write mode is safe; the ~65 ms extra overhead is acceptable for a developer/power-user tool.

**`no_auto_upgrade` on `get_database()` vs on `init_db()`**: `get_database()` always passes `no_auto_upgrade=True`. `init_db()` (the `db init` entry point) constructs `Database` directly with `no_auto_upgrade=False`. This keeps migrations strictly in the init path, not triggered by normal app operations.

## Testing Strategy

### Unit tests

- `build_attach_sql(path, key, read_only=True)` includes `READ_ONLY` in output.
- `Database(read_only=True)` on missing `db_path` raises `DatabaseNotInitializedError` before DuckDB connect (mock `db_path.exists()` → False).
- `Database(read_only=True)` on existing file skips `init_schemas()` call (mock and verify not called).
- Encryption key cache: second `Database.__init__()` call does not call `store.get_key()` (mock `SecretStore`).
- `get_database()` retry logic: first two calls raise `DatabaseLockError`, third succeeds → verifies exponential backoff (mock `time.sleep`).
- `get_database()` exhausts `max_wait` → raises `DatabaseLockError`.
- `interrupt_and_reset_database()` calls `interrupt_and_reset()` on `_active_write_conn`.
- `interrupt_and_reset_database()` is a no-op when no write connection is active.
- `DatabaseNotInitializedError` produces a one-line message via `classify_user_error()`.

### Integration tests (real encrypted DB, single process)

- `Database(read_only=True)` on initialized DB can query `core.fct_transactions`.
- `Database(read_only=False)` writes to `app.*`; subsequent `Database(read_only=True)` sees the change.
- Context manager releases the connection: after `with get_database() as db: pass`, opening a new write connection succeeds immediately.
- `get_database(read_only=True)` raises `DatabaseNotInitializedError` on uninitialised path (file exists but is a partial stub with no tables) — confirmed via `duckdb.CatalogException` catch.

### Multi-process tests (`tests/e2e/test_concurrent_access.py`)

Use `subprocess.Popen` with a shared temp database (initialized once in a session-scoped fixture). Each scenario spawns worker scripts via `uv run python -c "..."` or small helper modules, coordinating via `time.sleep` and exit codes.

| # | Scenario | Expected |
|---|---|---|
| 1 | Two processes open `read_only=True` simultaneously | Both succeed; no contention |
| 2 | Process A holds write for 1 s; Process B opens write immediately | B retries and succeeds after A exits |
| 3 | Process A holds write for 10 s (sleep); Process B opens write with `max_wait=2.0` | B exits with `DatabaseLockError` after ~2 s |
| 4 | `read_only=True` on non-existent DB path | `DatabaseNotInitializedError` raised before connect |
| 5 | MCP timeout simulation: thread holds write; `interrupt_and_reset_database()` called from another thread | `_active_write_conn.interrupt_and_reset()` fires; subsequent write open succeeds within `max_wait` |

Scenarios 1–4 are fully deterministic. Scenario 5 is timing-sensitive; use a threading event to coordinate rather than sleep.

## Dependencies

No new packages. All changes use stdlib (`threading`, `time`) and existing dependencies (`duckdb`, `sqlmesh`).

## Writer identity and `db ps`

Writers do not need to publish identity explicitly. `moneybin db ps` (already implemented in `src/moneybin/cli/commands/db.py`) uses `lsof -F pcn <db_path>` + `ps -p <pid> -o args=` to find all processes with the database file open and display their command lines. This gives sufficient identity for the meaningful contention cases:

- `moneybin transform apply` — the only long-duration write-lock holder; fully identifiable by argv
- `moneybin mcp serve` — short-lived per-tool-call writes in the new model; rarely visible during contention
- `moneybin import inbox sync` — batch duration (1–3 s); argv identifies it

The `DatabaseLockError` message should include a hint: `"Another moneybin process may be writing. Check with 'moneybin db ps'."` No lockfile, no `setproctitle`, no new dependencies.

The one gap — knowing which *tool* within an MCP session holds the lock — is not worth addressing. Sub-second write ops are gone before `db ps` could observe them; only `transform apply` holds the lock long enough to inspect.

## Out of Scope

- IPC socket server (named in ADR-010 as the upgrade path if retry-based contention becomes unacceptable at higher write rates).
- ATTACH journal pattern for background writers.
- Connection pooling within a single process.
- `transform apply` progress notifications to unblock other writers mid-run.
- Lockfile-based writer identity visible during retry — `moneybin db ps` is the inspection path.
