# Feature: Database Migration System

## Status
<!-- draft | ready | in-progress | implemented -->
ready

## Goal
Provide a seamless, automatic upgrade experience for MoneyBin's DuckDB databases
so that schema changes across releases are applied transparently — the way modern
desktop and mobile apps handle updates. Users should never need to run manual
migration commands; upgrades happen on first invocation after a package update.

The system handles MoneyBin's own schema changes (`app.*`/`raw.*` tables), detects
third-party library state drift (SQLMesh), and supports periodic rebaselining so
fresh installs never replay a long migration history.

## Background
- SQLMesh stores its own state in the DuckDB file and requires `sqlmesh migrate`
  when the SQLMesh package version changes. Today this can fail silently at runtime.
- MoneyBin's own `app.*` / `raw.*` schema DDL is applied idempotently at startup via
  `init_schemas`, but there is no mechanism for destructive or additive changes after
  initial creation (e.g. renaming a column, adding a NOT NULL constraint, backfilling
  data).
- Without a migration layer, users who upgrade the package against an existing database
  can encounter silent data corruption or hard crashes.
- Investment tracking (Level 2) and multi-currency (Level 3) will introduce breaking
  DDL changes that every existing database must absorb automatically.

## Architecture

### Dual-Path Model (Rails-style)

Fresh installs and upgrades take different paths to the same result:

```
Fresh install (db init):
  init_schemas() → current DDL → full schema created
  MigrationRunner → baseline stamps old versions → new migrations applied
  Version state recorded in app.versions

Upgrade (first invocation after package update):
  Auto-upgrade callback detects version mismatch
  init_schemas() → CREATE IF NOT EXISTS → no-ops on existing tables
  MigrationRunner → pending migrations applied
  sqlmesh migrate → if SQLMesh version changed
  Version state updated in app.versions
```

**Key invariant:** `init_schemas()` always produces a schema identical to "empty DB +
all migrations through the current baseline applied." This is enforced by the
rebaseline process, not by automation.

### Separation of Concerns

- **`init_schemas()`** — owns baseline DDL, knows nothing about migrations.
- **`MigrationRunner`** — owns versioned upgrades. Receives an open connection.
  Encryption-unaware. Lives in the service layer (`src/moneybin/services/`) following
  the existing pattern alongside `CategorizationService`, `ImportService`, etc.
- **`Database` class** (`src/moneybin/database.py`) — owns the full initialization
  sequence: key retrieval → in-memory connect → attach encrypted file → load extensions
  → `init_schemas()` → `MigrationRunner.apply_all()` → SQLMesh version check →
  `sqlmesh migrate` if needed → record version state. Defined in
  [`data-protection.md`](data-protection.md). The `Database` class is the single entry
  point for all database access — CLI, MCP, loaders, and services all use it.
- **Entry points** (CLI, MCP) — call `get_database()` to obtain the initialized
  `Database` instance. The auto-upgrade sequence runs inside `Database.__init__`, not in
  per-entry-point callbacks.

### Auto-Upgrade on First Invocation

The `Database` class ([`data-protection.md`](data-protection.md)) runs the full
initialization sequence — including version check and migrations — every time
`get_database()` is called. This replaces the need for separate per-entry-point
callbacks:

1. Compare installed `moneybin` package version against `app.versions` stored version.
2. If they match, proceed (negligible latency — one SELECT).
3. If they differ, run the full upgrade sequence:
   a. `init_schemas()` — idempotent baseline DDL
   b. `MigrationRunner.apply_all()` — pending schema migrations
   c. `sqlmesh migrate` — if SQLMesh version changed
   d. Record new versions in `app.versions`
4. On success, proceed to the command with a brief console summary.
5. On failure, block the command with an error, log path, and issue tracker link.

The version check and upgrade sequence live in `Database.__init__()`. CLI commands and
MCP server startup both trigger it by calling `get_database()`. Skip with
`MONEYBIN_NO_AUTO_UPGRADE=1` (encryption and schema init still run; only versioned
migrations and SQLMesh migrate are skipped).

**Console output (success):**
```
$ moneybin status
⚙️  MoneyBin upgraded (0.4.0 → 0.5.0). Applying updates...
  ✅ 2 migrations applied, SQLMesh state updated

  [normal command output]
```

**Console output (failure):**
```
$ moneybin status
⚙️  MoneyBin upgraded (0.4.0 → 0.5.0). Applying updates...
  ❌ Migration V048__add_tax_lots failed. Database rolled back.
  💡 See logs/default/moneybin.log for details
  🐛 Report issues at https://github.com/bsaffel/moneybin/issues
```

**Non-interactive mode (CI, MCP):**
- Auto-upgrade runs without prompting.
- Failures exit non-zero with full error in both stderr and log file.
- Disable with `MONEYBIN_NO_AUTO_UPGRADE=1` for environments that want explicit control.

## Requirements

### Schema Migrations
1. Migrations are versioned with a monotonic integer and stored as
   `V<NNN>__<description>.sql` or `V<NNN>__<description>.py` files under
   `src/moneybin/sql/migrations/`.
2. An `app.schema_migrations` table tracks applied migration versions, filenames,
   SHA-256 checksums, success/failure state, execution time, and timestamps.
3. `moneybin db init` auto-applies pending migrations after baseline schema creation;
   no manual step required for normal upgrades.
4. First invocation after a package upgrade auto-applies pending migrations
   transparently via an app-level callback. No separate command needed.
5. A `moneybin data migrate` CLI group provides explicit control for power users:
   - `apply [--dry-run]` — apply pending migrations or preview them
   - `status` — show applied, pending, and drifted migrations
6. Migrations are transactional: each file is wrapped in `BEGIN`/`COMMIT`. A failed
   migration issues `ROLLBACK`, records `success=false` in the tracking table, and
   surfaces a clear error; later migrations are not attempted.
7. Checksum drift (applied migration file edited after apply) produces a warning, not
   a failure. Rationale: year-1 tooling for a small user base; hard-fail on drift is
   high-friction and not yet needed.
8. Downgrade / rollback is out of scope for v1 — migrations are forward-only.
9. Stuck migration detection: if the runner finds a row with `success=false`, it
   surfaces a clear error rather than silently skipping.

### Migration File Formats
10. **SQL migrations** (default, ~80% of cases): plain `.sql` files containing DDL and
    DML. The runner reads and executes the file contents as a single transaction.
11. **Python migrations** (escape hatch): `.py` files that export a single
    `migrate(conn: duckdb.DuckDBPyConnection) -> None` function. The runner imports the
    module and calls `migrate()` within a transaction.
12. Both formats follow the same naming convention: `V<NNN>__<snake_case>.{sql,py}`
    (3+ digit version, double underscore separator).
13. The runner dispatches based on file extension. Both formats are discovered and
    ordered together by version number.

### When to Use SQL vs Python

| Reach for `.sql` when... | Reach for `.py` when... |
|---|---|
| Adding/altering/dropping tables or columns | Backfilling data based on runtime state (settings, config) |
| Creating/dropping indexes | Importing seed data from external files |
| Simple INSERT/UPDATE with literal values | Conditional logic (if column exists, skip) |
| Any DDL that is purely structural | Reading Python package versions or metadata |
| | Any operation that needs a Python library |

### SQLMesh State Detection
14. On every auto-upgrade check and on `data migrate apply`, detect when the installed
    SQLMesh version differs from the version last recorded in `app.versions`.
15. When a mismatch is detected, run `sqlmesh migrate` as part of the unified upgrade
    sequence (after schema migrations, before recording the new version).
16. Track the SQLMesh version in `app.versions` alongside the MoneyBin package version.

### Rebaseline
17. At major level boundaries, old migrations are absorbed into `init_schemas` DDL files
    and deleted from the codebase (preserved in git history).
18. A baseline migration file takes the version number of the last absorbed migration
    and stamps all prior versions as applied on fresh databases.
19. Migrations containing non-idempotent operations (renames, type changes, data
    backfills) require careful handling at rebaseline time. For v1, if a database
    predates the baseline, the runner fails with a clear message and instructions
    rather than attempting automatic recovery.
20. Rebaselining is a manual, deliberate process — not automated or scheduled.

## Data Model

### `app.schema_migrations`

```sql
/* Schema migration history; one row per applied migration file */
CREATE TABLE IF NOT EXISTS app.schema_migrations (
    version INTEGER PRIMARY KEY, -- Monotonic integer parsed from the migration filename prefix (V### → integer)
    filename VARCHAR NOT NULL, -- Full migration filename including the V### prefix and extension
    checksum VARCHAR NOT NULL, -- Lowercase hex SHA-256 of the migration file contents at apply time
    success BOOLEAN NOT NULL DEFAULT TRUE, -- FALSE if the migration failed mid-execution (stuck state)
    execution_ms INTEGER, -- Migration duration in milliseconds
    applied_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP -- Timestamp when this migration was applied
);
```

### `app.versions`

```sql
/* Component version tracking for upgrade detection */
CREATE TABLE IF NOT EXISTS app.versions (
    component VARCHAR PRIMARY KEY, -- Component identifier: 'moneybin', 'sqlmesh', etc.
    version VARCHAR NOT NULL, -- Current version string (semver)
    previous_version VARCHAR, -- Version before the last update (NULL on first install)
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, -- When the version was last changed
    installed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP -- When this component was first recorded
);
```

### `analytics` schema

```sql
CREATE SCHEMA IF NOT EXISTS analytics;
```

Created by `init_schemas` at baseline. Owned by user SQLMesh models. MoneyBin ships
starter analytics models so users see the pattern and can extend it. Never touched by
migrations.

### Protected Schemas

Migrations must not alter these schemas — they are owned by SQLMesh:

- `core.*` — canonical models (dimensions and facts)
- `prep.*` — staging views
- `analytics.*` — user-defined analytics models

Migrations may alter:

- `raw.*` — loader-owned tables
- `app.*` — application state tables

## Implementation Plan

### Files to Create
- `src/moneybin/sql/migrations/` — directory for `V###__description.{sql,py}` files
- `src/moneybin/sql/migrations/README.md` — authoring conventions
- `src/moneybin/sql/schema/app_schema_migrations.sql` — tracking table DDL
- `src/moneybin/sql/schema/app_versions.sql` — version tracking table DDL
- `src/moneybin/sql/schema/analytics_schema.sql` — analytics schema DDL
- `src/moneybin/migrations.py` — `Migration` dataclass, `MigrationRunner` class
- `src/moneybin/cli/commands/migrate.py` — `moneybin data migrate` commands
- `tests/moneybin/test_migrations.py` — unit tests for MigrationRunner
- `tests/moneybin/test_cli/test_migrate_command.py` — CLI tests
- `docs/reference/migrations.md` — user-facing authoring guide

### Files to Modify
- `src/moneybin/schema.py` — add new schema files to `_SCHEMA_FILES`, add `analytics` schema
- `src/moneybin/cli/commands/db.py` — `db init` delegates to `Database` class
  ([`data-protection.md`](data-protection.md)) which orchestrates init_schemas +
  migrations + version recording
- `src/moneybin/cli/commands/data.py` — register `migrate` subcommand group
- `src/moneybin/cli/main.py` — no app-level callback needed; auto-upgrade runs inside
  `Database.__init__()` when any command calls `get_database()`

### Key Decisions
- **Architecture**: Rails dual-path — `init_schemas` for fresh installs, migrations for
  upgrades. `init_schemas` always reflects the current baseline schema state.
- **File format**: SQL-default with Python escape hatch. Follows data engineering
  conventions (Flyway, dbt, SQLMesh). SQL for ~80% of migrations; Python when
  conditional logic, runtime values, or library access is needed.
- **Discovery**: migrations are `.sql` or `.py` files named `V<NNN>__<snake_case>.ext`
  (Flyway naming convention, 3+ digit version, double underscore separator).
- **Scope**: migrations only touch `raw.*` and `app.*` schemas. `core.*`, `prep.*`, and
  `analytics.*` are SQLMesh territory — never write migrations that alter them.
- **Checksums**: SHA-256 of the file's raw bytes, stored as lowercase hex. Drift warns
  but does not fail.
- **Transactions**: each migration file is wrapped in `BEGIN TRANSACTION` / `COMMIT`.
  On error, `ROLLBACK` and record `success=false`.
- **Startup behavior**: first invocation after upgrade auto-applies all pending updates
  (schema migrations + SQLMesh migrate) transparently.
- **Idempotency**: re-running an already-applied migration is a silent no-op (version
  already in tracking table).
- **Rebaseline**: at level boundaries, absorb old migrations into baseline DDL. Baseline
  migration takes the version of the last absorbed migration. Old files deleted from
  codebase, preserved in git history.
- **Encryption**: migration runner is encryption-unaware. Receives an open connection
  from `Database.__init__()`. Connection management, key retrieval, and encrypted
  attachment are owned by the `Database` class
  ([`data-protection.md`](data-protection.md)).
- **Orchestration**: the auto-upgrade sequence (init_schemas → migrations → sqlmesh
  migrate → version recording) is orchestrated by `Database.__init__()`, not by
  per-entry-point callbacks. Entry points call `get_database()`.
- **Service layer**: `MigrationRunner` follows the existing service pattern in
  `src/moneybin/services/`, consumed by `Database.__init__()` during initialization.

## CLI Interface

### Automatic (default experience)

Every CLI/MCP invocation checks for pending upgrades via an app-level callback.
Upgrades apply transparently with a brief console summary. Disable with
`MONEYBIN_NO_AUTO_UPGRADE=1`.

### Explicit commands (power users / troubleshooting)

```
moneybin data migrate apply [--dry-run]
moneybin data migrate status
```

- `apply`: apply all pending migrations in version order.
- `apply --dry-run`: list pending migrations without executing.
- `status`: show applied migrations, pending migrations, checksum drift warnings, and
  SQLMesh version state.
- Output uses standard CLI icons: `⚙️` for working, `✅` for success, `⚠️` for drift,
  `🐛` for issue reporting.

### `moneybin db init`

Creates baseline schema, applies pending migrations, records version state. The
explicit "set up from scratch" command. Safe to re-run (idempotent).

## MCP Interface
N/A — migrations are an infrastructure concern, not exposed via MCP tools.

## Logging

- **Console**: terse summary (1-2 lines success, 3-4 lines on failure).
- **Log file**: every step with timestamps, migration names, execution times. Follows
  existing profile-based convention (`logs/<profile>/moneybin.log`).
- Failed upgrades block command execution and surface the log path.

## Testing Strategy

### Unit: MigrationRunner
- Discovery: finds `.sql` and `.py` files, parses version/name, sorts by version,
  ignores non-migration files, rejects malformed names, rejects duplicate versions.
- Checksums: deterministic, differs for different content, lowercase hex, 64 chars.
- State: `applied_versions()` reads tracking table, `pending()` excludes applied.
- apply_one (SQL): executes DDL, records tracking row, idempotent on re-run, rolls
  back on error and sets `success=false`.
- apply_one (Python): imports module, calls `migrate(conn)`, same tracking/rollback.
- apply_all: runs pending in order, skips applied, stops on first failure.
- Drift: detects file changes, detects missing files, ignores unapplied.
- Stuck migration: detects `success=false` rows, surfaces clear error.
- Version check: detects moneybin/SQLMesh version changes, no-ops when matching.

### CLI: migrate commands
- `apply` runs pending, exits 0. `--dry-run` lists without modifying. Bad SQL exits 1.
- `status` shows applied, pending, drift warnings.

### Auto-upgrade callback
- First invocation after upgrade: detects mismatch, runs migrations, updates versions.
- No upgrade: version matches, no migration logic runs.
- `MONEYBIN_NO_AUTO_UPGRADE=1`: callback skipped.
- Failed auto-upgrade: blocks command, exits 1, logs error path.

### Integration (manual smoke test)
- `db init` → create migration → `apply --dry-run` → `apply` → `status` → verify.

## Dependencies
- DuckDB (already a dependency)
- hashlib (stdlib) — SHA-256 checksums
- importlib.metadata (stdlib) — package version detection

## Out of Scope
- **Rollback / down migrations** — forward-only; write a corrective migration instead.
- **Auto-squash tooling** — manual rebaseline at landmarks is predictable.
- **Concurrent migration locking** — DuckDB is single-writer; one process gets a lock error.
- **Migration generation** — hand-authored SQL/Python; no auto-generation from schema diffs.
- **Python-only migration format** — SQL-default with Python escape hatch; data
  engineering conventions win.
- **Encryption handling** — runner receives an open connection; encryption is
  `data-protection.md`'s responsibility.
- **`sqlmesh plan` execution** — different concern ("is your data up to date" vs "is
  your schema correct"). May be added to the auto-upgrade sequence later if users
  consistently forget.
- **MCP exposure** — infrastructure concern, not user-facing financial operations.

## Success Criteria
- `moneybin db init` on a fresh database creates all schemas and applies any pending
  migrations in a single command.
- Upgrading the MoneyBin package and running any CLI command automatically applies
  pending schema migrations and SQLMesh state updates with zero manual intervention.
- A user who goes dormant and upgrades across a rebaseline boundary gets a clear error
  message with recovery instructions, not a corrupted database.
- `moneybin data migrate status` provides full visibility into migration state for
  troubleshooting.
- All migration operations are logged with enough detail to diagnose failures from
  the log file alone.
