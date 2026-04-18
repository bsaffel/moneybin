# Feature: Database Migration System

## Status
<!-- draft | ready | in-progress | implemented -->
ready

## Goal
Provide a versioned migration system for MoneyBin's DuckDB database that handles
moneybin schema changes (`app.*`/`raw.*` tables) and detects third-party library
state drift (SQLMesh), so that database upgrades are automated, repeatable, and safe.

## Background
- SQLMesh stores its own state in the DuckDB file and requires `sqlmesh migrate` when
  the SQLMesh package version changes. Today this can fail silently at runtime
  (see `categorization_service.py:ensure_seed_table`).
- MoneyBin's own `app.*` / `raw.*` schema DDL is applied idempotently at startup via
  `init_schemas`, but there is no mechanism for destructive or additive changes after
  initial creation (e.g. renaming a column, adding a NOT NULL constraint, backfilling data).
- Without a migration layer, users who upgrade the package against an existing database
  can encounter silent data corruption or hard crashes.

## Requirements

### MoneyBin Schema Migrations
1. Migrations are versioned with a monotonic integer and stored as Flyway-style
   `V<NNN>__<description>.sql` files under `src/moneybin/sql/migrations/`.
2. An `app.schema_migrations` table tracks applied migration versions, filenames,
   SHA-256 checksums, and timestamps.
3. `moneybin db init` auto-applies pending migrations after baseline schema creation;
   no manual step required for normal upgrades.
4. A `moneybin data migrate` CLI group provides explicit control:
   - `apply [--dry-run]` — apply pending migrations or preview them
   - `status` — show applied, pending, and drifted migrations
5. Migrations are transactional: each file is wrapped in `BEGIN`/`COMMIT`. A failed
   migration issues `ROLLBACK` and surfaces a clear error; later migrations are not
   attempted.
6. Checksum drift (applied migration file edited after apply) produces a warning, not
   a failure. Rationale: year-1 tooling for a small user base; hard-fail on drift is
   high-friction and not yet needed.
7. Downgrade / rollback is out of scope for v1 — migrations are forward-only.

### SQLMesh State Detection (Future — v2)
8. On `db init` and `data migrate apply`, detect when the installed SQLMesh version
   differs from the version last recorded in the database.
9. When a mismatch is detected in interactive CLI mode, prompt the user:
   `SQLMesh upgraded (0.230.0 → 0.233.0). Run sqlmesh migrate now? [Y/n]`.
   In non-interactive mode (CI, MCP), log a warning with instructions.
10. Track the last-migrated SQLMesh version in `app.schema_migrations` using a
    reserved version (e.g. version 0) or a dedicated `app.library_versions` table.

## Data Model

```sql
/* Schema migration history; one record per applied migration file */
CREATE TABLE IF NOT EXISTS app.schema_migrations (
    version INTEGER PRIMARY KEY, -- Monotonic integer parsed from the migration filename prefix (V### → integer)
    filename VARCHAR NOT NULL, -- Full migration filename including the V### prefix and .sql suffix
    checksum VARCHAR NOT NULL, -- Lowercase hex SHA-256 of the migration file contents at apply time
    applied_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP -- Timestamp when this migration was applied to the current database
);
```

## Implementation Plan

### Files to Create
- `src/moneybin/sql/migrations/` — directory for `V###__description.sql` files
- `src/moneybin/sql/migrations/README.md` — authoring conventions
- `src/moneybin/sql/schema/app_schema_migrations.sql` — tracking table DDL
- `src/moneybin/migrations.py` — `Migration` dataclass, `MigrationRunner` class
- `src/moneybin/cli/commands/migrate.py` — `moneybin data migrate` commands
- `tests/moneybin/test_migrations.py` — unit tests for MigrationRunner
- `tests/moneybin/test_cli/test_migrate_command.py` — CLI tests
- `docs/reference/migrations.md` — user-facing authoring guide

### Files to Modify
- `src/moneybin/schema.py` — add `app_schema_migrations.sql` to `_SCHEMA_FILES`
- `src/moneybin/cli/commands/db.py` — `db init` auto-runs `MigrationRunner.apply_all()`
- `src/moneybin/cli/commands/data.py` — register `migrate` subcommand group

### Key Decisions
- **Discovery**: migrations are plain `.sql` files named `V<NNN>__<snake_case>.sql`
  (Flyway naming convention, 3+ digit version, double underscore separator).
- **Scope**: migrations only touch `raw.*` and `app.*` schemas. `core.*` is SQLMesh
  territory — never write migrations that alter `core.*`.
- **Checksums**: SHA-256 of the file's raw bytes, stored as lowercase hex. Drift
  warns but does not fail (see requirement 6).
- **Startup behavior**: fail fast with a clear message if a migration fails rather
  than continuing with a potentially corrupt state.
- **Idempotency**: re-running an already-applied migration is a silent no-op (version
  already in tracking table).

## CLI Interface

```
moneybin data migrate apply [--dry-run] [--database PATH]
moneybin data migrate status [--database PATH]
```

- `apply`: apply all pending migrations in version order
- `apply --dry-run`: list pending migrations without executing
- `status`: show applied migrations, pending migrations, and checksum drift warnings
- Output uses standard CLI icons: `⚙️` for working, `✅` for success, `⚠️` for drift

## MCP Interface
N/A — migrations are an infrastructure concern, not exposed via MCP tools.

## Testing Strategy
- Unit: `MigrationRunner` with a temp DuckDB file; verify ordered application,
  idempotency on re-run, correct version/checksum tracking, rollback on error,
  and drift detection.
- CLI: mock `get_database_path` and `_get_migrations_dir`; test apply, dry-run,
  status output, and error exit codes.
- Integration: `db init` auto-applies pending migrations after baseline creation.

## Dependencies
- DuckDB (already a dependency)
- hashlib (stdlib) — SHA-256 checksums

## Out of Scope
- Downgrade / rollback
- Multi-user / concurrent migration locking (single-user local DB)
- Migration generation tooling (migrations are written by hand for v1)
- Python-based migrations (SQL-only for v1; add Python hook later if needed)
- SQLMesh auto-migrate (v2 — requirements 8-10 above)
