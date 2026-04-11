# Feature: Database Migration System

## Status
<!-- draft | ready | in-progress | implemented -->
draft

## Goal
Provide a versioned migration system for MoneyBin's DuckDB database that handles both
moneybin schema changes (app/raw tables) and third-party library state upgrades (e.g.
SQLMesh state tables), so that database upgrades are automated, repeatable, and safe.

## Background
- SQLMesh stores its own state in the DuckDB file and requires `sqlmesh migrate` when
  the SQLMesh package version changes. Today this fails silently at runtime
  (see `sqlmesh/config.py` and `categorization_service.py:ensure_seed_table`).
- MoneyBin's own `app.*` / `raw.*` schema DDL is applied idempotently at startup via
  `init_schemas`, but there is no mechanism for destructive or additive changes after
  initial creation (e.g. renaming a column, adding a NOT NULL constraint, backfilling data).
- Without a migration layer, users who upgrade the package against an existing database
  can encounter silent data corruption or hard crashes.

## Requirements

1. Migrations are versioned with a monotonic integer (e.g. `0001`, `0002`) and stored
   under `src/moneybin/sql/migrations/`.
2. A `schema_versions` table in the `app` schema tracks applied migration IDs, applied
   timestamps, and the moneybin package version at time of application.
3. Migrations run automatically at app startup (before `init_schemas` logic) if unapplied
   versions exist; no manual `migrate` command required for normal upgrades.
4. A `moneybin db migrate` CLI command allows running migrations explicitly and supports
   `--dry-run` to preview SQL without executing.
5. SQLMesh state upgrades (`sqlmesh migrate`) are detected and invoked automatically when
   the installed SQLMesh version differs from the version recorded in the database; this
   replaces the current crash-at-runtime behavior.
6. Migrations are transactional where DuckDB supports it; a failed migration rolls back
   and surfaces a clear error rather than leaving the database in a partial state.
7. Downgrade / rollback is out of scope for v1 — migrations are forward-only.

## Data Model

```sql
/* Tracks applied moneybin schema migrations */
CREATE TABLE IF NOT EXISTS app.schema_versions (
    migration_id    VARCHAR NOT NULL,    -- Zero-padded integer, e.g. '0001'
    description     VARCHAR NOT NULL,    -- Human-readable summary of the migration
    applied_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    moneybin_version VARCHAR,            -- moneybin package version at apply time
    PRIMARY KEY (migration_id)
);
```

## Implementation Plan

### Files to Create
- `src/moneybin/sql/migrations/` — directory for numbered `.sql` migration files
- `src/moneybin/db/migrations.py` — `MigrationRunner` class: discover, filter, apply
- `src/moneybin/cli/db.py` (or extend existing) — `moneybin db migrate` command

### Files to Modify
- `src/moneybin/db/schema.py` — call `MigrationRunner` before `init_schemas` at startup
- `sqlmesh/config.py` — replace crash-on-version-mismatch with auto `sqlmesh migrate`
- `src/moneybin/config.py` — expose installed SQLMesh version for version-check logic

### Key Decisions
- **Discovery**: migrations are plain `.sql` files named `NNNN_description.sql`; no Python
  migration objects needed for v1.
- **SQLMesh version tracking**: store last-migrated SQLMesh version in a separate
  `app.library_versions` table (or a dedicated row in `schema_versions` with a reserved
  prefix like `sqlmesh_*`).
- **Startup behavior**: fail fast with a clear message if migration fails rather than
  continuing with a potentially corrupt state.

## CLI Interface

```
moneybin db migrate [--dry-run] [--profile PROFILE]
```

- `--dry-run`: print SQL that would be executed, then exit 0
- Output: one line per migration applied, e.g. `✅ Applied 0003_add_budget_table`

## MCP Interface
N/A — migrations are an infrastructure concern, not exposed via MCP tools.

## Testing Strategy
- Unit: `MigrationRunner` with an in-memory DuckDB; verify ordered application, idempotency
  on re-run, and correct version tracking.
- Integration: apply a sequence of migrations against a temp DB; verify final schema state.
- Regression: test that a version mismatch between installed SQLMesh and recorded DB version
  triggers auto-migration rather than a crash.

## Dependencies
- DuckDB (already a dependency)
- SQLMesh (already a dependency) — `sqlmesh migrate` CLI or Python API

## Out of Scope
- Downgrade / rollback
- Multi-user / concurrent migration locking (single-user local DB)
- Migration generation tooling (migrations are written by hand for v1)
