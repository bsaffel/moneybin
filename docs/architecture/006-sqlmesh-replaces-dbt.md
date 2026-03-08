# ADR-006: SQLMesh Replaces dbt as Transform Engine

**Status**: accepted

## Context

MoneyBin had a dual-track transform system that was drifting apart:

- **Runtime path**: Inline SQL in `src/moneybin/sql/transforms/` ran on every import via `import_service.py`
- **Unused path**: dbt models in `dbt/models/` duplicated the same logic but were never invoked programmatically

The app needs first-class SQL management (column-level lineage, smart rebuilds, audits) without requiring users to run separate commands. dbt's subprocess model and heavy dependency footprint are a poor fit for an embedded personal finance app.

## Decision

Replace dbt with SQLMesh as the transform engine:

- SQLMesh project lives in `sqlmesh/` (`sqlmesh/config.yaml`, `sqlmesh/models/`)
- Transforms run automatically on import via the Python API (`Context.plan(auto_apply=True)`)
- Prep views are added for the full medallion pipeline (`sqlmesh/models/prep/` maps to the `prep` schema)
- CLI commands (`moneybin transform plan/apply/ui`) wrap SQLMesh for development and exploration

## Alternatives Considered

1. **Keep inline SQL only** -- Simple but no lineage, audits, or state tracking
2. **Keep dbt for docs only** -- Accepts duplication between inline SQL and dbt models
3. **Invoke dbt programmatically** -- Subprocess overhead, connection coordination issues, heavy dependency tree

## Consequences

- Single source of truth for transforms (no more drift between inline SQL and dbt models)
- Column-level lineage via `sqlmesh ui`
- Smart rebuilds (only changed models are re-executed)
- Built-in audits and SQL validation
- SQLMesh state tables are added to the DuckDB database
- New dependency: `sqlmesh[duckdb]` (replaces `dbt-core` + `dbt-duckdb`)
- `dbt_updated_at` and `dbt_loaded_at` columns renamed to `updated_at` and `loaded_at`
