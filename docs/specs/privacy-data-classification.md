# Feature: Privacy Data Classification

## Status
in-progress

## Goal
Establish a typed, source-of-truth registry that maps every column in
`core.*` and `app.*` to a `DataClass` (and via that, a privacy `Tier`).
The registry is the foundation that later PRs build redaction
middleware, consent gates, and SQL lineage on top of. Surface the
classification in DuckDB's catalog (comment sigil) and enforce
completeness in CI.

## Background
- `privacy-and-ai-trust.md` — framework spec describing tiers, consent,
  and the redaction engine (revision pending parallel MCP rename work).
- `privacy-data-protection.md` — implemented: encryption at rest, log
  sanitizer.
- `architecture-shared-primitives.md` — names the `core.*` / `app.*`
  layer split this spec classifies.
- `.claude/rules/identifiers.md` — distinguishes content-hash and
  truncated-UUID record IDs (`RECORD_ID`) from source-provided
  account-bound IDs (`ACCOUNT_IDENTIFIER`).
- `.claude/rules/security.md` — sqlglot identifier-quoting requirement
  used by the comment sync.

## Requirements
1. A `DataClass` StrEnum defines every privacy class MoneyBin
   recognizes. Adding a new class is a one-line change.
2. Each `DataClass` member maps to exactly one `Tier`
   (LOW=1, MEDIUM=2, HIGH=3, CRITICAL=4). Tier ordering supports
   `max(tier)` aggregation in future PRs. The framework spec defines
   the four tiers semantically; this spec introduces the numeric
   ordering for downstream aggregation.
3. A `CLASSIFICATION` dict keyed by `(schema, table) -> {column:
   DataClass}` covers every column in `core.*` and `app.*` that exists
   at startup.
4. A `sync_classification_comments(db)` function writes
   `[class: <name>]` as a suffix on the existing comment for every
   classified column. Re-running is idempotent (zero
   `COMMENT ON COLUMN` statements executed when the catalog already
   matches the registry). If a column's entry is removed from
   `CLASSIFICATION`, the next sync strips its sigil and leaves the
   original human comment intact.
5. The sync runs after `init_schemas` (for app/raw DDL comments) and
   after `sqlmesh_context()` (for SQLMesh-managed core comments).
6. A pytest test enumerates `duckdb_columns()` and fails if any
   `core.*` / `app.*` column has no entry in `CLASSIFICATION`. The
   failure message names every missing column.
7. A reverse test fails if `CLASSIFICATION` contains an entry for a
   column or table that no longer exists.

## Data Model
No new tables. The registry lives in Python; the catalog change is a
suffix on existing `COMMENT ON COLUMN` strings.

## Classification Audit

> **Pending:** populated by the audit step of the implementation plan.
> Will list every `_id`-suffixed column in `core.*` and `app.*` (and
> any other column whose class required judgment), with its assigned
> `DataClass` and a one-sentence justification. This is the durable
> reference for "why this class is what it is" arguments later.

## Implementation Plan

The step-by-step task breakdown is tracked separately (ephemeral). The
durable design decisions that flow out of this work and into later PRs:

### Key Decisions

- **Sigil format.** Append ` [class: <DataClass value>]` as a suffix on
  the existing DuckDB column comment. The class value is the lowercase
  snake-case form of the enum member name (e.g., `account_identifier`,
  `record_id`). A trailing-anchor regex strips the sigil before
  reapplication so re-syncing never duplicates the marker.
- **Sync ordering.** Classification sync runs *after* both existing
  comment-writing paths: `schema._apply_comments` (per-startup DDL
  comments for `app.*` and `raw.*`) and SQLMesh's `register_comments`
  (per-run comments for `core.*` models). Human descriptions are the
  prefix; the class sigil is the suffix.
- **Suffix, not replace.** The sync never rewrites the human
  description — it strips any prior sigil and appends the current one.
  Removing a column's entry from the registry restores the original
  comment on the next sync.
- **Source of truth is Python, not the catalog.** The DuckDB sigil is a
  mirror for `DESCRIBE` / DBeaver convenience. Downstream privacy
  controls (redaction, consent gates, lineage) read `CLASSIFICATION`
  directly. The catalog is observable, not authoritative.

## Testing Strategy
- Completeness test: registry covers every live column.
- Reverse test: every registry entry corresponds to a live column.
- Idempotency test: second sync run produces zero updates.
- Description-preservation test: human comment stays as the prefix; sigil
  is the suffix; stripping the registry entry restores the original.

## Dependencies
None new. Uses existing `Database`, `duckdb_columns()`, sqlglot, pytest.

## Out of Scope
- `Annotated[..., DataClass.X]` propagation on service return types (PR 2).
- Redaction engine (`redact_typed`, `redact_polars_frame`) (PR 2).
- `privacy.log` JSONL writer (PR 2).
- `app.ai_consent_grants` schema + `moneybin privacy grant/revoke/status`
  CLI + consent MCP tools (PR 3).
- sqlglot lineage on `sql_query` (PR 4).
- Presidio integration for unstructured-text scrubbing (deferred).
- MCP elicitation fallback when consent is missing (deferred).
- Per-tool consent granularity (schema supports, UX deferred).
- Revisions to `privacy-and-ai-trust.md` (blocked on parallel MCP rename
  work; this PR does not touch the MCP layer).
