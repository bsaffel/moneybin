# Same-Record Dedup & Golden-Record Merge Rules

> Last updated: 2026-04-18
> Status: Draft
> Parent: [`transaction-matching.md`](transaction-matching.md) (pillars A + C)
> Companions: `CLAUDE.md` "Architecture: Data Layers", `.claude/rules/database.md` (column naming, model prefixes)

## Goal

When multiple raw rows from different sources describe the same real-world transaction, resolve them to a single gold record in `core.fct_transactions` with a deterministic synthetic key, full provenance, and per-field merge rules driven by a configurable source-priority ranking. Every merge is logged, auditable, and reversible.

## Background

Today `core.fct_transactions` is a `UNION ALL` of OFX and CSV staging CTEs with no cross-source dedup. If the same transaction appears in both an OFX download and a CSV export, it counts twice. This spec fixes that by introducing a matching engine and a layered prep pipeline that produces deduplicated, merged gold records.

This spec covers pillars A (same-record dedup) and C (golden-record merge rules) from the [transaction-matching umbrella](transaction-matching.md). They ship together because dedup without merge rules leaves the gold record undefined, and merge rules without dedup have nothing to merge.

### Relevant prior art

- [transaction-matching.md](transaction-matching.md) — umbrella vision, scope, build order
- [fct_transactions.sql](../../sqlmesh/models/core/fct_transactions.sql) — current core model (VIEW, no dedup)
- [stg_csv__transactions.sql](../../sqlmesh/models/prep/stg_csv__transactions.sql) — CSV staging with within-source dedup
- [stg_ofx__transactions.sql](../../sqlmesh/models/prep/stg_ofx__transactions.sql) — OFX staging without within-source dedup (gap)

## Requirements

### Dedup

1. Transactions from different sources that describe the same real-world event resolve to one gold record in `core.fct_transactions`.
2. The matcher never matches within the same `source_system`. Two rows from OFX with different `source_transaction_id` values are always treated as distinct transactions.
3. Each source row participates in at most one match (1:1 bipartite assignment).
4. Matches are scored with a confidence value. Three tiers determine behavior:
   - `>= high_confidence_threshold` (default 0.95): auto-merge, logged, reversible
   - `>= review_threshold` (default 0.70): queued for user review
   - `< review_threshold`: dropped (logged for debugging, not surfaced)
5. Thresholds are configurable via Pydantic settings.
6. All match decisions (auto and user) are persisted in `app.match_decisions` with confidence score, signals used, decider, and timestamp.
7. Any auto-merge can be reversed. After reversal, re-running the matcher re-proposes (does not re-apply) the same pair.
8. Rejected pairs are not re-proposed by the matcher.

### Merge rules

9. When source rows merge, per-field values are selected by a single source-priority ranking (configurable, default: `plaid > csv > ofx`).
10. For each field, the value from the highest-priority source with a non-NULL value wins.
11. `transaction_date` is an exception: earliest non-NULL posted date across sources (most accurate settlement date).
12. The source-priority list must include every supported `source_system`. Adding a new source type requires inserting it into this list.

### Identity

13. Gold records are keyed by `transaction_id` — a deterministic UUID v5 derived from the contributing source rows.
14. Source-native IDs are carried as `source_transaction_id`, with the same logical meaning everywhere the column appears (raw, prep, provenance).
15. Unmatched records get a `transaction_id` derived from `(source_system, source_transaction_id, account_id)` — stable across SQLMesh reruns.
16. Matched groups get a `transaction_id` derived from the sorted set of contributing `(source_system, source_transaction_id, account_id)` tuples — also stable.

### Provenance

17. `meta.fct_transaction_provenance` links every gold record to every contributing source row.
18. Provenance rows are never deleted, only superseded (a reversal appends a new row).

## Three-Tier Dedup Taxonomy

Each tier has a strictly simpler matching problem than the next:

| Tier | Scope | Method | Owner |
|---|---|---|---|
| 1. Same-file re-import | Same source, same file | Raw table PKs prevent duplicate inserts | `raw.*` INSERT logic |
| 2. Overlapping statements | Same source, different files, same source-native ID | `ROW_NUMBER()` on `(source_transaction_id, account_id)`, latest load wins | `prep.stg_*` views |
| 3. Cross-source | Different `source_system` values | Python matcher with confidence scoring, 1:1 assignment | Python → `app.match_decisions` |

Tiers 1 and 2 are deterministic SQL prerequisites. Tier 3 is the fuzzy matching engine this spec primarily designs.

### Tier 2 gap

OFX staging (`stg_ofx__transactions`) currently has no within-source dedup. This spec adds `ROW_NUMBER() OVER (PARTITION BY source_transaction_id, account_id ORDER BY loaded_at DESC)` to match what CSV staging already does.

## Data Model

### Renamed columns (migration)

Raw tables rename `transaction_id` → `source_transaction_id` to free up `transaction_id` for the gold key. This follows the column naming consistency rule: a column name must contain the same logical values everywhere it appears.

### New app tables

```sql
/* Match decisions from the Python matcher and user review; one row per proposed pair */
CREATE TABLE IF NOT EXISTS app.match_decisions (
    match_id VARCHAR NOT NULL,           -- UUID, primary key
    source_transaction_id_a VARCHAR NOT NULL, -- Source-native ID of first row
    source_system_a VARCHAR NOT NULL,    -- source_system of first row
    source_transaction_id_b VARCHAR NOT NULL, -- Source-native ID of second row
    source_system_b VARCHAR NOT NULL,    -- source_system of second row
    account_id VARCHAR NOT NULL,         -- Shared account (blocking requirement)
    confidence_score DECIMAL(5, 4),      -- 0.0000 to 1.0000
    match_signals JSON,                  -- Per-signal scores: {"date_distance": 0, "description_similarity": 0.87}
    match_status VARCHAR NOT NULL,       -- pending, accepted, rejected
    match_reason VARCHAR,                -- Human-readable explanation of why this match was proposed
    decided_by VARCHAR NOT NULL,         -- auto, user, system
    decided_at TIMESTAMP NOT NULL,       -- When the decision was made
    reversed_at TIMESTAMP,              -- When the match was undone; NULL if active
    reversed_by VARCHAR,                -- Who reversed: user or system; NULL if active
    PRIMARY KEY (match_id)
);

/* Source-priority ranking for golden-record merge rules; lower priority value = higher precedence */
CREATE TABLE IF NOT EXISTS app.source_priority (
    source_system VARCHAR NOT NULL,      -- e.g. plaid, csv, ofx
    priority INTEGER NOT NULL,           -- Lower = higher precedence (1 = best)
    PRIMARY KEY (source_system)
);
```

### New meta model

`meta.fct_transaction_provenance` (SQLMesh model):

| Column | Type | Description |
|---|---|---|
| `transaction_id` | VARCHAR | FK to gold record in `core.fct_transactions` |
| `source_transaction_id` | VARCHAR | Source-native ID, joinable to raw/prep |
| `source_system` | VARCHAR | Origin system |
| `source_file` | VARCHAR | File that produced this source row |
| `source_extracted_at` | TIMESTAMP | When the source row was parsed |
| `match_id` | VARCHAR | FK to `app.match_decisions`; NULL for unmatched records |

### New and modified prep models

**`prep.stg_ofx__transactions`** (modified) — add tier 2 dedup via `ROW_NUMBER()`.

**`prep.int_transactions__unioned`** (new) — `UNION ALL` of all `stg_*__transactions` with standardized column names and types. Replaces the CTEs in today's `fct_transactions`. Each row carries `source_transaction_id` and `source_system`.

**`prep.int_transactions__matched`** (new) — joins `int_transactions__unioned` with `app.match_decisions` to assign `transaction_id` (gold key). Unmatched rows get a deterministic UUID from `(source_system, source_transaction_id, account_id)`. Matched groups get a deterministic UUID from the sorted contributing tuple set. Output has both `transaction_id` and `source_transaction_id`.

**`prep.int_transactions__merged`** (new) — collapses matched groups to one row per `transaction_id`. Applies source-priority merge: for each field, COALESCE in priority order across the group's source rows. Exception: `transaction_date` takes the earliest non-NULL value. Adds `canonical_source_system` and `source_count`.

### Modified core model

**`core.fct_transactions`** (modified) — grain changes to `transaction_id` (gold key). Becomes a thin enrichment layer over `int_transactions__merged`:
- JOINs `app.transaction_categories` and `app.merchants` (same as today)
- Adds derived columns: `transaction_direction`, `amount_absolute`, date-part extractions (same as today)
- New columns from merged layer: `canonical_source_system`, `source_count`, `match_confidence`

### App table FK migration

`app.transaction_categories` and `app.transaction_notes` rename their FK from `transaction_id` (source-native) to `transaction_id` (gold key). Values are backfilled as deterministic UUID v5 from each row's `(source_system, source_transaction_id, account_id)` — a 1:1 mapping since no merges exist yet.

## Matching Engine

### Architecture

Python orchestrator, DuckDB compute engine. The matcher sends SQL queries to DuckDB for data operations and handles control flow + assignment logic in Python.

### Control flow

```
1. Python loader         → writes to raw.*
2. Python matcher        → reads prep.stg_* views (always current — views over raw)
                         → blocking query (DuckDB SQL)
                         → scoring query (DuckDB SQL)
                         → 1:1 assignment (Python, small candidate set)
                         → writes to app.match_decisions
3. sqlmesh run           → int_*__unioned → __matched → __merged
                         → core.fct_transactions
                         → meta.fct_transaction_provenance
```

### Candidate blocking

SQL query against prep views. Returns pairs where:
- `source_system` differs
- `account_id` matches exactly
- `amount` matches exactly (to the penny, same-currency only)
- `transaction_date` within `±date_window_days` (default 3)

This produces a narrow candidate set. No fuzzy logic at this stage.

### Scoring

For each candidate pair, DuckDB computes:
- **Date distance:** days between the two `transaction_date` values. Closer = higher score component.
- **Description similarity:** `jaro_winkler_similarity(description_a, description_b)` via DuckDB. Used as a confidence modifier, not a gate.

The matcher combines these into a single confidence score. Exact weighting is an implementation detail — the spec requires that the signals and their individual scores are persisted in `match_signals` JSON for auditability.

### 1:1 assignment

When multiple candidates compete (e.g., two CSV rows could match the same OFX row), the matcher uses greedy best-score-first assignment:
1. Sort all scored pairs by confidence descending
2. Iterate: if neither row in the pair is already claimed, accept the pair, mark both rows as claimed
3. Remaining unclaimed candidates are discarded

### Match decision persistence

Every scored pair above `review_threshold` is written to `app.match_decisions`:
- `>= high_confidence_threshold`: `match_status = 'accepted'`, `decided_by = 'auto'`
- `>= review_threshold`: `match_status = 'pending'`, `decided_by = 'auto'`

Pairs below `review_threshold` are not persisted (logged at DEBUG level only).

## Golden-Record Merge Rules

### Source-priority ranking

A single ordered list stored in `app.source_priority`. Default: `plaid (1) > csv (2) > ofx (3)`. Extended as sources ship. The list must include every supported `source_system` value.

### Field selection

In `int_transactions__merged`, matched groups collapse to one row per `transaction_id`:

| Field | Rule |
|---|---|
| `transaction_id` | Deterministic UUID v5 from sorted contributing tuples |
| `account_id` | Identical across group (blocking requirement) |
| `transaction_date` | Earliest non-NULL posted date across sources |
| `authorized_date` | Highest-priority non-NULL value |
| `amount` | Identical across group (blocking requirement); take from highest-priority source |
| `description` | Highest-priority non-NULL value |
| `merchant_name` | Highest-priority non-NULL value |
| `memo` | Highest-priority non-NULL value |
| `category` / `subcategory` | Not merged here — owned by `app.transaction_categories`, applied in core JOIN |
| `payment_channel` | Highest-priority non-NULL value |
| `transaction_type` | Highest-priority non-NULL value |
| `check_number` | Any non-NULL value |
| `location_*` fields | Highest-priority non-NULL value |
| `currency_code` | Identical across group (same-currency matching only) |
| `canonical_source_system` | The highest-priority source present in the group |
| `source_count` | Count of contributing source rows |
| `match_confidence` | Confidence score from `app.match_decisions`; NULL for unmatched records |

### Unmatched records

Pass through `int_transactions__merged` unchanged. They get a deterministic `transaction_id`, `source_count = 1`, `canonical_source_system` = their own `source_system`, and `match_confidence = NULL`.

## CLI Interface

### Import integration

Standard import commands gain matching output:
```
✅ Imported 142 transactions from chase_checking_2026-03.csv
⚙️  Matching: 8 auto-merged, 3 pending review
👀 Run 'moneybin matches review' when ready
```

### Match commands

| Command | Description |
|---|---|
| `moneybin matches run` | Run matcher + SQLMesh without importing. For re-running after config changes or reviews. |
| `moneybin matches review` | Interactive review of pending matches. `[a]ccept / [r]eject / [s]kip / [q]uit` |
| `moneybin matches log` | Show recent match decisions (auto and user) |
| `moneybin matches undo <match_id>` | Reverse a match decision. Sets `reversed_at`/`reversed_by`. |
| `moneybin matches backfill` | One-time scan of all existing transactions for latent duplicates |

### Backfill

`moneybin matches backfill` runs the matcher against all existing prep rows. Same scoring and confidence tiers as import-time matching. Idempotent: already-decided pairs are skipped.

```
⚙️  Scanning 4,230 existing transactions for duplicates...
✅ Backfill complete: 47 auto-merged, 12 pending review
👀 Run 'moneybin matches review' when ready
```

## MCP Interface (Phase 2)

Not part of the initial build. Planned tools from the umbrella spec:

| Tool | Type | Description |
|---|---|---|
| `list_pending_matches` | Read | Show pending match proposals with confidence scores |
| `confirm_match` | Write | Accept a pending match |
| `reject_match` | Write | Reject a pending match |
| `undo_match` | Write | Reverse a previously accepted match |
| `get_match_log` | Read | Recent match decisions |

## Configuration

```python
class MatchingSettings(BaseModel):
    high_confidence_threshold: float = 0.95
    review_threshold: float = 0.70
    date_window_days: int = 3
    source_priority: list[str] = ["plaid", "csv", "ofx"]
    # Extended as sources ship. Must include every supported source_system.
```

Env var overrides follow the `MONEYBIN_` convention:
- `MONEYBIN_MATCHING__HIGH_CONFIDENCE_THRESHOLD=0.90`
- `MONEYBIN_MATCHING__DATE_WINDOW_DAYS=5`
- `MONEYBIN_MATCHING__SOURCE_PRIORITY='["csv", "plaid", "ofx"]'`

### What is not configurable

| Invariant | Rationale |
|---|---|
| Cross-source only (never match within same source) | Same-source rows with different IDs are genuinely different transactions |
| Exact account match as blocking requirement | Transactions can't match across accounts |
| Exact amount match as blocking requirement | Amount is the strongest identity signal |
| 1:1 assignment | Each source row describes at most one real-world transaction |

## Testing Strategy

### Unit tests

- **Scoring function**: given two transaction records, verify confidence score calculation with known inputs/outputs
- **1:1 assignment**: given a set of scored pairs with conflicts, verify greedy assignment picks optimal non-overlapping set
- **UUID v5 generation**: verify deterministic — same inputs always produce same `transaction_id`; verify stability — sorted tuple order doesn't depend on insertion order
- **Merge rules**: given N source rows with known priorities, verify field selection per the COALESCE policy
- **Date exception**: verify `transaction_date` picks earliest, not highest-priority

### Integration tests

- **End-to-end import**: load OFX file, load CSV file with overlapping transactions, run matcher, run SQLMesh, verify `fct_transactions` has deduplicated gold records
- **Tier 2 dedup**: load two overlapping OFX statements, verify prep produces one row per `source_transaction_id`
- **Review flow**: create a pending match, accept via test harness, verify gold record appears after SQLMesh run
- **Reversal**: accept a match, undo it, verify separate gold records are restored
- **Rejection persistence**: reject a pair, re-run matcher, verify pair is not re-proposed
- **Backfill idempotency**: run backfill twice, verify no duplicate match decisions

### What to mock

- No mocking of DuckDB — tests use real DuckDB instances with fixture data
- Mock external file I/O for loader tests (already covered by existing test patterns)

## Dependencies

- DuckDB `jaro_winkler_similarity()` function (available since DuckDB 0.8.0)
- UUID v5 generation (Python stdlib `uuid.uuid5`)
- Existing prep staging views and raw schema
- Database migration system ([database-migration.md](database-migration.md)) for the `transaction_id` → `source_transaction_id` rename and app table FK changes

## Out of Scope

- **Cross-currency matching** — deferred to multi-currency initiative. This spec requires exact amount match in same currency.
- **Transfer detection** — pillar B, separate spec (`transfer-detection.md`). Different semantics: links two records rather than collapsing them.
- **Per-field or per-transaction merge overrides** — v1 uses a single global source-priority ranking. Enhancement spec if needed.
- **ML/learned matching** — v1 uses deterministic scoring. Learned promotions deferred per umbrella spec.
- **Investment transaction dedup** — depends on investment-tracking spec.
- **Account-level dedup** — `dim_accounts` already handles this via `account_id` partitioning.
- **MCP tools** — Phase 2. CLI-only for v1.

## Checklist: Adding a New Data Source

When a new `source_system` is added to MoneyBin, the following must be updated for matching to work correctly:

1. Create staging model in `sqlmesh/models/prep/` with tier 2 dedup (`ROW_NUMBER`)
2. Add a CTE to `int_transactions__unioned` and `UNION ALL` into the combined set
3. Insert the new `source_system` into the default `source_priority` list at the appropriate position
4. Add the new source to matching integration tests (load + match against existing sources)

## Open Questions

Decisions deferred to implementation:

1. **Confidence score formula.** The exact weighting of date distance and description similarity. Spec requires the formula to be auditable (signals persisted in JSON) but doesn't fix the weights — they should be tuned against real data during implementation.
2. **UUID v5 namespace.** Need a stable namespace UUID for the project. Generate once and store as a constant.
3. **`app.source_priority` seeding.** Populate from `MatchingSettings.source_priority` on first run? Or treat the table as the source of truth and settings as the initial seed?
4. **`int_transactions__matched` model kind.** VIEW (always current, recomputes on read) vs TABLE (materialized, faster queries but needs refresh). VIEW is consistent with existing prep models; TABLE may be needed if the join against `app.match_decisions` is expensive at scale.
