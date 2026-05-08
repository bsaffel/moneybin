# Same-Record Dedup & Golden-Record Merge Rules

> Last updated: 2026-04-26
> Status: implemented
> Parent: [`matching-overview.md`](matching-overview.md) (pillars A + C)
> Companions: `CLAUDE.md` "Architecture: Data Layers", `.claude/rules/database.md` (column naming, model prefixes)

## Goal

When multiple raw rows from different sources describe the same real-world transaction, resolve them to a single gold record in `core.fct_transactions` with a deterministic synthetic key, full provenance, and per-field merge rules driven by a configurable source-priority ranking. Every merge is logged, auditable, and reversible.

## Background

Today `core.fct_transactions` is a `UNION ALL` of OFX and CSV staging CTEs with no cross-source dedup. If the same transaction appears in both an OFX download and a CSV export, it counts twice. This spec fixes that by introducing a matching engine and a layered prep pipeline that produces deduplicated, merged gold records.

This spec covers pillars A (same-record dedup) and C (golden-record merge rules) from the [transaction-matching umbrella](matching-overview.md). They ship together because dedup without merge rules leaves the gold record undefined, and merge rules without dedup have nothing to merge.

### Relevant prior art

- [matching-overview.md](matching-overview.md) — umbrella vision, scope, build order
- [fct_transactions.sql](../../sqlmesh/models/core/fct_transactions.sql) — current core model (VIEW, no dedup)
- [smart-import-tabular.md](smart-import-tabular.md) — universal tabular importer that produces `raw.tabular_*` records with `source_transaction_id`, per-format `source_type` values, and `source_origin` (institution/format identifier)
- [stg_tabular__transactions.sql](../../sqlmesh/models/tabular/stg_tabular__transactions.sql) — tabular staging (replaces CSV staging) with within-source dedup
- [stg_ofx__transactions.sql](../../sqlmesh/models/prep/stg_ofx__transactions.sql) — OFX staging without within-source dedup (gap)

## Requirements

### Dedup

1. Transactions from different sources that describe the same real-world event resolve to one gold record in `core.fct_transactions`.
2. The matcher matches within the same source type only for Tier 2b (overlapping statements without source-native IDs). Within-source matches require high confidence — no review queue. Two rows from the same source with different `source_transaction_id` values are always treated as distinct transactions (Tier 2a handles those deterministically).
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

9. When source rows merge, per-field values are selected by a single source-priority ranking (configurable, default: `plaid > csv > excel > tsv > parquet > feather > pipe > ofx`).
10. For each field, the value from the highest-priority source with a non-NULL value wins.
11. `transaction_date` is an exception: earliest non-NULL posted date across sources (most accurate settlement date).
12. The source-priority list must include every supported `source_type`. The smart tabular importer produces format-specific `source_type` values (`csv`, `tsv`, `excel`, `parquet`, `feather`, `pipe`) rather than a single `tabular` value. Adding a new source type requires inserting it into this list.

### Identity

13. Gold records are keyed by `transaction_id` — a deterministic SHA-256 hash derived from the contributing source rows, consistent with the hash-based ID strategy used by the tabular importer (`smart-import-tabular.md`).
14. Source-native IDs are carried as `source_transaction_id`, with the same logical meaning everywhere the column appears (raw, prep, provenance). The smart tabular importer populates this from institution-assigned IDs when present in the source file (e.g., Tiller's Transaction ID, bank reference numbers identified as unique IDs).
15. Unmatched records get a `transaction_id` via `SHA256(source_type || '|' || source_transaction_id || '|' || account_id)` — stable across SQLMesh reruns.
16. Matched groups get a `transaction_id` via `SHA256()` over the sorted, pipe-delimited set of contributing `(source_type, source_transaction_id, account_id)` tuples — also stable.

### Provenance

17. `meta.fct_transaction_provenance` links every gold record to every contributing source row.
18. Provenance rows are never deleted, only superseded (a reversal appends a new row).

## Dedup Taxonomy

Each tier has a strictly simpler matching problem than the next:

| Tier | Scope | Method | Owner |
|---|---|---|---|
| 1. Same-file re-import | Same source, same file | Raw table PKs prevent duplicate inserts | `raw.*` INSERT logic |
| 2a. Overlapping statements (ID-based) | Same source type, different files, same source-native ID | `ROW_NUMBER()` on `(source_transaction_id, account_id)`, latest load wins | `prep.stg_*` views |
| 2b. Overlapping statements (hash-based) | Same `source_origin` + `source_type`, different files, no source-native ID | Same matching logic as Tier 3 but within a single origin; restricted to high-confidence only | Python → `app.match_decisions` |
| 3. Cross-source | Different `source_type` or different `source_origin` | Python matcher with confidence scoring, 1:1 assignment | Python → `app.match_decisions` |

Tiers 1 and 2a are deterministic SQL prerequisites. Tiers 2b and 3 use the same Python matching engine with the same scoring and persistence model.

### Tier 2a gap

OFX staging (`stg_ofx__transactions`) currently has no within-source dedup. This spec adds `ROW_NUMBER() OVER (PARTITION BY source_transaction_id, account_id ORDER BY loaded_at DESC)` to match what CSV staging already does.

### Tier 2b: overlapping statements without source-native IDs

Many bank CSV exports lack institution-assigned transaction IDs. When a user imports overlapping statements from the same source type (e.g., `chase-march.csv` and `chase-q1.csv`), the hash-based `transaction_id` in raw includes `row_number` and `source_file`, producing different IDs for the same real-world transaction. Tier 2a can't help because there's no `source_transaction_id` to deduplicate on.

Tier 2b addresses this by running the same matching engine used in Tier 3, but relaxing the "different source type" constraint. The key differences from Tier 3:

- **Same source origin and type required.** Pairs must share the same `source_origin` and `source_type`. Two CSVs from the same institution (`source_origin = 'chase_credit'`) are candidates; two CSVs from different institutions are not — those go to Tier 3.
- **Same account required.** `account_id` must match exactly (same as Tier 3).
- **Exact amount required.** `amount` must match to the penny (same as Tier 3).
- **Date within window.** `transaction_date` within `±date_window_days` (same as Tier 3).
- **High confidence only.** Only pairs scoring `>= high_confidence_threshold` are auto-merged. No review queue for within-source matches — if confidence isn't high enough, both rows survive. This is conservative: same-day, same-amount transactions from the same source (two coffees at Starbucks) have different descriptions and won't score high enough to merge.
- **Description similarity is the discriminator.** Since amount and date are already exact-matched, description similarity (`jaro_winkler_similarity`) is what separates genuine duplicates (high similarity) from distinct same-day same-amount transactions (different descriptions).

**Execution order:** Tier 2b runs before Tier 3. Within-source duplicates are resolved first so that Tier 3 sees a clean set of distinct transactions per source type.

**Match decisions:** Persisted in the same `app.match_decisions` table as Tier 3, with a `match_tier = '2b'` marker for auditability. Same reversal and re-proposal semantics.

## Data Model

### Renamed columns (migration)

Raw tables rename `transaction_id` → `source_transaction_id` to free up `transaction_id` for the gold key. This follows the column naming consistency rule: a column name must contain the same logical values everywhere it appears.

### New app tables

```sql
/* Match decisions from the Python matcher and user review; one row per proposed pair */
CREATE TABLE IF NOT EXISTS app.match_decisions (
    match_id VARCHAR NOT NULL,           -- UUID, primary key
    source_transaction_id_a VARCHAR NOT NULL, -- Source-native ID of first row
    source_type_a VARCHAR NOT NULL,      -- source_type of first row
    source_origin_a VARCHAR NOT NULL,    -- source_origin of first row (institution/connection/format)
    source_transaction_id_b VARCHAR NOT NULL, -- Source-native ID of second row
    source_type_b VARCHAR NOT NULL,      -- source_type of second row
    source_origin_b VARCHAR NOT NULL,    -- source_origin of second row (institution/connection/format)
    account_id VARCHAR NOT NULL,         -- Shared account (blocking requirement)
    confidence_score DECIMAL(5, 4),      -- 0.0000 to 1.0000
    match_signals JSON,                  -- Per-signal scores: {"date_distance": 0, "description_similarity": 0.87}
    match_type VARCHAR NOT NULL DEFAULT 'dedup', -- 'dedup' or 'transfer' (matching-transfer-detection.md adds transfer mode)
    match_tier VARCHAR,                   -- Dedup-specific: '2b' (within-source overlap) or '3' (cross-source); NULL for transfers
    account_id_b VARCHAR,                -- Second account; NULL for dedup (same account); populated for transfers (different accounts)
    match_status VARCHAR NOT NULL,       -- pending, accepted, rejected
    match_reason VARCHAR,                -- Human-readable explanation of why this match was proposed
    decided_by VARCHAR NOT NULL,         -- auto, user, system
    decided_at TIMESTAMP NOT NULL,       -- When the decision was made
    reversed_at TIMESTAMP,              -- When the match was undone; NULL if active
    reversed_by VARCHAR,                -- Who reversed: user or system; NULL if active
    PRIMARY KEY (match_id)
);

/* Source-priority ranking for golden-record merge rules; rebuilt from MatchingSettings on every run */
CREATE TABLE IF NOT EXISTS prep.seed_source_priority (
    source_type VARCHAR NOT NULL,         -- e.g. plaid, csv, ofx
    priority INTEGER NOT NULL,           -- Lower = higher precedence (1 = best)
    PRIMARY KEY (source_type)
);
```

### New meta model

`meta.fct_transaction_provenance` (SQLMesh model):

| Column | Type | Description |
|---|---|---|
| `transaction_id` | VARCHAR | FK to gold record in `core.fct_transactions` |
| `source_transaction_id` | VARCHAR | Source-native ID, joinable to raw/prep |
| `source_type` | VARCHAR | Import pathway / origin system |
| `source_origin` | VARCHAR | Institution/connection/format that produced this row |
| `source_file` | VARCHAR | File that produced this source row |
| `source_extracted_at` | TIMESTAMP | When the source row was parsed |
| `match_id` | VARCHAR | FK to `app.match_decisions`; NULL for unmatched records |

### New and modified prep models

**`prep.stg_ofx__transactions`** (modified) — add tier 2 dedup via `ROW_NUMBER()`.

**`prep.int_transactions__unioned`** (new) — `UNION ALL` of all `stg_*__transactions` with standardized column names and types. Replaces the CTEs in today's `fct_transactions`. Each row carries `source_transaction_id`, `source_type`, and `source_origin`.

**`prep.int_transactions__matched`** (new) — joins `int_transactions__unioned` with `app.match_decisions` to assign `transaction_id` (gold key). Unmatched rows get a deterministic hash via `SHA256(source_type || '|' || source_transaction_id || '|' || account_id)`. Matched groups get a deterministic hash from the sorted contributing tuple set. Output has both `transaction_id` and `source_transaction_id`.

**`prep.int_transactions__merged`** (new) — collapses matched groups to one row per `transaction_id`. Applies source-priority merge: for each field, COALESCE in priority order across the group's source rows. Exception: `transaction_date` takes the earliest non-NULL value. Adds `canonical_source_type` and `source_count`.

### Modified core model

**`core.fct_transactions`** (modified) — grain changes to `transaction_id` (gold key). Becomes a thin enrichment layer over `int_transactions__merged`:
- JOINs `app.transaction_categories` and `app.merchants` (same as today)
- Adds derived columns: `transaction_direction`, `amount_absolute`, date-part extractions (same as today)
- New columns from merged layer: `canonical_source_type`, `source_count`, `match_confidence`

### App table FK migration

`app.transaction_categories` and `app.transaction_notes` rename their FK from `transaction_id` (source-native) to `transaction_id` (gold key). Values are backfilled as deterministic SHA-256 hashes from each row's `(source_type, source_transaction_id, account_id)` — a 1:1 mapping since no merges exist yet.

## Matching Engine

### Architecture

Python orchestrator, DuckDB compute engine. The matcher sends SQL queries to DuckDB for data operations and handles control flow + assignment logic in Python.

### Control flow

```
1. Python loader         → writes to raw.*
2. Python matcher        → reads prep.stg_* views (always current — views over raw)
                         → Tier 2b: within-source overlap matching (high-confidence only)
                         → Tier 3: cross-source matching (full confidence tiers)
                         → each tier: blocking → scoring → 1:1 assignment
                         → writes to app.match_decisions (with match_tier marker)
3. sqlmesh run           → int_*__unioned → __matched → __merged
                         → core.fct_transactions
                         → meta.fct_transaction_provenance
```

Tier 2b runs first so that within-source duplicates are resolved before Tier 3 sees the data. This prevents a cross-source match from competing with a within-source duplicate.

### Candidate blocking

SQL query against prep views. Returns pairs where:
- `account_id` matches exactly
- `amount` matches exactly (to the penny, same-currency only)
- `transaction_date` within `±date_window_days` (default 3)
- **Tier 2b:** same `source_origin` and `source_type`, different `source_file`, no `source_transaction_id` on at least one row (rows with source-native IDs are already handled by Tier 2a)
- **Tier 3:** different `source_type` OR different `source_origin` (cross-source matching)

This produces a narrow candidate set. No fuzzy logic at this stage.

**Manual-entry exemption.** Rows with `source_type = 'manual'` are excluded from candidate selection on both sides — they are never proposed as matches against imported rows in either direction. Manual entries express explicit user intent ("I am recording this transaction"); silently merging one with an OFX/CSV row would erase that intent without consent. Pairing a manual row with an imported row is reachable only via explicit `transactions matches confirm`. See [`transaction-curation.md`](transaction-curation.md) §Manual Entry for the broader rationale.

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
- `>= review_threshold` (Tier 3 only): `match_status = 'pending'`, `decided_by = 'auto'`

Tier 2b writes only high-confidence matches (`match_tier = '2b'`). Pairs below `high_confidence_threshold` are discarded — no review queue for within-source overlaps, since a false positive would silently drop a real transaction.

Tier 3 pairs below `review_threshold` are not persisted (logged at DEBUG level only).

## Golden-Record Merge Rules

### Source-priority ranking

A single ordered list stored in `prep.seed_source_priority`, rebuilt from `MatchingSettings.source_priority` on every run. Config is the sole source of truth; the table is a SQL-accessible projection for merge model joins. Default: `plaid (1) > csv (2) > excel (3) > tsv (4) > parquet (5) > feather (6) > pipe (7) > ofx (8)`. The tabular import formats are individually ranked per `smart-import-tabular.md` — CSV is most common and gets highest priority among tabular formats. The list must include every supported `source_type` value.

### Field selection

In `int_transactions__merged`, matched groups collapse to one row per `transaction_id`:

| Field | Rule |
|---|---|
| `transaction_id` | Deterministic SHA-256 hash from sorted contributing tuples |
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
| `canonical_source_type` | The highest-priority source present in the group |
| `source_count` | Count of contributing source rows |
| `match_confidence` | Confidence score from `app.match_decisions`; NULL for unmatched records |

### Unmatched records

Pass through `int_transactions__merged` unchanged. They get a deterministic `transaction_id`, `source_count = 1`, `canonical_source_type` = their own `source_type`, and `match_confidence = NULL`.

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
| `moneybin matches history` | Show recent match decisions (auto and user) |
| `moneybin matches undo <match_id>` | Reverse a match decision. Sets `reversed_at`/`reversed_by`. |
| `moneybin matches backfill` | One-time scan of all existing transactions for latent duplicates |

### Backfill

`moneybin matches backfill` runs the matcher against all existing prep rows. Same scoring and confidence tiers as import-time matching. Idempotent: already-decided pairs are skipped.

```
⚙️  Scanning 4,230 existing transactions for duplicates...
✅ Backfill complete: 47 auto-merged, 12 pending review
👀 Run 'moneybin matches review' when ready
```

## MCP Interface

Designed alongside CLI. Implementation may be sequenced after CLI, but the data model and `app.match_decisions` schema support MCP from day one. These tools are shared with transfer detection (`matching-transfer-detection.md`) — a `match_type` filter distinguishes dedup from transfer proposals.

| Tool | Type | Description |
|---|---|---|
| `list_pending_matches` | Read | Show pending match proposals with confidence scores. `match_type` filter for `dedup` or `transfer`. |
| `confirm_match` | Write | Accept a pending match by `match_id` |
| `reject_match` | Write | Reject a pending match by `match_id` |
| `undo_match` | Write | Reverse a previously accepted match |
| `get_match_log` | Read | Recent match decisions with signal breakdown. `match_type` and `match_status` filters. |

### Prompt

| Prompt | Purpose |
|---|---|
| `review_matches` | "Help me review pending transaction matches. Show dedup and transfer proposals, explain why each was proposed, and let me accept or reject them." |

## Configuration

```python
class MatchingSettings(BaseModel):
    high_confidence_threshold: float = 0.95
    review_threshold: float = 0.70
    date_window_days: int = 3
    source_priority: list[str] = [
        "plaid",
        "csv",
        "excel",
        "tsv",
        "parquet",
        "feather",
        "pipe",
        "ofx",
    ]
    # Must include every supported source_type. Tabular formats are
    # format-specific per smart-import-tabular.md.
```

Env var overrides follow the `MONEYBIN_` convention:
- `MONEYBIN_MATCHING__HIGH_CONFIDENCE_THRESHOLD=0.90`
- `MONEYBIN_MATCHING__DATE_WINDOW_DAYS=5`
- `MONEYBIN_MATCHING__SOURCE_PRIORITY='["plaid", "csv", "excel", "tsv", "parquet", "feather", "pipe", "ofx"]'`

### What is not configurable

| Invariant | Rationale |
|---|---|
| Source-scoped blocking (`source_origin` + `source_type` for Tier 2b, cross-source for Tier 3) | Same-origin rows with different source-native IDs are genuinely different; Tier 2b only handles hash-ID overlaps |
| Exact account match as blocking requirement | Transactions can't match across accounts |
| Exact amount match as blocking requirement | Amount is the strongest identity signal |
| 1:1 assignment | Each source row describes at most one real-world transaction |

## Testing Strategy

### Unit tests

- **Scoring function**: given two transaction records, verify confidence score calculation with known inputs/outputs
- **1:1 assignment**: given a set of scored pairs with conflicts, verify greedy assignment picks optimal non-overlapping set
- **Hash generation**: verify deterministic — same inputs always produce same `transaction_id`; verify stability — sorted tuple order doesn't depend on insertion order
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
- SHA-256 hashing (Python stdlib `hashlib.sha256`, DuckDB `sha256()`)
- Existing prep staging views and raw schema
- Database migration system ([database-migration.md](database-migration.md)) for the `transaction_id` → `source_transaction_id` rename and app table FK changes

## Out of Scope

- **Cross-currency matching** — deferred to multi-currency initiative. This spec requires exact amount match in same currency.
- **Transfer detection** — pillar B, separate spec (`matching-transfer-detection.md`). Different semantics: links two records rather than collapsing them.
- **Per-field or per-transaction merge overrides** — v1 uses a single global source-priority ranking. Enhancement spec if needed.
- **ML/learned matching** — v1 uses deterministic scoring. Learned promotions deferred per umbrella spec.
- **Investment transaction dedup** — depends on investment-tracking spec.
- **Account-level dedup** — `dim_accounts` already handles this via `account_id` partitioning.
- **MCP tools** — Phase 2. CLI-only for v1.

## Checklist: Adding a New Data Source

When a new source is added to MoneyBin, the following must be updated for matching to work correctly:

1. Create staging model in `sqlmesh/models/prep/` with tier 2 dedup (`ROW_NUMBER`)
2. Add a CTE to `int_transactions__unioned` and `UNION ALL` into the combined set — include `source_origin` column
3. Insert the new `source_type` into `MatchingSettings.source_priority` at the appropriate position
4. Define `source_origin` population logic (format name, institution ID, item ID, etc.)
5. Add the new source to matching integration tests (load + match against existing sources)

## Open Questions

### Resolved

- **ID generation strategy.** SHA-256 hashes, consistent with the tabular importer (`smart-import-tabular.md`). No namespace UUID needed.
- **`source_priority` ownership.** Config-only. `prep.seed_source_priority` is rebuilt from `MatchingSettings.source_priority` on every run. Not mutable app state.
- **`int_transactions__matched` model kind.** VIEW. DuckDB handles the join against `app.match_decisions` efficiently at personal finance scale. Consistent with existing prep models.

### Deferred to implementation tuning milestone

1. **Confidence score formula.** The exact weighting of date distance and description similarity. Spec requires the formula to be auditable (signals persisted in JSON) but doesn't fix the weights — they should be tuned against real data. See MVP roadmap M1 tuning milestone.
