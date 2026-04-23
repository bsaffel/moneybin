# Feature: Data Pipeline Reconciliation

## Status
draft

## Goal

Provide automated, continuous data quality validation across the raw → staging → core pipeline. Detect data loss, orphaned records, aggregate mismatches, and coverage gaps so that users can trust the analytics layer without manual verification.

## Background

MoneyBin is a data warehouse for personal financial data. Like any warehouse, data passes through multiple transformation layers (raw → prep → core) and arrives from multiple sources (OFX, tabular imports, future sync providers). Each layer and each source is an opportunity for silent data loss or corruption.

Financial reconciliation ("does my balance match the bank?") is covered by [`net-worth.md`](net-worth.md) via `reconciliation_delta` in `fct_balances_daily`. This spec covers **pipeline reconciliation** — the data engineering concern of "did my ETL preserve data integrity?"

The goal is warehouse-grade trust: every number in the analytics layer is provably traceable to a source record, and every source record is accounted for in the analytics layer.

Related specs:
- [`net-worth.md`](net-worth.md) — financial balance reconciliation (complementary, not overlapping)
- [`smart-import-tabular.md`](smart-import-tabular.md) — import pipeline with row-level rejection tracking
- [`matching-same-record-dedup.md`](matching-same-record-dedup.md) — cross-source dedup that intentionally reduces row counts
- [`observability.md`](observability.md) — metrics infrastructure used for reconciliation alerting

## Design Principles

1. **Computed, not stored.** Reconciliation checks run against live data on every `sqlmesh run` or on-demand via CLI/MCP. Results are views or query outputs, not materialized state that can go stale.
2. **Explain, don't block.** Mismatches produce warnings and detailed reports, not pipeline failures. The user decides what to investigate — the system doesn't silently discard data or refuse to proceed.
3. **Layer-aware.** Each pipeline boundary (raw→prep, prep→core) has its own checks because each layer has different semantics (raw allows duplicates; core deduplicates intentionally).
4. **Source-aware.** Checks run per source system (OFX, tabular, future Plaid) because each has different expected behavior (OFX transactions are unique by FITID; tabular may have content-hash collisions across files).

## Reconciliation Checks

### Layer 1: Raw → Staging (prep)

Staging views are 1:1 with raw tables (light cleaning, no row reduction except within-source dedup). Mismatches here indicate a bug in the staging model.

| Check | Query shape | Expected |
|---|---|---|
| **Row count** | `COUNT(*)` in raw vs prep per source table | Equal (staging dedup may reduce; delta should match `_row_num > 1` count) |
| **Amount sum** | `SUM(amount)` in raw vs prep per source table | Equal (staging doesn't transform amounts) |
| **Null injection** | Columns that are `NOT NULL` in raw but `NULL` in prep | Zero (staging shouldn't introduce nulls) |
| **Type coercion loss** | Rows where `CAST` in staging produces NULL from non-NULL raw values | Zero (indicates unparseable data that should have been caught at import) |

### Layer 2: Staging → Core

Core models union multiple sources and deduplicate. Row counts will differ intentionally. The checks focus on completeness (nothing lost) and consistency (dedup is intentional).

| Check | Query shape | Expected |
|---|---|---|
| **Source coverage** | Every `source_type` in staging appears in core | All sources represented |
| **Row accounting** | `staging_count = core_count + dedup_count + excluded_count` per source | Balanced (every staging row is either in core, deduped, or explicitly excluded) |
| **Amount preservation** | `SUM(amount)` in staging vs core, per source, after accounting for dedup | Equal (dedup removes rows but the kept row's amount should match) |
| **Orphan detection** | Transactions in core with `account_id` not in `dim_accounts` | Zero |
| **Orphan detection** | Accounts in `dim_accounts` with zero transactions in `fct_transactions` | Report (may be legitimate for new accounts, but worth surfacing) |

### Layer 3: Import Integrity

Per-import-batch validation using `raw.import_log` metadata.

| Check | Query shape | Expected |
|---|---|---|
| **Row count vs import_log** | `COUNT(*)` in raw where `import_id = X` vs `import_log.rows_imported` | Equal |
| **Rejection accounting** | `rows_total = rows_imported + rows_rejected + rows_skipped_trailing` | Balanced |
| **No abandoned imports** | `import_log` entries with `status = 'importing'` older than 1 hour | Zero (indicates a crashed import) |

### Layer 4: Temporal Coverage

Detect gaps in transaction history that may indicate missing imports.

| Check | Query shape | Expected |
|---|---|---|
| **Date gaps** | Per account, consecutive transaction dates with gaps > N days (configurable, default 45) | Report (not necessarily an error — dormant accounts exist) |
| **Future dates** | Transactions with `transaction_date > CURRENT_DATE` | Zero (post-dated transactions are suspicious) |
| **Stale accounts** | Accounts with no transactions in the last N days (configurable) | Report |

## Data Model

### SQLMesh model: `core.reconciliation_raw_to_prep` (VIEW)

Per-source-table row count and amount sum comparison between raw and prep layers.

```
Columns:
  source_table      VARCHAR       -- e.g. "tabular_transactions", "ofx_transactions"
  raw_row_count     BIGINT        -- COUNT(*) in raw table
  prep_row_count    BIGINT        -- COUNT(*) in prep view
  dedup_count       BIGINT        -- Rows removed by within-source dedup (raw - prep)
  raw_amount_sum    DECIMAL(18,2) -- SUM(amount) in raw
  prep_amount_sum   DECIMAL(18,2) -- SUM(amount) in prep
  amount_delta      DECIMAL(18,2) -- Difference (should be 0 for non-dedup sources)
  checked_at        TIMESTAMP     -- CURRENT_TIMESTAMP
```

### SQLMesh model: `core.reconciliation_import_batches` (VIEW)

Per-import-batch integrity check.

```
Columns:
  import_id           VARCHAR
  source_file         VARCHAR
  log_rows_imported   INTEGER       -- From import_log
  actual_row_count    INTEGER       -- COUNT(*) in raw table for this import_id
  row_count_match     BOOLEAN       -- log_rows_imported = actual_row_count
  log_rows_total      INTEGER
  log_rows_rejected   INTEGER
  log_rows_skipped    INTEGER
  accounting_balanced BOOLEAN       -- total = imported + rejected + skipped
  status              VARCHAR       -- From import_log
  checked_at          TIMESTAMP
```

### SQLMesh model: `core.reconciliation_coverage` (VIEW)

Per-account temporal coverage analysis.

```
Columns:
  account_id          VARCHAR
  first_transaction   DATE
  last_transaction    DATE
  total_transactions  BIGINT
  max_gap_days        INTEGER       -- Largest gap between consecutive transactions
  max_gap_start       DATE          -- Start of the largest gap
  max_gap_end         DATE          -- End of the largest gap
  has_future_dates    BOOLEAN
  days_since_last     INTEGER       -- CURRENT_DATE - last_transaction
```

## CLI Interface

```
moneybin reconciliation check [--layer raw-to-prep|import|coverage|all] [--output json|table]
```
- Default: `--layer all`
- Runs the relevant reconciliation views and reports mismatches
- Exit code 0 if clean, 1 if mismatches found (useful for CI/scripts)
- `--output json` returns structured results for MCP/automation

```
moneybin reconciliation check --layer import [--import-id UUID]
```
- Filter to a specific import batch

The financial balance reconciliation command (`moneybin reconciliation show`) is defined in [`net-worth.md`](net-worth.md) and covers the institution-balance-vs-transactions concern. Both commands live under the same `reconciliation` group.

## MCP Interface

**`reconciliation.check`** — Runs pipeline reconciliation checks.
- Params: `layer` (optional, default "all"), `import_id` (optional)
- Returns: per-check pass/fail with details for failures
- Sensitivity: `low` (aggregates and counts only, no transaction data)

## Metrics

| Metric | Type | Labels | Purpose |
|---|---|---|---|
| `reconciliation_check_mismatches` | Gauge | `layer`, `check_name` | Number of mismatches per check (0 = healthy) |
| `reconciliation_check_duration_seconds` | Histogram | `layer` | Time to run reconciliation checks |
| `reconciliation_abandoned_imports` | Gauge | — | Count of imports stuck in 'importing' state |

## Configuration

```python
class ReconciliationConfig(BaseModel, frozen=True):
    """Pipeline reconciliation settings."""

    coverage_gap_threshold_days: int = Field(default=45, ge=1)
    stale_account_threshold_days: int = Field(default=90, ge=1)
    abandoned_import_threshold_minutes: int = Field(default=60, ge=1)
```

## Testing Strategy

### Unit tests
- Verify each reconciliation view produces correct results against known fixture data
- Insert deliberately mismatched data (wrong row counts, orphaned records) and verify detection
- Verify clean data produces all-passing results

### Integration tests
- End-to-end: import file → `sqlmesh run` → `reconciliation check` → verify clean
- Import with rejections → verify import batch accounting balances
- Simulate gap → verify coverage check detects it

## Dependencies

- SQLMesh pipeline — reconciliation views query existing raw/prep/core tables
- [`observability.md`](observability.md) — metrics for reconciliation results
- [`net-worth.md`](net-worth.md) — financial reconciliation lives there; pipeline reconciliation lives here
- `config.py` — `ReconciliationConfig` for threshold settings

## Out of Scope

- **Financial balance reconciliation** — covered by `net-worth.md` (`reconciliation_delta` in `fct_balances_daily`)
- **Automated remediation** — reconciliation reports problems; it doesn't fix them. The user decides whether to reimport, revert, or investigate.
- **Cross-source dedup validation** — covered by `matching-same-record-dedup.md`. This spec validates pipeline integrity, not match quality.
- **Real-time monitoring** — reconciliation runs on-demand or after `sqlmesh run`, not as a continuous process.
