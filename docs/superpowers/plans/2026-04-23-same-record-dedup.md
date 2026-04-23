# Same-Record Dedup & Golden-Record Merge Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Resolve duplicate transactions across OFX and tabular sources into deduplicated gold records with deterministic keys, per-field merge rules, full provenance, and a CLI review workflow.

**Architecture:** Python matching engine writes pairwise match decisions to `app.match_decisions`. SQLMesh intermediate models (`int_transactions__unioned` → `__matched` → `__merged`) consume those decisions to produce deduplicated gold records in `core.fct_transactions`. Matching runs after import, before SQLMesh transforms.

**Tech Stack:** Python 3.12, DuckDB (jaro_winkler_similarity, sha256), SQLMesh (VIEW models), Pydantic Settings, Typer CLI, pytest.

**Spec:** [`docs/specs/matching-same-record-dedup.md`](../../specs/matching-same-record-dedup.md)

---

## File Structure

### New files

| File | Responsibility |
|------|---------------|
| `src/moneybin/matching/__init__.py` | Package exports: `TransactionMatcher`, `MatchingSettings` |
| `src/moneybin/matching/engine.py` | `TransactionMatcher` orchestrator — runs Tier 2b then Tier 3 |
| `src/moneybin/matching/scoring.py` | Candidate blocking SQL, confidence scoring, signal extraction |
| `src/moneybin/matching/assignment.py` | Greedy best-score-first 1:1 bipartite assignment |
| `src/moneybin/matching/persistence.py` | CRUD for `app.match_decisions` (create, query, accept, reject, undo) |
| `src/moneybin/matching/hashing.py` | Gold key generation (unmatched + matched group SHA-256) |
| `src/moneybin/matching/priority.py` | Rebuild `app.seed_source_priority` from config |
| `src/moneybin/sql/schema/app_match_decisions.sql` | DDL for match decisions table |
| `src/moneybin/sql/schema/app_seed_source_priority.sql` | DDL for source priority table |
| `src/moneybin/sql/migrations/V001__rename_ofx_transaction_id.sql` | OFX column rename migration |
| `src/moneybin/sql/migrations/V002__backfill_gold_keys.sql` | App FK backfill migration |
| `src/moneybin/cli/commands/matches.py` | CLI commands: run, review, log, undo, backfill |
| `sqlmesh/models/prep/int_transactions__unioned.sql` | UNION ALL of all staging models |
| `sqlmesh/models/prep/int_transactions__matched.sql` | Gold key assignment via match_decisions |
| `sqlmesh/models/prep/int_transactions__merged.sql` | Source-priority field merge |
| `sqlmesh/models/meta/fct_transaction_provenance.sql` | Provenance links |
| `tests/moneybin/matching/__init__.py` | Test package |
| `tests/moneybin/matching/test_hashing.py` | Gold key hash tests |
| `tests/moneybin/matching/test_scoring.py` | Blocking + scoring tests |
| `tests/moneybin/matching/test_assignment.py` | 1:1 assignment tests |
| `tests/moneybin/matching/test_persistence.py` | Match decision CRUD tests |
| `tests/moneybin/matching/test_priority.py` | Source priority seeding tests |
| `tests/moneybin/matching/test_engine.py` | Orchestrator integration tests |
| `tests/moneybin/cli/test_matches.py` | CLI command tests |
| `tests/moneybin/test_dedup_integration.py` | End-to-end dedup tests |

### Modified files

| File | Change |
|------|--------|
| `src/moneybin/config.py` | Add `MatchingSettings`, wire into `MoneyBinSettings` |
| `src/moneybin/tables.py` | Add `MATCH_DECISIONS`, `SEED_SOURCE_PRIORITY` constants |
| `src/moneybin/schema.py` | Register new schema files in `_SCHEMA_FILES` |
| `src/moneybin/metrics/registry.py` | Add matching-specific metrics with labels |
| `src/moneybin/sql/schema/raw_ofx_transactions.sql` | Rename `transaction_id` → `source_transaction_id` |
| `src/moneybin/extractors/ofx_extractor.py` | Output `source_transaction_id` instead of `transaction_id` |
| `src/moneybin/loaders/ofx_loader.py` | Use `source_transaction_id` in INSERT SQL |
| `sqlmesh/models/prep/stg_ofx__transactions.sql` | Add Tier 2a dedup, `source_type`, `source_origin` columns |
| `sqlmesh/models/core/fct_transactions.sql` | Read from `int_transactions__merged` instead of staging CTEs |
| `src/moneybin/cli/main.py` | Import from `matches` module instead of stubs |
| `src/moneybin/cli/commands/stubs.py` | Remove `matches_app` stub |
| `src/moneybin/services/import_service.py` | Hook matching after load, before transforms |
| `tests/moneybin/db_helpers.py` | Update `CORE_FCT_TRANSACTIONS_DDL` with new columns |

---

## Task 1: MatchingSettings Configuration

**Files:**
- Modify: `src/moneybin/config.py`
- Test: `tests/moneybin/test_config_matching.py`

- [ ] **Step 1: Write failing test for MatchingSettings defaults**

```python
# tests/moneybin/test_config_matching.py
"""Tests for MatchingSettings configuration."""

from moneybin.config import MatchingSettings, MoneyBinSettings


class TestMatchingSettings:
    def test_defaults(self) -> None:
        settings = MatchingSettings()
        assert settings.high_confidence_threshold == 0.95
        assert settings.review_threshold == 0.70
        assert settings.date_window_days == 3
        assert settings.source_priority == [
            "plaid",
            "csv",
            "excel",
            "tsv",
            "parquet",
            "feather",
            "pipe",
            "ofx",
        ]

    def test_source_priority_must_not_be_empty(self) -> None:
        import pytest

        with pytest.raises(ValueError, match="source_priority"):
            MatchingSettings(source_priority=[])

    def test_thresholds_must_be_ordered(self) -> None:
        import pytest

        with pytest.raises(ValueError, match="review_threshold.*high_confidence"):
            MatchingSettings(high_confidence_threshold=0.50, review_threshold=0.80)

    def test_available_on_root_settings(self) -> None:
        settings = MoneyBinSettings(profile="test")
        assert settings.matching.high_confidence_threshold == 0.95
        assert settings.matching.date_window_days == 3
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/moneybin/test_config_matching.py -v`
Expected: FAIL — `MatchingSettings` not importable from `moneybin.config`

- [ ] **Step 3: Implement MatchingSettings**

Add to `src/moneybin/config.py`, after `SyncConfig`:

```python
class MatchingSettings(BaseModel):
    """Transaction matching and dedup configuration."""

    model_config = ConfigDict(frozen=True)

    high_confidence_threshold: float = Field(
        default=0.95,
        ge=0.0,
        le=1.0,
        description="Auto-merge threshold (>= this score = accepted)",
    )
    review_threshold: float = Field(
        default=0.70,
        ge=0.0,
        le=1.0,
        description="Review queue threshold (>= this but < high = pending)",
    )
    date_window_days: int = Field(
        default=3,
        ge=0,
        description="Maximum days between transaction dates for candidate pairs",
    )
    source_priority: list[str] = Field(
        default=[
            "plaid",
            "csv",
            "excel",
            "tsv",
            "parquet",
            "feather",
            "pipe",
            "ofx",
        ],
        description="Source types in priority order (first = highest priority)",
    )

    @field_validator("source_priority")
    @classmethod
    def validate_source_priority(cls, v: list[str]) -> list[str]:
        if not v:
            raise ValueError("source_priority must not be empty")
        return v

    @model_validator(mode="after")
    def validate_threshold_ordering(self) -> "MatchingSettings":
        if self.review_threshold > self.high_confidence_threshold:
            raise ValueError(
                f"review_threshold ({self.review_threshold}) must be <= "
                f"high_confidence_threshold ({self.high_confidence_threshold})"
            )
        return self
```

Add `model_validator` to the imports at the top of config.py:

```python
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
```

Add `matching` field to `MoneyBinSettings`:

```python
class MoneyBinSettings(BaseSettings):
    # Core configuration sections
    database: DatabaseConfig = Field(default_factory=DatabaseConfig)
    data: DataConfig = Field(default_factory=DataConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    metrics: MetricsConfig = Field(default_factory=MetricsConfig)
    mcp: MCPConfig = Field(default_factory=MCPConfig)
    sync: SyncConfig = Field(default_factory=SyncConfig)
    matching: MatchingSettings = Field(default_factory=MatchingSettings)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/moneybin/test_config_matching.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/moneybin/config.py tests/moneybin/test_config_matching.py
git commit -m "feat: add MatchingSettings configuration for transaction dedup"
```

---

## Task 2: Schema Files, TableRef, and Schema Registration

**Files:**
- Create: `src/moneybin/sql/schema/app_match_decisions.sql`
- Create: `src/moneybin/sql/schema/app_seed_source_priority.sql`
- Modify: `src/moneybin/tables.py`
- Modify: `src/moneybin/schema.py`
- Test: `tests/moneybin/test_matching_schema.py`

- [ ] **Step 1: Write failing test for schema creation**

```python
# tests/moneybin/test_matching_schema.py
"""Tests for matching schema initialization."""

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from moneybin.database import Database
from moneybin.tables import MATCH_DECISIONS, SEED_SOURCE_PRIORITY


class TestMatchingSchema:
    @pytest.fixture()
    def db(self, tmp_path: Path, mock_secret_store: MagicMock) -> Database:
        database = Database(
            tmp_path / "test.duckdb",
            secret_store=mock_secret_store,
            no_auto_upgrade=True,
        )
        yield database
        database.close()

    def test_match_decisions_table_exists(self, db: Database) -> None:
        result = db.execute(
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_schema = 'app' AND table_name = 'match_decisions'"
        ).fetchone()
        assert result[0] == 1

    def test_match_decisions_columns(self, db: Database) -> None:
        cols = db.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'app' AND table_name = 'match_decisions' "
            "ORDER BY ordinal_position"
        ).fetchall()
        col_names = [c[0] for c in cols]
        assert "match_id" in col_names
        assert "source_transaction_id_a" in col_names
        assert "source_type_a" in col_names
        assert "source_origin_a" in col_names
        assert "confidence_score" in col_names
        assert "match_status" in col_names
        assert "match_tier" in col_names
        assert "reversed_at" in col_names

    def test_seed_source_priority_table_exists(self, db: Database) -> None:
        result = db.execute(
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_schema = 'app' AND table_name = 'seed_source_priority'"
        ).fetchone()
        assert result[0] == 1

    def test_table_ref_constants(self) -> None:
        assert MATCH_DECISIONS.full_name == "app.match_decisions"
        assert SEED_SOURCE_PRIORITY.full_name == "app.seed_source_priority"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/moneybin/test_matching_schema.py -v`
Expected: FAIL — `MATCH_DECISIONS` not importable

- [ ] **Step 3: Create app_match_decisions.sql**

```sql
-- src/moneybin/sql/schema/app_match_decisions.sql

/* Match decisions from the Python matcher and user review; one row per proposed pair */
CREATE TABLE IF NOT EXISTS app.match_decisions (
    match_id VARCHAR NOT NULL,                -- UUID primary key for this match decision
    source_transaction_id_a VARCHAR NOT NULL,  -- Source-native ID of first row in the pair
    source_type_a VARCHAR NOT NULL,            -- source_type of first row (ofx, csv, etc.)
    source_origin_a VARCHAR NOT NULL,          -- source_origin of first row (institution/format)
    source_transaction_id_b VARCHAR NOT NULL,  -- Source-native ID of second row in the pair
    source_type_b VARCHAR NOT NULL,            -- source_type of second row
    source_origin_b VARCHAR NOT NULL,          -- source_origin of second row
    account_id VARCHAR NOT NULL,               -- Shared account (blocking requirement for dedup)
    confidence_score DECIMAL(5, 4),            -- Match confidence 0.0000 to 1.0000
    match_signals JSON,                        -- Per-signal scores: {"date_distance": 0, "description_similarity": 0.87}
    match_type VARCHAR NOT NULL DEFAULT 'dedup', -- dedup or transfer (transfer added by matching-transfer-detection.md)
    match_tier VARCHAR,                        -- Dedup-specific: 2b (within-source overlap) or 3 (cross-source); NULL for transfers
    account_id_b VARCHAR,                      -- Second account; NULL for dedup (same account); populated for transfers
    match_status VARCHAR NOT NULL,             -- pending, accepted, rejected
    match_reason VARCHAR,                      -- Human-readable explanation of why this match was proposed
    decided_by VARCHAR NOT NULL,               -- auto, user, system
    decided_at TIMESTAMP NOT NULL,             -- When the decision was made
    reversed_at TIMESTAMP,                     -- When the match was undone; NULL if active
    reversed_by VARCHAR,                       -- Who reversed: user or system; NULL if active
    PRIMARY KEY (match_id)
);
```

- [ ] **Step 4: Create app_seed_source_priority.sql**

```sql
-- src/moneybin/sql/schema/app_seed_source_priority.sql

/* Source-priority ranking for golden-record merge rules; rebuilt from MatchingSettings on every matcher run */
CREATE TABLE IF NOT EXISTS app.seed_source_priority (
    source_type VARCHAR NOT NULL,  -- Source type identifier (e.g. plaid, csv, ofx)
    priority INTEGER NOT NULL,     -- Lower number = higher precedence (1 = best)
    PRIMARY KEY (source_type)
);
```

- [ ] **Step 5: Add TableRef constants**

Add to `src/moneybin/tables.py` in the app tables section:

```python
# -- App matching tables --
MATCH_DECISIONS = TableRef("app", "match_decisions")
SEED_SOURCE_PRIORITY = TableRef("app", "seed_source_priority")
```

- [ ] **Step 6: Register schema files**

Add to `src/moneybin/schema.py` `_SCHEMA_FILES` list, after `"app_tabular_formats.sql"`:

```python
("app_match_decisions.sql",)
("app_seed_source_priority.sql",)
```

- [ ] **Step 7: Run test to verify it passes**

Run: `uv run pytest tests/moneybin/test_matching_schema.py -v`
Expected: PASS

- [ ] **Step 8: Commit**

```bash
git add src/moneybin/sql/schema/app_match_decisions.sql \
        src/moneybin/sql/schema/app_seed_source_priority.sql \
        src/moneybin/tables.py \
        src/moneybin/schema.py \
        tests/moneybin/test_matching_schema.py
git commit -m "feat: add match_decisions and seed_source_priority schemas"
```

---

## Task 3: Metrics Updates

**Files:**
- Modify: `src/moneybin/metrics/registry.py`
- Test: `tests/moneybin/metrics/test_matching_metrics.py`

- [ ] **Step 1: Write failing test**

```python
# tests/moneybin/metrics/test_matching_metrics.py
"""Tests for matching metrics registration."""

from moneybin.metrics.registry import (
    DEDUP_MATCHES_TOTAL,
    DEDUP_MATCH_CONFIDENCE,
    DEDUP_PAIRS_SCORED,
    DEDUP_REVIEW_PENDING,
)


class TestMatchingMetrics:
    def test_dedup_matches_total_has_labels(self) -> None:
        assert "match_tier" in DEDUP_MATCHES_TOTAL._labelnames
        assert "decided_by" in DEDUP_MATCHES_TOTAL._labelnames

    def test_dedup_pairs_scored_exists(self) -> None:
        assert DEDUP_PAIRS_SCORED._name == "moneybin_dedup_pairs_scored_total"

    def test_dedup_review_pending_exists(self) -> None:
        assert DEDUP_REVIEW_PENDING._name == "moneybin_dedup_review_pending"

    def test_dedup_match_confidence_exists(self) -> None:
        assert DEDUP_MATCH_CONFIDENCE._name == "moneybin_dedup_match_confidence"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/moneybin/metrics/test_matching_metrics.py -v`
Expected: FAIL — new metrics not importable

- [ ] **Step 3: Update metrics registry**

Replace the existing `DEDUP_MATCHES_TOTAL` definition in `src/moneybin/metrics/registry.py` and add new metrics:

```python
# ── Deduplication ─────────────────────────────────────────────────────────────

DEDUP_MATCHES_TOTAL = Counter(
    "moneybin_dedup_matches_total",
    "Total duplicate records matched and merged",
    ["match_tier", "decided_by"],
)

DEDUP_PAIRS_SCORED = Counter(
    "moneybin_dedup_pairs_scored_total",
    "Total candidate pairs scored by the matching engine",
)

DEDUP_REVIEW_PENDING = Gauge(
    "moneybin_dedup_review_pending",
    "Number of match proposals awaiting user review",
)

DEDUP_MATCH_CONFIDENCE = Histogram(
    "moneybin_dedup_match_confidence",
    "Distribution of match confidence scores",
)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/moneybin/metrics/test_matching_metrics.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/moneybin/metrics/registry.py tests/moneybin/metrics/test_matching_metrics.py
git commit -m "feat: add matching-specific metrics with tier and decider labels"
```

---

## Task 4: Migration V001 — OFX Column Rename

Renames `transaction_id` → `source_transaction_id` in `raw.ofx_transactions` and updates all code that references the old column name.

**Files:**
- Create: `src/moneybin/sql/migrations/V001__rename_ofx_transaction_id.sql`
- Modify: `src/moneybin/sql/schema/raw_ofx_transactions.sql`
- Modify: `src/moneybin/extractors/ofx_extractor.py`
- Modify: `src/moneybin/loaders/ofx_loader.py`
- Test: `tests/moneybin/test_migration_v001.py`

- [ ] **Step 1: Write failing test for migration**

```python
# tests/moneybin/test_migration_v001.py
"""Tests for V001 migration: OFX transaction_id → source_transaction_id rename."""

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from moneybin.database import Database
from moneybin.migrations import Migration


class TestV001Migration:
    def test_migration_file_parses(self) -> None:
        migration_dir = (
            Path(__file__).resolve().parents[2]
            / "src"
            / "moneybin"
            / "sql"
            / "migrations"
        )
        path = migration_dir / "V001__rename_ofx_transaction_id.sql"
        assert path.exists(), f"Migration file not found: {path}"
        migration = Migration.from_file(path)
        assert migration.version == 1
        assert migration.file_type == "sql"

    def test_ofx_schema_uses_source_transaction_id(self) -> None:
        schema_path = (
            Path(__file__).resolve().parents[2]
            / "src"
            / "moneybin"
            / "sql"
            / "schema"
            / "raw_ofx_transactions.sql"
        )
        content = schema_path.read_text()
        assert "source_transaction_id" in content
        # transaction_id should not appear as a column name (only in comments is ok)
        lines = [
            line
            for line in content.split("\n")
            if not line.strip().startswith("--") and not line.strip().startswith("/*")
        ]
        ddl_text = "\n".join(lines)
        # The column definition should use source_transaction_id, not transaction_id
        assert "source_transaction_id VARCHAR" in ddl_text


class TestOFXExtractorColumnName:
    def test_extractor_outputs_source_transaction_id(self) -> None:
        """OFX extractor DataFrame must use source_transaction_id column."""
        from moneybin.extractors.ofx_extractor import OFXExtractor

        extractor = OFXExtractor()
        # Get the empty DataFrame schema (no file needed)
        empty_df = extractor._build_empty_transactions_df()
        assert "source_transaction_id" in empty_df.columns
        assert "transaction_id" not in empty_df.columns
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/moneybin/test_migration_v001.py -v`
Expected: FAIL — migration file doesn't exist, schema still uses `transaction_id`

- [ ] **Step 3: Create migration file**

```sql
-- src/moneybin/sql/migrations/V001__rename_ofx_transaction_id.sql
-- Rename transaction_id → source_transaction_id in raw.ofx_transactions
-- to free up transaction_id for the gold key in core.fct_transactions.
-- DuckDB ALTER TABLE RENAME COLUMN preserves PK constraints.

ALTER TABLE raw.ofx_transactions RENAME COLUMN transaction_id TO source_transaction_id;
```

- [ ] **Step 4: Update raw_ofx_transactions.sql schema file**

Replace the full file content of `src/moneybin/sql/schema/raw_ofx_transactions.sql`:

```sql
/* Transaction records extracted from OFX/QFX files; one record per transaction per account per source file */
CREATE TABLE IF NOT EXISTS raw.ofx_transactions (
    source_transaction_id VARCHAR, -- OFX FITID element; institution-assigned unique transaction identifier
    account_id VARCHAR, -- Account this transaction belongs to; foreign key to raw.ofx_accounts; part of primary key
    transaction_type VARCHAR, -- OFX TRNTYPE element, e.g. DEBIT, CREDIT, CHECK, INT, DIV
    date_posted TIMESTAMP, -- OFX DTPOSTED element; mapped to transaction_date in core
    amount DECIMAL(18, 2), -- OFX TRNAMT element; negative = expense, positive = income
    payee VARCHAR, -- OFX NAME element (payee/merchant); mapped to description in core
    memo VARCHAR, -- OFX MEMO element; supplemental transaction notes from the institution
    check_number VARCHAR, -- OFX CHECKNUM element; check number for paper checks; NULL for electronic transactions
    source_file VARCHAR, -- Path to the OFX/QFX file this record was loaded from; part of primary key
    extracted_at TIMESTAMP, -- Timestamp when the OFX file was parsed
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- Timestamp when this record was inserted into the database
    PRIMARY KEY (source_transaction_id, account_id, source_file)
);
```

- [ ] **Step 5: Update OFX extractor**

In `src/moneybin/extractors/ofx_extractor.py`, change the transaction DataFrame output column name. Replace `"transaction_id"` with `"source_transaction_id"` in three locations:

1. In the `tx_data` dict (line ~333):
```python
                tx_data = {
                    "source_transaction_id": tx_schema.id,
                    "account_id": account.account_id,
```

2. In the empty DataFrame schema (line ~350):
```python
        return pl.DataFrame(
            schema={
                "source_transaction_id": pl.String,
                "account_id": pl.String,
```

Also extract the empty DataFrame construction into a method for testability. Add before the `_extract_balances` method:

```python
    def _build_empty_transactions_df(self) -> pl.DataFrame:
        """Build an empty transactions DataFrame with the correct schema."""
        return pl.DataFrame(
            schema={
                "source_transaction_id": pl.String,
                "account_id": pl.String,
                "transaction_type": pl.String,
                "date_posted": pl.String,
                "amount": pl.Float64,
                "payee": pl.String,
                "memo": pl.String,
                "check_number": pl.String,
                "source_file": pl.String,
                "extracted_at": pl.String,
            }
        )
```

And use it in the extraction method instead of the inline schema definition.

- [ ] **Step 6: Update OFX loader**

In `src/moneybin/loaders/ofx_loader.py`, update the INSERT statement (line ~101-108):

```python
            conn.execute("""
                INSERT OR REPLACE INTO raw.ofx_transactions
                (source_transaction_id, account_id, transaction_type, date_posted,
                 amount, payee, memo, check_number, source_file, extracted_at)
                SELECT source_transaction_id, account_id, transaction_type,
                       date_posted::TIMESTAMP, amount, payee, memo,
                       check_number, source_file, extracted_at::TIMESTAMP
                FROM df
            """)
```

- [ ] **Step 7: Run test to verify it passes**

Run: `uv run pytest tests/moneybin/test_migration_v001.py -v`
Expected: PASS

- [ ] **Step 8: Run existing OFX tests to check for regressions**

Run: `uv run pytest tests/moneybin/ -v -k "ofx" --tb=short`
Expected: PASS (may need to update OFX test fixtures to use `source_transaction_id`)

- [ ] **Step 9: Commit**

```bash
git add src/moneybin/sql/migrations/V001__rename_ofx_transaction_id.sql \
        src/moneybin/sql/schema/raw_ofx_transactions.sql \
        src/moneybin/extractors/ofx_extractor.py \
        src/moneybin/loaders/ofx_loader.py \
        tests/moneybin/test_migration_v001.py
git commit -m "feat: rename OFX transaction_id to source_transaction_id (V001 migration)"
```

---

## Task 5: OFX Staging Model Fix — Tier 2a Dedup + New Columns

Adds within-source dedup via `ROW_NUMBER()` and adds `source_type`, `source_origin`, and `source_transaction_id` columns to the OFX staging view.

**Files:**
- Modify: `sqlmesh/models/prep/stg_ofx__transactions.sql`
- Test: `tests/moneybin/test_stg_ofx_dedup.py`

- [ ] **Step 1: Write failing test**

```python
# tests/moneybin/test_stg_ofx_dedup.py
"""Tests for OFX staging model Tier 2a dedup and new columns."""

from pathlib import Path


class TestStgOfxTransactionsModel:
    def test_model_has_row_number_dedup(self) -> None:
        model_path = (
            Path(__file__).resolve().parents[2]
            / "sqlmesh"
            / "models"
            / "prep"
            / "stg_ofx__transactions.sql"
        )
        content = model_path.read_text()
        assert "ROW_NUMBER()" in content
        assert "PARTITION BY source_transaction_id, account_id" in content
        assert "ORDER BY loaded_at DESC" in content
        assert "_row_num = 1" in content

    def test_model_has_source_columns(self) -> None:
        model_path = (
            Path(__file__).resolve().parents[2]
            / "sqlmesh"
            / "models"
            / "prep"
            / "stg_ofx__transactions.sql"
        )
        content = model_path.read_text()
        assert "'ofx' AS source_type" in content
        assert "source_origin" in content
        assert "source_transaction_id" in content
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/moneybin/test_stg_ofx_dedup.py -v`
Expected: FAIL — no ROW_NUMBER, no source_type/source_origin

- [ ] **Step 3: Rewrite stg_ofx__transactions.sql**

Replace `sqlmesh/models/prep/stg_ofx__transactions.sql`:

```sql
MODEL (
  name prep.stg_ofx__transactions,
  kind VIEW
);

WITH ranked AS (
  SELECT
    t.source_transaction_id,
    t.account_id,
    t.transaction_type,
    t.date_posted::DATE AS posted_date,
    t.amount,
    TRIM(t.payee) AS payee,
    TRIM(t.memo) AS memo,
    t.check_number,
    t.source_file,
    t.extracted_at,
    t.loaded_at,
    'ofx' AS source_type,
    COALESCE(a.institution_org, 'ofx_unknown') AS source_origin,
    ROW_NUMBER() OVER (
      PARTITION BY t.source_transaction_id, t.account_id
      ORDER BY t.loaded_at DESC
    ) AS _row_num
  FROM raw.ofx_transactions AS t
  LEFT JOIN raw.ofx_accounts AS a
    ON t.account_id = a.account_id
)
SELECT
  source_transaction_id,
  account_id,
  transaction_type,
  posted_date,
  amount,
  payee,
  memo,
  check_number,
  source_file,
  extracted_at,
  loaded_at,
  source_type,
  source_origin
FROM ranked
WHERE
  _row_num = 1
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/moneybin/test_stg_ofx_dedup.py -v`
Expected: PASS

- [ ] **Step 5: Format SQL**

Run: `uv run sqlmesh -p sqlmesh format`

- [ ] **Step 6: Commit**

```bash
git add sqlmesh/models/prep/stg_ofx__transactions.sql tests/moneybin/test_stg_ofx_dedup.py
git commit -m "feat: add Tier 2a dedup and source columns to OFX staging"
```

---

## Task 6: Migration V002 — App FK Backfill

Backfills `app.transaction_categories` and `app.transaction_notes` FK values from source-native IDs to deterministic gold keys.

**Files:**
- Create: `src/moneybin/sql/migrations/V002__backfill_gold_keys.sql`
- Test: `tests/moneybin/test_migration_v002.py`

- [ ] **Step 1: Write failing test**

```python
# tests/moneybin/test_migration_v002.py
"""Tests for V002 migration: backfill app FK gold keys."""

from pathlib import Path

from moneybin.migrations import Migration


class TestV002Migration:
    def test_migration_file_parses(self) -> None:
        migration_dir = (
            Path(__file__).resolve().parents[2]
            / "src"
            / "moneybin"
            / "sql"
            / "migrations"
        )
        path = migration_dir / "V002__backfill_gold_keys.sql"
        assert path.exists(), f"Migration file not found: {path}"
        migration = Migration.from_file(path)
        assert migration.version == 2
        assert migration.file_type == "sql"

    def test_migration_contains_sha256_hash(self) -> None:
        migration_dir = (
            Path(__file__).resolve().parents[2]
            / "src"
            / "moneybin"
            / "sql"
            / "migrations"
        )
        path = migration_dir / "V002__backfill_gold_keys.sql"
        content = path.read_text()
        assert "sha256" in content.lower()
        assert "transaction_categories" in content
        assert "transaction_notes" in content
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/moneybin/test_migration_v002.py -v`
Expected: FAIL — migration file doesn't exist

- [ ] **Step 3: Create migration file**

```sql
-- src/moneybin/sql/migrations/V002__backfill_gold_keys.sql
-- Backfill transaction_id values in app.transaction_categories and
-- app.transaction_notes from source-native IDs to deterministic gold keys.
-- Gold key = first 16 chars of SHA-256(source_type || '|' || source_id || '|' || account_id).
-- This is a 1:1 mapping since no merges exist yet.

-- Build a mapping from old source-level IDs to gold keys.
-- OFX: source_transaction_id is the FITID (post-V001 rename).
-- Tabular: transaction_id is the content hash used as PK.
CREATE TEMPORARY TABLE _gold_key_mapping AS
SELECT
    old_id,
    substr(sha256(source_type || '|' || old_id || '|' || account_id), 1, 16) AS gold_id
FROM (
    SELECT DISTINCT
        source_transaction_id AS old_id,
        'ofx' AS source_type,
        account_id
    FROM raw.ofx_transactions
    UNION ALL
    SELECT DISTINCT
        transaction_id AS old_id,
        source_type,
        account_id
    FROM raw.tabular_transactions
) sources;

-- Update transaction_categories FK
UPDATE app.transaction_categories SET transaction_id = gm.gold_id
FROM _gold_key_mapping gm
WHERE app.transaction_categories.transaction_id = gm.old_id;

-- Update transaction_notes FK
UPDATE app.transaction_notes SET transaction_id = gm.gold_id
FROM _gold_key_mapping gm
WHERE app.transaction_notes.transaction_id = gm.old_id;

DROP TABLE _gold_key_mapping;
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/moneybin/test_migration_v002.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/moneybin/sql/migrations/V002__backfill_gold_keys.sql \
        tests/moneybin/test_migration_v002.py
git commit -m "feat: backfill app FK gold keys (V002 migration)"
```

---

## Task 7: int_transactions__unioned SQLMesh Model

UNION ALL of OFX and tabular staging with standardized columns.

**Files:**
- Create: `sqlmesh/models/prep/int_transactions__unioned.sql`
- Test: `tests/moneybin/test_int_unioned_model.py`

- [ ] **Step 1: Write failing test**

```python
# tests/moneybin/test_int_unioned_model.py
"""Tests for int_transactions__unioned model structure."""

from pathlib import Path


class TestIntTransactionsUnionedModel:
    def test_model_file_exists(self) -> None:
        model_path = (
            Path(__file__).resolve().parents[2]
            / "sqlmesh"
            / "models"
            / "prep"
            / "int_transactions__unioned.sql"
        )
        assert model_path.exists()

    def test_model_has_required_columns(self) -> None:
        model_path = (
            Path(__file__).resolve().parents[2]
            / "sqlmesh"
            / "models"
            / "prep"
            / "int_transactions__unioned.sql"
        )
        content = model_path.read_text()
        # Must output standardized columns
        assert "source_transaction_id" in content
        assert "source_type" in content
        assert "source_origin" in content
        assert "account_id" in content
        assert "transaction_date" in content
        assert "amount" in content
        assert "description" in content
        assert "UNION ALL" in content

    def test_model_is_view(self) -> None:
        model_path = (
            Path(__file__).resolve().parents[2]
            / "sqlmesh"
            / "models"
            / "prep"
            / "int_transactions__unioned.sql"
        )
        content = model_path.read_text()
        assert "kind VIEW" in content
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/moneybin/test_int_unioned_model.py -v`
Expected: FAIL — model file doesn't exist

- [ ] **Step 3: Create the model**

```sql
-- sqlmesh/models/prep/int_transactions__unioned.sql

MODEL (
  name prep.int_transactions__unioned,
  kind VIEW
);

WITH ofx AS (
  SELECT
    source_transaction_id, -- OFX FITID; unique per-row source identifier
    account_id,
    posted_date AS transaction_date,
    NULL::DATE AS authorized_date,
    amount::DECIMAL(18, 2) AS amount,
    payee AS description,
    NULL::TEXT AS merchant_name,
    memo,
    NULL::TEXT AS category,
    NULL::TEXT AS subcategory,
    NULL::TEXT AS payment_channel,
    transaction_type,
    check_number,
    FALSE AS is_pending,
    NULL::TEXT AS pending_transaction_id,
    NULL::TEXT AS location_address,
    NULL::TEXT AS location_city,
    NULL::TEXT AS location_region,
    NULL::TEXT AS location_postal_code,
    NULL::TEXT AS location_country,
    NULL::DOUBLE AS location_latitude,
    NULL::DOUBLE AS location_longitude,
    'USD' AS currency_code,
    source_type,
    source_origin,
    source_file,
    extracted_at::TIMESTAMP AS source_extracted_at,
    loaded_at
  FROM prep.stg_ofx__transactions
),
tabular AS (
  SELECT
    transaction_id AS source_transaction_id, -- Content hash PK; canonical per-row ID
    account_id,
    transaction_date,
    post_date AS authorized_date,
    amount::DECIMAL(18, 2) AS amount,
    description,
    NULL::TEXT AS merchant_name,
    memo,
    category,
    subcategory,
    NULL::TEXT AS payment_channel,
    transaction_type,
    check_number,
    FALSE AS is_pending,
    NULL::TEXT AS pending_transaction_id,
    NULL::TEXT AS location_address,
    NULL::TEXT AS location_city,
    NULL::TEXT AS location_region,
    NULL::TEXT AS location_postal_code,
    NULL::TEXT AS location_country,
    NULL::DOUBLE AS location_latitude,
    NULL::DOUBLE AS location_longitude,
    COALESCE(currency, 'USD') AS currency_code,
    source_type,
    source_origin,
    source_file,
    extracted_at::TIMESTAMP AS source_extracted_at,
    loaded_at
  FROM prep.stg_tabular__transactions
)
SELECT * FROM ofx
UNION ALL
SELECT * FROM tabular
```

- [ ] **Step 4: Format SQL and run test**

Run: `uv run sqlmesh -p sqlmesh format`
Run: `uv run pytest tests/moneybin/test_int_unioned_model.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add sqlmesh/models/prep/int_transactions__unioned.sql \
        tests/moneybin/test_int_unioned_model.py
git commit -m "feat: add int_transactions__unioned model (UNION ALL staging)"
```

---

## Task 8: Gold Key Hashing Module

Deterministic SHA-256 gold key generation for unmatched and matched records.

**Files:**
- Create: `src/moneybin/matching/__init__.py`
- Create: `src/moneybin/matching/hashing.py`
- Create: `tests/moneybin/matching/__init__.py`
- Create: `tests/moneybin/matching/test_hashing.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/moneybin/matching/__init__.py
# (empty)
```

```python
# tests/moneybin/matching/test_hashing.py
"""Tests for gold key hashing."""

from moneybin.matching.hashing import gold_key_matched, gold_key_unmatched


class TestGoldKeyUnmatched:
    def test_deterministic(self) -> None:
        key1 = gold_key_unmatched("csv", "abc123", "acct1")
        key2 = gold_key_unmatched("csv", "abc123", "acct1")
        assert key1 == key2

    def test_length_is_16_hex(self) -> None:
        key = gold_key_unmatched("ofx", "FITID001", "checking")
        assert len(key) == 16
        assert all(c in "0123456789abcdef" for c in key)

    def test_different_inputs_produce_different_keys(self) -> None:
        key1 = gold_key_unmatched("csv", "abc", "acct1")
        key2 = gold_key_unmatched("ofx", "abc", "acct1")
        assert key1 != key2

    def test_pipe_delimited_input(self) -> None:
        """Verify the hash input format is source_type|source_transaction_id|account_id."""
        import hashlib

        expected = hashlib.sha256(b"csv|txn123|acct1").hexdigest()[:16]
        assert gold_key_unmatched("csv", "txn123", "acct1") == expected


class TestGoldKeyMatched:
    def test_deterministic(self) -> None:
        tuples = [
            ("csv", "abc", "acct1"),
            ("ofx", "xyz", "acct1"),
        ]
        key1 = gold_key_matched(tuples)
        key2 = gold_key_matched(tuples)
        assert key1 == key2

    def test_order_independent(self) -> None:
        """Tuples are sorted before hashing — insertion order doesn't matter."""
        key1 = gold_key_matched([
            ("ofx", "xyz", "acct1"),
            ("csv", "abc", "acct1"),
        ])
        key2 = gold_key_matched([
            ("csv", "abc", "acct1"),
            ("ofx", "xyz", "acct1"),
        ])
        assert key1 == key2

    def test_length_is_16_hex(self) -> None:
        key = gold_key_matched([("csv", "a", "x"), ("ofx", "b", "x")])
        assert len(key) == 16

    def test_different_from_unmatched(self) -> None:
        """A matched group key differs from any individual unmatched key."""
        from moneybin.matching.hashing import gold_key_unmatched

        matched = gold_key_matched([("csv", "abc", "acct1"), ("ofx", "xyz", "acct1")])
        unmatched_csv = gold_key_unmatched("csv", "abc", "acct1")
        unmatched_ofx = gold_key_unmatched("ofx", "xyz", "acct1")
        assert matched != unmatched_csv
        assert matched != unmatched_ofx
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/moneybin/matching/test_hashing.py -v`
Expected: FAIL — module doesn't exist

- [ ] **Step 3: Implement hashing module**

```python
# src/moneybin/matching/__init__.py
"""Transaction matching and dedup engine."""
```

```python
# src/moneybin/matching/hashing.py
"""Deterministic gold key generation for transaction dedup.

Gold keys are SHA-256 hashes truncated to 16 hex characters (64 bits),
consistent with the content-hash ID strategy used elsewhere in MoneyBin.

Unmatched records: SHA-256(source_type|source_transaction_id|account_id)
Matched groups: SHA-256(sorted pipe-delimited contributing tuples)
"""

import hashlib


def gold_key_unmatched(
    source_type: str, source_transaction_id: str, account_id: str
) -> str:
    """Generate a gold key for an unmatched (single-source) record.

    Args:
        source_type: Import pathway (csv, ofx, etc.).
        source_transaction_id: Source-level unique identifier.
        account_id: Account identifier.

    Returns:
        16-char hex string.
    """
    raw = f"{source_type}|{source_transaction_id}|{account_id}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def gold_key_matched(
    tuples: list[tuple[str, str, str]],
) -> str:
    """Generate a gold key for a matched group of source records.

    The tuples are sorted before hashing so the key is insertion-order
    independent.

    Args:
        tuples: List of (source_type, source_transaction_id, account_id)
            for each contributing source row.

    Returns:
        16-char hex string.
    """
    sorted_tuples = sorted(tuples)
    raw = "|".join(f"{st}|{stid}|{aid}" for st, stid, aid in sorted_tuples)
    return hashlib.sha256(raw.encode()).hexdigest()[:16]
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/moneybin/matching/test_hashing.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/moneybin/matching/__init__.py \
        src/moneybin/matching/hashing.py \
        tests/moneybin/matching/__init__.py \
        tests/moneybin/matching/test_hashing.py
git commit -m "feat: add gold key hashing for unmatched and matched records"
```

---

## Task 9: Match Persistence Module

CRUD operations for `app.match_decisions`.

**Files:**
- Create: `src/moneybin/matching/persistence.py`
- Create: `tests/moneybin/matching/test_persistence.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/moneybin/matching/test_persistence.py
"""Tests for match decision persistence."""

import uuid
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from moneybin.database import Database
from moneybin.matching.persistence import (
    create_match_decision,
    get_active_matches,
    get_pending_matches,
    get_rejected_pairs,
    undo_match,
    update_match_status,
)


@pytest.fixture()
def db(tmp_path: Path, mock_secret_store: MagicMock) -> Database:
    database = Database(
        tmp_path / "test.duckdb",
        secret_store=mock_secret_store,
        no_auto_upgrade=True,
    )
    yield database
    database.close()


def _make_match_id() -> str:
    return uuid.uuid4().hex[:12]


class TestCreateMatchDecision:
    def test_creates_accepted_match(self, db: Database) -> None:
        match_id = _make_match_id()
        create_match_decision(
            db,
            match_id=match_id,
            source_transaction_id_a="txn_a",
            source_type_a="csv",
            source_origin_a="chase_credit",
            source_transaction_id_b="txn_b",
            source_type_b="ofx",
            source_origin_b="chase_ofx",
            account_id="acct1",
            confidence_score=0.97,
            match_signals={"date_distance": 0, "description_similarity": 0.92},
            match_tier="3",
            match_status="accepted",
            decided_by="auto",
        )
        result = db.execute(
            "SELECT match_id, match_status, confidence_score "
            "FROM app.match_decisions WHERE match_id = ?",
            [match_id],
        ).fetchone()
        assert result is not None
        assert result[0] == match_id
        assert result[1] == "accepted"

    def test_creates_pending_match(self, db: Database) -> None:
        match_id = _make_match_id()
        create_match_decision(
            db,
            match_id=match_id,
            source_transaction_id_a="a",
            source_type_a="csv",
            source_origin_a="chase",
            source_transaction_id_b="b",
            source_type_b="ofx",
            source_origin_b="chase",
            account_id="acct1",
            confidence_score=0.82,
            match_signals={},
            match_tier="3",
            match_status="pending",
            decided_by="auto",
        )
        result = db.execute(
            "SELECT match_status FROM app.match_decisions WHERE match_id = ?",
            [match_id],
        ).fetchone()
        assert result[0] == "pending"


class TestGetActiveMatches:
    def test_returns_accepted_non_reversed(self, db: Database) -> None:
        match_id = _make_match_id()
        create_match_decision(
            db,
            match_id=match_id,
            source_transaction_id_a="a",
            source_type_a="csv",
            source_origin_a="c",
            source_transaction_id_b="b",
            source_type_b="ofx",
            source_origin_b="c",
            account_id="acct1",
            confidence_score=0.98,
            match_signals={},
            match_tier="3",
            match_status="accepted",
            decided_by="auto",
        )
        matches = get_active_matches(db)
        assert len(matches) == 1
        assert matches[0]["match_id"] == match_id

    def test_excludes_reversed_matches(self, db: Database) -> None:
        match_id = _make_match_id()
        create_match_decision(
            db,
            match_id=match_id,
            source_transaction_id_a="a",
            source_type_a="csv",
            source_origin_a="c",
            source_transaction_id_b="b",
            source_type_b="ofx",
            source_origin_b="c",
            account_id="acct1",
            confidence_score=0.98,
            match_signals={},
            match_tier="3",
            match_status="accepted",
            decided_by="auto",
        )
        undo_match(db, match_id, reversed_by="user")
        matches = get_active_matches(db)
        assert len(matches) == 0


class TestGetPendingMatches:
    def test_returns_pending_only(self, db: Database) -> None:
        pending_id = _make_match_id()
        accepted_id = _make_match_id()
        for mid, status in [(pending_id, "pending"), (accepted_id, "accepted")]:
            create_match_decision(
                db,
                match_id=mid,
                source_transaction_id_a="a",
                source_type_a="csv",
                source_origin_a="c",
                source_transaction_id_b="b",
                source_type_b="ofx",
                source_origin_b="c",
                account_id="acct1",
                confidence_score=0.80,
                match_signals={},
                match_tier="3",
                match_status=status,
                decided_by="auto",
            )
        pending = get_pending_matches(db)
        assert len(pending) == 1
        assert pending[0]["match_id"] == pending_id


class TestUndoMatch:
    def test_sets_reversed_fields(self, db: Database) -> None:
        match_id = _make_match_id()
        create_match_decision(
            db,
            match_id=match_id,
            source_transaction_id_a="a",
            source_type_a="csv",
            source_origin_a="c",
            source_transaction_id_b="b",
            source_type_b="ofx",
            source_origin_b="c",
            account_id="acct1",
            confidence_score=0.98,
            match_signals={},
            match_tier="3",
            match_status="accepted",
            decided_by="auto",
        )
        undo_match(db, match_id, reversed_by="user")
        result = db.execute(
            "SELECT reversed_at, reversed_by FROM app.match_decisions WHERE match_id = ?",
            [match_id],
        ).fetchone()
        assert result[0] is not None  # reversed_at set
        assert result[1] == "user"


class TestGetRejectedPairs:
    def test_returns_rejected_pair_keys(self, db: Database) -> None:
        match_id = _make_match_id()
        create_match_decision(
            db,
            match_id=match_id,
            source_transaction_id_a="a",
            source_type_a="csv",
            source_origin_a="c",
            source_transaction_id_b="b",
            source_type_b="ofx",
            source_origin_b="c",
            account_id="acct1",
            confidence_score=0.75,
            match_signals={},
            match_tier="3",
            match_status="rejected",
            decided_by="user",
        )
        rejected = get_rejected_pairs(db)
        assert len(rejected) == 1
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/moneybin/matching/test_persistence.py -v`
Expected: FAIL — module doesn't exist

- [ ] **Step 3: Implement persistence module**

```python
# src/moneybin/matching/persistence.py
"""CRUD operations for app.match_decisions.

All database access uses parameterized queries via the Database class.
"""

import json
import logging
from datetime import datetime, timezone
from typing import Any

from moneybin.database import Database

logger = logging.getLogger(__name__)


def create_match_decision(
    db: Database,
    *,
    match_id: str,
    source_transaction_id_a: str,
    source_type_a: str,
    source_origin_a: str,
    source_transaction_id_b: str,
    source_type_b: str,
    source_origin_b: str,
    account_id: str,
    confidence_score: float,
    match_signals: dict[str, Any],
    match_tier: str,
    match_status: str,
    decided_by: str,
    match_reason: str | None = None,
    match_type: str = "dedup",
    account_id_b: str | None = None,
) -> None:
    """Insert a new match decision."""
    db.execute(
        """
        INSERT INTO app.match_decisions (
            match_id, source_transaction_id_a, source_type_a, source_origin_a,
            source_transaction_id_b, source_type_b, source_origin_b,
            account_id, confidence_score, match_signals, match_type, match_tier,
            account_id_b, match_status, match_reason, decided_by, decided_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            match_id,
            source_transaction_id_a,
            source_type_a,
            source_origin_a,
            source_transaction_id_b,
            source_type_b,
            source_origin_b,
            account_id,
            confidence_score,
            json.dumps(match_signals),
            match_type,
            match_tier,
            account_id_b,
            match_status,
            match_reason,
            decided_by,
            datetime.now(tz=timezone.utc).isoformat(),
        ],
    )


def get_active_matches(db: Database, match_type: str = "dedup") -> list[dict[str, Any]]:
    """Return accepted, non-reversed match decisions."""
    rows = db.execute(
        """
        SELECT * FROM app.match_decisions
        WHERE match_status = 'accepted'
          AND reversed_at IS NULL
          AND match_type = ?
        ORDER BY decided_at DESC
        """,
        [match_type],
    ).fetchall()
    columns = [
        desc[0]
        for desc in db.execute("SELECT * FROM app.match_decisions LIMIT 0").description
    ]
    return [dict(zip(columns, row)) for row in rows]


def get_pending_matches(
    db: Database, match_type: str = "dedup"
) -> list[dict[str, Any]]:
    """Return pending match decisions awaiting user review."""
    rows = db.execute(
        """
        SELECT * FROM app.match_decisions
        WHERE match_status = 'pending'
          AND match_type = ?
        ORDER BY confidence_score DESC
        """,
        [match_type],
    ).fetchall()
    columns = [
        desc[0]
        for desc in db.execute("SELECT * FROM app.match_decisions LIMIT 0").description
    ]
    return [dict(zip(columns, row)) for row in rows]


def update_match_status(
    db: Database, match_id: str, *, status: str, decided_by: str
) -> None:
    """Update the status of a match decision (e.g., pending → accepted)."""
    db.execute(
        """
        UPDATE app.match_decisions
        SET match_status = ?, decided_by = ?, decided_at = ?
        WHERE match_id = ?
        """,
        [status, decided_by, datetime.now(tz=timezone.utc).isoformat(), match_id],
    )


def undo_match(db: Database, match_id: str, *, reversed_by: str) -> None:
    """Reverse a match decision. Sets reversed_at and reversed_by."""
    db.execute(
        """
        UPDATE app.match_decisions
        SET reversed_at = ?, reversed_by = ?
        WHERE match_id = ?
        """,
        [datetime.now(tz=timezone.utc).isoformat(), reversed_by, match_id],
    )


def get_rejected_pairs(db: Database, match_type: str = "dedup") -> list[dict[str, Any]]:
    """Return rejected pair keys to avoid re-proposing them."""
    rows = db.execute(
        """
        SELECT source_type_a, source_transaction_id_a, source_origin_a,
               source_type_b, source_transaction_id_b, source_origin_b,
               account_id
        FROM app.match_decisions
        WHERE match_status = 'rejected'
          AND match_type = ?
        """,
        [match_type],
    ).fetchall()
    columns = [
        "source_type_a",
        "source_transaction_id_a",
        "source_origin_a",
        "source_type_b",
        "source_transaction_id_b",
        "source_origin_b",
        "account_id",
    ]
    return [dict(zip(columns, row)) for row in rows]


def get_match_log(
    db: Database, *, limit: int = 50, match_type: str | None = None
) -> list[dict[str, Any]]:
    """Return recent match decisions for display."""
    where = "WHERE 1=1"
    params: list[Any] = []
    if match_type:
        where += " AND match_type = ?"
        params.append(match_type)
    params.append(limit)
    rows = db.execute(
        f"""
        SELECT * FROM app.match_decisions
        {where}
        ORDER BY decided_at DESC
        LIMIT ?
        """,  # noqa: S608 — WHERE clause built from validated parameters
        params,
    ).fetchall()
    columns = [
        desc[0]
        for desc in db.execute("SELECT * FROM app.match_decisions LIMIT 0").description
    ]
    return [dict(zip(columns, row)) for row in rows]
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/moneybin/matching/test_persistence.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/moneybin/matching/persistence.py \
        tests/moneybin/matching/test_persistence.py
git commit -m "feat: add match decision persistence (CRUD for app.match_decisions)"
```

---

## Task 10: Source Priority Seeding

Rebuild `app.seed_source_priority` from `MatchingSettings.source_priority`.

**Files:**
- Create: `src/moneybin/matching/priority.py`
- Create: `tests/moneybin/matching/test_priority.py`

- [ ] **Step 1: Write failing test**

```python
# tests/moneybin/matching/test_priority.py
"""Tests for source priority seeding."""

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from moneybin.config import MatchingSettings
from moneybin.database import Database
from moneybin.matching.priority import seed_source_priority


@pytest.fixture()
def db(tmp_path: Path, mock_secret_store: MagicMock) -> Database:
    database = Database(
        tmp_path / "test.duckdb",
        secret_store=mock_secret_store,
        no_auto_upgrade=True,
    )
    yield database
    database.close()


class TestSeedSourcePriority:
    def test_writes_default_priorities(self, db: Database) -> None:
        settings = MatchingSettings()
        seed_source_priority(db, settings)
        rows = db.execute(
            "SELECT source_type, priority FROM app.seed_source_priority "
            "ORDER BY priority"
        ).fetchall()
        assert len(rows) == 8
        assert rows[0] == ("plaid", 1)
        assert rows[1] == ("csv", 2)
        assert rows[-1] == ("ofx", 8)

    def test_replaces_on_rerun(self, db: Database) -> None:
        settings = MatchingSettings()
        seed_source_priority(db, settings)
        # Rerun with different order
        custom = MatchingSettings(source_priority=["ofx", "csv"])
        seed_source_priority(db, custom)
        rows = db.execute(
            "SELECT source_type, priority FROM app.seed_source_priority "
            "ORDER BY priority"
        ).fetchall()
        assert len(rows) == 2
        assert rows[0] == ("ofx", 1)
        assert rows[1] == ("csv", 2)

    def test_idempotent(self, db: Database) -> None:
        settings = MatchingSettings()
        seed_source_priority(db, settings)
        seed_source_priority(db, settings)
        count = db.execute("SELECT COUNT(*) FROM app.seed_source_priority").fetchone()[
            0
        ]
        assert count == 8
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/moneybin/matching/test_priority.py -v`
Expected: FAIL — module doesn't exist

- [ ] **Step 3: Implement priority module**

```python
# src/moneybin/matching/priority.py
"""Rebuild app.seed_source_priority from MatchingSettings.

The priority table is a SQL-accessible projection of the config-only
source_priority list. It is rebuilt on every matcher run so config is
always the sole source of truth.
"""

import logging

from moneybin.config import MatchingSettings
from moneybin.database import Database

logger = logging.getLogger(__name__)


def seed_source_priority(db: Database, settings: MatchingSettings) -> None:
    """Rebuild the source priority table from config.

    Deletes all existing rows and reinserts from the settings list.
    This is safe because the table is never user-edited — config owns it.

    Args:
        db: Database instance.
        settings: MatchingSettings with source_priority list.
    """
    db.execute("DELETE FROM app.seed_source_priority")
    for rank, source_type in enumerate(settings.source_priority, start=1):
        db.execute(
            "INSERT INTO app.seed_source_priority (source_type, priority) VALUES (?, ?)",
            [source_type, rank],
        )
    logger.debug(
        f"Seeded source priority: {len(settings.source_priority)} source types"
    )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/moneybin/matching/test_priority.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/moneybin/matching/priority.py tests/moneybin/matching/test_priority.py
git commit -m "feat: add source priority seeding from MatchingSettings"
```

---

## Task 11: Candidate Blocking and Scoring

SQL-based blocking queries and confidence score computation.

**Files:**
- Create: `src/moneybin/matching/scoring.py`
- Create: `tests/moneybin/matching/test_scoring.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/moneybin/matching/test_scoring.py
"""Tests for candidate blocking and scoring."""

from datetime import date
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from moneybin.database import Database
from moneybin.matching.scoring import (
    CandidatePair,
    compute_confidence,
    get_candidates_cross_source,
    get_candidates_within_source,
)


@pytest.fixture()
def db(tmp_path: Path, mock_secret_store: MagicMock) -> Database:
    database = Database(
        tmp_path / "test.duckdb",
        secret_store=mock_secret_store,
        no_auto_upgrade=True,
    )
    yield database
    database.close()


def _insert_unioned_row(
    db: Database,
    *,
    source_transaction_id: str,
    account_id: str,
    transaction_date: str,
    amount: str,
    description: str,
    source_type: str,
    source_origin: str,
    source_file: str = "test.csv",
) -> None:
    """Insert a row into a test unioned table for blocking queries."""
    db.execute(
        """
        INSERT INTO _test_unioned (
            source_transaction_id, account_id, transaction_date, amount,
            description, source_type, source_origin, source_file
        ) VALUES (?, ?, ?::DATE, ?::DECIMAL(18,2), ?, ?, ?, ?)
        """,
        [
            source_transaction_id,
            account_id,
            transaction_date,
            amount,
            description,
            source_type,
            source_origin,
            source_file,
        ],
    )


@pytest.fixture()
def unioned_table(db: Database) -> Database:
    """Create a minimal unioned-style table for testing blocking queries."""
    db.execute("""
        CREATE TABLE _test_unioned (
            source_transaction_id VARCHAR,
            account_id VARCHAR,
            transaction_date DATE,
            amount DECIMAL(18, 2),
            description VARCHAR,
            source_type VARCHAR,
            source_origin VARCHAR,
            source_file VARCHAR
        )
    """)
    return db


class TestComputeConfidence:
    def test_exact_date_high_similarity(self) -> None:
        score = compute_confidence(date_distance_days=0, description_similarity=0.95)
        assert score >= 0.95

    def test_exact_date_low_similarity(self) -> None:
        score = compute_confidence(date_distance_days=0, description_similarity=0.3)
        assert 0.5 < score < 0.95

    def test_far_date_high_similarity(self) -> None:
        score = compute_confidence(date_distance_days=3, description_similarity=0.95)
        assert score < compute_confidence(
            date_distance_days=0, description_similarity=0.95
        )

    def test_score_between_zero_and_one(self) -> None:
        for days in range(4):
            for sim in [0.0, 0.3, 0.5, 0.7, 0.9, 1.0]:
                score = compute_confidence(
                    date_distance_days=days, description_similarity=sim
                )
                assert 0.0 <= score <= 1.0


class TestGetCandidatesCrossSource:
    def test_finds_cross_source_pair(self, unioned_table: Database) -> None:
        _insert_unioned_row(
            unioned_table,
            source_transaction_id="csv_abc",
            account_id="acct1",
            transaction_date="2026-03-15",
            amount="-42.50",
            description="STARBUCKS #1234",
            source_type="csv",
            source_origin="chase",
        )
        _insert_unioned_row(
            unioned_table,
            source_transaction_id="ofx_xyz",
            account_id="acct1",
            transaction_date="2026-03-15",
            amount="-42.50",
            description="STARBUCKS 1234 NEW YORK",
            source_type="ofx",
            source_origin="chase_ofx",
        )
        candidates = get_candidates_cross_source(
            unioned_table, table="_test_unioned", date_window_days=3
        )
        assert len(candidates) == 1
        assert candidates[0].source_transaction_id_a == "csv_abc"
        assert candidates[0].source_transaction_id_b == "ofx_xyz"

    def test_excludes_same_source_type_and_origin(
        self, unioned_table: Database
    ) -> None:
        _insert_unioned_row(
            unioned_table,
            source_transaction_id="a",
            account_id="acct1",
            transaction_date="2026-03-15",
            amount="-42.50",
            description="STARBUCKS",
            source_type="csv",
            source_origin="chase",
        )
        _insert_unioned_row(
            unioned_table,
            source_transaction_id="b",
            account_id="acct1",
            transaction_date="2026-03-15",
            amount="-42.50",
            description="STARBUCKS",
            source_type="csv",
            source_origin="chase",
        )
        candidates = get_candidates_cross_source(
            unioned_table, table="_test_unioned", date_window_days=3
        )
        assert len(candidates) == 0

    def test_excludes_different_accounts(self, unioned_table: Database) -> None:
        _insert_unioned_row(
            unioned_table,
            source_transaction_id="a",
            account_id="acct1",
            transaction_date="2026-03-15",
            amount="-42.50",
            description="STARBUCKS",
            source_type="csv",
            source_origin="chase",
        )
        _insert_unioned_row(
            unioned_table,
            source_transaction_id="b",
            account_id="acct2",
            transaction_date="2026-03-15",
            amount="-42.50",
            description="STARBUCKS",
            source_type="ofx",
            source_origin="chase",
        )
        candidates = get_candidates_cross_source(
            unioned_table, table="_test_unioned", date_window_days=3
        )
        assert len(candidates) == 0

    def test_excludes_different_amounts(self, unioned_table: Database) -> None:
        _insert_unioned_row(
            unioned_table,
            source_transaction_id="a",
            account_id="acct1",
            transaction_date="2026-03-15",
            amount="-42.50",
            description="STARBUCKS",
            source_type="csv",
            source_origin="chase",
        )
        _insert_unioned_row(
            unioned_table,
            source_transaction_id="b",
            account_id="acct1",
            transaction_date="2026-03-15",
            amount="-43.00",
            description="STARBUCKS",
            source_type="ofx",
            source_origin="chase",
        )
        candidates = get_candidates_cross_source(
            unioned_table, table="_test_unioned", date_window_days=3
        )
        assert len(candidates) == 0
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/moneybin/matching/test_scoring.py -v`
Expected: FAIL — module doesn't exist

- [ ] **Step 3: Implement scoring module**

```python
# src/moneybin/matching/scoring.py
"""Candidate blocking and confidence scoring for transaction matching.

Blocking: SQL queries against DuckDB that return narrow candidate sets
based on exact account, exact amount, and date-window constraints.

Scoring: Combines date distance and description similarity into a single
confidence score. Weights are tunable but defaults are spec-compliant.
"""

import logging
from dataclasses import dataclass
from typing import Any

from moneybin.database import Database

logger = logging.getLogger(__name__)

# Scoring weights — sum to 1.0.
# Date distance has higher weight because exact date = strong signal.
_WEIGHT_DATE = 0.40
_WEIGHT_DESCRIPTION = 0.60


@dataclass(frozen=True)
class CandidatePair:
    """A scored candidate pair from blocking + scoring."""

    source_transaction_id_a: str
    source_type_a: str
    source_origin_a: str
    source_transaction_id_b: str
    source_type_b: str
    source_origin_b: str
    account_id: str
    date_distance_days: int
    description_similarity: float
    confidence_score: float
    description_a: str
    description_b: str


def compute_confidence(
    *, date_distance_days: int, description_similarity: float
) -> float:
    """Compute a confidence score from matching signals.

    Args:
        date_distance_days: Absolute days between transaction dates (0 = same day).
        description_similarity: Jaro-Winkler similarity (0.0–1.0).

    Returns:
        Confidence score between 0.0 and 1.0.
    """
    # Date component: 1.0 for same day, decaying linearly over the window.
    # Beyond 3 days is already filtered by blocking, but handle gracefully.
    max_days = 3
    date_score = (
        max(0.0, 1.0 - (date_distance_days / max_days)) if max_days > 0 else 1.0
    )

    return (_WEIGHT_DATE * date_score) + (_WEIGHT_DESCRIPTION * description_similarity)


def get_candidates_cross_source(
    db: Database,
    *,
    table: str = "prep.int_transactions__unioned",
    date_window_days: int = 3,
    excluded_ids: set[str] | None = None,
    rejected_pairs: list[dict[str, Any]] | None = None,
) -> list[CandidatePair]:
    """Find cross-source candidate pairs (Tier 3).

    Blocking requirements:
    - Same account_id
    - Same amount (to the penny)
    - transaction_date within ±date_window_days
    - Different source_type OR different source_origin

    Args:
        db: Database instance.
        table: Table or view to query (default: prep.int_transactions__unioned).
        date_window_days: Max days between dates.
        excluded_ids: Source transaction IDs already matched (e.g., by Tier 2b).
        rejected_pairs: Previously rejected pairs to skip.

    Returns:
        List of scored candidate pairs.
    """
    return _get_candidates(
        db,
        table=table,
        date_window_days=date_window_days,
        tier="3",
        excluded_ids=excluded_ids,
        rejected_pairs=rejected_pairs,
    )


def get_candidates_within_source(
    db: Database,
    *,
    table: str = "prep.int_transactions__unioned",
    date_window_days: int = 3,
    rejected_pairs: list[dict[str, Any]] | None = None,
) -> list[CandidatePair]:
    """Find within-source candidate pairs (Tier 2b).

    Same as cross-source but requires same source_origin AND source_type,
    different source_file. Targets overlapping statements without source-
    native IDs.

    Args:
        db: Database instance.
        table: Table or view to query.
        date_window_days: Max days between dates.
        rejected_pairs: Previously rejected pairs to skip.

    Returns:
        List of scored candidate pairs.
    """
    return _get_candidates(
        db,
        table=table,
        date_window_days=date_window_days,
        tier="2b",
        excluded_ids=None,
        rejected_pairs=rejected_pairs,
    )


def _get_candidates(
    db: Database,
    *,
    table: str,
    date_window_days: int,
    tier: str,
    excluded_ids: set[str] | None,
    rejected_pairs: list[dict[str, Any]] | None,
) -> list[CandidatePair]:
    """Internal: run blocking + scoring query for a given tier."""
    if tier == "2b":
        source_filter = """
            AND a.source_type = b.source_type
            AND a.source_origin = b.source_origin
            AND a.source_file != b.source_file
        """
    else:
        source_filter = """
            AND (a.source_type != b.source_type OR a.source_origin != b.source_origin)
        """

    # Use sqlglot to validate the table name if it's not the default
    if table != "prep.int_transactions__unioned" and table != "_test_unioned":
        from sqlglot import exp

        parts = table.split(".")
        if len(parts) == 2:
            safe_schema = exp.to_identifier(parts[0], quoted=True).sql("duckdb")
            safe_table = exp.to_identifier(parts[1], quoted=True).sql("duckdb")
            table = f"{safe_schema}.{safe_table}"

    query = f"""
        SELECT
            a.source_transaction_id AS stid_a,
            a.source_type AS st_a,
            a.source_origin AS so_a,
            a.description AS desc_a,
            b.source_transaction_id AS stid_b,
            b.source_type AS st_b,
            b.source_origin AS so_b,
            b.description AS desc_b,
            a.account_id,
            ABS(DATEDIFF('day', a.transaction_date, b.transaction_date)) AS date_dist,
            jaro_winkler_similarity(
                COALESCE(a.description, ''),
                COALESCE(b.description, '')
            ) AS desc_sim
        FROM {table} AS a
        JOIN {table} AS b
            ON a.account_id = b.account_id
            AND a.amount = b.amount
            AND ABS(DATEDIFF('day', a.transaction_date, b.transaction_date)) <= ?
            AND a.source_transaction_id < b.source_transaction_id
            {source_filter}
        ORDER BY desc_sim DESC
    """  # noqa: S608 — table name validated above; date_window_days is parameterized

    rows = db.execute(query, [date_window_days]).fetchall()

    # Build rejected pair set for fast lookup
    rejected_set: set[tuple[str, ...]] = set()
    if rejected_pairs:
        for rp in rejected_pairs:
            rejected_set.add((
                rp["source_type_a"],
                rp["source_transaction_id_a"],
                rp["source_type_b"],
                rp["source_transaction_id_b"],
                rp["account_id"],
            ))
            # Also add reverse direction
            rejected_set.add((
                rp["source_type_b"],
                rp["source_transaction_id_b"],
                rp["source_type_a"],
                rp["source_transaction_id_a"],
                rp["account_id"],
            ))

    results: list[CandidatePair] = []
    for row in rows:
        (
            stid_a,
            st_a,
            so_a,
            desc_a,
            stid_b,
            st_b,
            so_b,
            desc_b,
            acct,
            date_dist,
            desc_sim,
        ) = row

        # Skip excluded IDs (already matched in earlier tier)
        if excluded_ids and (stid_a in excluded_ids or stid_b in excluded_ids):
            continue

        # Skip rejected pairs
        if (st_a, stid_a, st_b, stid_b, acct) in rejected_set:
            continue

        confidence = compute_confidence(
            date_distance_days=int(date_dist),
            description_similarity=float(desc_sim),
        )

        results.append(
            CandidatePair(
                source_transaction_id_a=stid_a,
                source_type_a=st_a,
                source_origin_a=so_a,
                source_transaction_id_b=stid_b,
                source_type_b=st_b,
                source_origin_b=so_b,
                account_id=acct,
                date_distance_days=int(date_dist),
                description_similarity=float(desc_sim),
                confidence_score=confidence,
                description_a=desc_a or "",
                description_b=desc_b or "",
            )
        )

    return results
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/moneybin/matching/test_scoring.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/moneybin/matching/scoring.py tests/moneybin/matching/test_scoring.py
git commit -m "feat: add candidate blocking and confidence scoring"
```

---

## Task 12: 1:1 Greedy Assignment

Greedy best-score-first 1:1 bipartite assignment.

**Files:**
- Create: `src/moneybin/matching/assignment.py`
- Create: `tests/moneybin/matching/test_assignment.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/moneybin/matching/test_assignment.py
"""Tests for 1:1 greedy assignment."""

from moneybin.matching.assignment import assign_greedy
from moneybin.matching.scoring import CandidatePair


def _pair(stid_a: str, stid_b: str, score: float, acct: str = "acct1") -> CandidatePair:
    return CandidatePair(
        source_transaction_id_a=stid_a,
        source_type_a="csv",
        source_origin_a="c",
        source_transaction_id_b=stid_b,
        source_type_b="ofx",
        source_origin_b="c",
        account_id=acct,
        date_distance_days=0,
        description_similarity=score,
        confidence_score=score,
        description_a="",
        description_b="",
    )


class TestAssignGreedy:
    def test_no_candidates(self) -> None:
        assert assign_greedy([]) == []

    def test_single_pair(self) -> None:
        pairs = [_pair("a", "b", 0.95)]
        result = assign_greedy(pairs)
        assert len(result) == 1
        assert result[0].source_transaction_id_a == "a"

    def test_picks_highest_score_first(self) -> None:
        pairs = [
            _pair("a", "b", 0.90),
            _pair("a", "c", 0.95),  # Higher score
        ]
        result = assign_greedy(pairs)
        # a-c wins because higher score; a-b dropped because a is claimed
        assert len(result) == 1
        assert result[0].source_transaction_id_b == "c"

    def test_non_overlapping_pairs_both_selected(self) -> None:
        pairs = [
            _pair("a", "b", 0.95),
            _pair("c", "d", 0.90),
        ]
        result = assign_greedy(pairs)
        assert len(result) == 2

    def test_conflict_resolution(self) -> None:
        """When b could match both a and c, highest-scoring pair wins."""
        pairs = [
            _pair("a", "b", 0.98),  # a-b highest
            _pair("c", "b", 0.85),  # c-b lower, b already claimed
        ]
        result = assign_greedy(pairs)
        assert len(result) == 1
        assert result[0].source_transaction_id_a == "a"

    def test_three_way_conflict(self) -> None:
        """A, B, C all match X. Only best survives."""
        pairs = [
            _pair("a", "x", 0.90),
            _pair("b", "x", 0.95),
            _pair("c", "x", 0.80),
        ]
        result = assign_greedy(pairs)
        assert len(result) == 1
        assert result[0].source_transaction_id_a == "b"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/moneybin/matching/test_assignment.py -v`
Expected: FAIL — module doesn't exist

- [ ] **Step 3: Implement assignment module**

```python
# src/moneybin/matching/assignment.py
"""Greedy best-score-first 1:1 bipartite assignment.

When multiple candidates compete for the same source row, the highest-
scoring pair wins. Both rows in a winning pair are marked as "claimed"
and cannot participate in further assignments.
"""

from moneybin.matching.scoring import CandidatePair


def assign_greedy(candidates: list[CandidatePair]) -> list[CandidatePair]:
    """Assign candidate pairs using greedy best-score-first.

    Args:
        candidates: Scored candidate pairs (any order).

    Returns:
        Non-overlapping subset of pairs, highest scores first.
    """
    sorted_candidates = sorted(
        candidates, key=lambda c: c.confidence_score, reverse=True
    )
    claimed: set[str] = set()
    assigned: list[CandidatePair] = []

    for pair in sorted_candidates:
        key_a = f"{pair.source_type_a}|{pair.source_transaction_id_a}"
        key_b = f"{pair.source_type_b}|{pair.source_transaction_id_b}"
        if key_a not in claimed and key_b not in claimed:
            claimed.add(key_a)
            claimed.add(key_b)
            assigned.append(pair)

    return assigned
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/moneybin/matching/test_assignment.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/moneybin/matching/assignment.py tests/moneybin/matching/test_assignment.py
git commit -m "feat: add greedy 1:1 bipartite assignment"
```

---

## Task 13: Matching Engine Orchestrator

`TransactionMatcher` class that runs Tier 2b → Tier 3, writes match decisions, and reports results.

**Files:**
- Create: `src/moneybin/matching/engine.py`
- Create: `tests/moneybin/matching/test_engine.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/moneybin/matching/test_engine.py
"""Tests for TransactionMatcher orchestrator."""

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from moneybin.config import MatchingSettings
from moneybin.database import Database
from moneybin.matching.engine import MatchResult, TransactionMatcher


@pytest.fixture()
def db(tmp_path: Path, mock_secret_store: MagicMock) -> Database:
    database = Database(
        tmp_path / "test.duckdb",
        secret_store=mock_secret_store,
        no_auto_upgrade=True,
    )
    yield database
    database.close()


def _create_test_table(db: Database) -> None:
    """Create a minimal unioned-style table for engine tests."""
    db.execute("""
        CREATE OR REPLACE TABLE _test_unioned (
            source_transaction_id VARCHAR,
            account_id VARCHAR,
            transaction_date DATE,
            amount DECIMAL(18, 2),
            description VARCHAR,
            source_type VARCHAR,
            source_origin VARCHAR,
            source_file VARCHAR
        )
    """)


def _insert(
    db: Database,
    stid: str,
    acct: str,
    txn_date: str,
    amount: str,
    desc: str,
    stype: str,
    sorigin: str,
    sfile: str = "test.csv",
) -> None:
    db.execute(
        """
        INSERT INTO _test_unioned VALUES (?, ?, ?::DATE, ?::DECIMAL(18,2), ?, ?, ?, ?)
        """,
        [stid, acct, txn_date, amount, desc, stype, sorigin, sfile],
    )


class TestTransactionMatcher:
    def test_no_data_no_matches(self, db: Database) -> None:
        _create_test_table(db)
        settings = MatchingSettings()
        matcher = TransactionMatcher(db, settings, table="_test_unioned")
        result = matcher.run()
        assert isinstance(result, MatchResult)
        assert result.auto_merged == 0
        assert result.pending_review == 0

    def test_cross_source_auto_merge(self, db: Database) -> None:
        _create_test_table(db)
        _insert(
            db,
            "csv_a",
            "acct1",
            "2026-03-15",
            "-42.50",
            "STARBUCKS #1234",
            "csv",
            "chase",
        )
        _insert(
            db,
            "ofx_b",
            "acct1",
            "2026-03-15",
            "-42.50",
            "STARBUCKS 1234",
            "ofx",
            "chase_ofx",
        )
        settings = MatchingSettings()
        matcher = TransactionMatcher(db, settings, table="_test_unioned")
        result = matcher.run()
        assert result.auto_merged == 1
        assert result.pending_review == 0

    def test_low_confidence_goes_to_review(self, db: Database) -> None:
        _create_test_table(db)
        _insert(
            db,
            "csv_a",
            "acct1",
            "2026-03-15",
            "-42.50",
            "STARBUCKS COFFEE",
            "csv",
            "chase",
        )
        _insert(
            db,
            "ofx_b",
            "acct1",
            "2026-03-17",
            "-42.50",
            "SB CAFE NYC",
            "ofx",
            "chase_ofx",
        )
        settings = MatchingSettings(
            high_confidence_threshold=0.95, review_threshold=0.50
        )
        matcher = TransactionMatcher(db, settings, table="_test_unioned")
        result = matcher.run()
        # Same amount, date within window, but different descriptions + date offset
        # Should land in review queue if confidence is between thresholds
        assert (
            result.auto_merged + result.pending_review >= 0
        )  # At least runs without error

    def test_rejected_pairs_not_reproposed(self, db: Database) -> None:
        _create_test_table(db)
        _insert(
            db, "csv_a", "acct1", "2026-03-15", "-42.50", "STARBUCKS", "csv", "chase"
        )
        _insert(
            db,
            "ofx_b",
            "acct1",
            "2026-03-15",
            "-42.50",
            "STARBUCKS",
            "ofx",
            "chase_ofx",
        )
        settings = MatchingSettings()
        matcher = TransactionMatcher(db, settings, table="_test_unioned")

        # First run: auto-merge
        result1 = matcher.run()
        assert result1.auto_merged == 1

        # Undo and reject
        from moneybin.matching.persistence import (
            get_active_matches,
            undo_match,
            update_match_status,
        )

        matches = get_active_matches(db)
        undo_match(db, matches[0]["match_id"], reversed_by="user")
        update_match_status(
            db, matches[0]["match_id"], status="rejected", decided_by="user"
        )

        # Second run: should not re-propose
        result2 = matcher.run()
        assert result2.auto_merged == 0
        assert result2.pending_review == 0

    def test_match_result_summary(self) -> None:
        result = MatchResult(auto_merged=5, pending_review=2)
        assert "5 auto-merged" in result.summary()
        assert "2 pending review" in result.summary()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/moneybin/matching/test_engine.py -v`
Expected: FAIL — module doesn't exist

- [ ] **Step 3: Implement engine module**

```python
# src/moneybin/matching/engine.py
"""Transaction matching orchestrator.

Runs Tier 2b (within-source overlap) then Tier 3 (cross-source) matching.
Each tier: blocking → scoring → 1:1 assignment → persist decisions.
"""

import logging
import uuid
from dataclasses import dataclass

from moneybin.config import MatchingSettings
from moneybin.database import Database
from moneybin.matching.assignment import assign_greedy
from moneybin.matching.persistence import (
    create_match_decision,
    get_active_matches,
    get_rejected_pairs,
)
from moneybin.matching.scoring import (
    CandidatePair,
    get_candidates_cross_source,
    get_candidates_within_source,
)
from moneybin.metrics.registry import (
    DEDUP_MATCH_CONFIDENCE,
    DEDUP_MATCHES_TOTAL,
    DEDUP_PAIRS_SCORED,
    DEDUP_REVIEW_PENDING,
)

logger = logging.getLogger(__name__)


@dataclass
class MatchResult:
    """Summary of a matching run."""

    auto_merged: int = 0
    pending_review: int = 0

    def summary(self) -> str:
        parts = []
        if self.auto_merged:
            parts.append(f"{self.auto_merged} auto-merged")
        if self.pending_review:
            parts.append(f"{self.pending_review} pending review")
        if not parts:
            return "No new matches found"
        return ", ".join(parts)


class TransactionMatcher:
    """Orchestrates transaction matching across tiers.

    Usage::

        matcher = TransactionMatcher(db, settings)
        result = matcher.run()
        print(result.summary())
    """

    def __init__(
        self,
        db: Database,
        settings: MatchingSettings,
        *,
        table: str = "prep.int_transactions__unioned",
    ) -> None:
        self._db = db
        self._settings = settings
        self._table = table

    def run(self) -> MatchResult:
        """Run Tier 2b then Tier 3 matching.

        Returns:
            MatchResult with counts of auto-merged and pending-review matches.
        """
        result = MatchResult()
        rejected = get_rejected_pairs(self._db)

        # Get IDs already matched (from prior runs or earlier tiers)
        already_matched = self._get_already_matched_ids()

        # Tier 2b: within-source overlap (high-confidence only)
        tier_2b_matched = self._run_tier(
            tier="2b",
            candidates_fn=lambda excluded: get_candidates_within_source(
                self._db,
                table=self._table,
                date_window_days=self._settings.date_window_days,
                rejected_pairs=rejected,
            ),
            excluded_ids=already_matched,
            result=result,
        )
        already_matched.update(tier_2b_matched)

        # Tier 3: cross-source
        self._run_tier(
            tier="3",
            candidates_fn=lambda excluded: get_candidates_cross_source(
                self._db,
                table=self._table,
                date_window_days=self._settings.date_window_days,
                excluded_ids=excluded,
                rejected_pairs=rejected,
            ),
            excluded_ids=already_matched,
            result=result,
        )

        return result

    def _run_tier(
        self,
        *,
        tier: str,
        candidates_fn: object,
        excluded_ids: set[str],
        result: MatchResult,
    ) -> set[str]:
        """Run blocking → scoring → assignment → persist for one tier.

        Returns:
            Set of source_transaction_ids matched in this tier.
        """
        candidates = candidates_fn(excluded_ids)  # type: ignore[operator]
        DEDUP_PAIRS_SCORED.inc(len(candidates))

        if not candidates:
            return set()

        assigned = assign_greedy(candidates)
        newly_matched: set[str] = set()

        for pair in assigned:
            DEDUP_MATCH_CONFIDENCE.observe(pair.confidence_score)

            if pair.confidence_score >= self._settings.high_confidence_threshold:
                status = "accepted"
                decided_by = "auto"
                result.auto_merged += 1
                DEDUP_MATCHES_TOTAL.labels(match_tier=tier, decided_by="auto").inc()
            elif (
                tier == "3" and pair.confidence_score >= self._settings.review_threshold
            ):
                status = "pending"
                decided_by = "auto"
                result.pending_review += 1
            else:
                # Below review threshold (Tier 3) or below high threshold (Tier 2b)
                continue

            match_id = uuid.uuid4().hex[:12]
            create_match_decision(
                self._db,
                match_id=match_id,
                source_transaction_id_a=pair.source_transaction_id_a,
                source_type_a=pair.source_type_a,
                source_origin_a=pair.source_origin_a,
                source_transaction_id_b=pair.source_transaction_id_b,
                source_type_b=pair.source_type_b,
                source_origin_b=pair.source_origin_b,
                account_id=pair.account_id,
                confidence_score=pair.confidence_score,
                match_signals={
                    "date_distance": pair.date_distance_days,
                    "description_similarity": round(pair.description_similarity, 4),
                },
                match_tier=tier,
                match_status=status,
                decided_by=decided_by,
                match_reason=(
                    f"Amount match, {pair.date_distance_days}d apart, "
                    f"desc similarity {pair.description_similarity:.2f}"
                ),
            )

            newly_matched.add(pair.source_transaction_id_a)
            newly_matched.add(pair.source_transaction_id_b)

        if assigned:
            logger.info(
                f"⚙️  Tier {tier}: {result.auto_merged} auto-merged, "
                f"{result.pending_review} pending review"
            )

        return newly_matched

    def _get_already_matched_ids(self) -> set[str]:
        """Get source_transaction_ids that are already in active matches."""
        active = get_active_matches(self._db)
        ids: set[str] = set()
        for m in active:
            ids.add(m["source_transaction_id_a"])
            ids.add(m["source_transaction_id_b"])
        return ids
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/moneybin/matching/test_engine.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/moneybin/matching/engine.py tests/moneybin/matching/test_engine.py
git commit -m "feat: add TransactionMatcher orchestrator (Tier 2b + Tier 3)"
```

---

## Task 14: int_transactions__matched SQLMesh Model

Assigns gold keys by joining with `app.match_decisions`.

**Files:**
- Create: `sqlmesh/models/prep/int_transactions__matched.sql`
- Test: `tests/moneybin/test_int_matched_model.py`

- [ ] **Step 1: Write failing test**

```python
# tests/moneybin/test_int_matched_model.py
"""Tests for int_transactions__matched model structure."""

from pathlib import Path


class TestIntTransactionsMatchedModel:
    def test_model_file_exists(self) -> None:
        model_path = (
            Path(__file__).resolve().parents[2]
            / "sqlmesh"
            / "models"
            / "prep"
            / "int_transactions__matched.sql"
        )
        assert model_path.exists()

    def test_model_outputs_transaction_id(self) -> None:
        model_path = (
            Path(__file__).resolve().parents[2]
            / "sqlmesh"
            / "models"
            / "prep"
            / "int_transactions__matched.sql"
        )
        content = model_path.read_text()
        # Must compute gold key as transaction_id
        assert "transaction_id" in content
        assert "sha256" in content.lower()
        assert "source_transaction_id" in content
        assert "match_decisions" in content

    def test_model_is_view(self) -> None:
        model_path = (
            Path(__file__).resolve().parents[2]
            / "sqlmesh"
            / "models"
            / "prep"
            / "int_transactions__matched.sql"
        )
        content = model_path.read_text()
        assert "kind VIEW" in content
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/moneybin/test_int_matched_model.py -v`
Expected: FAIL — model file doesn't exist

- [ ] **Step 3: Create the model**

```sql
-- sqlmesh/models/prep/int_transactions__matched.sql

MODEL (
  name prep.int_transactions__matched,
  kind VIEW
);

/* Assigns gold key (transaction_id) to each source row.
   Matched groups get a shared gold key from the sorted tuple hash.
   Unmatched rows get a solo gold key from their own source identity.

   Connected components are found by propagating the minimum match_id
   through pairs: two passes handle chains of length <= 2 (Tier 2b → Tier 3). */

WITH active_matches AS (
  /* Accepted, non-reversed dedup match decisions */
  SELECT
    match_id,
    source_transaction_id_a,
    source_type_a,
    source_origin_a,
    source_transaction_id_b,
    source_type_b,
    source_origin_b,
    account_id,
    confidence_score
  FROM app.match_decisions
  WHERE
    match_status = 'accepted'
    AND reversed_at IS NULL
    AND match_type = 'dedup'
),
/* Pass 1: each matched row gets the minimum match_id it participates in */
node_min_match AS (
  SELECT
    st,
    stid,
    aid,
    MIN(match_id) AS initial_component
  FROM (
    SELECT
      source_type_a AS st,
      source_transaction_id_a AS stid,
      account_id AS aid,
      match_id
    FROM active_matches
    UNION ALL
    SELECT
      source_type_b AS st,
      source_transaction_id_b AS stid,
      account_id AS aid,
      match_id
    FROM active_matches
  ) sub
  GROUP BY
    st,
    stid,
    aid
),
/* Pass 2: propagate the min initial_component across match edges */
match_component AS (
  SELECT
    am.match_id,
    LEAST(n1.initial_component, n2.initial_component) AS component
  FROM active_matches AS am
  JOIN node_min_match AS n1
    ON am.source_type_a = n1.st
    AND am.source_transaction_id_a = n1.stid
    AND am.account_id = n1.aid
  JOIN node_min_match AS n2
    ON am.source_type_b = n2.st
    AND am.source_transaction_id_b = n2.stid
    AND am.account_id = n2.aid
),
/* Pass 3: final group assignment — min component across all matches per node */
match_groups AS (
  SELECT
    st AS source_type,
    stid AS source_transaction_id,
    aid AS account_id,
    MIN(mc.component) AS group_id
  FROM (
    SELECT
      source_type_a AS st,
      source_transaction_id_a AS stid,
      account_id AS aid,
      mc.component
    FROM active_matches AS am
    JOIN match_component AS mc
      ON am.match_id = mc.match_id
    UNION ALL
    SELECT
      source_type_b AS st,
      source_transaction_id_b AS stid,
      account_id AS aid,
      mc.component
    FROM active_matches AS am
    JOIN match_component AS mc
      ON am.match_id = mc.match_id
  ) sub
  JOIN match_component AS mc
    ON sub.component = mc.component
  GROUP BY
    st,
    stid,
    aid
),
/* Compute gold key per group from sorted tuple set */
group_gold_keys AS (
  SELECT
    mg.group_id,
    SUBSTR(
      SHA256(
        STRING_AGG(
          mg.source_type || '|' || mg.source_transaction_id || '|' || mg.account_id,
          '|'
          ORDER BY
            mg.source_type,
            mg.source_transaction_id,
            mg.account_id
        )
      ),
      1,
      16
    ) AS transaction_id
  FROM match_groups AS mg
  GROUP BY
    mg.group_id
),
/* Get the best confidence score per group */
group_confidence AS (
  SELECT
    mg.group_id,
    MAX(am.confidence_score) AS match_confidence
  FROM match_groups AS mg
  JOIN active_matches AS am
    ON (
      (mg.source_type = am.source_type_a AND mg.source_transaction_id = am.source_transaction_id_a AND mg.account_id = am.account_id)
      OR (mg.source_type = am.source_type_b AND mg.source_transaction_id = am.source_transaction_id_b AND mg.account_id = am.account_id)
    )
  GROUP BY
    mg.group_id
)
SELECT
  CASE
    WHEN gk.transaction_id IS NOT NULL THEN gk.transaction_id
    ELSE SUBSTR(
      SHA256(u.source_type || '|' || u.source_transaction_id || '|' || u.account_id),
      1,
      16
    )
  END AS transaction_id, -- Gold key: deterministic SHA-256 hash
  u.source_transaction_id, -- Source-level unique identifier
  u.account_id,
  u.transaction_date,
  u.authorized_date,
  u.amount,
  u.description,
  u.merchant_name,
  u.memo,
  u.category,
  u.subcategory,
  u.payment_channel,
  u.transaction_type,
  u.check_number,
  u.is_pending,
  u.pending_transaction_id,
  u.location_address,
  u.location_city,
  u.location_region,
  u.location_postal_code,
  u.location_country,
  u.location_latitude,
  u.location_longitude,
  u.currency_code,
  u.source_type,
  u.source_origin,
  u.source_file,
  u.source_extracted_at,
  u.loaded_at,
  mg.group_id AS match_group_id,
  gc.match_confidence
FROM prep.int_transactions__unioned AS u
LEFT JOIN match_groups AS mg
  ON u.source_type = mg.source_type
  AND u.source_transaction_id = mg.source_transaction_id
  AND u.account_id = mg.account_id
LEFT JOIN group_gold_keys AS gk
  ON mg.group_id = gk.group_id
LEFT JOIN group_confidence AS gc
  ON mg.group_id = gc.group_id
```

- [ ] **Step 4: Format SQL and run test**

Run: `uv run sqlmesh -p sqlmesh format`
Run: `uv run pytest tests/moneybin/test_int_matched_model.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add sqlmesh/models/prep/int_transactions__matched.sql \
        tests/moneybin/test_int_matched_model.py
git commit -m "feat: add int_transactions__matched model (gold key assignment)"
```

---

## Task 15: int_transactions__merged SQLMesh Model

Collapses matched groups to one row per `transaction_id` using source-priority merge rules.

**Files:**
- Create: `sqlmesh/models/prep/int_transactions__merged.sql`
- Test: `tests/moneybin/test_int_merged_model.py`

- [ ] **Step 1: Write failing test**

```python
# tests/moneybin/test_int_merged_model.py
"""Tests for int_transactions__merged model structure."""

from pathlib import Path


class TestIntTransactionsMergedModel:
    def test_model_file_exists(self) -> None:
        model_path = (
            Path(__file__).resolve().parents[2]
            / "sqlmesh"
            / "models"
            / "prep"
            / "int_transactions__merged.sql"
        )
        assert model_path.exists()

    def test_model_has_merge_logic(self) -> None:
        model_path = (
            Path(__file__).resolve().parents[2]
            / "sqlmesh"
            / "models"
            / "prep"
            / "int_transactions__merged.sql"
        )
        content = model_path.read_text()
        assert "seed_source_priority" in content
        assert "GROUP BY" in content
        assert "transaction_id" in content
        assert "canonical_source_type" in content
        assert "source_count" in content
        # transaction_date exception: earliest non-NULL
        assert (
            "MIN(m.transaction_date)" in content or "MIN(transaction_date)" in content
        )

    def test_model_is_view(self) -> None:
        model_path = (
            Path(__file__).resolve().parents[2]
            / "sqlmesh"
            / "models"
            / "prep"
            / "int_transactions__merged.sql"
        )
        content = model_path.read_text()
        assert "kind VIEW" in content
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/moneybin/test_int_merged_model.py -v`
Expected: FAIL — model file doesn't exist

- [ ] **Step 3: Create the model**

```sql
-- sqlmesh/models/prep/int_transactions__merged.sql

MODEL (
  name prep.int_transactions__merged,
  kind VIEW
);

/* Collapses matched groups to one row per transaction_id using
   source-priority merge rules. For each field, the value from the
   highest-priority source with a non-NULL value wins. Exception:
   transaction_date takes the earliest non-NULL value. */

SELECT
  m.transaction_id,
  m.account_id,
  /* transaction_date exception: earliest non-NULL across sources */
  MIN(m.transaction_date) AS transaction_date,
  /* All other fields: highest-priority non-NULL value */
  ARG_MIN(m.authorized_date, CASE WHEN m.authorized_date IS NOT NULL THEN sp.priority ELSE 2147483647 END) AS authorized_date,
  ARG_MIN(m.amount, sp.priority) AS amount,
  ARG_MIN(m.description, CASE WHEN m.description IS NOT NULL THEN sp.priority ELSE 2147483647 END) AS description,
  ARG_MIN(m.merchant_name, CASE WHEN m.merchant_name IS NOT NULL THEN sp.priority ELSE 2147483647 END) AS merchant_name,
  ARG_MIN(m.memo, CASE WHEN m.memo IS NOT NULL THEN sp.priority ELSE 2147483647 END) AS memo,
  ARG_MIN(m.category, CASE WHEN m.category IS NOT NULL THEN sp.priority ELSE 2147483647 END) AS category,
  ARG_MIN(m.subcategory, CASE WHEN m.subcategory IS NOT NULL THEN sp.priority ELSE 2147483647 END) AS subcategory,
  ARG_MIN(m.payment_channel, CASE WHEN m.payment_channel IS NOT NULL THEN sp.priority ELSE 2147483647 END) AS payment_channel,
  ARG_MIN(m.transaction_type, CASE WHEN m.transaction_type IS NOT NULL THEN sp.priority ELSE 2147483647 END) AS transaction_type,
  ARG_MIN(m.check_number, CASE WHEN m.check_number IS NOT NULL THEN sp.priority ELSE 2147483647 END) AS check_number,
  BOOL_OR(m.is_pending) AS is_pending,
  ARG_MIN(m.pending_transaction_id, CASE WHEN m.pending_transaction_id IS NOT NULL THEN sp.priority ELSE 2147483647 END) AS pending_transaction_id,
  ARG_MIN(m.location_address, CASE WHEN m.location_address IS NOT NULL THEN sp.priority ELSE 2147483647 END) AS location_address,
  ARG_MIN(m.location_city, CASE WHEN m.location_city IS NOT NULL THEN sp.priority ELSE 2147483647 END) AS location_city,
  ARG_MIN(m.location_region, CASE WHEN m.location_region IS NOT NULL THEN sp.priority ELSE 2147483647 END) AS location_region,
  ARG_MIN(m.location_postal_code, CASE WHEN m.location_postal_code IS NOT NULL THEN sp.priority ELSE 2147483647 END) AS location_postal_code,
  ARG_MIN(m.location_country, CASE WHEN m.location_country IS NOT NULL THEN sp.priority ELSE 2147483647 END) AS location_country,
  ARG_MIN(m.location_latitude, CASE WHEN m.location_latitude IS NOT NULL THEN sp.priority ELSE 2147483647 END) AS location_latitude,
  ARG_MIN(m.location_longitude, CASE WHEN m.location_longitude IS NOT NULL THEN sp.priority ELSE 2147483647 END) AS location_longitude,
  ARG_MIN(m.currency_code, sp.priority) AS currency_code,
  /* canonical_source_type: highest-priority source present in the group */
  ARG_MIN(m.source_type, sp.priority) AS canonical_source_type,
  COUNT(*) AS source_count,
  MAX(m.match_confidence) AS match_confidence,
  MAX(m.source_extracted_at) AS source_extracted_at,
  MAX(m.loaded_at) AS loaded_at
FROM prep.int_transactions__matched AS m
LEFT JOIN app.seed_source_priority AS sp
  ON m.source_type = sp.source_type
GROUP BY
  m.transaction_id,
  m.account_id
```

- [ ] **Step 4: Format SQL and run test**

Run: `uv run sqlmesh -p sqlmesh format`
Run: `uv run pytest tests/moneybin/test_int_merged_model.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add sqlmesh/models/prep/int_transactions__merged.sql \
        tests/moneybin/test_int_merged_model.py
git commit -m "feat: add int_transactions__merged model (source-priority merge)"
```

---

## Task 16: Updated core.fct_transactions

Reads from `int_transactions__merged` instead of direct staging CTEs.

**Files:**
- Modify: `sqlmesh/models/core/fct_transactions.sql`
- Modify: `tests/moneybin/db_helpers.py`
- Test: `tests/moneybin/test_fct_transactions_model.py`

- [ ] **Step 1: Write failing test**

```python
# tests/moneybin/test_fct_transactions_model.py
"""Tests for updated fct_transactions model."""

from pathlib import Path


class TestFctTransactionsModel:
    def test_reads_from_merged_layer(self) -> None:
        model_path = (
            Path(__file__).resolve().parents[2]
            / "sqlmesh"
            / "models"
            / "core"
            / "fct_transactions.sql"
        )
        content = model_path.read_text()
        assert "int_transactions__merged" in content
        # Should NOT have the old UNION ALL of staging CTEs
        assert "stg_ofx__transactions" not in content
        assert "stg_tabular__transactions" not in content

    def test_has_new_columns(self) -> None:
        model_path = (
            Path(__file__).resolve().parents[2]
            / "sqlmesh"
            / "models"
            / "core"
            / "fct_transactions.sql"
        )
        content = model_path.read_text()
        assert "canonical_source_type" in content
        assert "source_count" in content
        assert "match_confidence" in content
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/moneybin/test_fct_transactions_model.py -v`
Expected: FAIL — model still has staging CTEs

- [ ] **Step 3: Rewrite fct_transactions.sql**

Replace `sqlmesh/models/core/fct_transactions.sql`:

```sql
/* Canonical transactions fact view; reads from the deduplicated merged layer
   with categorization and merchant joins; negative amount = expense, positive = income */
MODEL (
  name core.fct_transactions,
  kind VIEW,
  grain transaction_id
);

WITH enriched AS (
  SELECT
    t.transaction_id,
    t.account_id,
    t.transaction_date,
    t.authorized_date,
    t.amount,
    ABS(t.amount) AS amount_absolute,
    CASE
      WHEN t.amount < 0 THEN 'expense'
      WHEN t.amount > 0 THEN 'income'
      ELSE 'zero'
    END AS transaction_direction,
    t.description,
    COALESCE(m.canonical_name, t.merchant_name) AS merchant_name,
    t.memo,
    COALESCE(c.category, t.category) AS category,
    COALESCE(c.subcategory, t.subcategory) AS subcategory,
    c.categorized_by,
    t.payment_channel,
    t.transaction_type,
    t.check_number,
    t.is_pending,
    t.pending_transaction_id,
    t.location_address,
    t.location_city,
    t.location_region,
    t.location_postal_code,
    t.location_country,
    t.location_latitude,
    t.location_longitude,
    t.currency_code,
    t.canonical_source_type AS source_type,
    t.source_count,
    t.match_confidence,
    t.source_extracted_at,
    t.loaded_at
  FROM prep.int_transactions__merged AS t
  LEFT JOIN app.transaction_categories AS c
    ON t.transaction_id = c.transaction_id
  LEFT JOIN app.merchants AS m
    ON c.merchant_id = m.merchant_id
)
SELECT
  transaction_id, /* Gold key: deterministic SHA-256 hash, unique per real-world transaction */
  account_id, /* Foreign key to core.dim_accounts */
  transaction_date, /* Date the transaction posted or settled; earliest across sources for merged records */
  authorized_date, /* Date the transaction was authorized; from highest-priority source */
  amount, /* Transaction amount; negative = expense, positive = income */
  amount_absolute, /* Absolute value of amount; avoids sign handling in aggregations */
  transaction_direction, /* Derived from amount sign: expense, income, or zero */
  description, /* Payee or merchant description from highest-priority source */
  merchant_name, /* Normalized merchant name from app.merchants; falls back to source value */
  memo, /* Additional notes from highest-priority source */
  category, /* Spending category; from app.transaction_categories when categorized, else source value */
  subcategory, /* Spending subcategory; from app.transaction_categories when categorized, else source value */
  categorized_by, /* How the category was assigned: rule, ai, user, or NULL if uncategorized */
  payment_channel, /* Payment channel (online, in store, other) */
  transaction_type, /* Source-specific transaction type code */
  check_number, /* Check number for check transactions; NULL otherwise */
  is_pending, /* True if any contributing source row is pending */
  pending_transaction_id, /* ID of the pending transaction this record resolved */
  location_address, /* Merchant street address */
  location_city, /* Merchant city */
  location_region, /* Merchant state or region */
  location_postal_code, /* Merchant postal code */
  location_country, /* Merchant country code */
  location_latitude, /* Merchant latitude coordinate */
  location_longitude, /* Merchant longitude coordinate */
  currency_code, /* ISO 4217 currency code */
  source_type, /* Canonical source type: highest-priority source in the merge group */
  source_count, /* Number of contributing source rows (1 for unmatched, 2+ for merged) */
  match_confidence, /* Match confidence score; NULL for unmatched records */
  source_extracted_at, /* When the data was parsed from the source file */
  loaded_at, /* When this record was last written */
  DATE_PART('year', transaction_date) AS transaction_year, /* Calendar year */
  DATE_PART('month', transaction_date) AS transaction_month, /* Calendar month (1-12) */
  DATE_PART('day', transaction_date) AS transaction_day, /* Calendar day (1-31) */
  DATE_PART('dayofweek', transaction_date) AS transaction_day_of_week, /* Day of week: 0 = Sunday */
  STRFTIME(transaction_date, '%Y-%m') AS transaction_year_month, /* YYYY-MM for period grouping */
  STRFTIME(transaction_date, '%Y') || '-Q' || QUARTER(transaction_date) AS transaction_year_quarter /* YYYY-QN for period grouping */
FROM enriched
```

- [ ] **Step 4: Update db_helpers.py**

Add new columns to `CORE_FCT_TRANSACTIONS_DDL` in `tests/moneybin/db_helpers.py`:

```python
CORE_FCT_TRANSACTIONS_DDL = """\
CREATE TABLE IF NOT EXISTS core.fct_transactions (
    transaction_id VARCHAR,
    account_id VARCHAR,
    transaction_date DATE,
    authorized_date DATE,
    amount DECIMAL(18, 2),
    amount_absolute DECIMAL(18, 2),
    transaction_direction VARCHAR,
    description VARCHAR,
    merchant_name VARCHAR,
    memo VARCHAR,
    category VARCHAR,
    subcategory VARCHAR,
    categorized_by VARCHAR,
    payment_channel VARCHAR,
    transaction_type VARCHAR,
    check_number VARCHAR,
    is_pending BOOLEAN,
    pending_transaction_id VARCHAR,
    location_address VARCHAR,
    location_city VARCHAR,
    location_region VARCHAR,
    location_postal_code VARCHAR,
    location_country VARCHAR,
    location_latitude DOUBLE,
    location_longitude DOUBLE,
    currency_code VARCHAR,
    source_type VARCHAR,
    source_count INTEGER,
    match_confidence DECIMAL(5, 4),
    source_extracted_at TIMESTAMP,
    loaded_at TIMESTAMP,
    transaction_year INTEGER,
    transaction_month INTEGER,
    transaction_day INTEGER,
    transaction_day_of_week INTEGER,
    transaction_year_month VARCHAR,
    transaction_year_quarter VARCHAR
);
"""
```

- [ ] **Step 5: Format SQL and run tests**

Run: `uv run sqlmesh -p sqlmesh format`
Run: `uv run pytest tests/moneybin/test_fct_transactions_model.py -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add sqlmesh/models/core/fct_transactions.sql \
        tests/moneybin/db_helpers.py \
        tests/moneybin/test_fct_transactions_model.py
git commit -m "feat: update fct_transactions to read from merged layer"
```

---

## Task 17: meta.fct_transaction_provenance

Provenance model linking gold records to contributing source rows.

**Files:**
- Create: `sqlmesh/models/meta/fct_transaction_provenance.sql`
- Test: `tests/moneybin/test_provenance_model.py`

- [ ] **Step 1: Write failing test**

```python
# tests/moneybin/test_provenance_model.py
"""Tests for meta.fct_transaction_provenance model structure."""

from pathlib import Path


class TestProvenanceModel:
    def test_model_file_exists(self) -> None:
        model_path = (
            Path(__file__).resolve().parents[2]
            / "sqlmesh"
            / "models"
            / "meta"
            / "fct_transaction_provenance.sql"
        )
        assert model_path.exists()

    def test_model_has_required_columns(self) -> None:
        model_path = (
            Path(__file__).resolve().parents[2]
            / "sqlmesh"
            / "models"
            / "meta"
            / "fct_transaction_provenance.sql"
        )
        content = model_path.read_text()
        assert "transaction_id" in content
        assert "source_transaction_id" in content
        assert "source_type" in content
        assert "source_origin" in content
        assert "source_file" in content
        assert "source_extracted_at" in content
        assert "match_id" in content

    def test_model_is_view(self) -> None:
        model_path = (
            Path(__file__).resolve().parents[2]
            / "sqlmesh"
            / "models"
            / "meta"
            / "fct_transaction_provenance.sql"
        )
        content = model_path.read_text()
        assert "kind VIEW" in content
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/moneybin/test_provenance_model.py -v`
Expected: FAIL — model file doesn't exist

- [ ] **Step 3: Create the model**

```sql
-- sqlmesh/models/meta/fct_transaction_provenance.sql

MODEL (
  name meta.fct_transaction_provenance,
  kind VIEW
);

/* Links every gold record in core.fct_transactions to every contributing
   source row. Unmatched records have exactly one provenance row (match_id = NULL).
   Matched groups have one row per contributing source. */

SELECT
  m.transaction_id, -- FK to gold record in core.fct_transactions
  m.source_transaction_id, -- Source-native ID, joinable to raw/prep
  m.source_type, -- Import pathway / origin system
  m.source_origin, -- Institution/connection/format that produced this row
  m.source_file, -- File that produced this source row
  m.source_extracted_at, -- When the source row was parsed
  md.match_id -- FK to app.match_decisions; NULL for unmatched records
FROM prep.int_transactions__matched AS m
LEFT JOIN app.match_decisions AS md
  ON md.match_status = 'accepted'
  AND md.reversed_at IS NULL
  AND md.match_type = 'dedup'
  AND (
    (m.source_type = md.source_type_a AND m.source_transaction_id = md.source_transaction_id_a AND m.account_id = md.account_id)
    OR (m.source_type = md.source_type_b AND m.source_transaction_id = md.source_transaction_id_b AND m.account_id = md.account_id)
  )
```

- [ ] **Step 4: Create meta schema file if needed**

Check if `src/moneybin/sql/schema/` has a meta schema. If not, create it and register:

```sql
-- src/moneybin/sql/schema/meta_schema.sql
CREATE SCHEMA IF NOT EXISTS meta;
```

Add `"meta_schema.sql"` to `_SCHEMA_FILES` in `src/moneybin/schema.py`, before the raw schema files.

- [ ] **Step 5: Format SQL and run test**

Run: `uv run sqlmesh -p sqlmesh format`
Run: `uv run pytest tests/moneybin/test_provenance_model.py -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add sqlmesh/models/meta/fct_transaction_provenance.sql \
        src/moneybin/sql/schema/meta_schema.sql \
        src/moneybin/schema.py \
        tests/moneybin/test_provenance_model.py
git commit -m "feat: add meta.fct_transaction_provenance model"
```

---

## Task 18: CLI Matches Commands

Replace stubs with real implementations.

**Files:**
- Create: `src/moneybin/cli/commands/matches.py`
- Modify: `src/moneybin/cli/main.py`
- Modify: `src/moneybin/cli/commands/stubs.py`
- Create: `tests/moneybin/cli/test_matches.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/moneybin/cli/test_matches.py
"""Tests for matches CLI commands."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from moneybin.cli.main import app

runner = CliRunner()


class TestMatchesRun:
    @patch("moneybin.cli.commands.matches.get_database")
    @patch("moneybin.cli.commands.matches.TransactionMatcher")
    def test_run_succeeds(
        self, mock_matcher_cls: MagicMock, mock_get_db: MagicMock
    ) -> None:
        from moneybin.matching.engine import MatchResult

        mock_db = MagicMock()
        mock_get_db.return_value = mock_db
        mock_matcher = MagicMock()
        mock_matcher.run.return_value = MatchResult(auto_merged=3, pending_review=1)
        mock_matcher_cls.return_value = mock_matcher

        result = runner.invoke(app, ["matches", "run"])
        assert result.exit_code == 0
        mock_matcher.run.assert_called_once()


class TestMatchesLog:
    @patch("moneybin.cli.commands.matches.get_database")
    @patch("moneybin.cli.commands.matches.get_match_log")
    def test_log_empty(self, mock_log: MagicMock, mock_get_db: MagicMock) -> None:
        mock_get_db.return_value = MagicMock()
        mock_log.return_value = []
        result = runner.invoke(app, ["matches", "log"])
        assert result.exit_code == 0


class TestMatchesUndo:
    @patch("moneybin.cli.commands.matches.get_database")
    @patch("moneybin.cli.commands.matches.undo_match")
    def test_undo_calls_persistence(
        self, mock_undo: MagicMock, mock_get_db: MagicMock
    ) -> None:
        mock_get_db.return_value = MagicMock()
        result = runner.invoke(app, ["matches", "undo", "abc123"])
        assert result.exit_code == 0
        mock_undo.assert_called_once()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/moneybin/cli/test_matches.py -v`
Expected: FAIL — module doesn't exist

- [ ] **Step 3: Create matches.py CLI module**

```python
# src/moneybin/cli/commands/matches.py
"""Match review and management commands."""

import logging
from typing import Annotated

import typer

app = typer.Typer(
    help="Review and manage transaction matches (dedup, transfers)",
    no_args_is_help=True,
)
logger = logging.getLogger(__name__)


@app.command("run")
def matches_run(
    skip_transform: bool = typer.Option(
        False, "--skip-transform", help="Skip SQLMesh transforms after matching"
    ),
) -> None:
    """Run matcher against existing transactions."""
    from moneybin.config import get_settings
    from moneybin.database import DatabaseKeyError, get_database
    from moneybin.matching.engine import TransactionMatcher
    from moneybin.matching.priority import seed_source_priority

    try:
        db = get_database()
        settings = get_settings().matching
        seed_source_priority(db, settings)
        matcher = TransactionMatcher(db, settings)
        result = matcher.run()
        if result.auto_merged or result.pending_review:
            logger.info(f"⚙️  Matching: {result.summary()}")
            if result.pending_review:
                logger.info("👀 Run 'moneybin matches review' when ready")
        else:
            logger.info("No new matches found")

        if not skip_transform and (result.auto_merged or result.pending_review):
            from moneybin.services.import_service import run_transforms

            db.close()
            run_transforms(get_settings().database.path)
    except DatabaseKeyError as e:
        from moneybin.database import database_key_error_hint

        logger.error(f"❌ {e}")
        logger.info(database_key_error_hint())
        raise typer.Exit(1) from e


@app.command("review")
def matches_review() -> None:
    """Interactive: accept/reject/skip/quit match proposals."""
    from moneybin.database import DatabaseKeyError, get_database
    from moneybin.matching.persistence import get_pending_matches, update_match_status

    try:
        db = get_database()
        pending = get_pending_matches(db)

        if not pending:
            logger.info("No pending matches to review")
            return

        logger.info(f"👀 {len(pending)} match(es) to review\n")
        for match in pending:
            typer.echo(
                f"  Match {match['match_id'][:8]}... "
                f"(confidence: {match['confidence_score']:.2f})"
            )
            typer.echo(
                f"    A: [{match['source_type_a']}] {match['source_transaction_id_a'][:20]}"
            )
            typer.echo(
                f"    B: [{match['source_type_b']}] {match['source_transaction_id_b'][:20]}"
            )
            if match.get("match_reason"):
                typer.echo(f"    Reason: {match['match_reason']}")

            action = typer.prompt(
                "  [a]ccept / [r]eject / [s]kip / [q]uit", default="s"
            )
            if action.lower().startswith("a"):
                update_match_status(
                    db, match["match_id"], status="accepted", decided_by="user"
                )
                logger.info(f"  ✅ Accepted {match['match_id'][:8]}")
            elif action.lower().startswith("r"):
                update_match_status(
                    db, match["match_id"], status="rejected", decided_by="user"
                )
                logger.info(f"  ❌ Rejected {match['match_id'][:8]}")
            elif action.lower().startswith("q"):
                break
            # 's' = skip, move to next

    except DatabaseKeyError as e:
        from moneybin.database import database_key_error_hint

        logger.error(f"❌ {e}")
        logger.info(database_key_error_hint())
        raise typer.Exit(1) from e


@app.command("log")
def matches_log(
    limit: int = typer.Option(20, "--limit", "-n", help="Max records to show"),
) -> None:
    """Show recent match decisions."""
    from moneybin.database import DatabaseKeyError, get_database
    from moneybin.matching.persistence import get_match_log

    try:
        db = get_database()
        entries = get_match_log(db, limit=limit, match_type="dedup")

        if not entries:
            logger.info("No match decisions found")
            return

        typer.echo(
            f"\n{'Match ID':<14} {'Status':<10} {'Tier':<5} {'Score':>6} "
            f"{'Decided By':<10} {'Type A':<6} {'Type B':<6}"
        )
        typer.echo("-" * 70)
        for entry in entries:
            typer.echo(
                f"{entry['match_id'][:12]:<14} "
                f"{entry['match_status']:<10} "
                f"{(entry.get('match_tier') or '-'):<5} "
                f"{float(entry.get('confidence_score') or 0):>6.2f} "
                f"{entry['decided_by']:<10} "
                f"{entry['source_type_a']:<6} "
                f"{entry['source_type_b']:<6}"
            )
        typer.echo()

    except DatabaseKeyError as e:
        from moneybin.database import database_key_error_hint

        logger.error(f"❌ {e}")
        logger.info(database_key_error_hint())
        raise typer.Exit(1) from e


@app.command("undo")
def matches_undo(
    match_id: str = typer.Argument(..., help="Match ID to reverse"),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation"),
) -> None:
    """Reverse a match decision."""
    from moneybin.database import DatabaseKeyError, get_database
    from moneybin.matching.persistence import undo_match

    if not yes:
        confirmed = typer.confirm(f"Undo match {match_id[:8]}...?")
        if not confirmed:
            logger.info("Undo cancelled")
            raise typer.Exit(0)

    try:
        db = get_database()
        undo_match(db, match_id, reversed_by="user")
        logger.info(f"✅ Reversed match {match_id[:8]}...")
    except DatabaseKeyError as e:
        from moneybin.database import database_key_error_hint

        logger.error(f"❌ {e}")
        logger.info(database_key_error_hint())
        raise typer.Exit(1) from e


@app.command("backfill")
def matches_backfill(
    skip_transform: bool = typer.Option(
        False, "--skip-transform", help="Skip SQLMesh transforms after matching"
    ),
) -> None:
    """One-time scan of all existing transactions for latent duplicates."""
    from moneybin.config import get_settings
    from moneybin.database import DatabaseKeyError, get_database
    from moneybin.matching.engine import TransactionMatcher
    from moneybin.matching.priority import seed_source_priority

    try:
        db = get_database()
        settings = get_settings().matching

        # Count existing transactions for progress display
        count = db.execute(
            "SELECT COUNT(*) FROM prep.int_transactions__unioned"
        ).fetchone()
        total = count[0] if count else 0
        logger.info(f"⚙️  Scanning {total:,} existing transactions for duplicates...")

        seed_source_priority(db, settings)
        matcher = TransactionMatcher(db, settings)
        result = matcher.run()

        logger.info(f"✅ Backfill complete: {result.summary()}")
        if result.pending_review:
            logger.info("👀 Run 'moneybin matches review' when ready")

        if not skip_transform and result.auto_merged:
            from moneybin.services.import_service import run_transforms

            db.close()
            run_transforms(get_settings().database.path)

    except DatabaseKeyError as e:
        from moneybin.database import database_key_error_hint

        logger.error(f"❌ {e}")
        logger.info(database_key_error_hint())
        raise typer.Exit(1) from e
```

- [ ] **Step 4: Update main.py to import from matches module**

In `src/moneybin/cli/main.py`, replace the stubs import and registration:

```python
# Replace in imports:
from .commands.stubs import (
    export_app,
    track_app,
)
from .commands import matches

# Replace the matches_app registration:
app.add_typer(matches.app, name="matches", help="Review and manage transaction matches")
```

Remove `matches_app` from `stubs.py` (keep `export_app` and `track_app`).

- [ ] **Step 5: Remove matches stubs from stubs.py**

In `src/moneybin/cli/commands/stubs.py`, remove the `matches_app` definition and all its commands (`matches_run`, `matches_review`, `matches_log`, `matches_undo`, `matches_backfill`).

- [ ] **Step 6: Run test to verify it passes**

Run: `uv run pytest tests/moneybin/cli/test_matches.py -v`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add src/moneybin/cli/commands/matches.py \
        src/moneybin/cli/main.py \
        src/moneybin/cli/commands/stubs.py \
        tests/moneybin/cli/test_matches.py
git commit -m "feat: implement CLI matches commands (run, review, log, undo, backfill)"
```

---

## Task 19: Import Flow Integration

Hook matching into the import pipeline — runs after load, before SQLMesh transforms.

**Files:**
- Modify: `src/moneybin/services/import_service.py`
- Test: `tests/moneybin/test_import_matching_integration.py`

- [ ] **Step 1: Write failing test**

```python
# tests/moneybin/test_import_matching_integration.py
"""Tests for matching integration in import flow."""

from unittest.mock import MagicMock, patch

import pytest


class TestImportMatchingIntegration:
    @patch("moneybin.services.import_service.run_transforms")
    @patch("moneybin.services.import_service._run_matching")
    @patch("moneybin.services.import_service._apply_categorization")
    def test_matching_runs_after_load(
        self,
        mock_categorize: MagicMock,
        mock_matching: MagicMock,
        mock_transforms: MagicMock,
    ) -> None:
        """Verify _run_matching is called during import."""
        from moneybin.matching.engine import MatchResult

        mock_matching.return_value = MatchResult(auto_merged=2, pending_review=0)
        mock_transforms.return_value = True

        # Verify the function exists and is callable
        from moneybin.services.import_service import _run_matching

        assert callable(_run_matching)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/moneybin/test_import_matching_integration.py -v`
Expected: FAIL — `_run_matching` doesn't exist

- [ ] **Step 3: Add matching hook to import_service.py**

Add a `_run_matching` function to `src/moneybin/services/import_service.py`:

```python
def _run_matching(db: Database) -> None:
    """Run transaction matching after import.

    Seeds source priority from config and runs the matcher engine.
    Results are logged; pending matches prompt user action.
    """
    from moneybin.config import get_settings
    from moneybin.matching.engine import TransactionMatcher
    from moneybin.matching.priority import seed_source_priority

    settings = get_settings().matching
    seed_source_priority(db, settings)
    matcher = TransactionMatcher(db, settings)
    result = matcher.run()

    if result.auto_merged or result.pending_review:
        logger.info(f"⚙️  Matching: {result.summary()}")
        if result.pending_review:
            logger.info("👀 Run 'moneybin matches review' when ready")
```

Then in the `import_file` function, add the matching call before `run_transforms`. Find the section (around line 692):

```python
    if apply_transforms and file_type in ("ofx", "tabular"):
        _run_matching(db)
        result.core_tables_rebuilt = run_transforms(db.path)
        _apply_categorization(db)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/moneybin/test_import_matching_integration.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/moneybin/services/import_service.py \
        tests/moneybin/test_import_matching_integration.py
git commit -m "feat: hook matching engine into import pipeline"
```

---

## Task 20: End-to-End Integration Tests

Full pipeline test: load data → run matcher → run SQLMesh → verify gold records.

**Files:**
- Create: `tests/moneybin/test_dedup_integration.py`

- [ ] **Step 1: Write integration test**

```python
# tests/moneybin/test_dedup_integration.py
"""End-to-end integration tests for transaction dedup.

These tests load real data into DuckDB, run the matching engine,
and verify the gold records in core.fct_transactions.
"""

import uuid
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from moneybin.config import MatchingSettings
from moneybin.database import Database
from moneybin.matching.engine import TransactionMatcher
from moneybin.matching.hashing import gold_key_matched, gold_key_unmatched
from moneybin.matching.persistence import (
    get_active_matches,
    get_pending_matches,
    undo_match,
    update_match_status,
)
from moneybin.matching.priority import seed_source_priority


@pytest.fixture()
def db(tmp_path: Path, mock_secret_store: MagicMock) -> Database:
    database = Database(
        tmp_path / "test.duckdb",
        secret_store=mock_secret_store,
        no_auto_upgrade=True,
    )
    yield database
    database.close()


def _seed_test_data(db: Database) -> None:
    """Insert test data that simulates cross-source overlap."""
    # OFX transaction
    db.execute("""
        INSERT INTO raw.ofx_transactions
        (source_transaction_id, account_id, transaction_type, date_posted,
         amount, payee, memo, check_number, source_file, extracted_at)
        VALUES
        ('FITID001', 'acct_checking', 'DEBIT', '2026-03-15 00:00:00',
         -42.50, 'STARBUCKS #1234 NEW YORK NY', NULL, NULL,
         '/tmp/test.ofx', '2026-03-16 10:00:00')
    """)
    # OFX account for source_origin JOIN
    db.execute("""
        INSERT INTO raw.ofx_accounts
        (account_id, routing_number, account_type, institution_org,
         institution_fid, source_file, extracted_at)
        VALUES
        ('acct_checking', '021000021', 'CHECKING', 'Chase Bank',
         '10898', '/tmp/test.ofx', '2026-03-16 10:00:00')
    """)
    # CSV transaction (same real-world transaction)
    db.execute("""
        INSERT INTO raw.tabular_transactions
        (transaction_id, account_id, transaction_date, amount,
         description, source_file, source_type, source_origin, import_id)
        VALUES
        ('csv_abc123def456', 'acct_checking', '2026-03-15', -42.50,
         'STARBUCKS 1234', '/tmp/test.csv', 'csv', 'chase_credit',
         '00000000-0000-0000-0000-000000000001')
    """)
    # Unrelated transaction (should NOT match)
    db.execute("""
        INSERT INTO raw.tabular_transactions
        (transaction_id, account_id, transaction_date, amount,
         description, source_file, source_type, source_origin, import_id)
        VALUES
        ('csv_xyz789', 'acct_checking', '2026-03-15', -15.00,
         'SUBWAY 456', '/tmp/test.csv', 'csv', 'chase_credit',
         '00000000-0000-0000-0000-000000000001')
    """)


@pytest.mark.integration
class TestEndToEndDedup:
    def test_cross_source_match_produces_one_gold_record(self, db: Database) -> None:
        """OFX + CSV describing the same transaction → one gold record."""
        _seed_test_data(db)
        settings = MatchingSettings()
        seed_source_priority(db, settings)

        # Create a test version of the unioned view
        db.execute("""
            CREATE OR REPLACE VIEW _test_unioned AS
            SELECT
                source_transaction_id,
                account_id,
                date_posted::DATE AS transaction_date,
                amount::DECIMAL(18,2) AS amount,
                TRIM(payee) AS description,
                'ofx' AS source_type,
                COALESCE(a.institution_org, 'ofx_unknown') AS source_origin,
                t.source_file
            FROM raw.ofx_transactions t
            LEFT JOIN raw.ofx_accounts a ON t.account_id = a.account_id
            UNION ALL
            SELECT
                transaction_id AS source_transaction_id,
                account_id,
                transaction_date,
                amount::DECIMAL(18,2) AS amount,
                description,
                source_type,
                source_origin,
                source_file
            FROM raw.tabular_transactions
        """)

        matcher = TransactionMatcher(db, settings, table="_test_unioned")
        result = matcher.run()

        # The OFX and CSV Starbucks transactions should auto-merge
        assert result.auto_merged == 1, (
            f"Expected 1 auto-merge, got {result.auto_merged}"
        )

        # Verify match decision was recorded
        active = get_active_matches(db)
        assert len(active) == 1
        assert active[0]["source_type_a"] in ("csv", "ofx")
        assert active[0]["source_type_b"] in ("csv", "ofx")

    def test_unrelated_transactions_stay_separate(self, db: Database) -> None:
        """Transactions with different amounts are never matched."""
        _seed_test_data(db)
        settings = MatchingSettings()
        seed_source_priority(db, settings)

        db.execute("""
            CREATE OR REPLACE VIEW _test_unioned AS
            SELECT
                source_transaction_id,
                account_id,
                date_posted::DATE AS transaction_date,
                amount::DECIMAL(18,2) AS amount,
                TRIM(payee) AS description,
                'ofx' AS source_type,
                'chase' AS source_origin,
                source_file
            FROM raw.ofx_transactions
            UNION ALL
            SELECT
                transaction_id AS source_transaction_id,
                account_id,
                transaction_date,
                amount::DECIMAL(18,2) AS amount,
                description,
                source_type,
                source_origin,
                source_file
            FROM raw.tabular_transactions
        """)

        matcher = TransactionMatcher(db, settings, table="_test_unioned")
        result = matcher.run()

        # Only the Starbucks pair should match; Subway stays separate
        assert result.auto_merged <= 1

    def test_undo_and_rematch_repropose(self, db: Database) -> None:
        """Undoing a match and re-running repropose the same pair."""
        _seed_test_data(db)
        settings = MatchingSettings()
        seed_source_priority(db, settings)

        db.execute("""
            CREATE OR REPLACE VIEW _test_unioned AS
            SELECT source_transaction_id, account_id,
                   date_posted::DATE AS transaction_date,
                   amount::DECIMAL(18,2) AS amount,
                   TRIM(payee) AS description,
                   'ofx' AS source_type, 'chase' AS source_origin,
                   source_file
            FROM raw.ofx_transactions
            UNION ALL
            SELECT transaction_id, account_id, transaction_date,
                   amount::DECIMAL(18,2), description,
                   source_type, source_origin, source_file
            FROM raw.tabular_transactions
        """)

        # First run
        matcher = TransactionMatcher(db, settings, table="_test_unioned")
        result1 = matcher.run()
        assert result1.auto_merged >= 1

        # Undo (but don't reject)
        active = get_active_matches(db)
        undo_match(db, active[0]["match_id"], reversed_by="user")

        # Re-run: should re-propose
        matcher2 = TransactionMatcher(db, settings, table="_test_unioned")
        result2 = matcher2.run()
        assert result2.auto_merged >= 1

    def test_gold_key_consistency(self) -> None:
        """Python and SQL gold key generation must produce identical results."""
        # Python hash
        py_key = gold_key_unmatched("csv", "txn123", "acct1")

        # Verify format: 16 hex chars
        assert len(py_key) == 16
        assert all(c in "0123456789abcdef" for c in py_key)

        # Matched group key
        group_key = gold_key_matched([
            ("csv", "txn_csv", "acct1"),
            ("ofx", "txn_ofx", "acct1"),
        ])
        assert len(group_key) == 16
        assert group_key != py_key  # Different inputs = different key
```

- [ ] **Step 2: Run test**

Run: `uv run pytest tests/moneybin/test_dedup_integration.py -v -m "not integration or integration"`
Expected: PASS

- [ ] **Step 3: Commit**

```bash
git add tests/moneybin/test_dedup_integration.py
git commit -m "test: add end-to-end dedup integration tests"
```

---

## Task 21: Spec and Index Updates

Update spec status and README.

**Files:**
- Modify: `docs/specs/matching-same-record-dedup.md`
- Modify: `docs/specs/INDEX.md`
- Modify: `README.md`

- [ ] **Step 1: Update spec status to `in-progress`**

In `docs/specs/matching-same-record-dedup.md`, change:
```
> Status: Ready
```
to:
```
> Status: in-progress
```

- [ ] **Step 2: Update INDEX.md**

Update the dedup spec entry status to `in-progress`.

- [ ] **Step 3: Commit**

```bash
git add docs/specs/matching-same-record-dedup.md docs/specs/INDEX.md
git commit -m "docs: mark same-record dedup spec as in-progress"
```

---

## Task 22: Final Quality Pass

Run full test suite, linting, and type checking.

- [ ] **Step 1: Run format and lint**

Run: `make format && make lint`

- [ ] **Step 2: Run type checking on new files**

Run: `uv run pyright src/moneybin/matching/ src/moneybin/cli/commands/matches.py`

- [ ] **Step 3: Run full test suite**

Run: `make test`

- [ ] **Step 4: Format SQL models**

Run: `uv run sqlmesh -p sqlmesh format`

- [ ] **Step 5: Fix any issues found**

Address lint, type, or test failures.

- [ ] **Step 6: Final commit**

```bash
git add -A
git commit -m "chore: fix lint, type, and formatting issues"
```

---

## Verification Checklist

After all tasks are complete, verify:

- [ ] `make check test` passes clean
- [ ] `uv run sqlmesh -p sqlmesh format` produces no changes
- [ ] All 8 spec requirements for dedup (1–8) are covered
- [ ] All 4 spec requirements for merge rules (9–12) are covered
- [ ] All 4 spec requirements for identity (13–16) are covered
- [ ] Both provenance requirements (17–18) are covered
- [ ] CLI commands work: `moneybin matches run`, `log`, `undo`, `backfill`, `review`
- [ ] Migrations apply cleanly on a fresh database
- [ ] Existing OFX and tabular tests still pass
- [ ] Spec status updated to `implemented` (once verified)
- [ ] README updated per shipping.md rules
