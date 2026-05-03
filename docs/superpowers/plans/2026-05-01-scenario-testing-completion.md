# Scenario Testing Completion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Finish Phases 3–6 of `docs/specs/testing-scenario-comprehensive.md` (Tier 1 backfill, four new scenarios, Tier 2/4 enrichment, contributor recipe + governance) in a single PR with phase-scoped commits, then mark the spec `implemented`.

**Architecture:** Three layers stack:
1. **Primitives** — extend `src/moneybin/validation/assertions/` with the four missing Tier 1 checks plus `assert_ground_truth_coverage` (Tier 4). Register each in `tests/scenarios/_runner/_assertion_registry.py`.
2. **Harnesses** — new `tests/scenarios/_harnesses.py` for pipeline-execution patterns (idempotency, incremental, empty-input, malformed-input). These drive `run_step`/`run_scenario` and are NOT data assertions, per spec §170-175.
3. **Scenarios** — wire Tier 1 across the six existing scenarios (replacing `±15%` and `min_rows ≥ 100` with deterministic-generator-derived exact counts), add four new scenarios with hand-authored fixtures, and enrich applicable tests with P/R breakdowns and Tier 4 quality checks.

**Tech Stack:** Python 3.12, pytest, pytest-xdist, DuckDB (encrypted), SQLMesh, Pydantic v2, Polars. Worktree `.worktrees/scenario-testing-completion` on branch `feat/scenario-testing-completion` (already created off `origin/main` at `6fe3ef1`).

**Commit cadence:** One commit per phase boundary (4 commits total). Each phase finishes with `make check test` green before committing.

---

## Phase Map

| Phase | Tasks | Commit subject |
|---|---|---|
| 3 — Tier 1 backfill | 1–6 | `Add Tier 1 backfill: source attribution, schema, amount, dates` |
| 4 — New scenarios | 7–13 | `Add idempotency, dedup-negative, empty, malformed scenarios` |
| 5 — Tier 2/4 enrichment | 14–17 | `Wire P/R breakdowns, ground-truth coverage, date continuity` |
| 6 — Recipe + governance | 18–22 | `Mark scenario-testing-comprehensive implemented` |

---

## File Structure (overview)

**New files:**
- `tests/scenarios/_harnesses.py` — pipeline-execution helpers
- `tests/scenarios/test_idempotency_rerun.py`
- `tests/scenarios/test_dedup_negative_fixture.py`
- `tests/scenarios/test_empty_input_handling.py`
- `tests/scenarios/test_malformed_input_rejection.py`
- `tests/scenarios/data/idempotency-rerun.yaml`
- `tests/scenarios/data/dedup-negative-fixture.yaml`
- `tests/scenarios/data/fixtures/dedup-negative/{distinct_csv,distinct_ofx}.csv`
- `tests/scenarios/data/fixtures/empty-input/{empty.csv,empty.ofx.csv}`
- `tests/scenarios/data/fixtures/malformed/{missing_header.csv,truncated.ofx.csv}`
- Test files for new primitives under `tests/moneybin/test_validation/`

**Modified files:**
- `src/moneybin/validation/assertions/{completeness,schema,domain,distribution}.py` — new primitives
- `src/moneybin/validation/assertions/__init__.py` — re-export new primitives
- `tests/scenarios/_runner/_assertion_registry.py` — register new YAML-callable primitives
- All six existing `tests/scenarios/data/*.yaml` and `tests/scenarios/test_*.py` — wire Tier 1 + replace tolerance bands
- `docs/specs/testing-scenario-comprehensive.md` — status `implemented`
- `docs/specs/INDEX.md`, `docs/specs/testing-overview.md`, `docs/specs/testing-scenario-runner.md`
- `docs/guides/scenario-authoring.md` — reference new primitives
- `CONTRIBUTING.md` — reference completed taxonomy
- `README.md` — roadmap icon → ✅

---

# Phase 3 — Tier 1 Backfill

Adds the four Tier 1 primitives missing from the existing validation library:
`assert_source_system_populated`, `assert_schema_snapshot`, `assert_amount_precision`, `assert_date_bounds`. Then wires them into all six existing scenarios and replaces the two known observe-and-paste expectations (`family-full-pipeline ±15%` and `encryption-key-propagation min_rows ≥ 100`) with deterministic-generator-derived exact counts. Adds pre/post row-count parity to `migration-roundtrip`.

## Task 1: `assert_source_system_populated`

Tier 1 check: `core.fct_transactions.source_system` is non-null and ⊆ an expected set.

**Files:**
- Modify: `src/moneybin/validation/assertions/completeness.py`
- Modify: `src/moneybin/validation/assertions/__init__.py`
- Test: `tests/moneybin/test_validation/test_assertions_completeness.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/moneybin/test_validation/test_assertions_completeness.py` (or extend existing):

```python
"""Tests for completeness assertion primitives."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from moneybin.database import Database
from moneybin.validation.assertions.completeness import (
    assert_no_nulls,
    assert_source_system_populated,
)


@pytest.fixture()
def db(tmp_path: Path, mock_secret_store: MagicMock) -> Database:
    database = Database(
        tmp_path / "test.duckdb", secret_store=mock_secret_store, no_auto_upgrade=True
    )
    database.execute("CREATE TABLE t (id INT, source_system VARCHAR)")
    return database


def test_source_system_populated_passes_when_all_rows_have_value(db: Database) -> None:
    db.execute("INSERT INTO t VALUES (1, 'csv'), (2, 'ofx')")
    r = assert_source_system_populated(db, table="t", expected_sources={"csv", "ofx"})
    assert r.passed
    assert r.details["null_count"] == 0
    assert r.details["unexpected_values"] == []


def test_source_system_populated_fails_on_null(db: Database) -> None:
    db.execute("INSERT INTO t VALUES (1, 'csv'), (2, NULL)")
    r = assert_source_system_populated(db, table="t", expected_sources={"csv"})
    assert not r.passed
    assert r.details["null_count"] == 1


def test_source_system_populated_fails_on_unexpected_value(db: Database) -> None:
    db.execute("INSERT INTO t VALUES (1, 'csv'), (2, 'plaid')")
    r = assert_source_system_populated(db, table="t", expected_sources={"csv", "ofx"})
    assert not r.passed
    assert "plaid" in r.details["unexpected_values"]


def test_source_system_populated_passes_for_empty_table(db: Database) -> None:
    """Empty tables vacuously satisfy 'all rows populated'."""
    r = assert_source_system_populated(db, table="t", expected_sources={"csv"})
    assert r.passed
```

- [ ] **Step 2: Run test to verify it fails**

```
uv run pytest tests/moneybin/test_validation/test_assertions_completeness.py -v
```
Expected: FAIL with `ImportError: cannot import name 'assert_source_system_populated'`.

- [ ] **Step 3: Implement the primitive**

Append to `src/moneybin/validation/assertions/completeness.py`:

```python
def assert_source_system_populated(
    db: Database,
    *,
    table: str,
    expected_sources: set[str],
    column: str = "source_system",
) -> AssertionResult:
    """Assert ``column`` is non-null on every row and all values are in ``expected_sources``."""
    if not expected_sources:
        raise ValueError("expected_sources must be non-empty")
    t = quote_ident(table)
    c = quote_ident(column)
    null_row = db.execute(
        f"SELECT COUNT(*) FROM {t} WHERE {c} IS NULL"  # noqa: S608  # identifiers validated by quote_ident
    ).fetchone()
    null_count = int(null_row[0]) if null_row else 0
    value_rows = db.execute(
        f"SELECT DISTINCT {c} FROM {t} WHERE {c} IS NOT NULL"  # noqa: S608  # identifiers validated by quote_ident
    ).fetchall()
    observed = {str(r[0]) for r in value_rows}
    unexpected = sorted(observed - expected_sources)
    return AssertionResult(
        name="source_system_populated",
        passed=null_count == 0 and not unexpected,
        details={
            "null_count": null_count,
            "expected_sources": sorted(expected_sources),
            "observed_sources": sorted(observed),
            "unexpected_values": unexpected,
        },
    )
```

- [ ] **Step 4: Re-export from package init**

Edit `src/moneybin/validation/assertions/__init__.py`:
- Add `assert_source_system_populated` to the `from moneybin.validation.assertions.completeness import (...)` block
- Add it to `__all__` (alphabetical order)

- [ ] **Step 5: Run tests to verify they pass**

```
uv run pytest tests/moneybin/test_validation/test_assertions_completeness.py -v
```
Expected: 4 passed.

## Task 2: `assert_schema_snapshot`

Tier 1: assert a table has exactly the expected `column → type` mapping (no missing, no extra).

**Files:**
- Modify: `src/moneybin/validation/assertions/schema.py`
- Modify: `src/moneybin/validation/assertions/__init__.py`
- Test: `tests/moneybin/test_validation/test_assertions_schema.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/moneybin/test_validation/test_assertions_schema.py`:

```python
"""Tests for schema-snapshot assertion."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from moneybin.database import Database
from moneybin.validation.assertions.schema import assert_schema_snapshot


@pytest.fixture()
def db(tmp_path: Path, mock_secret_store: MagicMock) -> Database:
    database = Database(
        tmp_path / "test.duckdb", secret_store=mock_secret_store, no_auto_upgrade=True
    )
    database.execute("CREATE TABLE t (id INTEGER, name VARCHAR, amount DECIMAL(18,2))")
    return database


def test_schema_snapshot_passes_on_exact_match(db: Database) -> None:
    r = assert_schema_snapshot(
        db,
        table="t",
        expected={"id": "INTEGER", "name": "VARCHAR", "amount": "DECIMAL(18,2)"},
    )
    assert r.passed, r.details


def test_schema_snapshot_fails_on_missing_column(db: Database) -> None:
    r = assert_schema_snapshot(
        db,
        table="t",
        expected={
            "id": "INTEGER",
            "name": "VARCHAR",
            "amount": "DECIMAL(18,2)",
            "extra": "VARCHAR",
        },
    )
    assert not r.passed
    assert "extra" in r.details["missing"]


def test_schema_snapshot_fails_on_extra_column(db: Database) -> None:
    r = assert_schema_snapshot(db, table="t", expected={"id": "INTEGER"})
    assert not r.passed
    assert set(r.details["extra"]) == {"name", "amount"}


def test_schema_snapshot_fails_on_type_mismatch(db: Database) -> None:
    r = assert_schema_snapshot(
        db,
        table="t",
        expected={"id": "BIGINT", "name": "VARCHAR", "amount": "DECIMAL(18,2)"},
    )
    assert not r.passed
    assert r.details["mismatched"]["id"] == {"expected": "BIGINT", "actual": "INTEGER"}
```

- [ ] **Step 2: Run to verify failure**

```
uv run pytest tests/moneybin/test_validation/test_assertions_schema.py -v
```
Expected: FAIL with `ImportError`.

- [ ] **Step 3: Implement primitive**

Append to `src/moneybin/validation/assertions/schema.py`:

```python
def assert_schema_snapshot(
    db: Database, *, table: str, expected: dict[str, str]
) -> AssertionResult:
    """Assert table's columns match ``expected`` exactly — no missing, no extra, types match."""
    actual = _columns_with_types(db, table)
    missing = sorted(set(expected) - set(actual))
    extra = sorted(set(actual) - set(expected))
    mismatched = {
        col: {"expected": exp_type, "actual": actual[col]}
        for col, exp_type in expected.items()
        if col in actual and actual[col] != exp_type
    }
    return AssertionResult(
        name="schema_snapshot",
        passed=not missing and not extra and not mismatched,
        details={
            "missing": missing,
            "extra": extra,
            "mismatched": mismatched,
        },
    )
```

- [ ] **Step 4: Re-export**

Edit `src/moneybin/validation/assertions/__init__.py` — add `assert_schema_snapshot` to the schema imports and to `__all__`.

- [ ] **Step 5: Run tests to verify pass**

```
uv run pytest tests/moneybin/test_validation/test_assertions_schema.py -v
```
Expected: 4 passed.

## Task 3: `assert_amount_precision`

Tier 1: assert a numeric column is `DECIMAL(p, s)` with no truncation (no value loses precision when round-tripped).

**Files:**
- Modify: `src/moneybin/validation/assertions/domain.py`
- Modify: `src/moneybin/validation/assertions/__init__.py`
- Test: `tests/moneybin/test_validation/test_assertions_domain.py` (extend existing)

- [ ] **Step 1: Write the failing tests**

Create or extend `tests/moneybin/test_validation/test_assertions_domain.py`:

```python
from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from moneybin.database import Database
from moneybin.validation.assertions.domain import assert_amount_precision


@pytest.fixture()
def db(tmp_path: Path, mock_secret_store: MagicMock) -> Database:
    database = Database(
        tmp_path / "test.duckdb", secret_store=mock_secret_store, no_auto_upgrade=True
    )
    return database


def test_amount_precision_passes_for_decimal_18_2_column(db: Database) -> None:
    db.execute("CREATE TABLE t (amount DECIMAL(18,2))")
    db.execute("INSERT INTO t VALUES (47.99), (-1500.00), (0.01)")
    r = assert_amount_precision(db, table="t", column="amount", precision=18, scale=2)
    assert r.passed, r.details


def test_amount_precision_fails_when_column_is_double(db: Database) -> None:
    db.execute("CREATE TABLE t (amount DOUBLE)")
    db.execute("INSERT INTO t VALUES (47.99)")
    r = assert_amount_precision(db, table="t", column="amount", precision=18, scale=2)
    assert not r.passed
    assert "DOUBLE" in r.details["actual_type"]


def test_amount_precision_fails_when_scale_too_small(db: Database) -> None:
    db.execute("CREATE TABLE t (amount DECIMAL(18,1))")
    r = assert_amount_precision(db, table="t", column="amount", precision=18, scale=2)
    assert not r.passed
```

- [ ] **Step 2: Run to verify failure**

```
uv run pytest tests/moneybin/test_validation/test_assertions_domain.py -v
```
Expected: FAIL with `ImportError`.

- [ ] **Step 3: Implement primitive**

Append to `src/moneybin/validation/assertions/domain.py`:

```python
def assert_amount_precision(
    db: Database,
    *,
    table: str,
    column: str,
    precision: int,
    scale: int,
) -> AssertionResult:
    """Assert ``column`` in ``table`` is ``DECIMAL(precision, scale)``.

    Catches the silent regression where an upstream cast drops a money column
    to ``DOUBLE``, losing exact representation. Compares against
    ``information_schema.columns.data_type`` — DuckDB renders DECIMAL types
    as ``DECIMAL(p,s)`` literally, so a string-equality check is sufficient.
    """
    expected_type = f"DECIMAL({precision},{scale})"
    schema, name = _split(table)
    if schema is None:
        rows = db.execute(
            "SELECT data_type FROM information_schema.columns "
            "WHERE table_name = ? AND column_name = ?",
            [name, column],
        ).fetchall()
    else:
        rows = db.execute(
            "SELECT data_type FROM information_schema.columns "
            "WHERE table_schema = ? AND table_name = ? AND column_name = ?",
            [schema, name, column],
        ).fetchall()
    actual_type = str(rows[0][0]) if rows else "<missing>"
    return AssertionResult(
        name="amount_precision",
        passed=actual_type == expected_type,
        details={
            "expected_type": expected_type,
            "actual_type": actual_type,
        },
    )
```

Note: `assert_amount_precision` lives in `domain.py` because precision is a domain rule (money must be exact), even though the underlying check reads `information_schema`. Add the `_split` import at the top of `domain.py`:

```python
from moneybin.validation.assertions.schema import _split  # noqa: PLC0415 — internal helper reuse
```

If `_split` is private and pyright complains about cross-module private use, copy the four-line implementation inline in `domain.py` rather than importing — it's tiny and the duplication is preferable to widening visibility.

- [ ] **Step 4: Re-export**

Edit `src/moneybin/validation/assertions/__init__.py` — add `assert_amount_precision` to the `domain` imports and to `__all__`.

- [ ] **Step 5: Run tests**

```
uv run pytest tests/moneybin/test_validation/test_assertions_domain.py -v
```
Expected: 3 passed (plus any pre-existing tests in the file).

## Task 4: `assert_date_bounds`

Tier 1: assert all values in a date column fall within `[min_date, max_date]` inclusive.

**Files:**
- Modify: `src/moneybin/validation/assertions/domain.py`
- Modify: `src/moneybin/validation/assertions/__init__.py`
- Test: `tests/moneybin/test_validation/test_assertions_domain.py`

- [ ] **Step 1: Write the failing tests**

Append to `tests/moneybin/test_validation/test_assertions_domain.py`:

```python
from datetime import date

from moneybin.validation.assertions.domain import assert_date_bounds


def test_date_bounds_passes_when_all_in_range(db: Database) -> None:
    db.execute("CREATE TABLE t (d DATE)")
    db.execute("INSERT INTO t VALUES ('2024-01-01'), ('2024-06-15'), ('2024-12-31')")
    r = assert_date_bounds(
        db,
        table="t",
        column="d",
        min_date=date(2024, 1, 1),
        max_date=date(2024, 12, 31),
    )
    assert r.passed, r.details


def test_date_bounds_fails_below_min(db: Database) -> None:
    db.execute("CREATE TABLE t (d DATE)")
    db.execute("INSERT INTO t VALUES ('2023-12-31'), ('2024-06-15')")
    r = assert_date_bounds(
        db,
        table="t",
        column="d",
        min_date=date(2024, 1, 1),
        max_date=date(2024, 12, 31),
    )
    assert not r.passed
    assert r.details["below_min_count"] == 1


def test_date_bounds_fails_above_max(db: Database) -> None:
    db.execute("CREATE TABLE t (d DATE)")
    db.execute("INSERT INTO t VALUES ('2024-06-15'), ('2025-01-01')")
    r = assert_date_bounds(
        db,
        table="t",
        column="d",
        min_date=date(2024, 1, 1),
        max_date=date(2024, 12, 31),
    )
    assert not r.passed
    assert r.details["above_max_count"] == 1


def test_date_bounds_passes_for_empty_table(db: Database) -> None:
    db.execute("CREATE TABLE t (d DATE)")
    r = assert_date_bounds(
        db,
        table="t",
        column="d",
        min_date=date(2024, 1, 1),
        max_date=date(2024, 12, 31),
    )
    assert r.passed
```

- [ ] **Step 2: Run to verify failure**

```
uv run pytest tests/moneybin/test_validation/test_assertions_domain.py::test_date_bounds_passes_when_all_in_range -v
```
Expected: FAIL with `ImportError`.

- [ ] **Step 3: Implement primitive**

Append to `src/moneybin/validation/assertions/domain.py`:

```python
from datetime import date as _date  # alphabetical; place near top imports


def assert_date_bounds(
    db: Database,
    *,
    table: str,
    column: str,
    min_date: _date,
    max_date: _date,
) -> AssertionResult:
    """Assert every ``column`` value falls within ``[min_date, max_date]`` inclusive.

    Empty tables pass — there are no out-of-range rows to find. Authors who
    require non-empty input should pair this with ``assert_min_rows``.
    """
    if min_date > max_date:
        raise ValueError(f"min_date {min_date} must be <= max_date {max_date}")
    t = quote_ident(table)
    c = quote_ident(column)
    row = db.execute(
        f"SELECT "  # noqa: S608  # identifiers validated by quote_ident
        f"  SUM(CASE WHEN {c} < ? THEN 1 ELSE 0 END), "
        f"  SUM(CASE WHEN {c} > ? THEN 1 ELSE 0 END), "
        f"  MIN({c}), MAX({c}) "
        f"FROM {t}",
        [min_date, max_date],
    ).fetchone()
    below = int(row[0]) if row and row[0] is not None else 0
    above = int(row[1]) if row and row[1] is not None else 0
    observed_min = row[2] if row else None
    observed_max = row[3] if row else None
    return AssertionResult(
        name="date_bounds",
        passed=below == 0 and above == 0,
        details={
            "min_date": min_date.isoformat(),
            "max_date": max_date.isoformat(),
            "observed_min": observed_min.isoformat() if observed_min else None,
            "observed_max": observed_max.isoformat() if observed_max else None,
            "below_min_count": below,
            "above_max_count": above,
        },
    )
```

- [ ] **Step 4: Re-export**

Edit `src/moneybin/validation/assertions/__init__.py` — add `assert_date_bounds` to the `domain` imports and `__all__`.

- [ ] **Step 5: Run tests**

```
uv run pytest tests/moneybin/test_validation/test_assertions_domain.py -v
```
Expected: all date-bounds tests pass.

## Task 5: Register the four new primitives in the YAML registry

**Files:**
- Modify: `tests/scenarios/_runner/_assertion_registry.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/scenarios/_runner_tests/test_assertion_registry.py`:

```python
def test_registry_includes_phase3_primitives() -> None:
    """Phase 3 Tier 1 primitives must be YAML-callable."""
    from tests.scenarios._runner._assertion_registry import ASSERTION_REGISTRY

    expected = {
        "assert_source_system_populated",
        "assert_schema_snapshot",
        "assert_amount_precision",
        "assert_date_bounds",
    }
    assert expected.issubset(ASSERTION_REGISTRY.keys())
```

- [ ] **Step 2: Run to verify failure**

```
uv run pytest tests/scenarios/_runner_tests/test_assertion_registry.py::test_registry_includes_phase3_primitives -v
```
Expected: FAIL — keys missing.

- [ ] **Step 3: Register the primitives**

Edit `tests/scenarios/_runner/_assertion_registry.py`:
1. Add to imports:
   ```python
   from moneybin.validation.assertions.completeness import (
       assert_no_nulls,
       assert_source_system_populated,
   )
   from moneybin.validation.assertions.domain import (
       assert_amount_precision,
       assert_balanced_transfers,
       assert_date_bounds,
       assert_date_continuity,
       assert_sign_convention,
   )
   from moneybin.validation.assertions.schema import (
       assert_column_types,
       assert_columns_exist,
       assert_row_count_delta,
       assert_row_count_exact,
       assert_schema_snapshot,
   )
   ```
2. Add to `ASSERTION_REGISTRY` (alphabetical):
   ```python
   "assert_amount_precision": assert_amount_precision,
   "assert_date_bounds": assert_date_bounds,
   "assert_schema_snapshot": assert_schema_snapshot,
   "assert_source_system_populated": assert_source_system_populated,
   ```

- [ ] **Step 4: Run test to verify pass**

```
uv run pytest tests/scenarios/_runner_tests/test_assertion_registry.py -v
```
Expected: PASS.

## Task 6: Wire Tier 1 across the six existing scenarios + replace tolerance-band expectations

The six scenarios each need the missing Tier 1 checks added: `source_system_populated`, `schema_snapshot` (on `core.fct_transactions`), `amount_precision`, `date_bounds`. The two known observe-and-paste expectations get replaced with deterministic-generator-derived exact counts. `migration-roundtrip` gets pre/post row-count parity.

**Files (modify):**
- `tests/scenarios/data/basic-full-pipeline.yaml`
- `tests/scenarios/data/family-full-pipeline.yaml`
- `tests/scenarios/data/dedup-cross-source.yaml`
- `tests/scenarios/data/transfer-detection-cross-account.yaml`
- `tests/scenarios/data/migration-roundtrip.yaml`
- `tests/scenarios/data/encryption-key-propagation.yaml`
- `tests/scenarios/test_family_full_pipeline.py`
- `tests/scenarios/test_encryption_key_propagation.py`
- `tests/scenarios/test_migration_roundtrip.py`

- [ ] **Step 1: Determine the deterministic counts**

The independently-derived expected count for a generator-driven scenario IS the count of transactions the deterministic `GeneratorEngine(persona, seed, years)` produces. The persona YAML (income, recurring, spending categories with mean txns/month) is the formula; the generator with a fixed seed materializes it to an integer. This is the "persona / generator config — derive expected values via a deterministic formula over declared parameters" path from `.claude/rules/testing.md` line 160.

Run a one-off to capture the counts:

```
uv run python -c "
from moneybin.testing.synthetic.engine import GeneratorEngine
for persona, years in [('basic', 1), ('family', 3), ('basic', 2), ('family', 2)]:
    r = GeneratorEngine(persona, seed=42, years=years).generate()
    print(f'{persona} years={years}: {len(r.transactions)} txns')
"
```

Record the four numbers — these are the exact expected counts. Use them in steps 2–4 below. (Replace `<count>` in the YAML edits with the actual integer.)

- [ ] **Step 2: Update `family-full-pipeline.yaml`**

Replace the `row_count_within_tolerance` block:

```yaml
  - name: row_count_exact
    fn: assert_row_count_exact
    args:
      table: core.fct_transactions
      # Derived from GeneratorEngine('family', seed=42, years=3) — the
      # persona's income + recurring + spending categories materialize
      # deterministically. If this number changes, either the persona
      # config changed or the generator regressed; investigate before
      # updating.
      expected: <count_from_step_1>
```

Add Tier 1 backfill assertions (after `transfers_balance_to_zero`):

```yaml
  - name: source_system_populated
    fn: assert_source_system_populated
    args:
      table: core.fct_transactions
      expected_sources: [csv, ofx]
  - name: amount_precision_decimal_18_2
    fn: assert_amount_precision
    args:
      table: core.fct_transactions
      column: amount
      precision: 18
      scale: 2
  - name: date_bounds_three_year_window
    fn: assert_date_bounds
    args:
      table: core.fct_transactions
      column: transaction_date
      # Family persona generates 3 years ending today; declare the window
      # as [today - 3y - 31d, today + 1d] to absorb month-edge variance.
      # The pytest test computes the window dynamically — see
      # test_family_full_pipeline.py.
      min_date: from_runtime
      max_date: from_runtime
  - name: schema_snapshot_fct_transactions
    fn: assert_schema_snapshot
    args:
      table: core.fct_transactions
      expected:
        transaction_id: VARCHAR
        account_id: VARCHAR
        amount: DECIMAL(18,2)
        transaction_date: DATE
        description: VARCHAR
        category: VARCHAR
        is_transfer: BOOLEAN
        source_system: VARCHAR
        # add the rest of the columns observed on a known-good run, but
        # populate this list ONCE by inspecting the schema, not by running
        # the test — read sqlmesh/models/core/fct_transactions.sql to
        # enumerate the SELECT list.
```

For the schema_snapshot column list: read `sqlmesh/models/core/fct_transactions.sql` and enumerate the SELECT columns by hand — do not run a query and paste the result. If `fct_transactions` has more columns than listed above, add them with the types declared in the SQL (or the types DuckDB derives for the SELECT expressions).

The `from_runtime` sentinel for `date_bounds` requires a runner change — extend `_resolve_runtime_args` in `tests/scenarios/_runner/runner.py` to handle `min_date`/`max_date` keys by computing them from `scenario.setup.years`. Alternatively (preferred for simplicity): drop the YAML-level `date_bounds` and assert dates from a pytest-native test that has `setup.years` in scope.

**Recommended path:** keep YAML scenario flat — only put easy-to-express assertions in YAML. Move date_bounds, schema_snapshot and the `row_count_exact` for non-stable persona-derived counts into the per-scenario pytest test, where you can compute the expected value at test time. The pytest tests are already thin wrappers; thickening them slightly is OK.

Concrete pattern for `tests/scenarios/test_family_full_pipeline.py`:

```python
"""Scenario: end-to-end pipeline correctness for the family persona."""

from __future__ import annotations

from datetime import date, timedelta

import pytest

from moneybin.database import get_database
from moneybin.testing.synthetic.engine import GeneratorEngine
from moneybin.validation.assertions import (
    assert_amount_precision,
    assert_date_bounds,
    assert_row_count_exact,
    assert_source_system_populated,
)
from tests.scenarios._runner import load_shipped_scenario, run_scenario


@pytest.mark.scenarios
@pytest.mark.slow
def test_family_full_pipeline() -> None:
    """tiers: T1, T2-balanced-transfers, T2-categorization-pr, T2-transfer-f1, T3-idempotency, T4-date-continuity, T4-ground-truth-coverage."""
    scenario = load_shipped_scenario("family-full-pipeline")
    assert scenario is not None

    # Independent derivation: ask the generator (deterministic given seed)
    # how many txns it WILL produce, before running the pipeline.
    expected_txns = len(
        GeneratorEngine(
            scenario.setup.persona,
            seed=scenario.setup.seed,
            years=scenario.setup.years,
        )
        .generate()
        .transactions
    )

    result = run_scenario(scenario, keep_tmpdir=True)
    assert result.passed, result.failure_summary()

    # Phase 3 Tier 1 backfill — assertions that need test-time-computed
    # values stay in pytest (not YAML).
    db = get_database()
    today = date.today()
    window_start = today - timedelta(days=365 * scenario.setup.years + 31)
    window_end = today + timedelta(days=1)

    assert_row_count_exact(
        db, table="core.fct_transactions", expected=expected_txns
    ).raise_if_failed()
    assert_amount_precision(
        db, table="core.fct_transactions", column="amount", precision=18, scale=2
    ).raise_if_failed()
    assert_date_bounds(
        db,
        table="core.fct_transactions",
        column="transaction_date",
        min_date=window_start,
        max_date=window_end,
    ).raise_if_failed()
    assert_source_system_populated(
        db,
        table="core.fct_transactions",
        expected_sources={"csv", "ofx"},
    ).raise_if_failed()
```

The runner already closes the DB and removes the tempdir on exit when `keep_tmpdir=False`. Setting `keep_tmpdir=True` is required for the post-run assertions to find the encrypted DB still on disk and the singleton still valid. **Critical:** wrap the test in a try/finally that calls `shutil.rmtree(result.tmpdir, ignore_errors=True)` on exit, OR refactor to use the runner's existing context. Simplest fix: store `result.tmpdir` and `shutil.rmtree(result.tmpdir, ignore_errors=True)` in a finally. Alternatively, the cleanest solution is to extend `run_scenario` to accept an optional callback `post_run: Callable[[Database], None]` invoked before tempdir cleanup. **Choose the callback path** — it's the right abstraction. Add it to `run_scenario`:

```python
def run_scenario(
    scenario: Scenario,
    *,
    keep_tmpdir: bool = False,
    extra_assertions: Callable[[Database], list[AssertionResult]] | None = None,
) -> ScenarioResult:
```

Then in `_build_result` path (after the standard assertions/expectations/evaluations), if `extra_assertions` is set, call it on `db` and merge the results into `assertions`. Update the test to pass a lambda that returns the four extra `AssertionResult`s. **Implement this pattern in step 3 below.**

Either path is acceptable; pick one and apply it consistently. The plan uses the `extra_assertions` callback below.

- [ ] **Step 3: Add `extra_assertions` hook to the runner**

Edit `tests/scenarios/_runner/runner.py`. Update `run_scenario` signature and the `_build_result` call:

```python
def run_scenario(
    scenario: Scenario,
    *,
    keep_tmpdir: bool = False,
    extra_assertions: Callable[[Database], list[AssertionResult]] | None = None,
) -> ScenarioResult:
```

Inside the existing `try` block, after `evaluations = [_run_evaluation(...) for e in scenario.evaluations]`:

```python
extra: list[AssertionResult] = []
if extra_assertions is not None:
    try:
        extra = extra_assertions(db)
    except Exception as exc:  # noqa: BLE001 — surface as halted
        logger.error(f"extra_assertions crashed: {type(exc).__name__}")
        return _build_result(
            scenario=scenario,
            started=started,
            tmpdir=tmp,
            keep_tmpdir=keep_tmpdir,
            assertions=[preflight, *assertions],
            expectations=expectations,
            evaluations=evaluations,
            halted=f"extra_assertions crashed: {type(exc).__name__}",
        )

return _build_result(
    scenario=scenario,
    started=started,
    tmpdir=tmp,
    keep_tmpdir=keep_tmpdir,
    assertions=[preflight, *assertions, *extra],
    expectations=expectations,
    evaluations=evaluations,
)
```

Add to imports:
```python
from collections.abc import Callable
```

Add a unit test in `tests/scenarios/_runner_tests/test_runner.py`:

```python
def test_run_scenario_invokes_extra_assertions() -> None:
    """extra_assertions callback runs and its results are appended."""
    # Construct a TINY scenario inline; assert the callback's
    # AssertionResult appears in result.assertions and pass/fail
    # propagates to result.passed.
```

Sketch the test using a minimal scenario YAML the existing `_runner_tests` already builds (look for the `test_result.py` factory pattern). Run:
```
uv run pytest tests/scenarios/_runner_tests/test_runner.py -v
```

- [ ] **Step 4: Apply the same pattern to the other five scenarios**

For each of `test_basic_full_pipeline.py`, `test_dedup_cross_source.py`, `test_transfer_detection.py`, `test_migration_roundtrip.py`, `test_encryption_key_propagation.py`:

1. Compute the deterministic expected count from `GeneratorEngine` (or, for fixture-driven scenarios like `dedup-cross-source`, count fixture rows by hand — already done in `chase_amazon_overlap.expectations.yaml`).
2. Add the four Tier 1 backfill checks via `extra_assertions=lambda db: [...]` passed to `run_scenario`. Use the same pattern shown for `test_family_full_pipeline.py` above.

For **`test_encryption_key_propagation.py`**: replace `min_rows ≥ 100` (in the YAML) with `assert_row_count_exact` driven by `len(GeneratorEngine('basic', seed=42, years=1).generate().transactions)` in pytest, then drop the `subprocess_wrote_to_encrypted_db` YAML entry. Keep `no_unencrypted_artifacts` in YAML.

For **`test_migration_roundtrip.py`**: add pre/post row-count parity by switching to a step-driven pytest:

```python
@pytest.mark.scenarios
@pytest.mark.slow
def test_migration_roundtrip_preserves_row_counts(tmp_path: Path) -> None:
    """tiers: T1, T3-pre-post-parity. Migrating must not add or drop rows."""
    scenario = load_shipped_scenario("migration-roundtrip")
    assert scenario is not None

    pre_counts: dict[str, int] = {}
    post_counts: dict[str, int] = {}

    def snapshot(db: Database, target: dict[str, int]) -> None:
        for tbl in ("core.fct_transactions", "core.dim_accounts"):
            row = db.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()  # noqa: S608  # TableRef-equivalent constant
            target[tbl] = int(row[0]) if row else 0

    def extra(db: Database) -> list[AssertionResult]:
        # Final post-pipeline state — pre snapshot was taken mid-pipeline
        # via a pre-migrate hook (see below).
        snapshot(db, post_counts)
        results: list[AssertionResult] = []
        for tbl, before in pre_counts.items():
            after = post_counts[tbl]
            results.append(
                AssertionResult(
                    name=f"row_count_unchanged_{tbl}",
                    passed=before == after,
                    details={"before": before, "after": after},
                )
            )
        return results

    # NB: the simplest way to capture pre-migration state is to author the
    # scenario as a custom step sequence in the test rather than via YAML.
    # For minimal change, snapshot by inspecting the singleton between
    # explicit step calls. See test body for the in-place rewrite.
```

The cleanest variant of `test_migration_roundtrip` is to **stop using `run_scenario`** and drive the steps explicitly in pytest:

```python
from tests.scenarios._runner.steps import run_step

# ... bootstrap a profile + DB the same way run_scenario does, then:
run_step("generate", scenario.setup, db, env=env)
run_step("transform", scenario.setup, db, env=env)
snapshot(db, pre_counts)
run_step("migrate", scenario.setup, db, env=env)
run_step("match", scenario.setup, db, env=env)
snapshot(db, post_counts)
assert pre_counts == post_counts
```

To avoid duplicating the bootstrap code, factor `_bootstrap_scenario_env` out of `runner.py` into a public helper (`bootstrap_scenario_env(scenario) -> tuple[Database, str, dict]` returning `(db, tmpdir, env)`) and a `teardown_scenario_env(db, tmpdir)` cleanup. Use it from both `run_scenario` and the migration-roundtrip test. Implementation: extract the contents of `run_scenario`'s `_patched_env`/`_restored_profile`/`_bootstrap_database` block into a context manager `scenario_env(scenario)` that yields `(db, tmpdir, env)` and handles cleanup.

Concrete extraction:

```python
@contextmanager
def scenario_env(
    scenario: Scenario, *, keep_tmpdir: bool = False
) -> Generator[tuple[Database, str, dict[str, str]], None, None]:
    """Yield a fully bootstrapped scenario environment; clean up on exit."""
    started = time.perf_counter()  # only used for parity with run_scenario
    tmp = tempfile.mkdtemp(prefix=f"scenario-{scenario.name}-")
    env = _build_env(tmp)  # hoist the dict-building into a helper
    db: Database | None = None
    try:
        with _patched_env(env), _restored_profile():
            db = _bootstrap_database()
            yield db, tmp, env
    finally:
        if db is not None:
            close_database()
        if not keep_tmpdir:
            shutil.rmtree(tmp, ignore_errors=True)
```

Then `run_scenario` becomes a thin wrapper around `scenario_env`. Migration-roundtrip's pytest test uses `scenario_env` directly to drive steps step-by-step.

- [ ] **Step 5: Run all scenarios green**

```
uv run pytest tests/scenarios/ -m scenarios -v
```
Expected: 6 scenarios pass with the Tier 1 backfill in place.

```
make check test
```
Expected: all green (format, lint, type-check, full unit suite).

- [ ] **Step 6: Commit Phase 3**

```
git add src/moneybin/validation/assertions/ tests/moneybin/test_validation/ tests/scenarios/_runner/_assertion_registry.py tests/scenarios/_runner/runner.py tests/scenarios/_runner_tests/ tests/scenarios/test_*.py tests/scenarios/data/*.yaml
git commit -m "$(cat <<'EOF'
Add Tier 1 backfill: source attribution, schema, amount, dates

- Add assert_source_system_populated, assert_schema_snapshot,
  assert_amount_precision, assert_date_bounds primitives + tests
- Register all four in _assertion_registry for YAML callability
- Add extra_assertions callback + scenario_env context manager to runner
  so tests can run computed-at-test-time assertions without rewriting YAML
- Wire Tier 1 backfill into all six existing scenario tests
- Replace family-full-pipeline ±15% with deterministic-generator-derived
  exact count
- Replace encryption-key-propagation min_rows ≥ 100 with derived count
- Add pre/post row-count parity to migration-roundtrip via step-driven
  pytest using the new scenario_env helper
EOF
)"
```

---

# Phase 4 — Four New Scenarios

Four new scenarios — each shipping with hand-authored fixtures (where needed) and a pytest test driving harness primitives from `tests/scenarios/_harnesses.py`.

## Task 7: Create `_harnesses.py` with five pipeline-execution primitives

These are NOT data assertions (they don't go in `validation/`); they orchestrate pipeline runs and check execution-time invariants (duplicate rows after rerun, error raised on bad input, etc.).

**Files:**
- Create: `tests/scenarios/_harnesses.py`
- Test: `tests/scenarios/_runner_tests/test_harnesses.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/scenarios/_runner_tests/test_harnesses.py`:

```python
"""Tests for pipeline-execution harness primitives."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from moneybin.database import Database
from tests.scenarios._harnesses import (
    assert_empty_input_safe,
    assert_idempotent,
    assert_malformed_input_rejected,
)


@pytest.fixture()
def db(tmp_path: Path, mock_secret_store: MagicMock) -> Database:
    return Database(
        tmp_path / "test.duckdb", secret_store=mock_secret_store, no_auto_upgrade=True
    )


def test_idempotent_passes_when_counts_unchanged(db: Database) -> None:
    db.execute("CREATE TABLE t (id INT)")
    db.execute("INSERT INTO t VALUES (1), (2), (3)")
    r = assert_idempotent(
        db,
        tables=["t"],
        rerun=lambda: None,  # noop rerun — counts won't change
    )
    assert r.passed
    assert r.details["before"] == r.details["after"]


def test_idempotent_fails_when_rerun_adds_rows(db: Database) -> None:
    db.execute("CREATE TABLE t (id INT)")
    db.execute("INSERT INTO t VALUES (1)")

    def add_row() -> None:
        db.execute("INSERT INTO t VALUES (2)")

    r = assert_idempotent(db, tables=["t"], rerun=add_row)
    assert not r.passed


def test_empty_input_safe_passes_when_no_crash_and_tables_empty(
    db: Database,
) -> None:
    db.execute("CREATE TABLE t (id INT)")
    r = assert_empty_input_safe(
        db,
        run=lambda: None,  # noop — represents a run on empty input
        tables=["t"],
    )
    assert r.passed
    assert r.details["row_counts"]["t"] == 0


def test_malformed_input_rejected_passes_on_expected_exception() -> None:
    def bad_run() -> None:
        raise ValueError("missing required column 'amount'")

    r = assert_malformed_input_rejected(
        run=bad_run, expected_message_substring="missing required column"
    )
    assert r.passed


def test_malformed_input_rejected_fails_when_no_exception_raised() -> None:
    r = assert_malformed_input_rejected(
        run=lambda: None, expected_message_substring="anything"
    )
    assert not r.passed
    assert "no exception" in r.details["reason"].lower()


def test_malformed_input_rejected_fails_on_wrong_message() -> None:
    def bad_run() -> None:
        raise ValueError("disk full")

    r = assert_malformed_input_rejected(
        run=bad_run, expected_message_substring="missing column"
    )
    assert not r.passed
```

- [ ] **Step 2: Run to verify failure**

```
uv run pytest tests/scenarios/_runner_tests/test_harnesses.py -v
```
Expected: FAIL — `_harnesses` module doesn't exist.

- [ ] **Step 3: Implement the harness module**

Create `tests/scenarios/_harnesses.py`:

```python
"""Pipeline-execution harness primitives.

Distinct from ``moneybin.validation.assertions`` (which contain pure data
predicates): these helpers DRIVE pipeline operations (re-run, run with
empty input, run with bad input) and report on execution-time invariants
(no duplicate rows, no crash, expected error raised).

They live under ``tests/`` because they have no consumer outside the
scenario suite — ``data-reconciliation.md`` only consumes data
predicates. If a future runtime consumer emerges, lift the relevant
primitive into ``moneybin.validation``.
"""

from __future__ import annotations

import subprocess  # noqa: S404 — explicit command list, never shell=True
from collections.abc import Callable
from pathlib import Path

from moneybin.database import Database
from moneybin.validation.assertions._helpers import quote_ident
from moneybin.validation.result import AssertionResult


def assert_idempotent(
    db: Database,
    *,
    tables: list[str],
    rerun: Callable[[], None],
) -> AssertionResult:
    """Snapshot ``tables`` row counts, invoke ``rerun``, assert counts unchanged."""
    before = {t: _count(db, t) for t in tables}
    rerun()
    after = {t: _count(db, t) for t in tables}
    return AssertionResult(
        name="idempotent",
        passed=before == after,
        details={"before": before, "after": after},
    )


def assert_incremental_safe(
    db: Database,
    *,
    tables: list[str],
    load_a: Callable[[], None],
    load_b: Callable[[], None],
    expected_a_count: dict[str, int],
    expected_b_count: dict[str, int],
) -> AssertionResult:
    """Load A → assert counts; load B (overlapping) → assert only new rows added."""
    load_a()
    after_a = {t: _count(db, t) for t in tables}
    load_b()
    after_b = {t: _count(db, t) for t in tables}
    failures: list[str] = []
    for t in tables:
        if after_a[t] != expected_a_count.get(t):
            failures.append(
                f"{t} after-A: expected {expected_a_count[t]}, got {after_a[t]}"
            )
        if after_b[t] != expected_b_count.get(t):
            failures.append(
                f"{t} after-B: expected {expected_b_count[t]}, got {after_b[t]}"
            )
    return AssertionResult(
        name="incremental_safe",
        passed=not failures,
        details={
            "after_load_a": after_a,
            "after_load_b": after_b,
            "expected_a": expected_a_count,
            "expected_b": expected_b_count,
            "failures": failures,
        },
    )


def assert_empty_input_safe(
    db: Database,
    *,
    run: Callable[[], None],
    tables: list[str],
) -> AssertionResult:
    """Invoke ``run`` (with empty input fixture pre-loaded); assert no crash and tables empty."""
    try:
        run()
    except Exception as exc:  # noqa: BLE001 — surface as failure
        return AssertionResult(
            name="empty_input_safe",
            passed=False,
            details={"reason": "run raised", "exception_type": type(exc).__name__},
            error=str(exc),
        )
    counts = {t: _count(db, t) for t in tables}
    nonempty = {t: n for t, n in counts.items() if n > 0}
    return AssertionResult(
        name="empty_input_safe",
        passed=not nonempty,
        details={"row_counts": counts, "nonempty": nonempty},
    )


def assert_malformed_input_rejected(
    *,
    run: Callable[[], None],
    expected_message_substring: str,
    expected_exception_type: type[Exception] = Exception,
) -> AssertionResult:
    """Invoke ``run``; assert it raises ``expected_exception_type`` whose message contains the substring."""
    try:
        run()
    except expected_exception_type as exc:
        msg = str(exc)
        if expected_message_substring.lower() in msg.lower():
            return AssertionResult(
                name="malformed_input_rejected",
                passed=True,
                details={
                    "exception_type": type(exc).__name__,
                    "message_excerpt": msg[:200],
                },
            )
        return AssertionResult(
            name="malformed_input_rejected",
            passed=False,
            details={
                "reason": "exception raised but message did not match",
                "expected_substring": expected_message_substring,
                "actual_message": msg[:200],
            },
        )
    return AssertionResult(
        name="malformed_input_rejected",
        passed=False,
        details={"reason": "no exception was raised"},
    )


def assert_subprocess_parity(
    *,
    in_process_outputs: dict[str, int],
    subprocess_outputs: dict[str, int],
) -> AssertionResult:
    """Compare row counts produced by an in-process run vs a subprocess run; assert equal."""
    diff = {
        k: {
            "in_process": in_process_outputs.get(k),
            "subprocess": subprocess_outputs.get(k),
        }
        for k in set(in_process_outputs) | set(subprocess_outputs)
        if in_process_outputs.get(k) != subprocess_outputs.get(k)
    }
    return AssertionResult(
        name="subprocess_parity",
        passed=not diff,
        details={
            "in_process": in_process_outputs,
            "subprocess": subprocess_outputs,
            "diff": diff,
        },
    )


def _count(db: Database, table: str) -> int:
    row = db.execute(f"SELECT COUNT(*) FROM {quote_ident(table)}").fetchone()  # noqa: S608  # validated identifier
    return int(row[0]) if row else 0
```

- [ ] **Step 4: Run tests**

```
uv run pytest tests/scenarios/_runner_tests/test_harnesses.py -v
```
Expected: 6 passed.

## Task 8: Author the `idempotency-rerun` scenario

Drives `transform` twice on the same DB; row counts in core tables unchanged.

**Files:**
- Create: `tests/scenarios/data/idempotency-rerun.yaml`
- Create: `tests/scenarios/test_idempotency_rerun.py`

- [ ] **Step 1: Write the failing test**

Create `tests/scenarios/test_idempotency_rerun.py`:

```python
"""Scenario: re-running transform must not duplicate rows.

tiers: T1, T3-idempotency, T3-incremental.
"""

from __future__ import annotations

import pytest

from tests.scenarios._harnesses import assert_idempotent
from tests.scenarios._runner import load_shipped_scenario
from tests.scenarios._runner.runner import scenario_env
from tests.scenarios._runner.steps import run_step


@pytest.mark.scenarios
@pytest.mark.slow
def test_idempotency_rerun() -> None:
    scenario = load_shipped_scenario("idempotency-rerun")
    assert scenario is not None

    with scenario_env(scenario) as (db, _tmp, env):
        run_step("generate", scenario.setup, db, env=env)
        run_step("transform", scenario.setup, db, env=env)
        run_step("match", scenario.setup, db, env=env)

        result = assert_idempotent(
            db,
            tables=["core.fct_transactions", "core.dim_accounts"],
            rerun=lambda: run_step("transform", scenario.setup, db, env=env),
        )
    result.raise_if_failed()
```

- [ ] **Step 2: Create the YAML**

Create `tests/scenarios/data/idempotency-rerun.yaml`:

```yaml
scenario: idempotency-rerun
description: "Running transform twice on the same DB must not duplicate rows"

setup:
  persona: basic
  seed: 42
  years: 1
  fixtures: []

# Pipeline is empty — the test drives steps explicitly via scenario_env
# so it can snapshot row counts between the first and second transform.
pipeline: []
assertions: []
gates:
  required_assertions: all
```

- [ ] **Step 3: Run the test**

```
uv run pytest tests/scenarios/test_idempotency_rerun.py -v
```
Expected: PASS (or FAIL if a real idempotency bug exists — investigate per the spec's "fix the code" rule).

## Task 9: Author the `dedup-negative-fixture` scenario

Hand-authored CSV+OFX fixtures with three rows that look similar but should NOT collapse (different merchants on same day, different amounts within $0.01, etc.).

**Files:**
- Create: `tests/scenarios/data/fixtures/dedup-negative/distinct_csv.csv`
- Create: `tests/scenarios/data/fixtures/dedup-negative/distinct_ofx.csv`
- Create: `tests/scenarios/data/fixtures/dedup-negative/README.md`
- Create: `tests/scenarios/data/dedup-negative-fixture.yaml`
- Create: `tests/scenarios/test_dedup_negative_fixture.py`

- [ ] **Step 1: Hand-author fixture files**

Create `tests/scenarios/data/fixtures/dedup-negative/distinct_csv.csv`:
```
date,description,amount,source_transaction_id
2024-04-10,WHOLE FOODS,-32.45,NEG_CSV_2024-04-10_WF_32.45
2024-04-10,TRADER JOES,-28.10,NEG_CSV_2024-04-10_TJ_28.10
2024-04-15,STARBUCKS,-5.75,NEG_CSV_2024-04-15_SB_5.75
```

Create `tests/scenarios/data/fixtures/dedup-negative/distinct_ofx.csv`:
```
source_transaction_id,date,amount,payee,transaction_type
NEG_OFX_2024-04-10_AMZN,2024-04-10,-32.99,AMAZON.COM,DEBIT
NEG_OFX_2024-04-11_TJ,2024-04-11,-28.10,TRADER JOES,DEBIT
NEG_OFX_2024-04-15_DUNKIN,2024-04-15,-5.75,DUNKIN,DEBIT
```

These three pairs are *adversarial near-misses*:
- Same date + similar amount but different merchant (WHOLE FOODS vs AMAZON, both $32.x)
- Same merchant + same amount but different date (TJ on 04-10 vs 04-11)
- Same date + same amount but different merchant (STARBUCKS vs DUNKIN, both $5.75)

A dedup engine that collapses any of these is overmatching.

Create `tests/scenarios/data/fixtures/dedup-negative/README.md`:

```markdown
# dedup-negative fixture

Three pairs of (CSV, OFX) rows hand-authored to look superficially similar
but represent genuinely distinct transactions. The dedup engine MUST NOT
collapse any of them — see `tests/scenarios/test_dedup_negative_fixture.py`.

## Why each pair must not collapse

| CSV row | OFX row | Why distinct |
|---|---|---|
| WHOLE FOODS 2024-04-10 -$32.45 | AMAZON.COM 2024-04-10 -$32.99 | different merchants, different amounts |
| TRADER JOES 2024-04-10 -$28.10 | TRADER JOES 2024-04-11 -$28.10 | same merchant, different days |
| STARBUCKS 2024-04-15 -$5.75 | DUNKIN 2024-04-15 -$5.75 | different merchants on same day |

Expected `core.fct_transactions` count: 6 (no collapse).
```

- [ ] **Step 2: Write the YAML scenario**

Create `tests/scenarios/data/dedup-negative-fixture.yaml`:

```yaml
scenario: dedup-negative-fixture
description: "Adversarial near-miss rows MUST NOT collapse"

setup:
  persona: family
  seed: 42
  years: 1
  fixtures:
    - path: dedup-negative/distinct_csv.csv
      account: amazon-card
      source_type: csv
    - path: dedup-negative/distinct_ofx.csv
      account: amazon-card
      source_type: ofx

pipeline:
  - load_fixtures
  - transform
  - match

assertions:
  - name: row_count_no_collapse
    fn: assert_row_count_exact
    args:
      table: core.fct_transactions
      # Hand-counted: 3 CSV rows + 3 OFX rows, none should collapse.
      expected: 6
  - name: source_system_populated
    fn: assert_source_system_populated
    args:
      table: core.fct_transactions
      expected_sources: [csv, ofx]

expectations:
  - kind: match_decision
    description: "WHOLE FOODS 04-10 vs AMAZON 04-10 must NOT match (different merchants)"
    transactions:
      - source_transaction_id: NEG_CSV_2024-04-10_WF_32.45
        source_type: csv
      - source_transaction_id: NEG_OFX_2024-04-10_AMZN
        source_type: ofx
    expected: not_matched
  - kind: match_decision
    description: "TJ 04-10 vs TJ 04-11 must NOT match (same merchant, different days)"
    transactions:
      - source_transaction_id: NEG_CSV_2024-04-10_TJ_28.10
        source_type: csv
      - source_transaction_id: NEG_OFX_2024-04-11_TJ
        source_type: ofx
    expected: not_matched
  - kind: match_decision
    description: "STARBUCKS vs DUNKIN same day same amount must NOT match"
    transactions:
      - source_transaction_id: NEG_CSV_2024-04-15_SB_5.75
        source_type: csv
      - source_transaction_id: NEG_OFX_2024-04-15_DUNKIN
        source_type: ofx
    expected: not_matched

gates:
  required_assertions: all
  required_expectations: all
```

- [ ] **Step 3: Write the pytest test**

Create `tests/scenarios/test_dedup_negative_fixture.py`:

```python
"""Scenario: adversarial near-miss rows must not collapse.

tiers: T1, T2-negative-expectations.
"""

from __future__ import annotations

import pytest

from tests.scenarios._runner import load_shipped_scenario, run_scenario


@pytest.mark.scenarios
@pytest.mark.slow
def test_dedup_negative_fixture() -> None:
    scenario = load_shipped_scenario("dedup-negative-fixture")
    assert scenario is not None
    result = run_scenario(scenario)
    assert result.passed, result.failure_summary()
```

- [ ] **Step 4: Run the test**

```
uv run pytest tests/scenarios/test_dedup_negative_fixture.py -v
```
Expected: PASS. If FAIL, the dedup engine is over-collapsing — investigate; do not relax expectations.

## Task 10: Author the `empty-input-handling` scenario

**Files:**
- Create: `tests/scenarios/data/fixtures/empty-input/empty.csv`
- Create: `tests/scenarios/data/fixtures/empty-input/empty.ofx.csv`
- Create: `tests/scenarios/test_empty_input_handling.py`

- [ ] **Step 1: Create empty fixtures (header-only)**

Create `tests/scenarios/data/fixtures/empty-input/empty.csv`:
```
date,description,amount,source_transaction_id
```

Create `tests/scenarios/data/fixtures/empty-input/empty.ofx.csv`:
```
source_transaction_id,date,amount,payee,transaction_type
```

Header-only files are the realistic empty-input shape — a file produced by an export that found no transactions in the requested range.

- [ ] **Step 2: Write the test**

Create `tests/scenarios/test_empty_input_handling.py`:

```python
"""Scenario: empty input fixture must not crash; downstream tables stay empty.

tiers: T1, T3-empty-input.
"""

from __future__ import annotations

import pytest

from moneybin.database import Database
from tests.scenarios._harnesses import assert_empty_input_safe
from tests.scenarios._runner.fixture_loader import load_fixture_into_db
from tests.scenarios._runner.loader import FixtureSpec
from tests.scenarios._runner.runner import scenario_env
from tests.scenarios._runner.steps import run_step

# Synthetic in-test scenario name so `scenario_env` accepts it without a YAML.
from tests.scenarios._runner.loader import Scenario, SetupSpec


def _empty_scenario() -> Scenario:
    return Scenario(
        scenario="empty-input-handling",
        setup=SetupSpec(persona="basic", seed=42, years=1),
        pipeline=[],
    )


@pytest.mark.scenarios
@pytest.mark.slow
def test_empty_input_handling() -> None:
    scenario = _empty_scenario()
    csv_spec = FixtureSpec(
        path="empty-input/empty.csv", account="empty-card", source_type="csv"
    )
    ofx_spec = FixtureSpec(
        path="empty-input/empty.ofx.csv", account="empty-card", source_type="ofx"
    )

    with scenario_env(scenario) as (db, _tmp, env):
        load_fixture_into_db(db, csv_spec)
        load_fixture_into_db(db, ofx_spec)

        def run_pipeline() -> None:
            run_step("transform", scenario.setup, db, env=env)
            run_step("match", scenario.setup, db, env=env)

        result = assert_empty_input_safe(
            db,
            run=run_pipeline,
            tables=[
                "core.fct_transactions",
                "core.dim_accounts",
            ],
        )
    result.raise_if_failed()


def _ensure_db(d: Database) -> None:  # noqa: ARG001 — placeholder if migrations needed pre-load
    """Migrations should already be applied via _bootstrap_database."""
```

The `Scenario` model accepts an `alias="scenario"` for `name`, so `Scenario(scenario="empty-input-handling", ...)` constructs correctly.

- [ ] **Step 3: Run**

```
uv run pytest tests/scenarios/test_empty_input_handling.py -v
```
Expected: PASS. If transforms crash on empty raw tables, the pipeline has an empty-input bug — surface it as a failure, do not work around it.

## Task 11: Author the `malformed-input-rejection` scenario

**Files:**
- Create: `tests/scenarios/data/fixtures/malformed/missing_amount.csv`
- Create: `tests/scenarios/data/fixtures/malformed/truncated.ofx.csv`
- Create: `tests/scenarios/test_malformed_input_rejection.py`

- [ ] **Step 1: Hand-author malformed fixtures**

Create `tests/scenarios/data/fixtures/malformed/missing_amount.csv`:
```
date,description,source_transaction_id
2024-05-01,COFFEE,MAL_2024-05-01_001
```
(no `amount` column)

Create `tests/scenarios/data/fixtures/malformed/truncated.ofx.csv`:
```
source_transaction_id,date,amount
MAL_OFX_001,2024-05-01,-3.50
```
(missing required `payee` and `transaction_type` columns the OFX raw loader expects)

- [ ] **Step 2: Write the test**

Create `tests/scenarios/test_malformed_input_rejection.py`:

```python
"""Scenario: malformed input fixtures must raise a clear error, not silently load.

tiers: T1, T3-malformed-input.
"""

from __future__ import annotations

import pytest

from tests.scenarios._harnesses import assert_malformed_input_rejected
from tests.scenarios._runner.fixture_loader import load_fixture_into_db
from tests.scenarios._runner.loader import FixtureSpec, Scenario, SetupSpec
from tests.scenarios._runner.runner import scenario_env


def _malformed_scenario() -> Scenario:
    return Scenario(
        scenario="malformed-input-rejection",
        setup=SetupSpec(persona="basic", seed=42, years=1),
        pipeline=[],
    )


@pytest.mark.scenarios
@pytest.mark.slow
def test_malformed_csv_missing_amount_column() -> None:
    scenario = _malformed_scenario()
    spec = FixtureSpec(
        path="malformed/missing_amount.csv",
        account="bad-card",
        source_type="csv",
    )

    with scenario_env(scenario) as (db, _tmp, _env):
        result = assert_malformed_input_rejected(
            run=lambda: load_fixture_into_db(db, spec),
            expected_message_substring="amount",
        )
    result.raise_if_failed()


@pytest.mark.scenarios
@pytest.mark.slow
def test_malformed_ofx_missing_payee_column() -> None:
    scenario = _malformed_scenario()
    spec = FixtureSpec(
        path="malformed/truncated.ofx.csv",
        account="bad-card",
        source_type="ofx",
    )

    with scenario_env(scenario) as (db, _tmp, _env):
        result = assert_malformed_input_rejected(
            run=lambda: load_fixture_into_db(db, spec),
            expected_message_substring="payee",
        )
    result.raise_if_failed()
```

- [ ] **Step 3: Run**

```
uv run pytest tests/scenarios/test_malformed_input_rejection.py -v
```

Expected: PASS. If the loader silently produces null cells instead of raising, that's a malformed-input handling bug — file as a follow-up. If the error message text differs, update the substring to a substring that's actually in the raised error (this is a contract: pick a substring that uniquely identifies the missing-column scenario).

## Task 12: Update `tier matrix` test in spec to reference new scenarios (optional verification)

The spec's R2 matrix already lists all four new scenarios. No code changes — but verify the tier-matrix-lint check (Testing Strategy #1) covers the new tests by adding the `tiers:` declaration to each test docstring (already done in the test snippets above).

- [ ] **Step 1: Verify each new test docstring contains a `tiers:` line**

```
grep -l "tiers:" tests/scenarios/test_idempotency_rerun.py tests/scenarios/test_dedup_negative_fixture.py tests/scenarios/test_empty_input_handling.py tests/scenarios/test_malformed_input_rejection.py
```

Expected: all four files match. If not, add `tiers: T1, ...` to the module docstring.

## Task 13: Phase 4 commit

- [ ] **Step 1: Run full check**

```
make check test
uv run pytest tests/scenarios/ -m scenarios -v
```
Expected: green.

- [ ] **Step 2: Commit Phase 4**

```
git add tests/scenarios/_harnesses.py tests/scenarios/_runner_tests/test_harnesses.py tests/scenarios/data/idempotency-rerun.yaml tests/scenarios/data/dedup-negative-fixture.yaml tests/scenarios/data/fixtures/dedup-negative/ tests/scenarios/data/fixtures/empty-input/ tests/scenarios/data/fixtures/malformed/ tests/scenarios/test_idempotency_rerun.py tests/scenarios/test_dedup_negative_fixture.py tests/scenarios/test_empty_input_handling.py tests/scenarios/test_malformed_input_rejection.py
git commit -m "$(cat <<'EOF'
Add idempotency, dedup-negative, empty, malformed scenarios

- Add tests/scenarios/_harnesses.py with assert_idempotent,
  assert_incremental_safe, assert_empty_input_safe,
  assert_malformed_input_rejected, assert_subprocess_parity
- Author idempotency-rerun: drives transform twice; verifies row
  counts unchanged (Tier 3)
- Author dedup-negative-fixture: three adversarial near-miss pairs
  that MUST NOT collapse — same merchant different day, same day
  different merchant, similar amounts (Tier 2 negative expectations)
- Author empty-input-handling: header-only CSV+OFX fixtures; pipeline
  must not crash and core tables must stay empty (Tier 3)
- Author malformed-input-rejection: missing-column CSV and truncated
  OFX must raise with a clear, column-naming error (Tier 3)
EOF
)"
```

---

# Phase 5 — Tier 2/4 Enrichment

Adds per-category P/R thresholds to categorization scoring, transfer P+R as separate gates, ground-truth coverage assertion, and date-continuity wiring.

## Task 14: Add `assert_ground_truth_coverage` primitive

**Files:**
- Modify: `src/moneybin/validation/assertions/distribution.py`
- Modify: `src/moneybin/validation/assertions/__init__.py`
- Modify: `tests/scenarios/_runner/_assertion_registry.py`
- Test: `tests/moneybin/test_validation/test_assertions_distribution.py`

- [ ] **Step 1: Write the failing test**

Create or extend `tests/moneybin/test_validation/test_assertions_distribution.py`:

```python
"""Tests for ground-truth coverage assertion."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from moneybin.database import Database
from moneybin.validation.assertions.distribution import (
    assert_ground_truth_coverage,
)


@pytest.fixture()
def db(tmp_path: Path, mock_secret_store: MagicMock) -> Database:
    database = Database(
        tmp_path / "test.duckdb", secret_store=mock_secret_store, no_auto_upgrade=True
    )
    database.execute("CREATE SCHEMA IF NOT EXISTS core")
    database.execute("CREATE SCHEMA IF NOT EXISTS prep")
    database.execute("CREATE SCHEMA IF NOT EXISTS synthetic")
    database.execute("CREATE TABLE core.fct_transactions (transaction_id VARCHAR)")
    database.execute(
        "CREATE TABLE prep.int_transactions__matched (transaction_id VARCHAR, source_transaction_id VARCHAR)"
    )
    database.execute(
        "CREATE TABLE synthetic.ground_truth (source_transaction_id VARCHAR, expected_category VARCHAR)"
    )
    return database


def test_ground_truth_coverage_passes_when_threshold_met(db: Database) -> None:
    db.execute(
        "INSERT INTO core.fct_transactions VALUES ('T1'), ('T2'), ('T3'), ('T4'), ('T5')"
    )
    db.execute("""
        INSERT INTO prep.int_transactions__matched VALUES
            ('T1','S1'),('T2','S2'),('T3','S3'),('T4','S4'),('T5','S5')
    """)
    db.execute("""
        INSERT INTO synthetic.ground_truth VALUES
            ('S1','grocery'),('S2','grocery'),('S3','grocery'),
            ('S4','grocery'),('S5','grocery')
    """)
    r = assert_ground_truth_coverage(db, min_coverage=0.9)
    assert r.passed, r.details


def test_ground_truth_coverage_fails_below_threshold(db: Database) -> None:
    db.execute("INSERT INTO core.fct_transactions VALUES ('T1'),('T2'),('T3'),('T4')")
    db.execute("""
        INSERT INTO prep.int_transactions__matched VALUES
            ('T1','S1'),('T2','S2'),('T3','S3'),('T4','S4')
    """)
    db.execute(
        "INSERT INTO synthetic.ground_truth VALUES ('S1','grocery'),('S2','grocery')"
    )
    r = assert_ground_truth_coverage(db, min_coverage=0.9)
    assert not r.passed
    assert r.details["coverage"] == 0.5
```

- [ ] **Step 2: Run to verify failure**

```
uv run pytest tests/moneybin/test_validation/test_assertions_distribution.py -v
```
Expected: FAIL with `ImportError`.

- [ ] **Step 3: Implement primitive**

Append to `src/moneybin/validation/assertions/distribution.py`:

```python
from moneybin.tables import FCT_TRANSACTIONS, GROUND_TRUTH, INT_TRANSACTIONS_MATCHED


def assert_ground_truth_coverage(
    db: Database, *, min_coverage: float
) -> AssertionResult:
    """Assert ≥``min_coverage`` fraction of ``core.fct_transactions`` has a labeled
    ``synthetic.ground_truth.expected_category``.

    Coverage = (rows in fct that join through int_matched to ground_truth with
    a non-null expected_category) / (total rows in fct).

    Catches the failure mode where evaluations achieve high accuracy by
    scoring only a tiny labeled subset.
    """
    if not 0.0 <= min_coverage <= 1.0:
        raise ValueError(f"min_coverage must be in [0, 1], got {min_coverage}")
    total_row = db.execute(
        f"SELECT COUNT(*) FROM {FCT_TRANSACTIONS.full_name}"  # noqa: S608  # TableRef
    ).fetchone()
    total = int(total_row[0]) if total_row else 0
    labeled_row = db.execute(f"""
        SELECT COUNT(*) FROM {FCT_TRANSACTIONS.full_name} t
        JOIN {INT_TRANSACTIONS_MATCHED.full_name} m
          ON m.transaction_id = t.transaction_id
        JOIN {GROUND_TRUTH.full_name} gt
          ON gt.source_transaction_id = m.source_transaction_id
        WHERE gt.expected_category IS NOT NULL
    """).fetchone()  # noqa: S608  # TableRef constants
    labeled = int(labeled_row[0]) if labeled_row else 0
    coverage = (labeled / total) if total else 0.0
    return AssertionResult(
        name="ground_truth_coverage",
        passed=coverage >= min_coverage,
        details={
            "labeled": labeled,
            "total": total,
            "coverage": round(coverage, 4),
            "min_coverage": min_coverage,
        },
    )
```

- [ ] **Step 4: Re-export and register**

Edit `src/moneybin/validation/assertions/__init__.py` — add `assert_ground_truth_coverage` to the `distribution` imports and `__all__`.

Edit `tests/scenarios/_runner/_assertion_registry.py` — add to imports and the registry dict (alphabetical).

- [ ] **Step 5: Run**

```
uv run pytest tests/moneybin/test_validation/test_assertions_distribution.py tests/scenarios/_runner_tests/test_assertion_registry.py -v
```
Expected: PASS.

## Task 15: Wire P/R + ground-truth coverage + date continuity into `family-full-pipeline`

**Files:**
- Modify: `tests/scenarios/test_family_full_pipeline.py`

- [ ] **Step 1: Extend the test**

Append to the `extra_assertions` callback (or build a richer one):

```python
def extra(db: Database) -> list[AssertionResult]:
    return [
        # ... Phase 3 backfill assertions already here ...
        assert_ground_truth_coverage(db, min_coverage=0.9),
        assert_date_continuity(
            db,
            table="core.fct_transactions",
            date_col="transaction_date",
            account_col="account_id",
        ),
    ]
```

Then for evaluation P/R thresholds: assert against the breakdown after `run_scenario` returns:

```python
# Verify per-category recall — the categorization evaluation already
# computes this; we surface it as an explicit pass condition rather
# than relying on overall accuracy.
cat_eval = next(e for e in result.evaluations if e.name == "categorization_accuracy")
per_cat = cat_eval.breakdown["per_category"]
for category, stats in per_cat.items():
    if stats["support"] >= 5:
        # Only assert P/R on categories with ≥5 labeled rows; below
        # that, the per-category metric is too noisy to gate on.
        assert stats["recall"] >= 0.5, f"recall too low for {category}: {stats}"

# Verify transfer detection P and R separately, not just F1.
tx_eval = next(e for e in result.evaluations if e.name == "transfer_f1")
assert tx_eval.breakdown["precision"] >= 0.8, tx_eval.breakdown
assert tx_eval.breakdown["recall"] >= 0.8, tx_eval.breakdown
```

The `0.5`/`0.8` thresholds are the **floor** below which the test fails — they're not the target; investigate any failure rather than relax. Justify each threshold in a comment with a brief rationale.

- [ ] **Step 2: Run**

```
uv run pytest tests/scenarios/test_family_full_pipeline.py -v
```
Expected: PASS.

## Task 16: Add `assert_date_continuity` + transfer P/R to `transfer-detection-cross-account`

**Files:**
- Modify: `tests/scenarios/test_transfer_detection.py`

- [ ] **Step 1: Add the same patterns from Task 15**

Pass an `extra_assertions=lambda db: [assert_date_continuity(db, table="core.fct_transactions", date_col="transaction_date", account_col="account_id")]` to `run_scenario`. Then assert `precision`/`recall` thresholds from `result.evaluations`.

- [ ] **Step 2: Run**

```
uv run pytest tests/scenarios/test_transfer_detection.py -v
```
Expected: PASS.

## Task 17: Phase 5 commit

- [ ] **Step 1: Run full check**

```
make check test
uv run pytest tests/scenarios/ -m scenarios -v
```

- [ ] **Step 2: Commit Phase 5**

```
git add src/moneybin/validation/assertions/distribution.py src/moneybin/validation/assertions/__init__.py tests/moneybin/test_validation/test_assertions_distribution.py tests/scenarios/_runner/_assertion_registry.py tests/scenarios/test_family_full_pipeline.py tests/scenarios/test_transfer_detection.py
git commit -m "$(cat <<'EOF'
Wire P/R breakdowns, ground-truth coverage, date continuity

- Add assert_ground_truth_coverage primitive (Tier 4): labeled fraction
  of fct_transactions vs threshold, catches gaming of tiny labeled subset
- Wire ground-truth coverage and date-continuity into family-full-pipeline
- Assert per-category recall floor (categories with ≥5 support) instead
  of relying on overall accuracy
- Assert transfer detection precision and recall separately, not just F1,
  to catch one-sided bias (high-precision low-recall or the inverse)
- Wire date-continuity into transfer-detection-cross-account
EOF
)"
```

---

# Phase 6 — Recipe + Governance

Updates `docs/guides/scenario-authoring.md` with references to the new primitives, refreshes `CONTRIBUTING.md`, updates the two cross-referenced specs, marks `testing-scenario-comprehensive.md` as `implemented`, updates the spec INDEX, flips the README roadmap icon, and runs the `/simplify` pre-push pass per `.claude/rules/shipping.md`.

## Task 18: Update `docs/guides/scenario-authoring.md`

**Files:**
- Modify: `docs/guides/scenario-authoring.md`

- [ ] **Step 1: Update the Step-5 example to reference the post-Phase-3 primitives**

Edit `docs/guides/scenario-authoring.md` lines 84-110 (the "Use the assertion primitives" example). Replace `assert_row_count` (which doesn't exist as a single name) and `assert_negative_match` (also not a real primitive) with the actual public names:

```python
import pytest
from moneybin.validation.assertions import (
    assert_amount_precision,
    assert_date_bounds,
    assert_no_duplicates,
    assert_row_count_exact,
    assert_schema_snapshot,
    assert_source_system_populated,
)
from moneybin.validation.expectations import verify_match_decision  # for negative cases


@pytest.mark.scenarios
def test_csv_amazon_trailing_comma(scenario_db, load_fixture, run_pipeline):
    """Reproduce bug #142: trailing comma in Amazon CSV row dropped from raw.

    tiers: T1, T3-malformed-input
    derivation: 20 rows in input.csv counted by hand; 3 are intentional dupes.
    """
    load_fixture("csv-amazon-trailing-comma/input.csv")
    run_pipeline(["transform", "match"])

    assert_row_count_exact(
        scenario_db, table="raw.tabular_transactions", expected=20
    ).raise_if_failed()
    assert_row_count_exact(
        scenario_db, table="core.fct_transactions", expected=17
    ).raise_if_failed()
    assert_no_duplicates(
        scenario_db, table="core.fct_transactions", columns=["transaction_id"]
    ).raise_if_failed()
    assert_source_system_populated(
        scenario_db, table="core.fct_transactions", expected_sources={"csv"}
    ).raise_if_failed()
```

- [ ] **Step 2: Add a "Negative expectations" pointer**

Replace the (`assert_negative_match` reference with) negative expectations section:

```markdown
For a "should NOT match" assertion, use `verify_match_decision` with
`expected="not_matched"` — see `tests/scenarios/test_dedup_negative_fixture.py`
for a worked example.
```

- [ ] **Step 3: Add "Available primitives" mini-table**

After Step 5, before Step 6, add:

```markdown
### Available primitives (post-Phase-3)

| Primitive | Tier | Use for |
|---|---|---|
| `assert_row_count_exact` | T1 | Exact row count derived from fixture or generator |
| `assert_no_nulls` | T1 | Required columns must be populated |
| `assert_no_duplicates` | T1 | Natural key uniqueness |
| `assert_valid_foreign_keys` | T1 | Child rows reference existing parents |
| `assert_no_orphans` | T1 | Provenance completeness |
| `assert_source_system_populated` | T1 | Source attribution |
| `assert_schema_snapshot` | T1 | Column set + types |
| `assert_amount_precision` | T1 | Money columns are DECIMAL(p,s) |
| `assert_date_bounds` | T1 | Dates within declared window |
| `assert_sign_convention` | T1 | Expense<0, income>0, transfers exempt |
| `assert_balanced_transfers` | T2 | Transfer pairs sum to zero |
| `assert_distribution_within_bounds` | T2 | Match confidence / amount distribution |
| `assert_ground_truth_coverage` | T4 | Labeled subset is ≥X% of total |
| `assert_date_continuity` | T4 | No month gaps per account |
| `verify_match_decision` (with `expected="not_matched"`) | T2 | Negative expectations |
| Harnesses: `assert_idempotent`, `assert_empty_input_safe`, `assert_malformed_input_rejected` | T3 | Pipeline-execution patterns |
```

## Task 19: Update `CONTRIBUTING.md`

**Files:**
- Modify: `CONTRIBUTING.md`

- [ ] **Step 1: Confirm existing scenario section is current**

Read `CONTRIBUTING.md` lines 73-90 (the "Authoring a new scenario" section). The text already references `docs/guides/scenario-authoring.md`. Verify it points at all three sources of truth:
- The recipe guide
- The taxonomy spec
- The independent-expectations rule

If any of those three is missing, add a one-line bullet linking it.

- [ ] **Step 2: Add a one-paragraph summary of the five-tier taxonomy**

Insert after the existing recipe summary, before the next section:

```markdown
Every new scenario must declare its **tier coverage** (in the test
docstring as `tiers: T1, T2-...`). The five-tier taxonomy is defined in
[`docs/specs/testing-scenario-comprehensive.md`](docs/specs/testing-scenario-comprehensive.md):
T1 (structural invariants) is required everywhere; T2 (semantic
correctness), T3 (pipeline behavior), T4 (distribution / quality), and
T5 (operational) apply where relevant.
```

## Task 20: Update `testing-overview.md` and `testing-scenario-runner.md`

**Files:**
- Modify: `docs/specs/testing-overview.md`
- Modify: `docs/specs/testing-scenario-runner.md`

- [ ] **Step 1: Add cross-reference in `testing-overview.md`**

In the "Scenarios" section (around line 132), add a sentence:

```markdown
Scenario authoring rules — taxonomy, independent-expectations rule, and
contributor recipe — are the responsibility of
[`testing-scenario-comprehensive.md`](testing-scenario-comprehensive.md),
which is the architectural authority for all scenario work.
```

- [ ] **Step 2: Update `testing-scenario-runner.md`**

In the "Status" or top-of-file note (around line 1-10), add:

```markdown
> Scenario taxonomy and authoring recipe live in
> [`testing-scenario-comprehensive.md`](testing-scenario-comprehensive.md);
> this doc covers the runner mechanics (loader, registries, harness) only.
```

## Task 21: Mark spec implemented + update INDEX + README roadmap

**Files:**
- Modify: `docs/specs/testing-scenario-comprehensive.md`
- Modify: `docs/specs/INDEX.md`
- Modify: `README.md`

- [ ] **Step 1: Update spec status**

Edit `docs/specs/testing-scenario-comprehensive.md` line 4: change `in-progress` to `implemented`.

- [ ] **Step 2: Update spec INDEX**

Edit `docs/specs/INDEX.md`. Find the row for `testing-scenario-comprehensive.md` and change its status column from `in-progress` to `implemented`.

- [ ] **Step 3: Update README roadmap**

Edit `README.md`. Find the roadmap entry for "comprehensive scenario testing" (or whatever existing line references this spec) and change its leading icon from 📐 (or 🗓️) to ✅. Per `.claude/rules/shipping.md`, also ensure the "What Works Today" section mentions:
- Five-tier scenario taxonomy
- 10 scenarios (6 existing + 4 new)
- Bug-report → scenario authoring recipe at `docs/guides/scenario-authoring.md`

Add or extend a sentence in the testing/quality area:

```markdown
**Scenario test suite (10 scenarios):** Whole-pipeline regression coverage
across structural invariants, semantic correctness (categorization P/R,
transfer F1+P+R, negative expectations), pipeline behavior (idempotency,
empty/malformed input handling), and quality (date continuity,
ground-truth coverage). New scenarios follow the bug-report recipe at
[`docs/guides/scenario-authoring.md`](docs/guides/scenario-authoring.md).
```

## Task 22: `/simplify` pre-push pass + Phase 6 commit

Per `.claude/rules/shipping.md`, run `/simplify` against the changes before the final commit.

- [ ] **Step 1: Run /simplify**

In the worktree, invoke the `simplify` skill scoped to this branch's changes:

```
# (Run via the simplify skill — focus: validation/, _harnesses.py, new scenario tests)
```

Address findings inline (no separate commit needed unless the user prefers it; rolling fixes into the Phase 6 commit is fine).

- [ ] **Step 2: Run full check**

```
make check test-all
uv run pytest tests/scenarios/ -m scenarios -v
```
Expected: green across unit, integration, scenarios.

- [ ] **Step 3: Commit Phase 6**

```
git add docs/ README.md CONTRIBUTING.md
# plus any /simplify edits
git commit -m "$(cat <<'EOF'
Mark scenario-testing-comprehensive implemented

- Update docs/guides/scenario-authoring.md with post-Phase-3 primitive
  list and a worked negative-expectations example
- Add scenario taxonomy summary to CONTRIBUTING.md
- Cross-reference comprehensive spec from testing-overview.md and
  testing-scenario-runner.md
- Move docs/specs/testing-scenario-comprehensive.md status to
  'implemented'; update spec INDEX.md
- Flip README roadmap entry to ✅ and document the 10-scenario suite
  in 'What Works Today'
EOF
)"
```

- [ ] **Step 4: Push and open PR**

(Defer to the user's `commit-push-pr` flow — this plan ends after the four phase-scoped commits land on the branch. The user opens the single PR off `feat/scenario-testing-completion` covering Phases 3–6.)

---

## Self-Review Notes

**Spec coverage:**
- R1 Tier 1 → Tasks 1–6 (all four missing primitives + scenario wiring)
- R1 Tier 2 → Task 15 (P/R) + Task 9 (negative expectations)
- R1 Tier 3 → Tasks 7–11 (all four new harness-driven scenarios)
- R1 Tier 4 → Task 14 (ground_truth_coverage), Task 15 (date continuity)
- R1 Tier 5 → Out of scope per spec line 285-286
- R2 matrix → All scenarios in the matrix are addressed by Tasks 6–11
- R3 independent expectations → enforced via deterministic-generator-derived counts and hand-counted fixtures throughout
- R4 bug-report recipe → already shipped; Task 18 refreshes it
- R5 relocation → already done in Phase 1
- R6 validation library → Phase 3 extends it; primitives stay in `validation/`, harnesses stay in `tests/scenarios/_harnesses.py` per spec line 175

**Known judgment calls flagged for the implementer:**
- The `extra_assertions` callback vs. `scenario_env` extraction (Task 6 step 3 + step 4): both are introduced; pick scenario_env extraction as the unifying primitive and rebuild `run_scenario` on top of it. The `extra_assertions` callback is OK as a complementary lighter-weight hook; if it feels redundant after scenario_env exists, drop it and have all tests use `scenario_env` directly.
- Schema snapshot column lists: enumerate by reading `sqlmesh/models/core/*.sql`, not by querying the DB and pasting. If the SQL has computed columns whose DuckDB-derived types are non-obvious, that's signal — model the type in the SQL with an explicit `CAST`.
- Threshold floors in Task 15 (`recall >= 0.5`, `precision/recall >= 0.8`): these are starting points. Run once on green code; if any threshold is far below the actual value, raise it to `actual - 0.05` (rounded down) so the test catches regressions without being noisy.
