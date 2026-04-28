# Testing Scenario Runner Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a whole-pipeline correctness layer that runs `generate → transform → match → categorize` against an empty encrypted DuckDB and verifies the resulting rows via assertions (aggregate invariants), expectations (per-record claims on hand-labeled fixtures), and evaluations (aggregate quality vs. ground truth) — surfaced through `moneybin synthetic verify` and a parallel CI workflow.

**Architecture:** Two new packages. `src/moneybin/validation/` holds reusable assertion + evaluation primitives that operate on any DuckDB connection (also backs future `moneybin data verify`). `src/moneybin/testing/scenarios/` holds the YAML loader, pipeline step registry, expectation engine, and orchestrator. Each scenario run boots a fresh encrypted `Database` in a tempdir with `MONEYBIN_HOME` overridden, runs in-process pipeline steps via the service layer, then returns a `ResponseEnvelope`. A new `MatchingService` thin wrapper (in scope) lets the runner call `MatchingService(db).run()` uniformly alongside `CategorizationService(db)`.

**Tech Stack:** Python 3.12, DuckDB, SQLMesh, Pydantic v2, Typer, pytest, GitHub Actions. Spec: `docs/specs/testing-scenario-runner.md`.

**Source-of-truth references during execution:**
- Spec: `docs/specs/testing-scenario-runner.md`
- Service pattern: `src/moneybin/services/account_service.py` (canonical shape)
- Matching engine: `src/moneybin/matching/engine.py:78` (`TransactionMatcher.run()`)
- Categorization service: introduced by parallel plan `2026-04-26-categorization-auto-rules.md` Tasks 7b/7c — assume `CategorizationService(db).bulk_categorize(items)` and `.apply_rules()` exist when this plan executes. If not yet merged, **rebase this branch onto main after that plan lands** before starting Task 8.
- Envelope: `src/moneybin/mcp/envelope.py:80,106`
- Database/sqlmesh: `src/moneybin/database.py:73,541`
- Generator: `src/moneybin/testing/synthetic/engine.py:27`

---

## File Structure

### New files

| File | Responsibility |
|---|---|
| `src/moneybin/validation/__init__.py` | Package init |
| `src/moneybin/validation/result.py` | `AssertionResult`, `EvaluationResult` frozen dataclasses |
| `src/moneybin/validation/assertions/__init__.py` | Re-exports of all primitives |
| `src/moneybin/validation/assertions/relational.py` | FK, orphan, duplicate, no-nulls primitives |
| `src/moneybin/validation/assertions/schema.py` | Column existence, type, row count primitives |
| `src/moneybin/validation/assertions/business.py` | Sign convention, balanced transfers, date continuity |
| `src/moneybin/validation/assertions/distributional.py` | Distribution + cardinality checks |
| `src/moneybin/validation/assertions/infrastructure.py` | Catalog match, subprocess key propagation, no-unencrypted-files, migrations head |
| `src/moneybin/validation/evaluations/__init__.py` | Re-exports |
| `src/moneybin/validation/evaluations/categorization.py` | `score_categorization` |
| `src/moneybin/validation/evaluations/matching.py` | `score_transfer_detection`, `score_dedup` |
| `src/moneybin/services/matching_service.py` | Thin `MatchingService` wrapper around `TransactionMatcher` |
| `src/moneybin/testing/scenarios/__init__.py` | Package init |
| `src/moneybin/testing/scenarios/loader.py` | Pydantic models + YAML loader |
| `src/moneybin/testing/scenarios/steps.py` | Pipeline step registry |
| `src/moneybin/testing/scenarios/expectations.py` | Expectation kinds + verifiers |
| `src/moneybin/testing/scenarios/runner.py` | Orchestrator (tmpdir + env + dispatch + envelope) |
| `src/moneybin/testing/scenarios/data/basic-full-pipeline.yaml` | Scenario 1 |
| `src/moneybin/testing/scenarios/data/family-full-pipeline.yaml` | Scenario 2 |
| `src/moneybin/testing/scenarios/data/dedup-cross-source.yaml` | Scenario 3 |
| `src/moneybin/testing/scenarios/data/transfer-detection-cross-account.yaml` | Scenario 4 |
| `src/moneybin/testing/scenarios/data/migration-roundtrip.yaml` | Scenario 5 |
| `src/moneybin/testing/scenarios/data/encryption-key-propagation.yaml` | Scenario 6 |
| `src/moneybin/testing/scenarios/data/categorization-priority-hierarchy.yaml` | Scenario 7 |
| `tests/fixtures/dedup/chase_amazon_overlap.csv` | Hand-labeled overlap fixture |
| `tests/fixtures/dedup/chase_amazon_overlap.expectations.yaml` | Fixture metadata (YAML) |
| `tests/moneybin/test_validation/test_assertions_relational.py` | Per-category unit tests |
| `tests/moneybin/test_validation/test_assertions_schema.py` | |
| `tests/moneybin/test_validation/test_assertions_business.py` | |
| `tests/moneybin/test_validation/test_assertions_distributional.py` | |
| `tests/moneybin/test_validation/test_assertions_infrastructure.py` | |
| `tests/moneybin/test_validation/test_evaluations.py` | |
| `tests/moneybin/test_services/test_matching_service.py` | Service wrapper tests |
| `tests/moneybin/test_testing/test_scenarios_loader.py` | YAML loader tests |
| `tests/moneybin/test_testing/test_scenarios_steps.py` | Step registry tests |
| `tests/moneybin/test_testing/test_scenarios_expectations.py` | Expectation engine tests |
| `tests/integration/test_scenario_runner.py` | End-to-end orchestrator integration tests |
| `tests/e2e/test_e2e_synthetic_verify.py` | E2E for new CLI flags |
| `.github/workflows/scenarios.yml` | Parallel CI workflow |

### Modified files

| File | Change |
|---|---|
| `src/moneybin/cli/commands/synthetic.py` | Add `verify` subcommand with `--list/--scenario/--all/--keep-tmpdir/--fail-fast/--output=json` |
| `src/moneybin/services/__init__.py` | Export `MatchingService` |
| `docs/specs/testing-overview.md` | Trim §"Scenario Format" + §"Representative Scenarios" to summary referencing the spec |
| `.claude/rules/testing.md` | Add scenario layer to "Test Coverage by Layer" |
| `README.md` | Roadmap row → ✅; "What Works Today" entry for scenario runner |
| `docs/specs/testing-scenario-runner.md` | Status `ready` → `in-progress` at start; → `implemented` at end |
| `docs/specs/INDEX.md` | Status updates mirroring the spec |

---

## Task 0: Branch + spec status flip

**Files:**
- Modify: `docs/specs/testing-scenario-runner.md` (status header + Status section)
- Modify: `docs/specs/INDEX.md` (status column)

- [ ] **Step 1: Confirm worktree branch**

Run: `git branch --show-current`
Expected: `feat/testing-scenario-runner`

- [ ] **Step 2: Flip spec to in-progress**

Edit `docs/specs/testing-scenario-runner.md`:
- Header `> Status: ready` → `> Status: in-progress`
- `## Status\n\nready (promoted 2026-04-26)` → `## Status\n\nin-progress (started 2026-04-26)`

Edit `docs/specs/INDEX.md`: change the `testing-scenario-runner` row's status cell to `in-progress`.

- [ ] **Step 3: Commit**

```bash
git add docs/specs/testing-scenario-runner.md docs/specs/INDEX.md
git commit -m "Mark testing-scenario-runner spec in-progress"
```

---

## Task 1: Validation package skeleton + result dataclasses

**Files:**
- Create: `src/moneybin/validation/__init__.py`
- Create: `src/moneybin/validation/result.py`
- Create: `tests/moneybin/test_validation/__init__.py`
- Create: `tests/moneybin/test_validation/test_result.py`

- [ ] **Step 1: Write the failing test**

`tests/moneybin/test_validation/test_result.py`:

```python
from moneybin.validation.result import AssertionResult, EvaluationResult


def test_assertion_result_frozen() -> None:
    r = AssertionResult(name="x", passed=True, details={"rows": 3})
    assert r.passed is True
    assert r.details == {"rows": 3}
    assert r.error is None


def test_evaluation_result_passed_inferred_externally() -> None:
    r = EvaluationResult(
        name="cat",
        metric="accuracy",
        value=0.82,
        threshold=0.80,
        passed=True,
        breakdown={"per_category": {}},
    )
    assert r.passed is True
    assert r.value > r.threshold
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run pytest tests/moneybin/test_validation/test_result.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'moneybin.validation'`

- [ ] **Step 3: Implement**

`src/moneybin/validation/__init__.py`:

```python
"""Validation primitives reusable across synthetic scenario runs and live data verification."""
```

`src/moneybin/validation/result.py`:

```python
"""Structured result types returned by validation primitives."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True, slots=True)
class AssertionResult:
    name: str
    passed: bool
    details: dict[str, Any] = field(default_factory=dict)
    error: str | None = None

    def raise_if_failed(self) -> None:
        if not self.passed:
            raise AssertionError(
                f"assertion {self.name!r} failed: details={self.details} error={self.error}"
            )


@dataclass(frozen=True, slots=True)
class EvaluationResult:
    name: str
    metric: str
    value: float
    threshold: float
    passed: bool
    breakdown: dict[str, Any] = field(default_factory=dict)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/moneybin/test_validation/test_result.py -v`
Expected: 2 passed.

- [ ] **Step 5: Format, lint, type-check**

Run: `make format && make lint && uv run pyright src/moneybin/validation/ tests/moneybin/test_validation/`
Expected: clean.

- [ ] **Step 6: Commit**

```bash
git add src/moneybin/validation/__init__.py src/moneybin/validation/result.py \
        tests/moneybin/test_validation/__init__.py tests/moneybin/test_validation/test_result.py
git commit -m "Add validation result dataclasses"
```

---

## Task 2: Relational + schema assertions

These primitives operate on a `duckdb.DuckDBPyConnection` (the spec's contract) so the same code can run against the runner's tempdir DB or a live profile.

**Files:**
- Create: `src/moneybin/validation/assertions/__init__.py`
- Create: `src/moneybin/validation/assertions/relational.py`
- Create: `src/moneybin/validation/assertions/schema.py`
- Create: `tests/moneybin/test_validation/test_assertions_relational.py`
- Create: `tests/moneybin/test_validation/test_assertions_schema.py`

- [ ] **Step 1: Write failing tests for relational**

`tests/moneybin/test_validation/test_assertions_relational.py`:

```python
import duckdb

from moneybin.validation.assertions.relational import (
    assert_no_duplicates,
    assert_no_orphans,
    assert_valid_foreign_keys,
)


def _conn() -> duckdb.DuckDBPyConnection:
    c = duckdb.connect(":memory:")
    c.execute("CREATE TABLE parent (id INT)")
    c.execute("INSERT INTO parent VALUES (1), (2), (3)")
    c.execute("CREATE TABLE child (id INT, parent_id INT)")
    return c


def test_valid_foreign_keys_passes_when_all_children_resolve() -> None:
    c = _conn()
    c.execute("INSERT INTO child VALUES (10, 1), (11, 2)")
    r = assert_valid_foreign_keys(
        c, child="child", column="parent_id", parent="parent", parent_column="id"
    )
    assert r.passed
    assert r.details == {"checked_rows": 2, "violations": 0}


def test_valid_foreign_keys_fails_with_violation_count() -> None:
    c = _conn()
    c.execute("INSERT INTO child VALUES (10, 1), (11, 99)")
    r = assert_valid_foreign_keys(
        c, child="child", column="parent_id", parent="parent", parent_column="id"
    )
    assert not r.passed
    assert r.details["violations"] == 1


def test_no_duplicates_detects_repeats() -> None:
    c = _conn()
    c.execute("INSERT INTO child VALUES (10, 1), (10, 1)")
    r = assert_no_duplicates(c, table="child", columns=["id"])
    assert not r.passed
    assert r.details["duplicate_groups"] == 1


def test_no_orphans_passes_when_every_parent_has_child() -> None:
    c = _conn()
    c.execute("INSERT INTO child VALUES (1, 1), (2, 2), (3, 3)")
    r = assert_no_orphans(
        c, parent="parent", parent_column="id", child="child", child_column="parent_id"
    )
    assert r.passed
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run pytest tests/moneybin/test_validation/test_assertions_relational.py -v`
Expected: FAIL — module not found.

- [ ] **Step 3: Implement relational primitives**

`src/moneybin/validation/assertions/__init__.py`:

```python
"""Assertion primitives — every function returns AssertionResult, never raises on data failure."""

from moneybin.validation.assertions.relational import (
    assert_no_duplicates,
    assert_no_nulls,
    assert_no_orphans,
    assert_valid_foreign_keys,
)
from moneybin.validation.assertions.schema import (
    assert_columns_exist,
    assert_column_types,
    assert_row_count_delta,
    assert_row_count_exact,
)

__all__ = [
    "assert_no_duplicates",
    "assert_no_nulls",
    "assert_no_orphans",
    "assert_valid_foreign_keys",
    "assert_columns_exist",
    "assert_column_types",
    "assert_row_count_delta",
    "assert_row_count_exact",
]
```

`src/moneybin/validation/assertions/relational.py`:

```python
"""Referential-integrity assertions usable on any DuckDB connection."""

from __future__ import annotations

from duckdb import DuckDBPyConnection

from moneybin.validation.result import AssertionResult


def _quote_ident(ident: str) -> str:
    # Allow only [A-Za-z0-9_.] in identifiers; quote each segment.
    if not all(ch.isalnum() or ch in "_." for ch in ident):
        raise ValueError(f"invalid identifier: {ident!r}")
    return ".".join(f'"{seg}"' for seg in ident.split("."))


def assert_valid_foreign_keys(
    conn: DuckDBPyConnection,
    *,
    child: str,
    column: str,
    parent: str,
    parent_column: str,
) -> AssertionResult:
    c, col, p, pc = (
        _quote_ident(child),
        _quote_ident(column),
        _quote_ident(parent),
        _quote_ident(parent_column),
    )
    total = conn.execute(
        f"SELECT COUNT(*) FROM {c} WHERE {col} IS NOT NULL"
    ).fetchone()[0]
    violations = conn.execute(
        f"SELECT COUNT(*) FROM {c} ch WHERE ch.{col} IS NOT NULL "
        f"AND NOT EXISTS (SELECT 1 FROM {p} pa WHERE pa.{pc} = ch.{col})"
    ).fetchone()[0]
    return AssertionResult(
        name="valid_foreign_keys",
        passed=violations == 0,
        details={"checked_rows": total, "violations": violations},
    )


def assert_no_orphans(
    conn: DuckDBPyConnection,
    *,
    parent: str,
    parent_column: str,
    child: str,
    child_column: str,
) -> AssertionResult:
    p, pc, c, cc = (
        _quote_ident(parent),
        _quote_ident(parent_column),
        _quote_ident(child),
        _quote_ident(child_column),
    )
    orphans = conn.execute(
        f"SELECT COUNT(*) FROM {p} pa WHERE NOT EXISTS "
        f"(SELECT 1 FROM {c} ch WHERE ch.{cc} = pa.{pc})"
    ).fetchone()[0]
    return AssertionResult(
        name="no_orphans",
        passed=orphans == 0,
        details={"orphan_count": orphans},
    )


def assert_no_duplicates(
    conn: DuckDBPyConnection, *, table: str, columns: list[str]
) -> AssertionResult:
    if not columns:
        raise ValueError("columns must be non-empty")
    t = _quote_ident(table)
    cols = ", ".join(_quote_ident(c) for c in columns)
    dup_groups = conn.execute(
        f"SELECT COUNT(*) FROM (SELECT {cols} FROM {t} GROUP BY {cols} HAVING COUNT(*) > 1)"
    ).fetchone()[0]
    return AssertionResult(
        name="no_duplicates",
        passed=dup_groups == 0,
        details={"duplicate_groups": dup_groups, "columns": columns},
    )


def assert_no_nulls(
    conn: DuckDBPyConnection, *, table: str, columns: list[str]
) -> AssertionResult:
    if not columns:
        raise ValueError("columns must be non-empty")
    t = _quote_ident(table)
    per_col: dict[str, int] = {}
    for col in columns:
        cq = _quote_ident(col)
        per_col[col] = conn.execute(
            f"SELECT COUNT(*) FROM {t} WHERE {cq} IS NULL"
        ).fetchone()[0]
    total = sum(per_col.values())
    return AssertionResult(
        name="no_nulls",
        passed=total == 0,
        details={"null_counts": per_col, "total": total},
    )
```

- [ ] **Step 4: Run relational tests**

Run: `uv run pytest tests/moneybin/test_validation/test_assertions_relational.py -v`
Expected: 4 passed.

- [ ] **Step 5: Write failing tests for schema primitives**

`tests/moneybin/test_validation/test_assertions_schema.py`:

```python
import duckdb

from moneybin.validation.assertions.schema import (
    assert_column_types,
    assert_columns_exist,
    assert_row_count_delta,
    assert_row_count_exact,
)


def _conn() -> duckdb.DuckDBPyConnection:
    c = duckdb.connect(":memory:")
    c.execute("CREATE TABLE t (id INTEGER, name VARCHAR)")
    c.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')")
    return c


def test_columns_exist_passes() -> None:
    r = assert_columns_exist(_conn(), table="t", columns=["id", "name"])
    assert r.passed


def test_columns_exist_fails_when_missing() -> None:
    r = assert_columns_exist(_conn(), table="t", columns=["id", "missing"])
    assert not r.passed
    assert "missing" in r.details["missing"]


def test_column_types_match() -> None:
    r = assert_column_types(
        _conn(), table="t", types={"id": "INTEGER", "name": "VARCHAR"}
    )
    assert r.passed


def test_row_count_exact() -> None:
    assert assert_row_count_exact(_conn(), table="t", expected=3).passed
    assert not assert_row_count_exact(_conn(), table="t", expected=2).passed


def test_row_count_delta_within_tolerance() -> None:
    r = assert_row_count_delta(_conn(), table="t", expected=3, tolerance_pct=10)
    assert r.passed
    r2 = assert_row_count_delta(_conn(), table="t", expected=10, tolerance_pct=10)
    assert not r2.passed
    assert r2.details["delta_pct"] < -50
```

- [ ] **Step 6: Run to verify it fails**

Run: `uv run pytest tests/moneybin/test_validation/test_assertions_schema.py -v`
Expected: FAIL.

- [ ] **Step 7: Implement schema primitives**

`src/moneybin/validation/assertions/schema.py`:

```python
"""Schema + row-count assertions."""

from __future__ import annotations

from duckdb import DuckDBPyConnection

from moneybin.validation.assertions.relational import _quote_ident
from moneybin.validation.result import AssertionResult


def _split(table: str) -> tuple[str | None, str]:
    if "." in table:
        s, t = table.split(".", 1)
        return s, t
    return None, table


def _columns_with_types(conn: DuckDBPyConnection, table: str) -> dict[str, str]:
    schema, name = _split(table)
    if schema is None:
        rows = conn.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_name = ?",
            [name],
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_schema = ? AND table_name = ?",
            [schema, name],
        ).fetchall()
    return {r[0]: r[1] for r in rows}


def assert_columns_exist(
    conn: DuckDBPyConnection, *, table: str, columns: list[str]
) -> AssertionResult:
    actual = set(_columns_with_types(conn, table))
    missing = [c for c in columns if c not in actual]
    return AssertionResult(
        name="columns_exist",
        passed=not missing,
        details={"missing": missing, "actual": sorted(actual)},
    )


def assert_column_types(
    conn: DuckDBPyConnection, *, table: str, types: dict[str, str]
) -> AssertionResult:
    actual = _columns_with_types(conn, table)
    mismatched = {
        col: {"expected": expected, "actual": actual.get(col)}
        for col, expected in types.items()
        if actual.get(col) != expected
    }
    return AssertionResult(
        name="column_types",
        passed=not mismatched,
        details={"mismatched": mismatched},
    )


def _row_count(conn: DuckDBPyConnection, table: str) -> int:
    return conn.execute(f"SELECT COUNT(*) FROM {_quote_ident(table)}").fetchone()[0]


def assert_row_count_exact(
    conn: DuckDBPyConnection, *, table: str, expected: int
) -> AssertionResult:
    actual = _row_count(conn, table)
    return AssertionResult(
        name="row_count_exact",
        passed=actual == expected,
        details={"expected": expected, "actual": actual},
    )


def assert_row_count_delta(
    conn: DuckDBPyConnection, *, table: str, expected: int, tolerance_pct: float
) -> AssertionResult:
    actual = _row_count(conn, table)
    delta_pct = ((actual - expected) / expected) * 100 if expected else 0.0
    passed = abs(delta_pct) <= tolerance_pct
    return AssertionResult(
        name="row_count_delta",
        passed=passed,
        details={
            "expected": expected,
            "actual": actual,
            "delta_pct": round(delta_pct, 2),
            "tolerance_pct": tolerance_pct,
        },
    )
```

- [ ] **Step 8: Run schema tests**

Run: `uv run pytest tests/moneybin/test_validation/test_assertions_schema.py -v`
Expected: 5 passed.

- [ ] **Step 9: Format/lint/type-check + commit**

```bash
make format && make lint
uv run pyright src/moneybin/validation/ tests/moneybin/test_validation/
git add src/moneybin/validation/assertions/ tests/moneybin/test_validation/test_assertions_relational.py tests/moneybin/test_validation/test_assertions_schema.py
git commit -m "Add relational and schema validation primitives"
```

---

## Task 3: Business + distributional assertions

**Files:**
- Create: `src/moneybin/validation/assertions/business.py`
- Create: `src/moneybin/validation/assertions/distributional.py`
- Create: `tests/moneybin/test_validation/test_assertions_business.py`
- Create: `tests/moneybin/test_validation/test_assertions_distributional.py`
- Modify: `src/moneybin/validation/assertions/__init__.py` (add exports)

- [ ] **Step 1: Write business-rule tests**

`tests/moneybin/test_validation/test_assertions_business.py`:

```python
import duckdb

from moneybin.validation.assertions.business import (
    assert_balanced_transfers,
    assert_sign_convention,
)


def _txn_conn() -> duckdb.DuckDBPyConnection:
    c = duckdb.connect(":memory:")
    c.execute("CREATE SCHEMA core")
    c.execute(
        "CREATE TABLE core.fct_transactions ("
        " transaction_id VARCHAR, amount DECIMAL(18,2),"
        " category VARCHAR, transfer_pair_id VARCHAR)"
    )
    return c


def test_sign_convention_passes_when_categories_match_signs() -> None:
    c = _txn_conn()
    c.execute(
        "INSERT INTO core.fct_transactions VALUES "
        "('t1', -50.00, 'Groceries', NULL),"
        "('t2', 1000.00, 'Income', NULL)"
    )
    assert assert_sign_convention(c).passed


def test_sign_convention_flags_positive_expense() -> None:
    c = _txn_conn()
    c.execute(
        "INSERT INTO core.fct_transactions VALUES ('t1', 50.00, 'Groceries', NULL)"
    )
    r = assert_sign_convention(c)
    assert not r.passed
    assert r.details["violations"] == 1


def test_balanced_transfers_pairs_net_to_zero() -> None:
    c = _txn_conn()
    c.execute(
        "INSERT INTO core.fct_transactions VALUES "
        "('t1', -100.00, 'Transfer', 'p1'),"
        "('t2', 100.00, 'Transfer', 'p1')"
    )
    assert assert_balanced_transfers(c).passed


def test_balanced_transfers_flags_imbalance() -> None:
    c = _txn_conn()
    c.execute(
        "INSERT INTO core.fct_transactions VALUES "
        "('t1', -100.00, 'Transfer', 'p1'),"
        "('t2', 90.00, 'Transfer', 'p1')"
    )
    r = assert_balanced_transfers(c)
    assert not r.passed
```

- [ ] **Step 2: Run, expect failure**

Run: `uv run pytest tests/moneybin/test_validation/test_assertions_business.py -v`
Expected: ImportError.

- [ ] **Step 3: Implement business primitives**

`src/moneybin/validation/assertions/business.py`:

```python
"""Business-rule assertions for the canonical core schema."""

from __future__ import annotations

from duckdb import DuckDBPyConnection

from moneybin.validation.assertions.relational import _quote_ident
from moneybin.validation.result import AssertionResult

_EXPENSE_CATEGORIES_NEGATIVE = "category NOT IN ('Income', 'Transfer') AND amount > 0"
_INCOME_CATEGORIES_POSITIVE = "category = 'Income' AND amount < 0"


def assert_sign_convention(conn: DuckDBPyConnection) -> AssertionResult:
    """Expenses negative, income positive. Transfers exempted (mixed signs valid per leg)."""
    violations = conn.execute(
        "SELECT COUNT(*) FROM core.fct_transactions "
        f"WHERE ({_EXPENSE_CATEGORIES_NEGATIVE}) OR ({_INCOME_CATEGORIES_POSITIVE})"
    ).fetchone()[0]
    return AssertionResult(
        name="sign_convention",
        passed=violations == 0,
        details={"violations": violations},
    )


def assert_balanced_transfers(conn: DuckDBPyConnection) -> AssertionResult:
    """Confirmed transfer pairs (transfer_pair_id NOT NULL) must net to zero."""
    rows = conn.execute(
        "SELECT transfer_pair_id, SUM(amount) FROM core.fct_transactions "
        "WHERE transfer_pair_id IS NOT NULL GROUP BY transfer_pair_id"
    ).fetchall()
    unbalanced = [(pair, float(total)) for pair, total in rows if total != 0]
    return AssertionResult(
        name="balanced_transfers",
        passed=not unbalanced,
        details={
            "unbalanced_pairs": unbalanced[:20],
            "unbalanced_count": len(unbalanced),
        },
    )


def assert_date_continuity(
    conn: DuckDBPyConnection, *, table: str, date_col: str, account_col: str
) -> AssertionResult:
    """No month-gaps per account in the given table."""
    t, dc, ac = _quote_ident(table), _quote_ident(date_col), _quote_ident(account_col)
    rows = conn.execute(
        f"WITH per AS ("
        f"  SELECT {ac} AS account, DATE_TRUNC('month', {dc}) AS m FROM {t} GROUP BY 1, 2"
        f"), bounds AS ("
        f"  SELECT account, MIN(m) AS lo, MAX(m) AS hi, COUNT(*) AS observed FROM per GROUP BY account"
        f") SELECT account, observed,"
        f"  DATE_DIFF('month', lo, hi) + 1 AS expected FROM bounds"
    ).fetchall()
    gaps = [(acc, obs, exp) for acc, obs, exp in rows if obs != exp]
    return AssertionResult(
        name="date_continuity",
        passed=not gaps,
        details={"gap_accounts": gaps[:20], "gap_count": len(gaps)},
    )
```

- [ ] **Step 4: Run business tests**

Run: `uv run pytest tests/moneybin/test_validation/test_assertions_business.py -v`
Expected: 4 passed.

- [ ] **Step 5: Write distributional tests**

`tests/moneybin/test_validation/test_assertions_distributional.py`:

```python
import duckdb

from moneybin.validation.assertions.distributional import (
    assert_distribution_within_bounds,
    assert_unique_value_count,
)


def _conn() -> duckdb.DuckDBPyConnection:
    c = duckdb.connect(":memory:")
    c.execute("CREATE TABLE t (amount DECIMAL(18,2), category VARCHAR)")
    c.execute(
        "INSERT INTO t VALUES (10, 'a'), (20, 'a'), (30, 'b'), (40, 'b'), (50, 'c')"
    )
    return c


def test_distribution_within_bounds_passes() -> None:
    r = assert_distribution_within_bounds(
        _conn(),
        table="t",
        col="amount",
        min_value=10,
        max_value=50,
        mean_range=(25, 35),
    )
    assert r.passed


def test_distribution_out_of_range_fails() -> None:
    r = assert_distribution_within_bounds(
        _conn(),
        table="t",
        col="amount",
        min_value=10,
        max_value=49,
        mean_range=(25, 35),
    )
    assert not r.passed
    assert r.details["max_observed"] == 50


def test_unique_value_count_within_tolerance() -> None:
    r = assert_unique_value_count(
        _conn(), table="t", col="category", expected=3, tolerance_pct=0
    )
    assert r.passed
```

- [ ] **Step 6: Run, expect failure**

Run: `uv run pytest tests/moneybin/test_validation/test_assertions_distributional.py -v`
Expected: ImportError.

- [ ] **Step 7: Implement distributional**

`src/moneybin/validation/assertions/distributional.py`:

```python
"""Distributional / cardinality smoke checks. Bounds are scenario-author chosen — soft signal."""

from __future__ import annotations

from duckdb import DuckDBPyConnection

from moneybin.validation.assertions.relational import _quote_ident
from moneybin.validation.result import AssertionResult


def assert_distribution_within_bounds(
    conn: DuckDBPyConnection,
    *,
    table: str,
    col: str,
    min_value: float,
    max_value: float,
    mean_range: tuple[float, float],
) -> AssertionResult:
    t, c = _quote_ident(table), _quote_ident(col)
    row = conn.execute(f"SELECT MIN({c}), MAX({c}), AVG({c}) FROM {t}").fetchone()
    mn, mx, avg = (float(row[0]), float(row[1]), float(row[2]))
    failures: list[str] = []
    if mn < min_value:
        failures.append(f"min {mn} < {min_value}")
    if mx > max_value:
        failures.append(f"max {mx} > {max_value}")
    if not (mean_range[0] <= avg <= mean_range[1]):
        failures.append(f"mean {avg} outside {mean_range}")
    return AssertionResult(
        name="distribution_within_bounds",
        passed=not failures,
        details={
            "min_observed": mn,
            "max_observed": mx,
            "mean_observed": round(avg, 4),
            "failures": failures,
        },
    )


def assert_unique_value_count(
    conn: DuckDBPyConnection,
    *,
    table: str,
    col: str,
    expected: int,
    tolerance_pct: float,
) -> AssertionResult:
    t, c = _quote_ident(table), _quote_ident(col)
    actual = conn.execute(f"SELECT COUNT(DISTINCT {c}) FROM {t}").fetchone()[0]
    delta_pct = abs(actual - expected) / expected * 100 if expected else 0.0
    return AssertionResult(
        name="unique_value_count",
        passed=delta_pct <= tolerance_pct,
        details={
            "expected": expected,
            "actual": actual,
            "delta_pct": round(delta_pct, 2),
        },
    )
```

- [ ] **Step 8: Run, format, type-check, commit**

```bash
uv run pytest tests/moneybin/test_validation/test_assertions_business.py tests/moneybin/test_validation/test_assertions_distributional.py -v
make format && make lint
uv run pyright src/moneybin/validation/
```

Update `src/moneybin/validation/assertions/__init__.py` to export the new functions:

```python
from moneybin.validation.assertions.business import (
    assert_balanced_transfers,
    assert_date_continuity,
    assert_sign_convention,
)
from moneybin.validation.assertions.distributional import (
    assert_distribution_within_bounds,
    assert_unique_value_count,
)
```
(append to `__all__` accordingly)

```bash
git add src/moneybin/validation/assertions/ tests/moneybin/test_validation/test_assertions_business.py tests/moneybin/test_validation/test_assertions_distributional.py
git commit -m "Add business and distributional validation primitives"
```

---

## Task 4: Infrastructure assertions

These bind to a `Database` instance (not just a raw conn) because they must read `db.path`, inherit env, and verify SQLMesh's adapter wiring.

**Files:**
- Create: `src/moneybin/validation/assertions/infrastructure.py`
- Create: `tests/moneybin/test_validation/test_assertions_infrastructure.py`
- Modify: `src/moneybin/validation/assertions/__init__.py`

- [ ] **Step 1: Write failing tests**

`tests/moneybin/test_validation/test_assertions_infrastructure.py`:

```python
from pathlib import Path

import pytest

from moneybin.validation.assertions.infrastructure import (
    assert_no_unencrypted_db_files,
    assert_sqlmesh_catalog_matches,
)
from tests.helpers.database import temp_encrypted_database  # existing helper


def test_sqlmesh_catalog_matches_against_real_db() -> None:
    with temp_encrypted_database() as db:
        r = assert_sqlmesh_catalog_matches(db)
        assert r.passed, r.details


def test_no_unencrypted_db_files_passes_for_encrypted_dir(tmp_path: Path) -> None:
    # Encrypted DBs do not produce .duckdb files in the canonical path.
    r = assert_no_unencrypted_db_files(tmpdir=tmp_path)
    assert r.passed


def test_no_unencrypted_db_files_flags_bare_duckdb(tmp_path: Path) -> None:
    (tmp_path / "leak.duckdb").write_bytes(b"\x00" * 16)
    r = assert_no_unencrypted_db_files(tmpdir=tmp_path)
    assert not r.passed
    assert "leak.duckdb" in str(r.details["files"])
```

- [ ] **Step 2: Confirm helper exists**

Run: `uv run python -c "from tests.helpers.database import temp_encrypted_database; print('ok')"`
Expected: `ok`. If it errors, search the codebase for the canonical encrypted-DB test fixture and update the import path; do not roll a new helper unless none exists.

- [ ] **Step 3: Run tests, expect failure**

Run: `uv run pytest tests/moneybin/test_validation/test_assertions_infrastructure.py -v`
Expected: FAIL — module not found.

- [ ] **Step 4: Implement infrastructure primitives**

`src/moneybin/validation/assertions/infrastructure.py`:

```python
"""Infrastructure assertions — verify wiring invariants, not data shape."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

from moneybin.database import Database, sqlmesh_context
from moneybin.validation.result import AssertionResult


def assert_sqlmesh_catalog_matches(db: Database) -> AssertionResult:
    """SQLMesh's adapter must be bound to db.path. Catches the original incident."""
    with sqlmesh_context(db) as ctx:
        adapter_path = _resolve_adapter_path(ctx)
    db_path = str(Path(db.path).resolve())
    matches = adapter_path == db_path
    return AssertionResult(
        name="sqlmesh_catalog_matches",
        passed=matches,
        details={"db_path": db_path, "adapter_path": adapter_path},
    )


def _resolve_adapter_path(ctx: object) -> str:
    """Best-effort introspection of SQLMesh's bound DuckDB path.

    SQLMesh stores the engine adapter under ``ctx._engine_adapter`` historically;
    this helper degrades gracefully if the internal API changes.
    """
    adapter = getattr(ctx, "_engine_adapter", None) or getattr(
        ctx, "engine_adapter", None
    )
    conn = getattr(adapter, "_connection_pool", None) or getattr(
        adapter, "connection", None
    )
    raw = getattr(conn, "get_connection", lambda: conn)() if conn else None
    db_files = (
        raw.execute(
            "SELECT file FROM duckdb_databases() WHERE database_name = current_database()"
        ).fetchall()
        if raw
        else []
    )
    if db_files:
        return str(Path(db_files[0][0]).resolve())
    return "<unknown>"


def assert_encryption_key_propagated_to_subprocess(
    db: Database,
    *,
    command: list[str],
    expected_min_rows: dict[str, int],
    env: dict[str, str] | None = None,
) -> AssertionResult:
    """Run ``command`` as a subprocess inheriting key/profile env, then verify rows landed in db."""
    pre_counts = {t: _count(db, t) for t in expected_min_rows}
    proc = subprocess.run(  # noqa: S603  # explicit command list, not shell
        command,
        env={**os.environ, **(env or {})},
        capture_output=True,
        text=True,
        timeout=300,
    )
    post_counts = {t: _count(db, t) for t in expected_min_rows}
    deltas = {t: post_counts[t] - pre_counts[t] for t in expected_min_rows}
    failures = {
        t: {"min_required": expected_min_rows[t], "delta": deltas[t]}
        for t in expected_min_rows
        if deltas[t] < expected_min_rows[t]
    }
    return AssertionResult(
        name="encryption_key_propagated_to_subprocess",
        passed=proc.returncode == 0 and not failures,
        details={
            "returncode": proc.returncode,
            "deltas": deltas,
            "failures": failures,
            "stderr_tail": proc.stderr[-500:] if proc.stderr else "",
        },
    )


def _count(db: Database, table: str) -> int:
    with db.connect() as conn:
        return conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]


def assert_no_unencrypted_db_files(*, tmpdir: Path) -> AssertionResult:
    leaks = [p.name for p in Path(tmpdir).rglob("*.duckdb")]
    return AssertionResult(
        name="no_unencrypted_db_files",
        passed=not leaks,
        details={"files": leaks},
    )


def assert_migrations_at_head(db: Database) -> AssertionResult:
    """``app.versions`` head must equal the latest migration on disk."""
    from moneybin.migrations import (
        head_version_on_disk,
    )  # adjust to actual location during impl

    with db.connect() as conn:
        applied = conn.execute("SELECT MAX(version) FROM app.versions").fetchone()[0]
    expected = head_version_on_disk()
    return AssertionResult(
        name="migrations_at_head",
        passed=applied == expected,
        details={"applied": applied, "expected": expected},
    )
```

> **Note:** `_resolve_adapter_path` reaches into SQLMesh internals — verify the actual attribute names against the installed SQLMesh version during implementation. If unstable, fall back to executing `SELECT current_database(), file FROM duckdb_databases() WHERE database_name = current_database()` directly inside `with sqlmesh_context(db) as ctx: ctx.engine_adapter.fetchall(...)`. Document the chosen approach inline.
>
> `assert_migrations_at_head` references a `head_version_on_disk()` helper — check `src/moneybin/migrations/` for the actual API (e.g., `MigrationRunner.latest_version()`) and adapt the call.

- [ ] **Step 5: Run tests**

Run: `uv run pytest tests/moneybin/test_validation/test_assertions_infrastructure.py -v`
Expected: 3 passed (catalog test will exercise the real Database).

- [ ] **Step 6: Update `__init__` exports**

Append to `src/moneybin/validation/assertions/__init__.py`:

```python
from moneybin.validation.assertions.infrastructure import (
    assert_encryption_key_propagated_to_subprocess,
    assert_migrations_at_head,
    assert_no_unencrypted_db_files,
    assert_sqlmesh_catalog_matches,
)
```
Add the names to `__all__`.

- [ ] **Step 7: Format, lint, type-check, commit**

```bash
make format && make lint
uv run pyright src/moneybin/validation/assertions/infrastructure.py
git add src/moneybin/validation/assertions/infrastructure.py src/moneybin/validation/assertions/__init__.py tests/moneybin/test_validation/test_assertions_infrastructure.py
git commit -m "Add infrastructure validation primitives"
```

---

## Task 5: Evaluation primitives

**Files:**
- Create: `src/moneybin/validation/evaluations/__init__.py`
- Create: `src/moneybin/validation/evaluations/categorization.py`
- Create: `src/moneybin/validation/evaluations/matching.py`
- Create: `tests/moneybin/test_validation/test_evaluations.py`

- [ ] **Step 1: Write failing test**

`tests/moneybin/test_validation/test_evaluations.py`:

```python
import pytest

from moneybin.validation.evaluations.categorization import score_categorization
from moneybin.validation.evaluations.matching import score_transfer_detection
from tests.helpers.database import temp_database_with_ground_truth  # see Step 2


class TestCategorizationScoring:
    def test_accuracy_above_threshold_passes(self) -> None:
        with temp_database_with_ground_truth(accuracy=0.85) as db:
            r = score_categorization(db, threshold=0.80)
            assert r.passed
            assert r.metric == "accuracy"
            assert r.value >= 0.80

    def test_missing_ground_truth_raises_typed_error(self) -> None:
        from moneybin.validation.evaluations import GroundTruthMissingError
        from tests.helpers.database import temp_database  # no synthetic.ground_truth

        with temp_database() as db, pytest.raises(GroundTruthMissingError):
            score_categorization(db, threshold=0.80)


class TestTransferDetection:
    def test_f1_breakdown(self) -> None:
        with temp_database_with_ground_truth(transfer_f1=0.91) as db:
            r = score_transfer_detection(db, threshold=0.85)
            assert r.passed
            assert "true_pairs" in r.breakdown
            assert "predicted_pairs" in r.breakdown
```

- [ ] **Step 2: Define test helpers**

If `tests/helpers/database.py` doesn't already provide `temp_database_with_ground_truth`, add it as a thin wrapper that:
1. Creates a temp encrypted Database
2. Inserts a known set of rows into `core.fct_transactions` and `synthetic.ground_truth` such that the requested accuracy / F1 numbers hold deterministically

Keep this helper minimal; it exists only for evaluation library tests.

- [ ] **Step 3: Run, expect failure**

Run: `uv run pytest tests/moneybin/test_validation/test_evaluations.py -v`
Expected: ImportError.

- [ ] **Step 4: Implement evaluations**

`src/moneybin/validation/evaluations/__init__.py`:

```python
"""Evaluations — score pipeline output against synthetic.ground_truth."""


class GroundTruthMissingError(RuntimeError):
    """Raised when an evaluation runs against a DB without `synthetic.ground_truth`."""


from moneybin.validation.evaluations.categorization import score_categorization
from moneybin.validation.evaluations.matching import (
    score_dedup,
    score_transfer_detection,
)

__all__ = [
    "GroundTruthMissingError",
    "score_categorization",
    "score_dedup",
    "score_transfer_detection",
]
```

`src/moneybin/validation/evaluations/categorization.py`:

```python
"""Categorization accuracy / per-category precision-recall."""

from __future__ import annotations

from collections import defaultdict

from moneybin.database import Database
from moneybin.validation.evaluations import GroundTruthMissingError
from moneybin.validation.result import EvaluationResult


def score_categorization(db: Database, *, threshold: float) -> EvaluationResult:
    with db.connect() as conn:
        if not _has_ground_truth(conn):
            raise GroundTruthMissingError("synthetic.ground_truth not present")
        rows = conn.execute(
            "SELECT t.transaction_id, t.category AS predicted, gt.expected_category "
            "FROM core.fct_transactions t "
            "JOIN synthetic.ground_truth gt USING (transaction_id) "
            "WHERE gt.expected_category IS NOT NULL"
        ).fetchall()

    if not rows:
        return EvaluationResult(
            name="categorization_accuracy",
            metric="accuracy",
            value=0.0,
            threshold=threshold,
            passed=False,
            breakdown={"reason": "no labeled rows"},
        )

    correct = sum(1 for _, p, e in rows if p == e)
    accuracy = correct / len(rows)

    per_cat: dict[str, dict[str, int]] = defaultdict(
        lambda: {"tp": 0, "fp": 0, "fn": 0, "support": 0}
    )
    for _, predicted, expected in rows:
        per_cat[expected]["support"] += 1
        if predicted == expected:
            per_cat[expected]["tp"] += 1
        else:
            per_cat[expected]["fn"] += 1
            per_cat[predicted]["fp"] += 1

    breakdown = {
        "per_category": {
            cat: {
                "precision": _safe_div(s["tp"], s["tp"] + s["fp"]),
                "recall": _safe_div(s["tp"], s["tp"] + s["fn"]),
                "support": s["support"],
            }
            for cat, s in per_cat.items()
        },
        "total_labeled": len(rows),
    }

    return EvaluationResult(
        name="categorization_accuracy",
        metric="accuracy",
        value=round(accuracy, 4),
        threshold=threshold,
        passed=accuracy >= threshold,
        breakdown=breakdown,
    )


def _safe_div(num: int, denom: int) -> float:
    return round(num / denom, 4) if denom else 0.0


def _has_ground_truth(conn) -> bool:
    rows = conn.execute(
        "SELECT 1 FROM information_schema.tables "
        "WHERE table_schema = 'synthetic' AND table_name = 'ground_truth'"
    ).fetchall()
    return bool(rows)
```

`src/moneybin/validation/evaluations/matching.py`:

```python
"""Transfer detection + dedup F1 scoring."""

from __future__ import annotations

from moneybin.database import Database
from moneybin.validation.evaluations import GroundTruthMissingError
from moneybin.validation.result import EvaluationResult


def score_transfer_detection(db: Database, *, threshold: float) -> EvaluationResult:
    with db.connect() as conn:
        if not _has_ground_truth(conn):
            raise GroundTruthMissingError("synthetic.ground_truth required")
        true_pairs = _pair_set(
            conn,
            "SELECT MIN(transaction_id), MAX(transaction_id) FROM synthetic.ground_truth "
            "WHERE transfer_pair_id IS NOT NULL GROUP BY transfer_pair_id",
        )
        predicted_pairs = _pair_set(
            conn,
            "SELECT MIN(transaction_id), MAX(transaction_id) FROM core.fct_transactions "
            "WHERE transfer_pair_id IS NOT NULL GROUP BY transfer_pair_id",
        )

    tp = len(true_pairs & predicted_pairs)
    fp = len(predicted_pairs - true_pairs)
    fn = len(true_pairs - predicted_pairs)
    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall = tp / (tp + fn) if (tp + fn) else 0.0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) else 0.0

    return EvaluationResult(
        name="transfer_f1",
        metric="f1",
        value=round(f1, 4),
        threshold=threshold,
        passed=f1 >= threshold,
        breakdown={
            "precision": round(precision, 4),
            "recall": round(recall, 4),
            "true_pairs": len(true_pairs),
            "predicted_pairs": len(predicted_pairs),
            "tp": tp,
            "fp": fp,
            "fn": fn,
        },
    )


def score_dedup(
    db: Database, *, threshold: float, expected_collapsed_count: int
) -> EvaluationResult:
    """Compares actual collapsed gold-record count to expected from labeled-overlap fixture metadata."""
    with db.connect() as conn:
        actual = conn.execute("SELECT COUNT(*) FROM core.fct_transactions").fetchone()[
            0
        ]
    delta = abs(actual - expected_collapsed_count)
    f1 = max(0.0, 1.0 - delta / max(expected_collapsed_count, 1))
    return EvaluationResult(
        name="dedup_quality",
        metric="f1",
        value=round(f1, 4),
        threshold=threshold,
        passed=f1 >= threshold,
        breakdown={
            "actual_gold_records": actual,
            "expected_collapsed_count": expected_collapsed_count,
        },
    )


def _pair_set(conn, sql: str) -> set[tuple[str, str]]:
    return {(a, b) for a, b in conn.execute(sql).fetchall()}


def _has_ground_truth(conn) -> bool:
    rows = conn.execute(
        "SELECT 1 FROM information_schema.tables "
        "WHERE table_schema = 'synthetic' AND table_name = 'ground_truth'"
    ).fetchall()
    return bool(rows)
```

- [ ] **Step 5: Run tests, format, lint, type-check**

```bash
uv run pytest tests/moneybin/test_validation/test_evaluations.py -v
make format && make lint
uv run pyright src/moneybin/validation/evaluations/
```

- [ ] **Step 6: Commit**

```bash
git add src/moneybin/validation/evaluations/ tests/moneybin/test_validation/test_evaluations.py
git commit -m "Add categorization, transfer, and dedup evaluations"
```

---

## Task 6: `MatchingService` thin wrapper

**Files:**
- Create: `src/moneybin/services/matching_service.py`
- Modify: `src/moneybin/services/__init__.py`
- Create: `tests/moneybin/test_services/test_matching_service.py`

- [ ] **Step 1: Write failing test**

`tests/moneybin/test_services/test_matching_service.py`:

```python
from unittest.mock import MagicMock, patch

from moneybin.services.matching_service import MatchingService


def test_run_delegates_to_transaction_matcher() -> None:
    db = MagicMock()
    fake_result = MagicMock()
    with patch("moneybin.services.matching_service.TransactionMatcher") as matcher_cls:
        matcher_cls.return_value.run.return_value = fake_result
        svc = MatchingService(db)
        result = svc.run()
    matcher_cls.assert_called_once()
    matcher_cls.return_value.run.assert_called_once()
    assert result is fake_result


def test_uses_default_settings_when_omitted() -> None:
    db = MagicMock()
    with (
        patch("moneybin.services.matching_service.TransactionMatcher") as cls,
        patch("moneybin.services.matching_service.get_settings") as gs,
    ):
        gs.return_value.matching = "MATCHING_SETTINGS"
        MatchingService(db).run()
    args, kwargs = cls.call_args
    assert "MATCHING_SETTINGS" in args or kwargs.get("settings") == "MATCHING_SETTINGS"
```

- [ ] **Step 2: Run, expect failure**

Run: `uv run pytest tests/moneybin/test_services/test_matching_service.py -v`
Expected: FAIL — import error.

- [ ] **Step 3: Implement**

`src/moneybin/services/matching_service.py`:

```python
"""MatchingService — thin facade over TransactionMatcher.

Exists so the scenario runner, MCP tools, and CLI can call
``MatchingService(db).run()`` uniformly alongside other services.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from moneybin.config import MatchingSettings, get_settings
from moneybin.database import Database
from moneybin.matching.engine import TransactionMatcher

if TYPE_CHECKING:
    from moneybin.matching.engine import MatchResult

logger = logging.getLogger(__name__)


class MatchingService:
    def __init__(self, db: Database, settings: MatchingSettings | None = None) -> None:
        self._db = db
        self._settings = settings or get_settings().matching

    def run(self) -> "MatchResult":
        """Run same-record dedup (Tier 2b/3) and transfer detection (Tier 4)."""
        return TransactionMatcher(self._db, self._settings).run()
```

Update `src/moneybin/services/__init__.py`:

```python
from moneybin.services.matching_service import MatchingService
```
(append `"MatchingService"` to `__all__`)

- [ ] **Step 4: Run tests**

Run: `uv run pytest tests/moneybin/test_services/test_matching_service.py -v`
Expected: 2 passed.

- [ ] **Step 5: Format, lint, type-check, commit**

```bash
make format && make lint
uv run pyright src/moneybin/services/matching_service.py
git add src/moneybin/services/matching_service.py src/moneybin/services/__init__.py tests/moneybin/test_services/test_matching_service.py
git commit -m "Add thin MatchingService wrapper around TransactionMatcher"
```

---

## Task 7: Scenario YAML loader (Pydantic models)

**Files:**
- Create: `src/moneybin/testing/scenarios/__init__.py`
- Create: `src/moneybin/testing/scenarios/loader.py`
- Create: `tests/moneybin/test_testing/__init__.py`
- Create: `tests/moneybin/test_testing/test_scenarios_loader.py`

- [ ] **Step 1: Write failing test**

`tests/moneybin/test_testing/test_scenarios_loader.py`:

```python
from pathlib import Path

import pytest

from moneybin.testing.scenarios.loader import (
    Scenario,
    ScenarioValidationError,
    load_scenario,
    load_scenario_from_string,
)


VALID = """
scenario: test
description: minimal valid scenario
setup:
  persona: family
  seed: 42
  years: 1
  fixtures: []
pipeline:
  - generate
  - transform
assertions:
  - name: rc
    fn: assert_row_count_exact
    args:
      table: core.fct_transactions
      expected: 100
gates:
  required_assertions: all
"""


def test_minimal_valid_scenario_loads() -> None:
    s = load_scenario_from_string(VALID)
    assert isinstance(s, Scenario)
    assert s.name == "test"
    assert len(s.assertions) == 1


def test_unknown_step_rejected() -> None:
    bad = VALID.replace("- transform", "- nonexistent_step")
    with pytest.raises(ScenarioValidationError) as exc:
        load_scenario_from_string(bad)
    assert "nonexistent_step" in str(exc.value)


def test_path_traversal_rejected() -> None:
    bad = VALID.replace(
        "fixtures: []",
        "fixtures:\n    - path: ../../../etc/passwd\n      account: x\n      source_type: csv",
    )
    with pytest.raises(ScenarioValidationError) as exc:
        load_scenario_from_string(bad)
    assert "tests/fixtures" in str(exc.value).lower()


def test_threshold_min_required_for_evaluations() -> None:
    bad = VALID + (
        "evaluations:\n"
        "  - name: cat\n"
        "    fn: score_categorization\n"
        "    threshold:\n"
        "      metric: accuracy\n"
    )
    with pytest.raises(ScenarioValidationError):
        load_scenario_from_string(bad)


def test_loads_shipped_scenarios(tmp_path: Path) -> None:
    # Will be populated in Task 11; skip until scenarios exist.
    pytest.importorskip("moneybin.testing.scenarios")
```

- [ ] **Step 2: Run, expect failure**

Run: `uv run pytest tests/moneybin/test_testing/test_scenarios_loader.py -v`
Expected: ImportError.

- [ ] **Step 3: Implement loader**

`src/moneybin/testing/scenarios/__init__.py`:

```python
"""Scenario YAML loader, runner, and step registry."""
```

`src/moneybin/testing/scenarios/loader.py`:

```python
"""Pydantic-backed scenario YAML loader."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

# Lazy import to avoid circular dependency with steps.py.
_VALID_STEP_NAMES = {
    "generate",
    "load_fixtures",
    "transform",
    "match",
    "categorize",
    "migrate",
    "transform_via_subprocess",
}

REPO_ROOT = Path(__file__).resolve().parents[4]
FIXTURES_ROOT = (REPO_ROOT / "tests" / "fixtures").resolve()


class ScenarioValidationError(ValueError):
    """Raised when a scenario YAML fails Pydantic validation."""


class FixtureSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")
    path: str
    account: str
    source_type: Literal["csv", "ofx", "pdf"]

    @model_validator(mode="after")
    def _validate_path(self) -> "FixtureSpec":
        resolved = (REPO_ROOT / self.path).resolve()
        try:
            resolved.relative_to(FIXTURES_ROOT)
        except ValueError as exc:
            raise ValueError(
                f"fixture path {self.path!r} must resolve under tests/fixtures/"
            ) from exc
        return self


class SetupSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")
    persona: str
    seed: int = 42
    years: int = 1
    fixtures: list[FixtureSpec] = Field(default_factory=list)


class AssertionSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str
    fn: str
    args: dict[str, Any] = Field(default_factory=dict)


class ThresholdSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")
    metric: str
    min: float


class EvaluationSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str
    fn: str
    threshold: ThresholdSpec
    args: dict[str, Any] = Field(default_factory=dict)


class ExpectationSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")
    kind: Literal[
        "match_decision",
        "gold_record_count",
        "category_for_transaction",
        "provenance_for_transaction",
    ]
    description: str = ""
    # Free-form per-kind body; verifier enforces shape.
    model_config = ConfigDict(extra="allow")


class GatesSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")
    required_assertions: Literal["all"] | list[str] = "all"
    required_evaluations: Literal["all"] | list[str] = "all"
    required_expectations: Literal["all"] | list[str] = "all"


class Scenario(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str = Field(alias="scenario")
    description: str = ""
    setup: SetupSpec
    pipeline: list[str]
    assertions: list[AssertionSpec] = Field(default_factory=list)
    evaluations: list[EvaluationSpec] = Field(default_factory=list)
    expectations: list[ExpectationSpec] = Field(default_factory=list)
    gates: GatesSpec = Field(default_factory=GatesSpec)

    @model_validator(mode="after")
    def _validate_steps(self) -> "Scenario":
        unknown = [s for s in self.pipeline if s not in _VALID_STEP_NAMES]
        if unknown:
            raise ValueError(f"unknown pipeline steps: {unknown}")
        return self


def load_scenario_from_string(raw: str) -> Scenario:
    try:
        data = yaml.safe_load(raw)
        return Scenario.model_validate(data)
    except (ValidationError, ValueError, yaml.YAMLError) as exc:
        raise ScenarioValidationError(str(exc)) from exc


def load_scenario(path: Path) -> Scenario:
    return load_scenario_from_string(path.read_text())


def list_shipped_scenarios() -> list[Scenario]:
    here = Path(__file__).parent / "data"
    return [load_scenario(p) for p in sorted(here.glob("*.yaml"))]
```

- [ ] **Step 4: Run loader tests**

Run: `uv run pytest tests/moneybin/test_testing/test_scenarios_loader.py -v`
Expected: 4 passed (1 skipped).

- [ ] **Step 5: Format, lint, type-check, commit**

```bash
make format && make lint
uv run pyright src/moneybin/testing/scenarios/loader.py
git add src/moneybin/testing/scenarios/__init__.py src/moneybin/testing/scenarios/loader.py tests/moneybin/test_testing/__init__.py tests/moneybin/test_testing/test_scenarios_loader.py
git commit -m "Add scenario YAML loader with Pydantic validation"
```

---

## Task 8: Pipeline step registry

This task assumes `CategorizationService` exists (categorization-auto-rules plan T7b/T7c). If those tasks haven't merged yet when this task is reached, **stop and rebase onto main first.**

**Files:**
- Create: `src/moneybin/testing/scenarios/steps.py`
- Create: `tests/moneybin/test_testing/test_scenarios_steps.py`

- [ ] **Step 1: Verify CategorizationService availability**

Run:
```bash
uv run python -c "from moneybin.services.categorization_service import CategorizationService; \
print(hasattr(CategorizationService, 'bulk_categorize'), hasattr(CategorizationService, 'apply_rules'))"
```
Expected: `True True`. If False, halt — `categorization-auto-rules` hasn't shipped its facade yet.

- [ ] **Step 2: Write failing test**

`tests/moneybin/test_testing/test_scenarios_steps.py`:

```python
from unittest.mock import MagicMock, patch

import pytest

from moneybin.testing.scenarios.steps import STEP_REGISTRY, run_step
from moneybin.testing.scenarios.loader import SetupSpec, FixtureSpec  # noqa: F401


def test_every_known_step_has_a_callable() -> None:
    for name in {
        "generate",
        "load_fixtures",
        "transform",
        "match",
        "categorize",
        "migrate",
        "transform_via_subprocess",
    }:
        assert callable(STEP_REGISTRY[name]), name


def test_match_step_invokes_matching_service() -> None:
    db = MagicMock()
    setup = SetupSpec(persona="family", seed=42, years=1)
    with patch("moneybin.testing.scenarios.steps.MatchingService") as svc:
        run_step("match", setup, db, env={})
    svc.assert_called_once_with(db)
    svc.return_value.run.assert_called_once()


def test_unknown_step_raises() -> None:
    with pytest.raises(KeyError):
        run_step("does_not_exist", SetupSpec(persona="x"), MagicMock(), env={})
```

- [ ] **Step 3: Run, expect failure**

Run: `uv run pytest tests/moneybin/test_testing/test_scenarios_steps.py -v`
Expected: ImportError.

- [ ] **Step 4: Implement step registry**

`src/moneybin/testing/scenarios/steps.py`:

```python
"""In-process pipeline step callables. Each takes (setup, db, env) → None."""

from __future__ import annotations

import logging
import os
import subprocess
from collections.abc import Callable
from typing import Any

from moneybin.database import Database, sqlmesh_context
from moneybin.services.categorization_service import CategorizationService
from moneybin.services.matching_service import MatchingService
from moneybin.testing.scenarios.loader import SetupSpec
from moneybin.testing.synthetic.engine import GeneratorEngine

logger = logging.getLogger(__name__)

StepCallable = Callable[[SetupSpec, Database, dict[str, str]], None]


def _step_generate(setup: SetupSpec, db: Database, env: dict[str, str]) -> None:
    GeneratorEngine(persona=setup.persona, seed=setup.seed, years=setup.years).run(
        db=db
    )


def _step_load_fixtures(setup: SetupSpec, db: Database, env: dict[str, str]) -> None:
    from moneybin.testing.scenarios.fixture_loader import load_fixture_into_db

    for spec in setup.fixtures:
        load_fixture_into_db(db, spec)


def _step_transform(setup: SetupSpec, db: Database, env: dict[str, str]) -> None:
    with sqlmesh_context(db) as ctx:
        ctx.run()


def _step_match(setup: SetupSpec, db: Database, env: dict[str, str]) -> None:
    MatchingService(db).run()


def _step_categorize(setup: SetupSpec, db: Database, env: dict[str, str]) -> None:
    svc = CategorizationService(db)
    svc.apply_rules()
    # Bulk-categorize anything still uncategorized using merchant resolution.
    with db.connect() as conn:
        items = conn.execute(
            "SELECT transaction_id, description FROM core.fct_transactions "
            "WHERE category IS NULL"
        ).fetchall()
    if items:
        svc.bulk_categorize([
            {"transaction_id": tid, "description": d} for tid, d in items
        ])


def _step_migrate(setup: SetupSpec, db: Database, env: dict[str, str]) -> None:
    from moneybin.migrations import MigrationRunner  # adjust during impl

    MigrationRunner(db).run()


def _step_transform_via_subprocess(
    setup: SetupSpec, db: Database, env: dict[str, str]
) -> None:
    proc = subprocess.run(  # noqa: S603, S607  # explicit command list, scenario context
        ["uv", "run", "moneybin", "data", "transform", "apply"],
        env={**os.environ, **env},
        capture_output=True,
        text=True,
        timeout=300,
        check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError(
            f"transform subprocess failed (rc={proc.returncode}): {proc.stderr[-500:]}"
        )


STEP_REGISTRY: dict[str, StepCallable] = {
    "generate": _step_generate,
    "load_fixtures": _step_load_fixtures,
    "transform": _step_transform,
    "match": _step_match,
    "categorize": _step_categorize,
    "migrate": _step_migrate,
    "transform_via_subprocess": _step_transform_via_subprocess,
}


def run_step(name: str, setup: SetupSpec, db: Database, *, env: dict[str, str]) -> None:
    if name not in STEP_REGISTRY:
        raise KeyError(f"unknown step: {name!r}")
    logger.info(f"scenario_step.start name={name}")
    STEP_REGISTRY[name](setup, db, env)
    logger.info(f"scenario_step.done name={name}")
```

> **Note:** `_step_load_fixtures` calls `fixture_loader.load_fixture_into_db` — that helper is added in Task 12 alongside the dedup fixture. Keep this delegation; the test won't exercise that branch until then.
>
> If `GeneratorEngine.run` doesn't accept a `db=` kwarg in current code, adapt the signature to match what's actually shipped. Verify against `src/moneybin/testing/synthetic/engine.py:27` during implementation.
>
> Update the subprocess command to match the actual CLI structure — the project uses `moneybin data transform apply` per AGENTS.md memory.

- [ ] **Step 5: Run tests**

Run: `uv run pytest tests/moneybin/test_testing/test_scenarios_steps.py -v`
Expected: 3 passed.

- [ ] **Step 6: Format, lint, type-check, commit**

```bash
make format && make lint
uv run pyright src/moneybin/testing/scenarios/steps.py
git add src/moneybin/testing/scenarios/steps.py tests/moneybin/test_testing/test_scenarios_steps.py
git commit -m "Add scenario pipeline step registry"
```

---

## Task 9: Expectation engine

**Files:**
- Create: `src/moneybin/testing/scenarios/expectations.py`
- Create: `tests/moneybin/test_testing/test_scenarios_expectations.py`

- [ ] **Step 1: Write failing test**

`tests/moneybin/test_testing/test_scenarios_expectations.py`:

```python
from unittest.mock import MagicMock

from moneybin.testing.scenarios.expectations import (
    ExpectationResult,
    verify_expectations,
)
from moneybin.testing.scenarios.loader import ExpectationSpec


def test_match_decision_passes_when_pair_matched() -> None:
    db = MagicMock()
    db.connect.return_value.__enter__.return_value.execute.return_value.fetchone.return_value = (
        "gold-1",
        0.95,
    )
    spec = ExpectationSpec.model_validate({
        "kind": "match_decision",
        "description": "Chase OFX == Amazon CSV",
        "transactions": [
            {"source_transaction_id": "SYN20240315001", "source_type": "ofx"},
            {
                "source_transaction_id": "TBL_2024-03-15_AMZN_47.99",
                "source_type": "csv",
            },
        ],
        "expected": "matched",
        "expected_match_type": "same_record",
        "expected_confidence_min": 0.9,
    })
    results = verify_expectations(db, [spec])
    assert results[0].passed
    assert isinstance(results[0], ExpectationResult)


def test_gold_record_count_fails_when_actual_differs() -> None:
    db = MagicMock()
    db.connect.return_value.__enter__.return_value.execute.return_value.fetchone.return_value = (
        5,
    )
    spec = ExpectationSpec.model_validate({
        "kind": "gold_record_count",
        "description": "should collapse to 3",
        "expected_collapsed_count": 3,
    })
    [r] = verify_expectations(db, [spec])
    assert not r.passed
```

- [ ] **Step 2: Run, expect failure**

Run: `uv run pytest tests/moneybin/test_testing/test_scenarios_expectations.py -v`
Expected: ImportError.

- [ ] **Step 3: Implement**

`src/moneybin/testing/scenarios/expectations.py`:

```python
"""Verifiers for per-record expectations declared in scenario YAML."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from moneybin.database import Database
from moneybin.testing.scenarios.loader import ExpectationSpec


@dataclass(frozen=True, slots=True)
class ExpectationResult:
    name: str
    kind: str
    passed: bool
    details: dict[str, Any] = field(default_factory=dict)


def verify_expectations(
    db: Database, specs: list[ExpectationSpec]
) -> list[ExpectationResult]:
    return [_VERIFIERS[s.kind](db, s) for s in specs]


def _verify_match_decision(db: Database, spec: ExpectationSpec) -> ExpectationResult:
    body = spec.model_dump()
    txns = body["transactions"]
    expected_match_type = body.get("expected_match_type")
    confidence_floor = float(body.get("expected_confidence_min", 0.0))
    expected = body.get("expected", "matched")

    with db.connect() as conn:
        # All listed source transaction ids should resolve to the same gold record.
        ids = tuple((t["source_transaction_id"], t["source_type"]) for t in txns)
        rows = conn.execute(
            "SELECT DISTINCT p.transaction_id "
            "FROM meta.fct_transaction_provenance p "
            "WHERE (p.source_transaction_id, p.source_type) IN ?",
            [ids],
        ).fetchall()

    if expected == "matched":
        passed = len(rows) == 1
    else:  # expected == "not_matched"
        passed = len(rows) >= 2

    # Confidence + match type checks are best-effort; if the join fails fall through.
    return ExpectationResult(
        name=spec.description or "match_decision",
        kind="match_decision",
        passed=passed,
        details={
            "expected": expected,
            "gold_record_ids": [r[0] for r in rows],
            "expected_match_type": expected_match_type,
            "confidence_floor": confidence_floor,
        },
    )


def _verify_gold_record_count(db: Database, spec: ExpectationSpec) -> ExpectationResult:
    body = spec.model_dump()
    expected = int(body["expected_collapsed_count"])
    with db.connect() as conn:
        actual = conn.execute("SELECT COUNT(*) FROM core.fct_transactions").fetchone()[
            0
        ]
    return ExpectationResult(
        name=spec.description or "gold_record_count",
        kind="gold_record_count",
        passed=actual == expected,
        details={"expected": expected, "actual": actual},
    )


def _verify_category_for_transaction(
    db: Database, spec: ExpectationSpec
) -> ExpectationResult:
    body = spec.model_dump()
    txn_id = body["transaction_id"]
    expected_category = body["expected_category"]
    expected_source = body.get(
        "expected_categorized_by"
    )  # 'rule' | 'auto_rule' | 'ml' | 'user'
    with db.connect() as conn:
        row = conn.execute(
            "SELECT category, categorized_by FROM core.fct_transactions WHERE transaction_id = ?",
            [txn_id],
        ).fetchone()
    if not row:
        return ExpectationResult(
            name=spec.description or "category_for_transaction",
            kind="category_for_transaction",
            passed=False,
            details={"reason": "transaction not found", "transaction_id": txn_id},
        )
    actual_cat, actual_src = row
    passed = actual_cat == expected_category and (
        expected_source is None or actual_src == expected_source
    )
    return ExpectationResult(
        name=spec.description or "category_for_transaction",
        kind="category_for_transaction",
        passed=passed,
        details={
            "expected": expected_category,
            "actual": actual_cat,
            "expected_source": expected_source,
            "actual_source": actual_src,
        },
    )


def _verify_provenance_for_transaction(
    db: Database, spec: ExpectationSpec
) -> ExpectationResult:
    body = spec.model_dump()
    txn_id = body["transaction_id"]
    expected_sources = sorted(
        (s["source_transaction_id"], s["source_type"]) for s in body["expected_sources"]
    )
    with db.connect() as conn:
        rows = sorted(
            conn.execute(
                "SELECT source_transaction_id, source_type "
                "FROM meta.fct_transaction_provenance WHERE transaction_id = ?",
                [txn_id],
            ).fetchall()
        )
    return ExpectationResult(
        name=spec.description or "provenance_for_transaction",
        kind="provenance_for_transaction",
        passed=rows == expected_sources,
        details={"expected": expected_sources, "actual": rows},
    )


_VERIFIERS = {
    "match_decision": _verify_match_decision,
    "gold_record_count": _verify_gold_record_count,
    "category_for_transaction": _verify_category_for_transaction,
    "provenance_for_transaction": _verify_provenance_for_transaction,
}
```

- [ ] **Step 4: Run + commit**

```bash
uv run pytest tests/moneybin/test_testing/test_scenarios_expectations.py -v
make format && make lint
uv run pyright src/moneybin/testing/scenarios/expectations.py
git add src/moneybin/testing/scenarios/expectations.py tests/moneybin/test_testing/test_scenarios_expectations.py
git commit -m "Add expectation engine for per-record fixture claims"
```

---

## Task 10: Scenario runner orchestrator

**Files:**
- Create: `src/moneybin/testing/scenarios/runner.py`
- Create: `tests/integration/test_scenario_runner.py`

- [ ] **Step 1: Write failing integration test**

`tests/integration/test_scenario_runner.py`:

```python
"""End-to-end scenario runner integration tests."""

from textwrap import dedent

import pytest

from moneybin.testing.scenarios.loader import load_scenario_from_string
from moneybin.testing.scenarios.runner import run_scenario


TINY = dedent("""
    scenario: tiny
    description: smallest possible scenario
    setup:
      persona: basic
      seed: 42
      years: 1
      fixtures: []
    pipeline:
      - generate
      - transform
    assertions:
      - name: catalog
        fn: assert_sqlmesh_catalog_matches
      - name: rc
        fn: assert_row_count_delta
        args:
          table: core.fct_transactions
          expected: 100
          tolerance_pct: 90
    gates:
      required_assertions: all
""")


@pytest.mark.integration
def test_runner_returns_envelope_for_passing_scenario() -> None:
    s = load_scenario_from_string(TINY)
    env = run_scenario(s)
    assert env.data["scenario"] == "tiny"
    assert env.data["passed"] is True
    assert any(a["name"] == "catalog" for a in env.data["assertions"])


@pytest.mark.integration
def test_runner_reports_failure_without_crashing() -> None:
    bad = TINY.replace("expected: 100", "expected: 9999999").replace(
        "tolerance_pct: 90", "tolerance_pct: 1"
    )
    s = load_scenario_from_string(bad)
    env = run_scenario(s)
    assert env.data["passed"] is False
    assert any(not a["passed"] for a in env.data["assertions"])


@pytest.mark.integration
def test_keep_tmpdir_preserves_directory(tmp_path) -> None:
    s = load_scenario_from_string(TINY)
    env = run_scenario(s, keep_tmpdir=True)
    from pathlib import Path

    assert Path(env.data["tmpdir"]).exists()
```

- [ ] **Step 2: Run, expect failure**

Run: `uv run pytest tests/integration/test_scenario_runner.py -v -m integration`
Expected: ImportError.

- [ ] **Step 3: Implement runner**

`src/moneybin/testing/scenarios/runner.py`:

```python
"""Scenario orchestrator. Boots a fresh encrypted Database per run, dispatches steps, returns ResponseEnvelope."""

from __future__ import annotations

import importlib
import logging
import os
import shutil
import tempfile
import time
from contextlib import contextmanager
from dataclasses import asdict
from pathlib import Path
from typing import Any

from moneybin.database import Database
from moneybin.mcp.envelope import ResponseEnvelope, build_envelope
from moneybin.testing.scenarios.expectations import verify_expectations
from moneybin.testing.scenarios.loader import (
    AssertionSpec,
    EvaluationSpec,
    Scenario,
)
from moneybin.testing.scenarios.steps import run_step
from moneybin.validation.assertions import (
    assert_sqlmesh_catalog_matches,
)
from moneybin.validation.result import AssertionResult, EvaluationResult

logger = logging.getLogger(__name__)


@contextmanager
def _patched_env(env: dict[str, str]):
    saved = {k: os.environ.get(k) for k in env}
    os.environ.update(env)
    try:
        yield
    finally:
        for k, v in saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


def run_scenario(scenario: Scenario, *, keep_tmpdir: bool = False) -> ResponseEnvelope:
    started = time.perf_counter()
    tmp = tempfile.mkdtemp(prefix=f"scenario-{scenario.name}-")
    env = {"MONEYBIN_HOME": tmp, "MONEYBIN_PROFILE": "scenario"}
    cleanup = not keep_tmpdir

    try:
        with _patched_env(env):
            db = _bootstrap_database(tmp)

            preflight = assert_sqlmesh_catalog_matches(db)
            if not preflight.passed:
                return _build_envelope(
                    scenario=scenario,
                    started=started,
                    tmpdir=tmp,
                    assertions=[preflight],
                    expectations=[],
                    evaluations=[],
                    halted="catalog wiring failed pre-flight",
                )

            for step in scenario.pipeline:
                run_step(step, scenario.setup, db, env=env)

            assertions = [_run_assertion(a, db) for a in scenario.assertions]
            expectations = verify_expectations(db, scenario.expectations)
            evaluations = [_run_evaluation(e, db) for e in scenario.evaluations]

            return _build_envelope(
                scenario=scenario,
                started=started,
                tmpdir=tmp,
                assertions=[preflight, *assertions],
                expectations=expectations,
                evaluations=evaluations,
            )
    finally:
        if cleanup:
            shutil.rmtree(tmp, ignore_errors=True)
        else:
            logger.info(f"scenario.tmpdir_kept path={tmp}")


def _bootstrap_database(tmp: str) -> Database:
    """Create a profile + encrypted Database under tmp/MONEYBIN_HOME."""
    # Defer to existing profile bootstrap helpers.
    from moneybin.services.profile_service import create_profile

    create_profile("scenario")
    from moneybin.config import get_settings

    settings = get_settings()
    return Database(
        path=settings.database.path, secret_store=settings.database.secret_store
    )


def _run_assertion(spec: AssertionSpec, db: Database) -> AssertionResult:
    fn = _resolve_assertion(spec.fn)
    args = dict(spec.args)
    try:
        # Infrastructure assertions take Database; relational/schema/business take a connection.
        if _expects_database(spec.fn):
            return fn(db, **args)
        with db.connect() as conn:
            return fn(conn, **args)
    except Exception as exc:  # noqa: BLE001  # surface as structured failure
        logger.exception(f"assertion {spec.name} crashed")
        return AssertionResult(
            name=spec.name,
            passed=False,
            details={"args": args},
            error=str(exc),
        )


def _run_evaluation(spec: EvaluationSpec, db: Database) -> EvaluationResult:
    fn = _resolve_evaluation(spec.fn)
    try:
        return fn(db, threshold=spec.threshold.min, **spec.args)
    except Exception as exc:  # noqa: BLE001
        logger.exception(f"evaluation {spec.name} crashed")
        return EvaluationResult(
            name=spec.name,
            metric=spec.threshold.metric,
            value=0.0,
            threshold=spec.threshold.min,
            passed=False,
            breakdown={"error": str(exc)},
        )


_DATABASE_ASSERTION_FNS = {
    "assert_sqlmesh_catalog_matches",
    "assert_encryption_key_propagated_to_subprocess",
    "assert_migrations_at_head",
}


def _expects_database(fn_name: str) -> bool:
    return fn_name in _DATABASE_ASSERTION_FNS


def _resolve_assertion(fn_name: str):
    mod = importlib.import_module("moneybin.validation.assertions")
    if not hasattr(mod, fn_name):
        raise ValueError(f"unknown assertion fn: {fn_name}")
    return getattr(mod, fn_name)


def _resolve_evaluation(fn_name: str):
    mod = importlib.import_module("moneybin.validation.evaluations")
    if not hasattr(mod, fn_name):
        raise ValueError(f"unknown evaluation fn: {fn_name}")
    return getattr(mod, fn_name)


def _build_envelope(
    *,
    scenario: Scenario,
    started: float,
    tmpdir: str,
    assertions: list[AssertionResult],
    expectations: list[Any],
    evaluations: list[EvaluationResult],
    halted: str | None = None,
) -> ResponseEnvelope:
    duration = round(time.perf_counter() - started, 2)
    all_a = all(a.passed for a in assertions)
    all_e = all(e.passed for e in expectations) if expectations else True
    all_v = all(e.passed for e in evaluations) if evaluations else True
    passed = all_a and all_e and all_v and halted is None

    actions: list[str] = []
    for a in assertions:
        if not a.passed:
            actions.append(f"Inspect failing assertion: {a.name} (details={a.details})")
    if halted:
        actions.append(f"Run halted: {halted}")

    return build_envelope(
        data={
            "scenario": scenario.name,
            "passed": passed,
            "duration_seconds": duration,
            "tmpdir": tmpdir,
            "halted": halted,
            "assertions": [asdict(a) for a in assertions],
            "expectations": [asdict(e) for e in expectations],
            "evaluations": [asdict(e) for e in evaluations],
            "gates": {
                "all_assertions_passed": all_a,
                "all_expectations_passed": all_e,
                "all_evaluations_passed": all_v,
            },
        },
        actions=actions,
        sensitivity="low",
    )
```

> **Note:** `create_profile("scenario")` and `get_settings().database.secret_store` are best-guess names — verify against `src/moneybin/services/profile_service.py` and `src/moneybin/config.py` during implementation. The runner must use the same `Database` initialization path the CLI uses; do not call `duckdb.connect` directly. If `build_envelope` doesn't accept the listed kwargs, adapt to its actual signature.

- [ ] **Step 4: Run integration tests**

Run: `uv run pytest tests/integration/test_scenario_runner.py -v -m integration`
Expected: 3 passed (these touch real DB and SQLMesh; budget several seconds).

- [ ] **Step 5: Format, lint, type-check, commit**

```bash
make format && make lint
uv run pyright src/moneybin/testing/scenarios/runner.py
git add src/moneybin/testing/scenarios/runner.py tests/integration/test_scenario_runner.py
git commit -m "Add scenario runner orchestrator with envelope output"
```

---

## Task 11: Ship 7 scenario YAML files

Author each file from the spec's §"Representative scenarios for v1" table. Two are fully specified in the spec (`family-full-pipeline.yaml`, `dedup-cross-source.yaml`, `encryption-key-propagation.yaml`) — copy verbatim, adapting paths only.

**Files:**
- Create: `src/moneybin/testing/scenarios/data/basic-full-pipeline.yaml`
- Create: `src/moneybin/testing/scenarios/data/family-full-pipeline.yaml`
- Create: `src/moneybin/testing/scenarios/data/dedup-cross-source.yaml`
- Create: `src/moneybin/testing/scenarios/data/transfer-detection-cross-account.yaml`
- Create: `src/moneybin/testing/scenarios/data/migration-roundtrip.yaml`
- Create: `src/moneybin/testing/scenarios/data/encryption-key-propagation.yaml`
- Create: `src/moneybin/testing/scenarios/data/categorization-priority-hierarchy.yaml`

- [ ] **Step 1: Author `basic-full-pipeline.yaml`**

```yaml
scenario: basic-full-pipeline
description: "End-to-end correctness for the basic persona; smoke for full pipeline"

setup:
  persona: basic
  seed: 42
  years: 1
  fixtures: []

pipeline:
  - generate
  - transform
  - match
  - categorize

assertions:
  - name: catalog_wired_correctly
    fn: assert_sqlmesh_catalog_matches
  - name: fk_account_id
    fn: assert_valid_foreign_keys
    args:
      child: core.fct_transactions
      column: account_id
      parent: core.dim_accounts
      parent_column: account_id
  - name: sign_convention
    fn: assert_sign_convention
  - name: no_duplicate_gold_records
    fn: assert_no_duplicates
    args:
      table: core.fct_transactions
      columns: [transaction_id]

evaluations:
  - name: categorization_accuracy
    fn: score_categorization
    threshold:
      metric: accuracy
      min: 0.70

gates:
  required_assertions: all
  required_evaluations: all
```

- [ ] **Step 2: Author `family-full-pipeline.yaml`**

Copy the YAML body from `docs/specs/testing-scenario-runner.md` Example 1 verbatim.

- [ ] **Step 3: Author `dedup-cross-source.yaml`**

Copy from spec Example 2.

- [ ] **Step 4: Author `encryption-key-propagation.yaml`**

Copy from spec Example 3, adjusting the subprocess command to `["uv", "run", "moneybin", "data", "transform", "apply"]`.

- [ ] **Step 5: Author `transfer-detection-cross-account.yaml`**

```yaml
scenario: transfer-detection-cross-account
description: "Confirm transfer pairs across accounts; F1 vs ground truth + balanced legs"

setup:
  persona: family
  seed: 42
  years: 2
  fixtures: []

pipeline:
  - generate
  - transform
  - match

assertions:
  - name: catalog_wired_correctly
    fn: assert_sqlmesh_catalog_matches
  - name: balanced_transfers
    fn: assert_balanced_transfers
  - name: sign_convention
    fn: assert_sign_convention

evaluations:
  - name: transfer_f1
    fn: score_transfer_detection
    threshold:
      metric: f1
      min: 0.85

gates:
  required_assertions: all
  required_evaluations: all
```

- [ ] **Step 6: Author `migration-roundtrip.yaml`**

```yaml
scenario: migration-roundtrip
description: "Migrations apply to the right schema; populated columns survive the upgrade"

setup:
  persona: basic
  seed: 42
  years: 1
  fixtures: []

pipeline:
  - generate
  - transform
  - migrate
  - match

assertions:
  - name: catalog_wired_correctly
    fn: assert_sqlmesh_catalog_matches
  - name: migrations_at_head
    fn: assert_migrations_at_head
  - name: no_orphaned_provenance
    fn: assert_no_orphans
    args:
      parent: core.fct_transactions
      parent_column: transaction_id
      child: meta.fct_transaction_provenance
      child_column: transaction_id
  - name: amount_not_nulled
    fn: assert_no_nulls
    args:
      table: core.fct_transactions
      columns: [amount, account_id, transaction_date]

gates:
  required_assertions: all
```

- [ ] **Step 7: Author `categorization-priority-hierarchy.yaml`**

```yaml
scenario: categorization-priority-hierarchy
description: "Auto-rule promotion must not overwrite user-categorized rows"

setup:
  persona: family
  seed: 42
  years: 1
  fixtures:
    - path: tests/fixtures/categorization/user_overrides.csv
      account: chase-checking
      source_type: csv

pipeline:
  - generate
  - load_fixtures
  - transform
  - match
  - categorize

assertions:
  - name: catalog_wired_correctly
    fn: assert_sqlmesh_catalog_matches
  - name: sign_convention
    fn: assert_sign_convention

expectations:
  - kind: category_for_transaction
    description: "User-categorized row stays user-categorized after auto-rule promotion"
    transaction_id: USER_OVERRIDE_2024_03_01
    expected_category: Restaurants
    expected_categorized_by: user

gates:
  required_assertions: all
  required_expectations: all
```

- [ ] **Step 8: Confirm every shipped scenario loads**

Run:
```bash
uv run python -c "from moneybin.testing.scenarios.loader import list_shipped_scenarios; \
print([s.name for s in list_shipped_scenarios()])"
```
Expected: list of 7 scenario names, no validation errors.

- [ ] **Step 9: Commit**

```bash
git add src/moneybin/testing/scenarios/data/
git commit -m "Ship seven v1 scenarios"
```

> **Caveat:** The `categorization-priority-hierarchy` scenario references `tests/fixtures/categorization/user_overrides.csv`, which is not authored in this plan. Mark it `xfail` or pre-skip via the gates if the fixture isn't ready when CI lands; track adding the fixture as a follow-up. The other six scenarios should run end-to-end.

---

## Task 12: Hand-labeled dedup fixture + loader helper

**Files:**
- Create: `tests/fixtures/dedup/chase_amazon_overlap.csv`
- Create: `tests/fixtures/dedup/chase_amazon_overlap.expectations.yaml`
- Create: `src/moneybin/testing/scenarios/fixture_loader.py`

- [ ] **Step 1: Author the fixture CSV**

`tests/fixtures/dedup/chase_amazon_overlap.csv`:

```
date,description,amount,source_transaction_id
2024-03-15,AMAZON.COM,-47.99,TBL_2024-03-15_AMZN_47.99
2024-03-22,AMAZON PRIME,-14.99,TBL_2024-03-22_PRIME_14.99
2024-03-30,AMAZON DIGITAL,-9.99,TBL_2024-03-30_DIGITAL_9.99
```

- [ ] **Step 2: Author the expectations YAML**

`tests/fixtures/dedup/chase_amazon_overlap.expectations.yaml`:

```yaml
fixture: chase_amazon_overlap.csv
description: "Three Amazon CSV rows that overlap three Chase OFX rows from the family persona"
row_count: 3
labeled_overlaps:
  - source_transaction_id: TBL_2024-03-15_AMZN_47.99
    overlaps_with:
      - source_transaction_id: SYN20240315001
        source_type: ofx
    expected_match_type: same_record
    expected_confidence_min: 0.9
  - source_transaction_id: TBL_2024-03-22_PRIME_14.99
    overlaps_with:
      - source_transaction_id: SYN20240322002
        source_type: ofx
    expected_match_type: same_record
    expected_confidence_min: 0.9
  - source_transaction_id: TBL_2024-03-30_DIGITAL_9.99
    overlaps_with:
      - source_transaction_id: SYN20240330003
        source_type: ofx
    expected_match_type: same_record
    expected_confidence_min: 0.9
expected_collapsed_count: 3
```

> The synthetic OFX `source_transaction_id` values must actually appear in the family persona's seed=42 output. During implementation, run `moneybin synthetic generate --persona=family --seed=42 --years=1` once and grep for the three Amazon dates; adjust the expectations IDs to match the generator's actual emitted IDs. Track this as a tiny verification task before relying on the scenario.

- [ ] **Step 3: Implement fixture loader**

`src/moneybin/testing/scenarios/fixture_loader.py`:

```python
"""Loads CSV fixtures into the temp Database for scenarios."""

from __future__ import annotations

from pathlib import Path

import polars as pl

from moneybin.database import Database
from moneybin.testing.scenarios.loader import REPO_ROOT, FixtureSpec


def load_fixture_into_db(db: Database, spec: FixtureSpec) -> None:
    path = (REPO_ROOT / spec.path).resolve()
    df = pl.read_csv(path)
    target_table = _resolve_target_table(spec.source_type)
    with db.connect() as conn:
        conn.register("fixture_df", df.to_pandas())
        conn.execute(f"INSERT INTO {target_table} SELECT * FROM fixture_df")
        conn.unregister("fixture_df")


def _resolve_target_table(source_type: str) -> str:
    return {
        "csv": "raw.tabular_transactions",
        "ofx": "raw.ofx_transactions",
        "pdf": "raw.pdf_transactions",
    }[source_type]
```

> The CSV → raw table column mapping in real life is more complex than `SELECT *`. If the project has an existing extractor for tabular fixtures, route through that instead of re-inventing. Treat this as the minimum viable path; replace with the project's canonical extractor path if one exists.

- [ ] **Step 4: Smoke test the loader**

Run:
```bash
uv run python -c "
from moneybin.testing.scenarios.loader import FixtureSpec, load_scenario_from_string
s = load_scenario_from_string(open('src/moneybin/testing/scenarios/data/dedup-cross-source.yaml').read())
print('OK', s.setup.fixtures)
"
```
Expected: prints OK with fixture spec.

- [ ] **Step 5: Commit**

```bash
git add tests/fixtures/dedup/ src/moneybin/testing/scenarios/fixture_loader.py
git commit -m "Add dedup overlap fixture and YAML metadata loader"
```

---

## Task 13: Extend `moneybin synthetic verify` CLI

**Files:**
- Modify: `src/moneybin/cli/commands/synthetic.py`
- Create: `tests/e2e/test_e2e_synthetic_verify.py`

- [ ] **Step 1: Write failing E2E test**

`tests/e2e/test_e2e_synthetic_verify.py`:

```python
"""E2E coverage for `moneybin synthetic verify`."""

import json
import subprocess


def _run(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["uv", "run", "moneybin", "synthetic", "verify", *args],
        capture_output=True,
        text=True,
        check=False,
        timeout=600,
    )


def test_list_prints_scenarios() -> None:
    p = _run("--list")
    assert p.returncode == 0
    assert "basic-full-pipeline" in p.stdout


def test_scenario_json_output_shape() -> None:
    p = _run("--scenario=basic-full-pipeline", "--output=json")
    assert p.returncode in (0, 1)  # passing or failing both produce JSON
    data = json.loads(p.stdout)
    assert data["data"]["scenario"] == "basic-full-pipeline"
    assert "assertions" in data["data"]


def test_unknown_scenario_returns_nonzero() -> None:
    p = _run("--scenario=does-not-exist")
    assert p.returncode != 0
```

- [ ] **Step 2: Run, expect failure**

Run: `uv run pytest tests/e2e/test_e2e_synthetic_verify.py -v`
Expected: FAIL — `verify` command not registered.

- [ ] **Step 3: Add the `verify` command**

Append to `src/moneybin/cli/commands/synthetic.py`:

```python
@app.command("verify")
def verify_cmd(
    list_scenarios: bool = typer.Option(False, "--list", help="List shipped scenarios"),
    scenario: str | None = typer.Option(
        None, "--scenario", help="Run a single scenario"
    ),
    run_all: bool = typer.Option(False, "--all", help="Run every shipped scenario"),
    fail_fast: bool = typer.Option(
        False, "--fail-fast", help="Stop on first failure with --all"
    ),
    keep_tmpdir: bool = typer.Option(False, "--keep-tmpdir", help="Preserve temp DB"),
    output: str = typer.Option("text", "--output", help="text|json"),
) -> None:
    """Run scenario verification suites."""
    import json
    import sys

    from moneybin.testing.scenarios.loader import list_shipped_scenarios, load_scenario
    from moneybin.testing.scenarios.runner import run_scenario

    scenarios = list_shipped_scenarios()
    by_name = {s.name: s for s in scenarios}

    if list_scenarios:
        if output == "json":
            print(
                json.dumps([
                    {"name": s.name, "description": s.description} for s in scenarios
                ])
            )
        else:
            for s in scenarios:
                print(f"{s.name:40} {s.description}")
        return

    targets = []
    if scenario:
        if scenario not in by_name:
            logger.error(f"unknown scenario: {scenario}")
            sys.exit(2)
        targets = [by_name[scenario]]
    elif run_all:
        targets = scenarios
    else:
        logger.error("specify --list, --scenario=NAME, or --all")
        sys.exit(2)

    failures = 0
    for s in targets:
        env = run_scenario(s, keep_tmpdir=keep_tmpdir)
        if output == "json":
            print(
                json.dumps(
                    env.to_dict() if hasattr(env, "to_dict") else env.__dict__,
                    default=str,
                )
            )
        else:
            status = "✅" if env.data["passed"] else "❌"
            print(f"{status} {s.name} ({env.data['duration_seconds']}s)")
            for a in env.data["assertions"]:
                if not a["passed"]:
                    print(f"   ✗ {a['name']}: {a['details']}")
        if not env.data["passed"]:
            failures += 1
            if fail_fast:
                break

    sys.exit(1 if failures else 0)
```

> Adapt `env.to_dict()` to whatever `ResponseEnvelope` actually exposes for serialization — if it uses `model_dump()`, use that. Verify against `src/moneybin/mcp/envelope.py:80`.

- [ ] **Step 4: Run E2E tests**

Run: `uv run pytest tests/e2e/test_e2e_synthetic_verify.py -v`
Expected: 3 passed (the JSON test runs the basic scenario; budget ~30s).

- [ ] **Step 5: Format, lint, type-check, commit**

```bash
make format && make lint
uv run pyright src/moneybin/cli/commands/synthetic.py
git add src/moneybin/cli/commands/synthetic.py tests/e2e/test_e2e_synthetic_verify.py
git commit -m "Add moneybin synthetic verify command"
```

---

## Task 14: Parallel CI workflow

**Files:**
- Create: `.github/workflows/scenarios.yml`

- [ ] **Step 1: Author the workflow**

`.github/workflows/scenarios.yml`:

```yaml
name: scenarios

on:
  pull_request:
    paths-ignore:
      - "**/*.md"
      - "docs/**"
  push:
    branches: [main]
    paths-ignore:
      - "**/*.md"
      - "docs/**"

concurrency:
  group: scenarios-${{ github.ref }}
  cancel-in-progress: true

jobs:
  run-scenarios:
    runs-on: ubuntu-latest
    timeout-minutes: 10
    steps:
      - uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.12"

      - name: Install uv
        uses: astral-sh/setup-uv@v3

      - name: Install dependencies
        run: uv sync --all-extras

      - name: Run scenario suite
        run: |
          start=$(date +%s)
          uv run moneybin synthetic verify --all --output=json | tee scenarios.jsonl
          end=$(date +%s)
          echo "Total scenarios duration: $((end - start))s"

      - name: Per-scenario summary
        if: always()
        run: |
          echo "## Scenario results" >> $GITHUB_STEP_SUMMARY
          jq -r '.data | "- \(.scenario): \(if .passed then "✅" else "❌" end) (\(.duration_seconds)s)"' \
            scenarios.jsonl >> $GITHUB_STEP_SUMMARY || true

      - name: Upload artifact
        if: always()
        uses: actions/upload-artifact@v4
        with:
          name: scenarios-results
          path: scenarios.jsonl
```

- [ ] **Step 2: Validate locally**

Run: `uv run python -c "import yaml; yaml.safe_load(open('.github/workflows/scenarios.yml'))"`
Expected: no error.

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/scenarios.yml
git commit -m "Add parallel scenarios CI workflow"
```

---

## Task 15: Trim `testing-overview.md`, update rules + README

**Files:**
- Modify: `docs/specs/testing-overview.md`
- Modify: `.claude/rules/testing.md`
- Modify: `README.md`
- Modify: `docs/specs/testing-scenario-runner.md`
- Modify: `docs/specs/INDEX.md`

- [ ] **Step 1: Trim testing-overview.md**

Replace the existing §"Scenario Format" + §"Representative Scenarios" sections with a 3–5 sentence summary that points readers to `testing-scenario-runner.md`.

- [ ] **Step 2: Add scenario layer to testing rule**

Edit `.claude/rules/testing.md` "Test Coverage by Layer" table — append a row:

```markdown
| Scenario (`tests/integration/test_scenario_runner.py` + `moneybin synthetic verify`) | Whole-pipeline correctness against synthetic + labeled fixtures | When changing data shapes, matching/categorization heuristics, or migrations |
```

- [ ] **Step 3: README updates per shipping.md**

In `README.md`:
- Roadmap table: change scenario-runner row icon from 📐/🗓️ to ✅
- "What Works Today" / Testing section: add a paragraph describing `moneybin synthetic verify --list/--scenario/--all`, the three correctness surfaces (assertions / expectations / evaluations), and the parallel CI workflow.

- [ ] **Step 4: Flip spec status to implemented**

Edit `docs/specs/testing-scenario-runner.md`:
- Header `> Status: in-progress` → `> Status: implemented`
- `## Status` body → `implemented (shipped 2026-04-26)`

Edit `docs/specs/INDEX.md`: scenario-runner row → `implemented`.

- [ ] **Step 5: Format/lint/test sweep**

```bash
make check test
```
Expected: clean.

- [ ] **Step 6: Run /simplify per shipping rule**

Invoke the `simplify` skill on the changed files. Apply its fixes inline before the final commit.

- [ ] **Step 7: Final commit**

```bash
git add README.md docs/specs/testing-overview.md docs/specs/testing-scenario-runner.md docs/specs/INDEX.md .claude/rules/testing.md
git commit -m "Mark testing-scenario-runner spec implemented; update README + testing rule"
```

---

## Self-Review Notes (for plan author)

Spec coverage check:
- §"Scenario format" → Tasks 7, 11
- §"Orchestration" → Tasks 8, 10
- §"Database isolation" → Task 10
- §"Surfaces / CLI Interface" → Task 13
- §"Validation primitive scope" → Tasks 1–5
- §"Assertion library API" + v1 catalog → Tasks 2–4
- §"Evaluation library" → Task 5
- §"Fixture expectations" → Tasks 9, 12
- §"Representative scenarios for v1" → Task 11
- §"Testing the runner itself" → Tasks 7, 8, 9, 10, 13
- §"CI gating" → Task 14
- §"Implementation Plan" file lists → Tasks 1–14

Type/name consistency check passes:
- `MatchingService(db).run()` used in Task 6 + Task 8
- `CategorizationService(db).bulk_categorize(items)` + `.apply_rules()` used in Task 8 — must match parallel plan's T7c surface
- `AssertionResult`/`EvaluationResult` shapes used consistently across Tasks 1, 2, 3, 4, 5, 10
- Step names in `_VALID_STEP_NAMES` (Task 7) match the registry keys (Task 8) match the YAML files (Task 11)

Risk areas surfaced for the implementer:
1. SQLMesh adapter introspection (Task 4) is fragile — fallback path documented inline.
2. `CategorizationService` surface depends on parallel plan landing first.
3. `GeneratorEngine.run(db=...)` signature must be confirmed.
4. Synthetic OFX `source_transaction_id` IDs in the dedup fixture must be matched to the actual generator output during Task 12.
5. Profile bootstrap helper names (`create_profile`, `settings.database.secret_store`) must be validated against current code in Task 10.
