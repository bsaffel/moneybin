# Validation Library Extract — Phase 2 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reorganize `src/moneybin/validation/` into industry-aligned modules, standardize on `Database` as the first arg of every assertion, and decouple the per-record `expectations` verifiers from the YAML loader so they become reusable library API.

**Architecture:** Validation primitives live under `src/moneybin/validation/` in three peer subpackages — `assertions/`, `expectations/`, `evaluations/` — each split by industry-recognized concern. The scenario runner becomes the **only** place that translates YAML specs into library calls, via two explicit registries (`_assertion_registry.py`, `_expectation_registry.py`) co-located with the runner. Top-level `validation/__init__.py` re-exports only the three Result types; primitives are imported via their submodule.

**Tech Stack:** Python 3.12, DuckDB (via `Database` wrapper), Pydantic v2 (YAML loader only), pytest, pyright.

**Spec authority:** [`docs/specs/testing-scenario-comprehensive.md`](../../specs/testing-scenario-comprehensive.md) Phase 2.

---

## Background — read first

`src/moneybin/validation/` was created during PR #59 (scenario runner). It already contains:
- `assertions/{business,distributional,infrastructure,relational,schema}.py`
- `evaluations/{categorization,matching,_common}.py`
- `result.py` with `AssertionResult`, `EvaluationResult`

`src/moneybin/testing/scenarios/expectations.py` contains five per-record verifiers tightly coupled to the Pydantic `ExpectationSpec` (from `loader.py`).

This phase does **four** things, in order:

1. **Reorganize `assertions/` into seven industry-aligned modules.** Drop the spec's `structural`/`behavioral`/`semantic`/`quality` names (they collide with established meanings in SE/DE communities) in favor of `schema`/`completeness`/`uniqueness`/`integrity`/`domain`/`distribution`/`infrastructure`. Three renames (relational → split, business → domain, distributional → distribution) plus a split.
2. **Standardize first-arg type to `Database`.** Today some take `Database`, most take `DuckDBPyConnection` — the runner has a hardcoded `_DATABASE_ASSERTION_FNS` set to dispatch correctly. After this phase, every `assert_*` and `verify_*` takes `Database`. Removes the runner wart.
3. **Decouple expectations.** Create `validation/expectations/{matching,transactions}.py` with typed kwargs, `verify_*` (no underscore prefix), returning `ExpectationResult` (move from `scenarios/expectations.py` to `validation/result.py`). YAML adapter and registry move to `tests/scenarios/_expectation_*.py` (lives next to the runner, in `src/moneybin/testing/scenarios/` until Phase 1 relocation happens).
4. **Wire explicit registries.** `_assertion_registry.py` and `_expectation_registry.py` co-locate with the runner. Replace `importlib.import_module(...).getattr(...)` lookups with explicit `dict[str, Callable]` lookups. Adding a new YAML-callable primitive becomes one explicit line.

**Stable contract commitment** (per Q4 discussion): after this phase, `moneybin.validation.assertions.*`, `moneybin.validation.expectations.*`, `moneybin.validation.evaluations.*`, `moneybin.validation.result.*` are stable for `data-reconciliation`. Additive kwargs OK; rename/remove requires deprecation alias for one release. `details`/`breakdown` payloads are per-function, not cross-function contract.

**Non-goals (explicitly out of scope):**
- New assertion primitives — Phase 3.
- New scenarios — Phase 4.
- Scenario relocation `src/moneybin/testing/scenarios/` → `tests/scenarios/` — Phase 1 (independent of this work).
- Harness primitives (`assert_idempotent`, `assert_subprocess_parity`) — Phase 3+, will land in `tests/scenarios/_harnesses.py`, **not** in `validation/`.

---

## File Structure

### New files

| Path | Responsibility |
|---|---|
| `src/moneybin/validation/__init__.py` | Re-export `AssertionResult`, `EvaluationResult`, `ExpectationResult` only |
| `src/moneybin/validation/assertions/_helpers.py` | Shared `_quote_ident` helper (moved out of `relational.py`) |
| `src/moneybin/validation/assertions/completeness.py` | `assert_no_nulls` |
| `src/moneybin/validation/assertions/uniqueness.py` | `assert_no_duplicates` |
| `src/moneybin/validation/assertions/integrity.py` | `assert_valid_foreign_keys`, `assert_no_orphans` (renamed from `relational.py`) |
| `src/moneybin/validation/assertions/domain.py` | `assert_sign_convention`, `assert_balanced_transfers`, `assert_date_continuity` (renamed from `business.py`) |
| `src/moneybin/validation/assertions/distribution.py` | `assert_distribution_within_bounds`, `assert_unique_value_count` (renamed from `distributional.py`) |
| `src/moneybin/validation/expectations/__init__.py` | Re-export the five `verify_*` functions and `SourceTransactionRef` |
| `src/moneybin/validation/expectations/_types.py` | `SourceTransactionRef` dataclass |
| `src/moneybin/validation/expectations/matching.py` | `verify_match_decision`, `verify_transfers_match_ground_truth` |
| `src/moneybin/validation/expectations/transactions.py` | `verify_gold_record_count`, `verify_category_for_transaction`, `verify_provenance_for_transaction` |
| `src/moneybin/testing/scenarios/_assertion_registry.py` | Explicit `{yaml_name: callable}` for assertions, used by runner |
| `src/moneybin/testing/scenarios/_expectation_registry.py` | Explicit `{ExpectationSpec.kind: adapter_callable}` — adapter parses spec → typed kwargs and calls library predicate |
| `tests/moneybin/test_validation/test_assertions_completeness.py` | Test for `assert_no_nulls` (extracted from `test_assertions_relational.py`) |
| `tests/moneybin/test_validation/test_assertions_uniqueness.py` | Test for `assert_no_duplicates` (extracted) |
| `tests/moneybin/test_validation/test_assertions_integrity.py` | Tests for FK/orphans (renamed from `test_assertions_relational.py`) |
| `tests/moneybin/test_validation/test_expectations_matching.py` | Tests for `verify_match_decision`, `verify_transfers_match_ground_truth` |
| `tests/moneybin/test_validation/test_expectations_transactions.py` | Tests for the three transaction-level verifiers |

### Modified files

| Path | Change |
|---|---|
| `src/moneybin/validation/result.py` | Add `ExpectationResult` (moved from `scenarios/expectations.py`) |
| `src/moneybin/validation/assertions/__init__.py` | Re-import from new modules |
| `src/moneybin/validation/assertions/infrastructure.py` | No structural change; verify it already takes `Database` (it does) |
| `src/moneybin/testing/scenarios/runner.py` | Use new registries; drop `_DATABASE_ASSERTION_FNS` (no longer needed once all assertions take `Database`) |
| `tests/moneybin/test_validation/test_assertions_business.py` | Rename to `test_assertions_domain.py`; update imports |
| `tests/moneybin/test_validation/test_assertions_distributional.py` | Rename to `test_assertions_distribution.py`; update imports |
| `tests/integration/test_scenario_runner.py` | Update assertion-fn names if any are referenced literally |
| `docs/specs/testing-scenario-comprehensive.md` | Amend "Files to Create" and R6 — record actual layout decision and stable-contract commitment |

### Deleted files

| Path | Reason |
|---|---|
| `src/moneybin/validation/assertions/relational.py` | Split into `completeness.py`, `uniqueness.py`, `integrity.py` |
| `src/moneybin/validation/assertions/business.py` | Renamed to `domain.py` |
| `src/moneybin/validation/assertions/distributional.py` | Renamed to `distribution.py` |
| `src/moneybin/testing/scenarios/expectations.py` | Verifiers moved to `validation/expectations/`; spec parsing moved to `_expectation_registry.py` |
| `tests/moneybin/test_validation/test_assertions_relational.py` | Split into three files |
| `tests/moneybin/test_validation/test_assertions_business.py` | Renamed |
| `tests/moneybin/test_validation/test_assertions_distributional.py` | Renamed |

---

## Sequencing Strategy

This is a multi-stage refactor with a regression test suite as the safety net (the existing tests cover assertions, expectations, the runner, and one integration scenario). Each task either:
- Adds new code (covered by new tests written first), or
- Renames/restructures existing code (covered by existing tests that must stay green throughout).

We commit after every task. If any task breaks green, fix forward — never bypass `make check test`.

Stages:
- **Stage A (Tasks 1–4):** Plumb new exports and helpers without changing any function signature. Safe foundation.
- **Stage B (Tasks 5–9):** Reorganize `assertions/` modules. Pure mechanical moves + import fixes. Tests stay green.
- **Stage C (Tasks 10–12):** Switch first-arg from `DuckDBPyConnection` to `Database` across all assertions. Update tests. Drop `_DATABASE_ASSERTION_FNS`.
- **Stage D (Tasks 13–17):** Build `validation/expectations/` package with library predicates. New tests; existing tests still green via the legacy module.
- **Stage E (Tasks 18–20):** Wire explicit registries. Switch runner to registries. Delete legacy `expectations.py`.
- **Stage F (Task 21):** Amend the spec to lock the layout decisions.

---

## Stage A — Foundation

### Task 1: Add `ExpectationResult` to `validation/result.py`

**Files:**
- Modify: `src/moneybin/validation/result.py`
- Test: `tests/moneybin/test_validation/test_result.py`

- [ ] **Step 1: Write the failing test**

```python
# Append to tests/moneybin/test_validation/test_result.py
def test_expectation_result_has_expected_shape():
    from moneybin.validation.result import ExpectationResult
    r = ExpectationResult(name="x", kind="match_decision", passed=True, details={"a": 1})
    assert r.name == "x"
    assert r.kind == "match_decision"
    assert r.passed is True
    assert r.details == {"a": 1}


def test_expectation_result_default_details_empty():
    from moneybin.validation.result import ExpectationResult
    r = ExpectationResult(name="x", kind="k", passed=False)
    assert r.details == {}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/moneybin/test_validation/test_result.py -v -n0`
Expected: FAIL with `ImportError: cannot import name 'ExpectationResult'`.

- [ ] **Step 3: Add `ExpectationResult` to `result.py`**

```python
# Append to src/moneybin/validation/result.py
@dataclass(frozen=True, slots=True)
class ExpectationResult:
    """Outcome of verifying a single per-record expectation against the database."""

    name: str
    kind: str
    passed: bool
    details: dict[str, Any] = field(default_factory=dict)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/moneybin/test_validation/test_result.py -v -n0`
Expected: PASS, all four tests (two new + two existing).

- [ ] **Step 5: Commit**

```bash
git add src/moneybin/validation/result.py tests/moneybin/test_validation/test_result.py
git commit -m "Add ExpectationResult to validation result types"
```

---

### Task 2: Top-level `validation/__init__.py` re-exports the three Result types

**Files:**
- Modify: `src/moneybin/validation/__init__.py`
- Test: `tests/moneybin/test_validation/test_result.py`

- [ ] **Step 1: Write the failing test**

```python
# Append to tests/moneybin/test_validation/test_result.py
def test_result_types_importable_from_package_root():
    # Stable contract: data-reconciliation imports the three Result types here.
    from moneybin.validation import AssertionResult, EvaluationResult, ExpectationResult
    assert AssertionResult.__name__ == "AssertionResult"
    assert EvaluationResult.__name__ == "EvaluationResult"
    assert ExpectationResult.__name__ == "ExpectationResult"
```

- [ ] **Step 2: Run test, see it fail**

Run: `uv run pytest tests/moneybin/test_validation/test_result.py::test_result_types_importable_from_package_root -v -n0`
Expected: FAIL with `ImportError`.

- [ ] **Step 3: Update `validation/__init__.py`**

```python
"""Validation primitives reusable across synthetic scenario runs and live data verification.

Public stable contract (consumed by data-reconciliation):

- ``moneybin.validation.{AssertionResult, EvaluationResult, ExpectationResult}``
- ``moneybin.validation.assertions.{schema, completeness, uniqueness, integrity, domain, distribution, infrastructure}``
- ``moneybin.validation.expectations.{matching, transactions}``
- ``moneybin.validation.evaluations.{categorization, matching}``

Stability rules: additive kwargs OK; rename/remove requires deprecation alias for one
release. ``details``/``breakdown`` dicts are per-function, not cross-function contract.
"""

from moneybin.validation.result import (
    AssertionResult,
    EvaluationResult,
    ExpectationResult,
)

__all__ = ["AssertionResult", "EvaluationResult", "ExpectationResult"]
```

- [ ] **Step 4: Run tests**

Run: `uv run pytest tests/moneybin/test_validation/test_result.py -v -n0`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/moneybin/validation/__init__.py
git commit -m "Re-export Result types at validation package root"
```

---

### Task 3: Extract `_quote_ident` to `_helpers.py`

**Files:**
- Create: `src/moneybin/validation/assertions/_helpers.py`
- Modify: `src/moneybin/validation/assertions/relational.py`, `business.py`, `distributional.py`, `schema.py`

- [ ] **Step 1: Create `_helpers.py`**

```python
"""Internal helpers shared across assertion modules."""

from __future__ import annotations

from sqlglot import exp


def quote_ident(ident: str) -> str:
    """Quote a dotted identifier via sqlglot, per .claude/rules/security.md."""
    return ".".join(
        exp.to_identifier(seg, quoted=True).sql("duckdb") for seg in ident.split(".")
    )
```

(Public name — drop the underscore. The leading-`_` was only because callers reached across module boundaries; now it's a proper internal helper used by sibling modules under the same package.)

- [ ] **Step 2: Update `relational.py` to import from `_helpers`**

In `src/moneybin/validation/assertions/relational.py`, replace the local `_quote_ident` function with:
```python
from moneybin.validation.assertions._helpers import quote_ident as _quote_ident
```
(Keep the `_quote_ident` alias temporarily so `business.py`, `distributional.py`, `schema.py` continue to work via the existing cross-import.)

- [ ] **Step 3: Update `business.py`, `distributional.py`, `schema.py`**

In each, replace:
```python
from moneybin.validation.assertions.relational import (
    _quote_ident,  # pyright: ignore[reportPrivateUsage]
)
```
with:
```python
from moneybin.validation.assertions._helpers import quote_ident as _quote_ident
```

- [ ] **Step 4: Run all validation tests**

Run: `uv run pytest tests/moneybin/test_validation/ -v -n0`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/moneybin/validation/assertions/
git commit -m "Extract quote_ident helper to validation.assertions._helpers"
```

---

### Task 4: Verify pre-existing test baseline before reorganizing

**Files:** None modified.

- [ ] **Step 1: Run the full unit test suite**

Run: `make test`
Expected: PASS. Capture the count to compare against later.

- [ ] **Step 2: Run scenario integration test**

Run: `uv run pytest tests/integration/test_scenario_runner.py -v -n0`
Expected: PASS.

If either fails, **stop**. Investigate and fix before continuing — Stage B onwards assumes a green baseline.

---

## Stage B — Reorganize `assertions/` modules

### Task 5: Split `relational.py` into `completeness.py`, `uniqueness.py`, `integrity.py`

**Files:**
- Create: `src/moneybin/validation/assertions/completeness.py`
- Create: `src/moneybin/validation/assertions/uniqueness.py`
- Rename: `src/moneybin/validation/assertions/relational.py` → `integrity.py`
- Modify: `src/moneybin/validation/assertions/__init__.py`, `business.py`, `distributional.py`, `schema.py`

- [ ] **Step 1: Create `completeness.py` with `assert_no_nulls`**

```python
"""Completeness assertions — required values must be populated."""

from __future__ import annotations

from duckdb import DuckDBPyConnection

from moneybin.validation.assertions._helpers import quote_ident
from moneybin.validation.result import AssertionResult


def assert_no_nulls(
    conn: DuckDBPyConnection, *, table: str, columns: list[str]
) -> AssertionResult:
    """Assert no null values exist in the given columns."""
    if not columns:
        raise ValueError("columns must be non-empty")
    t = quote_ident(table)
    per_col: dict[str, int] = {}
    for col in columns:
        cq = quote_ident(col)
        null_sql = f"SELECT COUNT(*) FROM {t} WHERE {cq} IS NULL"  # noqa: S608  # identifiers validated by quote_ident
        per_col[col] = int(conn.execute(null_sql).fetchone()[0])  # type: ignore[index]
    total = sum(per_col.values())
    return AssertionResult(
        name="no_nulls",
        passed=total == 0,
        details={"null_counts": per_col, "total": total},
    )
```

(First-arg type stays `DuckDBPyConnection` for now; Stage C switches it.)

- [ ] **Step 2: Create `uniqueness.py` with `assert_no_duplicates`**

```python
"""Uniqueness assertions — natural keys must not repeat."""

from __future__ import annotations

from duckdb import DuckDBPyConnection

from moneybin.validation.assertions._helpers import quote_ident
from moneybin.validation.result import AssertionResult


def assert_no_duplicates(
    conn: DuckDBPyConnection, *, table: str, columns: list[str]
) -> AssertionResult:
    """Assert no duplicate rows exist across the given column set."""
    if not columns:
        raise ValueError("columns must be non-empty")
    t = quote_ident(table)
    cols = ", ".join(quote_ident(c) for c in columns)
    dup_sql = f"SELECT COUNT(*) FROM (SELECT {cols} FROM {t} GROUP BY {cols} HAVING COUNT(*) > 1)"  # noqa: S608  # identifiers validated by quote_ident
    dup_groups = int(conn.execute(dup_sql).fetchone()[0])  # type: ignore[index]
    return AssertionResult(
        name="no_duplicates",
        passed=dup_groups == 0,
        details={"duplicate_groups": dup_groups, "columns": columns},
    )
```

- [ ] **Step 3: Rename `relational.py` → `integrity.py`**

Run: `git mv src/moneybin/validation/assertions/relational.py src/moneybin/validation/assertions/integrity.py`

Then edit `integrity.py`:
- Update module docstring to `"""Referential-integrity assertions — every child row references a valid parent."""`
- Remove the `assert_no_nulls` function (now in `completeness.py`)
- Remove the `assert_no_duplicates` function (now in `uniqueness.py`)
- Remove the `_quote_ident` alias line (no longer needed; sibling modules now import directly from `_helpers`)
- Keep only `assert_valid_foreign_keys` and `assert_no_orphans`
- Replace `_quote_ident` references with `quote_ident` (imported from `_helpers`)

- [ ] **Step 4: Update `business.py`, `distributional.py`, `schema.py` import lines**

These files imported `_quote_ident` from `relational`. They were updated to use `_helpers` in Task 3 — verify each one no longer references `relational`. Grep:

```bash
grep -n "from moneybin.validation.assertions.relational" src/moneybin/
```
Expected: no output. If any remain, fix them to use `_helpers`.

- [ ] **Step 5: Update `assertions/__init__.py`**

```python
"""Assertion primitives — every function returns AssertionResult, never raises on data failure."""

from moneybin.validation.assertions.business import (
    assert_balanced_transfers,
    assert_date_continuity,
    assert_sign_convention,
)
from moneybin.validation.assertions.completeness import assert_no_nulls
from moneybin.validation.assertions.distributional import (
    assert_distribution_within_bounds,
    assert_unique_value_count,
)
from moneybin.validation.assertions.infrastructure import (
    assert_migrations_at_head,
    assert_min_rows,
    assert_no_unencrypted_db_files,
    assert_sqlmesh_catalog_matches,
)
from moneybin.validation.assertions.integrity import (
    assert_no_orphans,
    assert_valid_foreign_keys,
)
from moneybin.validation.assertions.schema import (
    assert_column_types,
    assert_columns_exist,
    assert_row_count_delta,
    assert_row_count_exact,
)
from moneybin.validation.assertions.uniqueness import assert_no_duplicates

__all__ = [
    "assert_balanced_transfers",
    "assert_column_types",
    "assert_columns_exist",
    "assert_date_continuity",
    "assert_distribution_within_bounds",
    "assert_migrations_at_head",
    "assert_min_rows",
    "assert_no_duplicates",
    "assert_no_nulls",
    "assert_no_orphans",
    "assert_no_unencrypted_db_files",
    "assert_row_count_delta",
    "assert_row_count_exact",
    "assert_sign_convention",
    "assert_sqlmesh_catalog_matches",
    "assert_unique_value_count",
    "assert_valid_foreign_keys",
]
```

(`business` and `distributional` get renamed in Tasks 7 and 8 — leaving them on the old name keeps this task atomic.)

- [ ] **Step 6: Run all validation tests**

The existing `test_assertions_relational.py` will fail because the module no longer has `assert_no_nulls`/`assert_no_duplicates`. That's expected — Task 6 splits the test file.

Run: `uv run pytest tests/moneybin/test_validation/ -v -n0`
Expected: failures in `test_assertions_relational.py` for `assert_no_nulls`/`assert_no_duplicates` imports. All other validation tests PASS.

- [ ] **Step 7: Commit**

```bash
git add src/moneybin/validation/assertions/
git commit -m "Split relational assertions into completeness, uniqueness, integrity"
```

---

### Task 6: Split `test_assertions_relational.py` into three files

**Files:**
- Create: `tests/moneybin/test_validation/test_assertions_completeness.py`
- Create: `tests/moneybin/test_validation/test_assertions_uniqueness.py`
- Rename: `tests/moneybin/test_validation/test_assertions_relational.py` → `test_assertions_integrity.py`

- [ ] **Step 1: Read the existing test file**

Run: `cat tests/moneybin/test_validation/test_assertions_relational.py`
Note which test functions cover which assertions — they group cleanly by assertion name.

- [ ] **Step 2: Create `test_assertions_completeness.py`**

Move every `test_*` function that exercises `assert_no_nulls` into this new file. At the top:
```python
from moneybin.validation.assertions.completeness import assert_no_nulls
```
Plus any shared fixtures the moved tests need (copy them — DRY across test files is OK if it keeps each test file self-contained).

- [ ] **Step 3: Create `test_assertions_uniqueness.py`**

Same procedure for `assert_no_duplicates` tests, importing from `moneybin.validation.assertions.uniqueness`.

- [ ] **Step 4: Rename `test_assertions_relational.py` → `test_assertions_integrity.py`**

Run: `git mv tests/moneybin/test_validation/test_assertions_relational.py tests/moneybin/test_validation/test_assertions_integrity.py`

Edit:
- Remove the `assert_no_nulls` and `assert_no_duplicates` test functions (now in their respective new files).
- Update the import line: `from moneybin.validation.assertions.integrity import assert_no_orphans, assert_valid_foreign_keys`.

- [ ] **Step 5: Run the three test files**

Run:
```bash
uv run pytest tests/moneybin/test_validation/test_assertions_completeness.py tests/moneybin/test_validation/test_assertions_uniqueness.py tests/moneybin/test_validation/test_assertions_integrity.py -v -n0
```
Expected: all PASS, total count equals the original `test_assertions_relational.py` count.

- [ ] **Step 6: Commit**

```bash
git add tests/moneybin/test_validation/
git commit -m "Split relational assertion tests into completeness, uniqueness, integrity"
```

---

### Task 7: Rename `business.py` → `domain.py`

**Files:**
- Rename: `src/moneybin/validation/assertions/business.py` → `domain.py`
- Rename: `tests/moneybin/test_validation/test_assertions_business.py` → `test_assertions_domain.py`
- Modify: `src/moneybin/validation/assertions/__init__.py`

- [ ] **Step 1: Rename the source file**

Run:
```bash
git mv src/moneybin/validation/assertions/business.py src/moneybin/validation/assertions/domain.py
git mv tests/moneybin/test_validation/test_assertions_business.py tests/moneybin/test_validation/test_assertions_domain.py
```

- [ ] **Step 2: Update the test file's import**

In `test_assertions_domain.py`, change:
```python
from moneybin.validation.assertions.business import (
```
to:
```python
from moneybin.validation.assertions.domain import (
```

- [ ] **Step 3: Update `assertions/__init__.py`**

Change:
```python
from moneybin.validation.assertions.business import (
```
to:
```python
from moneybin.validation.assertions.domain import (
```

- [ ] **Step 4: Update `domain.py` module docstring**

Change the module docstring to `"""Domain (business-rule) assertions for the canonical core schema."""`.

- [ ] **Step 5: Run all validation tests**

Run: `uv run pytest tests/moneybin/test_validation/ -v -n0`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/moneybin/validation/assertions/ tests/moneybin/test_validation/
git commit -m "Rename business assertions module to domain"
```

---

### Task 8: Rename `distributional.py` → `distribution.py`

**Files:**
- Rename: `src/moneybin/validation/assertions/distributional.py` → `distribution.py`
- Rename: `tests/moneybin/test_validation/test_assertions_distributional.py` → `test_assertions_distribution.py`
- Modify: `src/moneybin/validation/assertions/__init__.py`

- [ ] **Step 1: Rename source and test files**

```bash
git mv src/moneybin/validation/assertions/distributional.py src/moneybin/validation/assertions/distribution.py
git mv tests/moneybin/test_validation/test_assertions_distributional.py tests/moneybin/test_validation/test_assertions_distribution.py
```

- [ ] **Step 2: Update import in test file**

Change `from moneybin.validation.assertions.distributional` → `from moneybin.validation.assertions.distribution`.

- [ ] **Step 3: Update `assertions/__init__.py`**

Change `from moneybin.validation.assertions.distributional` → `from moneybin.validation.assertions.distribution`.

- [ ] **Step 4: Run all validation tests**

Run: `uv run pytest tests/moneybin/test_validation/ -v -n0`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/moneybin/validation/assertions/ tests/moneybin/test_validation/
git commit -m "Rename distributional assertions module to distribution"
```

---

### Task 9: Run integration scenario test as a regression checkpoint

**Files:** None modified.

- [ ] **Step 1: Run the scenario integration test**

Run: `uv run pytest tests/integration/test_scenario_runner.py -v -n0`
Expected: PASS.

- [ ] **Step 2: Run full type-check on touched modules**

Run: `uv run pyright src/moneybin/validation/ src/moneybin/testing/scenarios/`
Expected: no new errors. Pre-existing errors (if any) are documented in the codebase — the count should match Task 4's baseline.

If either step fails, fix before continuing.

---

## Stage C — Standardize first-arg to `Database`

The runner's `_DATABASE_ASSERTION_FNS` set in `runner.py:41-46` exists because some assertions take `Database`, others take `DuckDBPyConnection`. Per `.claude/rules/database.md` ("Use the `Database` class"), every assertion should take `Database`. After this stage, all `assert_*` functions take `Database` as the first positional arg. The runner can drop its dispatch table entirely.

The conversion is mechanical: at each call site, swap `conn.execute(...)` → `db.execute(...)`. `Database.execute` returns the same `DuckDBPyResult` shape.

Affected modules: `completeness`, `uniqueness`, `integrity`, `domain`, `distribution`, `schema` — six modules, ~15 functions.
Unaffected: `infrastructure` (already takes `Database`).

### Task 10: Switch `completeness`, `uniqueness`, `integrity` to take `Database`

**Files:**
- Modify: `src/moneybin/validation/assertions/completeness.py`, `uniqueness.py`, `integrity.py`
- Modify: `tests/moneybin/test_validation/test_assertions_completeness.py`, `test_assertions_uniqueness.py`, `test_assertions_integrity.py`

- [ ] **Step 1: Update `completeness.py`**

Change the function signature and call site:
```python
# Before
from duckdb import DuckDBPyConnection
def assert_no_nulls(conn: DuckDBPyConnection, *, table: str, columns: list[str]) -> AssertionResult:
    ...
    per_col[col] = int(conn.execute(null_sql).fetchone()[0])

# After
from moneybin.database import Database
def assert_no_nulls(db: Database, *, table: str, columns: list[str]) -> AssertionResult:
    ...
    per_col[col] = int(db.execute(null_sql).fetchone()[0])
```

Remove the `from duckdb import DuckDBPyConnection` line (no longer needed).

- [ ] **Step 2: Update `uniqueness.py`** — same pattern.

- [ ] **Step 3: Update `integrity.py`** — same pattern for both `assert_valid_foreign_keys` and `assert_no_orphans`.

- [ ] **Step 4: Update the three test files**

Find every callsite that passes `db.conn` or constructs a raw `DuckDBPyConnection` and pass `db` instead. Search:
```bash
grep -n "assert_no_nulls\|assert_no_duplicates\|assert_valid_foreign_keys\|assert_no_orphans" tests/moneybin/test_validation/test_assertions_completeness.py tests/moneybin/test_validation/test_assertions_uniqueness.py tests/moneybin/test_validation/test_assertions_integrity.py
```
For each match, update the call from `assert_X(conn, ...)` or `assert_X(db.conn, ...)` to `assert_X(db, ...)`.

If a test was constructing a raw `duckdb.connect()` connection (not a `Database`), wrap it in a `Database` test fixture. Use `mock_secret_store` from the project's root `conftest.py` and `no_auto_upgrade=True`:
```python
from moneybin.database import Database
db = Database(tmp_path / "test.duckdb", secret_store=mock_secret_store, no_auto_upgrade=True)
```

- [ ] **Step 5: Run the three test files**

Run: `uv run pytest tests/moneybin/test_validation/test_assertions_completeness.py tests/moneybin/test_validation/test_assertions_uniqueness.py tests/moneybin/test_validation/test_assertions_integrity.py -v -n0`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/moneybin/validation/assertions/ tests/moneybin/test_validation/
git commit -m "Switch completeness/uniqueness/integrity assertions to Database first-arg"
```

---

### Task 11: Switch `domain`, `distribution`, `schema` to take `Database`

**Files:**
- Modify: `src/moneybin/validation/assertions/domain.py`, `distribution.py`, `schema.py`
- Modify: `tests/moneybin/test_validation/test_assertions_domain.py`, `test_assertions_distribution.py`, `test_assertions_schema.py`

- [ ] **Step 1: Update each source module** — same `conn → db` mechanical conversion as Task 10. Functions affected:
  - `domain.py`: `assert_sign_convention`, `assert_balanced_transfers`, `assert_date_continuity`
  - `distribution.py`: `assert_distribution_within_bounds`, `assert_unique_value_count`
  - `schema.py`: `assert_columns_exist`, `assert_column_types`, `assert_row_count_exact`, `assert_row_count_delta`

For `schema.py`, also update the helpers `_columns_with_types` and `_row_count` to take `Database` instead of `DuckDBPyConnection`.

- [ ] **Step 2: Update each test file** — same conversion in test call sites.

- [ ] **Step 3: Run validation tests**

Run: `uv run pytest tests/moneybin/test_validation/ -v -n0`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add src/moneybin/validation/assertions/ tests/moneybin/test_validation/
git commit -m "Switch domain/distribution/schema assertions to Database first-arg"
```

---

### Task 12: Drop `_DATABASE_ASSERTION_FNS` from runner

**Files:**
- Modify: `src/moneybin/testing/scenarios/runner.py`

- [ ] **Step 1: Update `runner.py`**

Remove lines 41–46 (the `_DATABASE_ASSERTION_FNS` set).

In `_run_assertion` (around line 233), change:
```python
result = (
    fn(db, **args)
    if spec.fn in _DATABASE_ASSERTION_FNS
    else fn(db.conn, **args)
)
```
to:
```python
result = fn(db, **args)
```

- [ ] **Step 2: Run scenario integration test**

Run: `uv run pytest tests/integration/test_scenario_runner.py -v -n0`
Expected: PASS.

- [ ] **Step 3: Run full unit suite**

Run: `make test`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add src/moneybin/testing/scenarios/runner.py
git commit -m "Drop _DATABASE_ASSERTION_FNS dispatch — every assertion takes Database"
```

---

## Stage D — Decouple expectations

### Task 13: Create `validation/expectations/` package skeleton + `SourceTransactionRef`

**Files:**
- Create: `src/moneybin/validation/expectations/__init__.py`
- Create: `src/moneybin/validation/expectations/_types.py`
- Test: `tests/moneybin/test_validation/test_expectations_types.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/moneybin/test_validation/test_expectations_types.py
def test_source_transaction_ref_is_frozen_dataclass():
    from moneybin.validation.expectations import SourceTransactionRef
    ref = SourceTransactionRef(source_transaction_id="csv_abc123", source_type="csv")
    assert ref.source_transaction_id == "csv_abc123"
    assert ref.source_type == "csv"
    # Frozen — must reject mutation
    import dataclasses
    with __import__("pytest").raises(dataclasses.FrozenInstanceError):
        ref.source_type = "ofx"  # type: ignore[misc]
```

- [ ] **Step 2: Run test, see it fail**

Run: `uv run pytest tests/moneybin/test_validation/test_expectations_types.py -v -n0`
Expected: FAIL with `ImportError`.

- [ ] **Step 3: Create `_types.py`**

```python
"""Typed inputs shared across expectation predicates."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


@dataclass(frozen=True, slots=True)
class SourceTransactionRef:
    """Reference to a single source-system transaction by (id, source_type)."""

    source_transaction_id: str
    # Mirrors loader.FixtureSpec.source_type — extend together when a new
    # source type is added.
    source_type: Literal["csv", "ofx", "pdf"]
```

- [ ] **Step 4: Create `__init__.py`**

```python
"""Expectation primitives — per-record predicates returning ExpectationResult."""

from moneybin.validation.expectations._types import SourceTransactionRef

__all__ = ["SourceTransactionRef"]
```

- [ ] **Step 5: Run test**

Run: `uv run pytest tests/moneybin/test_validation/test_expectations_types.py -v -n0`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/moneybin/validation/expectations/ tests/moneybin/test_validation/test_expectations_types.py
git commit -m "Add expectations package skeleton with SourceTransactionRef"
```

---

### Task 14: Create `validation/expectations/matching.py`

**Files:**
- Create: `src/moneybin/validation/expectations/matching.py`
- Modify: `src/moneybin/validation/expectations/__init__.py`
- Test: `tests/moneybin/test_validation/test_expectations_matching.py`

These are predicate-level decoupled versions of `_verify_match_decision` and `_verify_transfers_match_ground_truth` from `src/moneybin/testing/scenarios/expectations.py`. The bodies translate near-1:1; the change is the input shape (typed kwargs instead of `ExpectationSpec`) and the first arg (`Database`).

- [ ] **Step 1: Write failing tests**

```python
# tests/moneybin/test_validation/test_expectations_matching.py
"""Tests for validation.expectations.matching predicates."""

from __future__ import annotations

import pytest
from moneybin.database import Database
from moneybin.validation.expectations import SourceTransactionRef
from moneybin.validation.expectations.matching import (
    verify_match_decision,
    verify_transfers_match_ground_truth,
)
from moneybin.validation.result import ExpectationResult


def test_verify_match_decision_returns_expectation_result(
    matched_dedup_db: Database,
):
    """All listed sources collapse to one gold row → passed=True."""
    result = verify_match_decision(
        matched_dedup_db,
        transactions=[
            SourceTransactionRef(source_transaction_id="csv_a", source_type="csv"),
            SourceTransactionRef(source_transaction_id="ofx_b", source_type="ofx"),
        ],
        expected="matched",
        expected_match_type="dedup",
        expected_confidence_min=0.5,
    )
    assert isinstance(result, ExpectationResult)
    assert result.kind == "match_decision"
    assert result.passed is True


def test_verify_match_decision_not_matched_passes_when_distinct(
    not_matched_db: Database,
):
    result = verify_match_decision(
        not_matched_db,
        transactions=[
            SourceTransactionRef(source_transaction_id="csv_x", source_type="csv"),
            SourceTransactionRef(source_transaction_id="ofx_y", source_type="ofx"),
        ],
        expected="not_matched",
    )
    assert result.passed is True


def test_verify_transfers_match_ground_truth_returns_expectation_result(
    transfer_db: Database,
):
    result = verify_transfers_match_ground_truth(transfer_db)
    assert isinstance(result, ExpectationResult)
    assert result.kind == "transfers_match_ground_truth"
```

The fixtures `matched_dedup_db`, `not_matched_db`, `transfer_db` belong in a local `conftest.py` — model them on whatever the existing scenario integration tests use. **If the existing test suite doesn't already have small-fixture builders for these states, create the fixtures by hand-inserting rows into `meta.fct_transaction_provenance`, `core.fct_transactions`, `app.match_decisions`, and `synthetic.ground_truth` directly.** Keep them <50 lines each.

- [ ] **Step 2: Run test, see it fail**

Run: `uv run pytest tests/moneybin/test_validation/test_expectations_matching.py -v -n0`
Expected: FAIL with ImportError on `verify_match_decision` or `verify_transfers_match_ground_truth`.

- [ ] **Step 3: Create `matching.py`**

Port the bodies of `_verify_match_decision` and `_verify_transfers_match_ground_truth` from `src/moneybin/testing/scenarios/expectations.py`. Concrete signature shapes:

```python
"""Per-record expectations about matching outcomes."""

from __future__ import annotations

from typing import Any, Literal

from moneybin.database import Database
from moneybin.tables import (
    FCT_TRANSACTION_PROVENANCE,
    FCT_TRANSACTIONS,
    GROUND_TRUTH,
    INT_TRANSACTIONS_MATCHED,
    MATCH_DECISIONS,
)
from moneybin.validation.expectations._types import SourceTransactionRef
from moneybin.validation.result import ExpectationResult


def verify_match_decision(
    db: Database,
    *,
    transactions: list[SourceTransactionRef],
    expected: Literal["matched", "not_matched"] = "matched",
    expected_match_type: Literal["dedup", "transfer"] | None = None,
    expected_confidence_min: float = 0.0,
    description: str = "",
) -> ExpectationResult:
    """Verify that listed source txns resolve to one (or distinct) gold rows.

    See ``docs/specs/testing-scenario-comprehensive.md`` §R1 Tier 2 for the
    matched-branch semantics (coverage, collapse, confidence, match_type).
    """
    # Body: port from _verify_match_decision in
    # src/moneybin/testing/scenarios/expectations.py.
    # - Replace `body = spec.model_dump()` followed by `body["transactions"]`
    #   etc. with direct kwarg access.
    # - Replace `db.execute` calls — same shape.
    # - Replace `spec.description or "match_decision"` with
    #   `description or "match_decision"`.
    ...


def verify_transfers_match_ground_truth(
    db: Database, *, description: str = ""
) -> ExpectationResult:
    """Assert every labeled transfer pair lands as one ``transfer_pair_id``.

    See ``_verify_transfers_match_ground_truth`` in the legacy module for
    detailed semantics.
    """
    # Body: port verbatim, dropping the spec parameter.
    ...
```

The legacy module is the source of truth for the SQL. Copy each block exactly; only the inputs and result construction change.

- [ ] **Step 4: Update `expectations/__init__.py`**

```python
"""Expectation primitives — per-record predicates returning ExpectationResult."""

from moneybin.validation.expectations._types import SourceTransactionRef
from moneybin.validation.expectations.matching import (
    verify_match_decision,
    verify_transfers_match_ground_truth,
)

__all__ = [
    "SourceTransactionRef",
    "verify_match_decision",
    "verify_transfers_match_ground_truth",
]
```

- [ ] **Step 5: Run tests**

Run: `uv run pytest tests/moneybin/test_validation/test_expectations_matching.py -v -n0`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/moneybin/validation/expectations/ tests/moneybin/test_validation/test_expectations_matching.py
git commit -m "Decouple matching expectation predicates from YAML loader"
```

---

### Task 15: Create `validation/expectations/transactions.py`

**Files:**
- Create: `src/moneybin/validation/expectations/transactions.py`
- Modify: `src/moneybin/validation/expectations/__init__.py`
- Test: `tests/moneybin/test_validation/test_expectations_transactions.py`

Three predicates: `verify_gold_record_count`, `verify_category_for_transaction`, `verify_provenance_for_transaction`. Same procedure as Task 14 — port from `_verify_*` legacy module, switch to typed kwargs.

- [ ] **Step 1: Write failing tests**

```python
# tests/moneybin/test_validation/test_expectations_transactions.py
from __future__ import annotations

from moneybin.database import Database
from moneybin.validation.expectations import SourceTransactionRef
from moneybin.validation.expectations.transactions import (
    verify_category_for_transaction,
    verify_gold_record_count,
    verify_provenance_for_transaction,
)
from moneybin.validation.result import ExpectationResult


def test_verify_gold_record_count_total(populated_db: Database):
    result = verify_gold_record_count(populated_db, expected_collapsed_count=10)
    assert isinstance(result, ExpectationResult)
    assert result.kind == "gold_record_count"


def test_verify_gold_record_count_scoped_by_fixture_ids(populated_db: Database):
    result = verify_gold_record_count(
        populated_db,
        expected_collapsed_count=2,
        fixture_source_ids=["csv_a", "ofx_b"],
    )
    assert result.kind == "gold_record_count"


def test_verify_category_for_transaction(categorized_db: Database):
    result = verify_category_for_transaction(
        categorized_db,
        transaction_id="txn_abc",
        expected_category="Groceries",
        expected_categorized_by="rule",
    )
    assert isinstance(result, ExpectationResult)
    assert result.kind == "category_for_transaction"


def test_verify_provenance_for_transaction(populated_db: Database):
    result = verify_provenance_for_transaction(
        populated_db,
        transaction_id="txn_abc",
        expected_sources=[
            SourceTransactionRef(source_transaction_id="csv_a", source_type="csv"),
        ],
    )
    assert result.kind == "provenance_for_transaction"
```

Fixtures: same approach as Task 14 — small hand-built `Database` instances in the test's `conftest.py`.

- [ ] **Step 2: Run, see it fail**

Run: `uv run pytest tests/moneybin/test_validation/test_expectations_transactions.py -v -n0`
Expected: FAIL on imports.

- [ ] **Step 3: Create `transactions.py`**

```python
"""Per-transaction expectations — categorization, provenance, collapse counts."""

from __future__ import annotations

from typing import Literal

from moneybin.database import Database
from moneybin.tables import FCT_TRANSACTION_PROVENANCE, FCT_TRANSACTIONS
from moneybin.validation.expectations._types import SourceTransactionRef
from moneybin.validation.result import ExpectationResult


def verify_gold_record_count(
    db: Database,
    *,
    expected_collapsed_count: int,
    fixture_source_ids: list[str] | None = None,
    description: str = "",
) -> ExpectationResult:
    """Verify gold record count, optionally scoped to fixture source IDs."""
    # Port from _verify_gold_record_count in legacy module.
    ...


def verify_category_for_transaction(
    db: Database,
    *,
    transaction_id: str,
    expected_category: str,
    expected_categorized_by: Literal["rule", "merchant", "ai", "user"] | None = None,
    description: str = "",
) -> ExpectationResult:
    """Verify a transaction's category (and optionally its categorizer source)."""
    # Port from _verify_category_for_transaction.
    ...


def verify_provenance_for_transaction(
    db: Database,
    *,
    transaction_id: str,
    expected_sources: list[SourceTransactionRef],
    description: str = "",
) -> ExpectationResult:
    """Verify the provenance source rows for a gold transaction match expected."""
    # Port from _verify_provenance_for_transaction. Convert
    # SourceTransactionRef list → list[tuple[str, str]] for the comparison.
    ...
```

For `expected_categorized_by`, check the actual values used in the DB against the loader/categorization-service code; if the set of values is open (not a `Literal`), drop the `Literal` annotation and accept `str | None`. **Read `src/moneybin/services/categorization_service.py` to confirm the categorizer-source vocabulary before locking the type.**

- [ ] **Step 4: Update `expectations/__init__.py`**

```python
from moneybin.validation.expectations._types import SourceTransactionRef
from moneybin.validation.expectations.matching import (
    verify_match_decision,
    verify_transfers_match_ground_truth,
)
from moneybin.validation.expectations.transactions import (
    verify_category_for_transaction,
    verify_gold_record_count,
    verify_provenance_for_transaction,
)

__all__ = [
    "SourceTransactionRef",
    "verify_category_for_transaction",
    "verify_gold_record_count",
    "verify_match_decision",
    "verify_provenance_for_transaction",
    "verify_transfers_match_ground_truth",
]
```

- [ ] **Step 5: Run tests**

Run: `uv run pytest tests/moneybin/test_validation/test_expectations_transactions.py -v -n0`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/moneybin/validation/expectations/ tests/moneybin/test_validation/test_expectations_transactions.py
git commit -m "Decouple transaction-level expectation predicates from YAML loader"
```

---

### Task 16: Verify legacy `expectations.py` still works (regression checkpoint)

**Files:** None modified.

The library predicates exist but the runner still uses the legacy module. Both should work in parallel right now.

- [ ] **Step 1: Run scenario integration test**

Run: `uv run pytest tests/integration/test_scenario_runner.py -v -n0`
Expected: PASS — the runner's existing path through `verify_expectations` in the legacy module still works.

- [ ] **Step 2: Run full unit suite**

Run: `make test`
Expected: PASS.

If either fails, fix before moving to Stage E. Stage E removes the legacy module, so it must be a clean swap.

---

## Stage E — Wire explicit registries; delete legacy module

### Task 17: Create `_assertion_registry.py`

**Files:**
- Create: `src/moneybin/testing/scenarios/_assertion_registry.py`
- Test: `tests/moneybin/test_testing/test_scenarios_assertion_registry.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/moneybin/test_testing/test_scenarios_assertion_registry.py
from moneybin.testing.scenarios._assertion_registry import ASSERTION_REGISTRY


def test_registry_includes_all_yaml_callable_assertions():
    expected = {
        "assert_balanced_transfers",
        "assert_column_types",
        "assert_columns_exist",
        "assert_date_continuity",
        "assert_distribution_within_bounds",
        "assert_migrations_at_head",
        "assert_min_rows",
        "assert_no_duplicates",
        "assert_no_nulls",
        "assert_no_orphans",
        "assert_no_unencrypted_db_files",
        "assert_row_count_delta",
        "assert_row_count_exact",
        "assert_sign_convention",
        "assert_sqlmesh_catalog_matches",
        "assert_unique_value_count",
        "assert_valid_foreign_keys",
    }
    assert set(ASSERTION_REGISTRY) == expected


def test_registry_values_are_callable():
    for name, fn in ASSERTION_REGISTRY.items():
        assert callable(fn), f"{name} is not callable"
```

- [ ] **Step 2: Run, see it fail**

Run: `uv run pytest tests/moneybin/test_testing/test_scenarios_assertion_registry.py -v -n0`
Expected: FAIL with ImportError.

- [ ] **Step 3: Create the registry**

```python
"""Explicit YAML-callable assertion registry.

Every entry is a contract: its name is part of scenario YAML's surface area.
Adding a new YAML-callable assertion requires explicitly registering it here —
this prevents accidental exposure of internal helpers that happen to start
with ``assert_``.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from moneybin.database import Database
from moneybin.validation.assertions.completeness import assert_no_nulls
from moneybin.validation.assertions.distribution import (
    assert_distribution_within_bounds,
    assert_unique_value_count,
)
from moneybin.validation.assertions.domain import (
    assert_balanced_transfers,
    assert_date_continuity,
    assert_sign_convention,
)
from moneybin.validation.assertions.infrastructure import (
    assert_migrations_at_head,
    assert_min_rows,
    assert_no_unencrypted_db_files,
    assert_sqlmesh_catalog_matches,
)
from moneybin.validation.assertions.integrity import (
    assert_no_orphans,
    assert_valid_foreign_keys,
)
from moneybin.validation.assertions.schema import (
    assert_column_types,
    assert_columns_exist,
    assert_row_count_delta,
    assert_row_count_exact,
)
from moneybin.validation.assertions.uniqueness import assert_no_duplicates
from moneybin.validation.result import AssertionResult

# AssertionFn signature: ``(db: Database, **kwargs) -> AssertionResult``.
AssertionFn = Callable[..., AssertionResult]

ASSERTION_REGISTRY: dict[str, AssertionFn] = {
    "assert_balanced_transfers": assert_balanced_transfers,
    "assert_column_types": assert_column_types,
    "assert_columns_exist": assert_columns_exist,
    "assert_date_continuity": assert_date_continuity,
    "assert_distribution_within_bounds": assert_distribution_within_bounds,
    "assert_migrations_at_head": assert_migrations_at_head,
    "assert_min_rows": assert_min_rows,
    "assert_no_duplicates": assert_no_duplicates,
    "assert_no_nulls": assert_no_nulls,
    "assert_no_orphans": assert_no_orphans,
    "assert_no_unencrypted_db_files": assert_no_unencrypted_db_files,
    "assert_row_count_delta": assert_row_count_delta,
    "assert_row_count_exact": assert_row_count_exact,
    "assert_sign_convention": assert_sign_convention,
    "assert_sqlmesh_catalog_matches": assert_sqlmesh_catalog_matches,
    "assert_unique_value_count": assert_unique_value_count,
    "assert_valid_foreign_keys": assert_valid_foreign_keys,
}


def resolve_assertion(name: str) -> AssertionFn:
    """Return the callable registered under ``name`` or raise ``KeyError``."""
    if name not in ASSERTION_REGISTRY:
        raise KeyError(f"unknown assertion fn: {name!r}")
    return ASSERTION_REGISTRY[name]


__all__ = ["ASSERTION_REGISTRY", "AssertionFn", "resolve_assertion"]


# Silence pyright's unused-import false positive on Any (kept for forward use).
_ = Any
```

(Drop the `Any` line and import if pyright doesn't complain — included only as belt-and-suspenders.)

- [ ] **Step 4: Run test**

Run: `uv run pytest tests/moneybin/test_testing/test_scenarios_assertion_registry.py -v -n0`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/moneybin/testing/scenarios/_assertion_registry.py tests/moneybin/test_testing/test_scenarios_assertion_registry.py
git commit -m "Add explicit assertion registry for YAML-callable predicates"
```

---

### Task 18: Create `_expectation_registry.py` (adapter + registry combined)

**Files:**
- Create: `src/moneybin/testing/scenarios/_expectation_registry.py`
- Test: `tests/moneybin/test_testing/test_scenarios_expectation_registry.py`

This file holds the YAML→library adapter for each `ExpectationSpec.kind`, plus a small `verify_expectations` entry point that the runner calls.

- [ ] **Step 1: Write the failing test**

```python
# tests/moneybin/test_testing/test_scenarios_expectation_registry.py
from moneybin.testing.scenarios._expectation_registry import (
    EXPECTATION_REGISTRY,
    verify_expectations,
)


def test_registry_covers_every_expectation_kind():
    expected = {
        "match_decision",
        "gold_record_count",
        "category_for_transaction",
        "provenance_for_transaction",
        "transfers_match_ground_truth",
    }
    assert set(EXPECTATION_REGISTRY) == expected


def test_registry_values_are_callable():
    for kind, fn in EXPECTATION_REGISTRY.items():
        assert callable(fn), f"{kind} adapter is not callable"


def test_verify_expectations_on_empty_list():
    # Smoke test: empty input → empty output, no DB needed.
    assert verify_expectations(db=None, specs=[]) == []  # type: ignore[arg-type]
```

- [ ] **Step 2: Run, see fail**

Run: `uv run pytest tests/moneybin/test_testing/test_scenarios_expectation_registry.py -v -n0`
Expected: FAIL on import.

- [ ] **Step 3: Create the registry**

```python
"""YAML-driven adapter for expectation predicates.

The library predicates in ``moneybin.validation.expectations`` take typed
kwargs. Scenario YAML provides loosely-typed dicts via ``ExpectationSpec``.
This module is the **only** place that translates between the two.

Adding a new ``ExpectationSpec.kind``:
1. Add a Literal to ``loader.ExpectationSpec.kind``.
2. Implement the predicate in ``moneybin.validation.expectations``.
3. Register an adapter here.
"""

from __future__ import annotations

from collections.abc import Callable

from moneybin.database import Database
from moneybin.testing.scenarios.loader import ExpectationSpec
from moneybin.validation.expectations import (
    SourceTransactionRef,
    verify_category_for_transaction,
    verify_gold_record_count,
    verify_match_decision,
    verify_provenance_for_transaction,
    verify_transfers_match_ground_truth,
)
from moneybin.validation.result import ExpectationResult

ExpectationAdapter = Callable[[Database, ExpectationSpec], ExpectationResult]


def _adapt_match_decision(db: Database, spec: ExpectationSpec) -> ExpectationResult:
    body = spec.model_dump()
    return verify_match_decision(
        db,
        transactions=[SourceTransactionRef(**t) for t in body["transactions"]],
        expected=body.get("expected", "matched"),
        expected_match_type=body.get("expected_match_type"),
        expected_confidence_min=float(body.get("expected_confidence_min", 0.0)),
        description=spec.description,
    )


def _adapt_gold_record_count(db: Database, spec: ExpectationSpec) -> ExpectationResult:
    body = spec.model_dump()
    return verify_gold_record_count(
        db,
        expected_collapsed_count=int(body["expected_collapsed_count"]),
        fixture_source_ids=body.get("fixture_source_ids"),
        description=spec.description,
    )


def _adapt_category_for_transaction(
    db: Database, spec: ExpectationSpec
) -> ExpectationResult:
    body = spec.model_dump()
    return verify_category_for_transaction(
        db,
        transaction_id=body["transaction_id"],
        expected_category=body["expected_category"],
        expected_categorized_by=body.get("expected_categorized_by"),
        description=spec.description,
    )


def _adapt_provenance_for_transaction(
    db: Database, spec: ExpectationSpec
) -> ExpectationResult:
    body = spec.model_dump()
    return verify_provenance_for_transaction(
        db,
        transaction_id=body["transaction_id"],
        expected_sources=[
            SourceTransactionRef(**s) for s in body["expected_sources"]
        ],
        description=spec.description,
    )


def _adapt_transfers_match_ground_truth(
    db: Database, spec: ExpectationSpec
) -> ExpectationResult:
    return verify_transfers_match_ground_truth(db, description=spec.description)


EXPECTATION_REGISTRY: dict[str, ExpectationAdapter] = {
    "match_decision": _adapt_match_decision,
    "gold_record_count": _adapt_gold_record_count,
    "category_for_transaction": _adapt_category_for_transaction,
    "provenance_for_transaction": _adapt_provenance_for_transaction,
    "transfers_match_ground_truth": _adapt_transfers_match_ground_truth,
}


def verify_expectations(
    db: Database, specs: list[ExpectationSpec]
) -> list[ExpectationResult]:
    """Dispatch each spec through its registered adapter and return results."""
    return [EXPECTATION_REGISTRY[s.kind](db, s) for s in specs]


__all__ = ["EXPECTATION_REGISTRY", "ExpectationAdapter", "verify_expectations"]
```

- [ ] **Step 4: Run tests**

Run: `uv run pytest tests/moneybin/test_testing/test_scenarios_expectation_registry.py -v -n0`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/moneybin/testing/scenarios/_expectation_registry.py tests/moneybin/test_testing/test_scenarios_expectation_registry.py
git commit -m "Add expectation registry — YAML adapter to library predicates"
```

---

### Task 19: Switch runner to use the registries

**Files:**
- Modify: `src/moneybin/testing/scenarios/runner.py`

- [ ] **Step 1: Update imports in `runner.py`**

Replace:
```python
from moneybin.testing.scenarios.expectations import (
    ExpectationResult,
    verify_expectations,
)
```
with:
```python
from moneybin.testing.scenarios._expectation_registry import verify_expectations
from moneybin.validation.result import ExpectationResult
```

- [ ] **Step 2: Replace `_resolve_assertion`**

Find the `_resolve_assertion` helper (around line 279) and the `_resolve_evaluation` helper (around line 286).

Replace `_resolve_assertion`:
```python
from moneybin.testing.scenarios._assertion_registry import resolve_assertion as _resolve_assertion
```

(`_resolve_evaluation` stays as-is — evaluations aren't part of this phase. Add a TODO if you want, but don't change it.)

Then **delete** the standalone `_resolve_assertion` def block (the one that does `importlib.import_module(...)`).

- [ ] **Step 3: Run scenario integration test**

Run: `uv run pytest tests/integration/test_scenario_runner.py -v -n0`
Expected: PASS.

- [ ] **Step 4: Run full suite**

Run: `make test`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/moneybin/testing/scenarios/runner.py
git commit -m "Wire runner to assertion + expectation registries"
```

---

### Task 20: Delete legacy `expectations.py`

**Files:**
- Delete: `src/moneybin/testing/scenarios/expectations.py`

- [ ] **Step 1: Verify no remaining imports**

Run: `grep -rn "from moneybin.testing.scenarios.expectations" src/ tests/`
Expected: no output. (The runner now imports from `_expectation_registry` and `validation.result`.)

If any remain, fix them before deleting the file.

- [ ] **Step 2: Delete the file**

Run: `git rm src/moneybin/testing/scenarios/expectations.py`

- [ ] **Step 3: Run full suite**

Run: `make check test`
Expected: PASS.

- [ ] **Step 4: Run integration scenario test**

Run: `uv run pytest tests/integration/test_scenario_runner.py -v -n0`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/moneybin/testing/scenarios/
git commit -m "Remove legacy expectations.py — replaced by validation.expectations + registry"
```

---

## Stage F — Lock the spec

### Task 21: Amend `testing-scenario-comprehensive.md`

**Files:**
- Modify: `docs/specs/testing-scenario-comprehensive.md`

- [ ] **Step 1: Update the "Files to Create" subsection under Implementation Plan**

Replace the four bullets that name `structural.py` / `behavioral.py` / `quality.py` / `semantic.py` with:

```markdown
- `src/moneybin/validation/assertions/{schema,completeness,uniqueness,integrity,domain,distribution,infrastructure}.py` — assertion primitives, organized along industry-recognized data-quality categories. New Phase 3 primitives slot into the existing module that matches their shape (no new module names without a corresponding new category).
- `src/moneybin/validation/expectations/{matching,transactions}.py` — per-record predicate library, decoupled from YAML loader. Public API: `verify_*` functions returning `ExpectationResult`.
- `src/moneybin/testing/scenarios/_assertion_registry.py`, `_expectation_registry.py` — explicit YAML-name → callable maps. Adding a new YAML-callable primitive requires a registry entry.
- Harness primitives (`assert_idempotent`, `assert_subprocess_parity`, `assert_incremental_safe`, `assert_empty_input_safe`, `assert_malformed_input_rejected`) live in `tests/scenarios/_harnesses.py` (Phase 4), **not** in `validation/`. They are pipeline-execution patterns, not data assertions.
```

- [ ] **Step 2: Update R6 to record the stable contract**

Replace R6's body with:

```markdown
### R6 — Shared validation library

Reusable check primitives live at `src/moneybin/validation/`, split into three peer subpackages reflecting the three Result types they return:

- `assertions/` — table-level predicates returning `AssertionResult`. Categories: `schema`, `completeness`, `uniqueness`, `integrity`, `domain`, `distribution`, `infrastructure`.
- `expectations/` — per-record predicates returning `ExpectationResult`. Modules: `matching`, `transactions`.
- `evaluations/` — metric scoring against thresholds, returning `EvaluationResult`. Modules: `categorization`, `matching`.

Every primitive takes ``Database`` as its first positional argument (per `.claude/rules/database.md`). Top-level `moneybin.validation` re-exports only the three Result types.

This library is the **stable contract** consumed by both this spec's pytest suite and `data-reconciliation.md`'s runtime views. Stability rules:

- Additive optional kwargs are non-breaking.
- Renaming or removing a primitive requires a deprecation alias for one release.
- `details` (on `AssertionResult`/`ExpectationResult`) and `breakdown` (on `EvaluationResult`) payloads are per-function, not cross-function contract — consumers must not pattern-match on them across primitives.
```

- [ ] **Step 3: Update the Phase 2 sequencing entry**

Find the line under "Sequencing" that reads:

```markdown
2. **Phase 2 — Validation library.** Extract shared primitives to `src/moneybin/validation/`. Update existing scenarios to use them. No new assertions yet.
```

Replace with:

```markdown
2. **Phase 2 — Validation library.** Reorganize `src/moneybin/validation/` into seven industry-aligned assertion modules; standardize on `Database` as every primitive's first argument; decouple per-record expectations from the YAML loader; wire two explicit registries (assertions, expectations) co-located with the runner; lock the public API as a stable contract. No new assertions yet — Phase 3 backfills the missing Tier 1 primitives.
```

- [ ] **Step 4: Update spec status**

In the Status section at the top, change `draft` → `in-progress`.

In `docs/specs/INDEX.md`, find the entry for `testing-scenario-comprehensive.md` and update its status column to match.

- [ ] **Step 5: Run lint to ensure markdown is well-formed**

Run: `make lint` (or whatever markdown linter the project uses; if none, skip).
Expected: clean.

- [ ] **Step 6: Commit**

```bash
git add docs/specs/testing-scenario-comprehensive.md docs/specs/INDEX.md
git commit -m "Amend Phase 2 spec — lock validation library taxonomy and contract"
```

---

## Final verification

### Task 22: Pre-push quality pass

**Files:** None modified (or fixes to whatever `/simplify` flags).

- [ ] **Step 1: Run `/simplify` on the changed code**

Per `.claude/rules/shipping.md`, run `/simplify` before pushing. Have it review the diff range from this branch.

- [ ] **Step 2: Address findings inline**

Apply non-trivial findings as additional commits. Trivial findings (one-liner cleanups) can be folded into a single "polish" commit.

- [ ] **Step 3: Run `make check test` one final time**

Run: `make check test`
Expected: PASS — format, lint, type-check, tests all green.

- [ ] **Step 4: Push and open PR**

Run:
```bash
git push -u origin feat/validation-library-extract
gh pr create --title "Reorganize validation library; decouple expectations" --body "$(cat <<'EOF'
## Summary
- Split assertions into seven industry-aligned modules: schema / completeness / uniqueness / integrity / domain / distribution / infrastructure
- Standardize every primitive on `Database` as first arg; drop runner's `_DATABASE_ASSERTION_FNS` dispatch
- Decouple per-record expectations from the YAML loader — `validation/expectations/{matching,transactions}.py` with typed kwargs
- Wire two explicit registries (`_assertion_registry`, `_expectation_registry`) co-located with the runner
- Lock `moneybin.validation.*` as the stable contract consumed by data-reconciliation

Implements Phase 2 of [`testing-scenario-comprehensive.md`](docs/specs/testing-scenario-comprehensive.md). Spec amended to record the actual layout decisions.

## Test plan
- [ ] `make check test` green on this branch
- [ ] `uv run pytest tests/integration/test_scenario_runner.py -v` green
- [ ] No public API change beyond the locked contract (verified by importing every `validation.*` symbol)
- [ ] Pyright clean on `src/moneybin/validation/` and `src/moneybin/testing/scenarios/`
EOF
)"
```

---

## Out-of-band considerations

**Pyright on private-attr access.** `runner.py` uses `_config._current_profile`. That's pre-existing; this plan does not touch it.

**Test parallelism.** Tests use `pytest-xdist`. The new fixtures in `test_expectations_*.py` create per-test `Database` instances under `tmp_path`, so they're parallel-safe by construction. No `autouse` fixtures.

**No-PII rule.** All log messages added in this plan emit type names and counts only, never amounts/descriptions. The legacy module already followed this rule; ports preserve it.

**Stable contract enforcement.** Once this PR lands, any future PR that renames or removes a `validation.*` symbol must add a deprecation alias. There is no automated linter for this — code review is the enforcement. Consider adding a CHANGELOG entry as a manual checkpoint if the project has one.

**Phase 1 (relocation) intersects this work.** When Phase 1 moves `src/moneybin/testing/scenarios/` → `tests/scenarios/`, the two registry files (`_assertion_registry.py`, `_expectation_registry.py`) move with the runner. They're already named with a leading underscore to mark them runner-private, so no public API changes when they relocate.
