# Scenario Relocation (Phase 1) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Relocate the scenario runner, steps, loader, expectations, fixtures, and YAML scenarios from `src/moneybin/testing/scenarios/` to `tests/scenarios/`; drive scenarios through `pytest` instead of the `moneybin synthetic verify` CLI; replace the bespoke `ResponseEnvelope` output with `pytest-json-report` + a small internal result dataclass. Behavior of every existing assertion, expectation, and evaluation is preserved 1:1 — no new tiers, no new scenarios.

**Architecture:** The runner stops being product code. It becomes a pytest test harness: `tests/scenarios/_runner/` holds the moved orchestrator, step registry, loader, fixture loader, seed-merchants helper, and expectations verifier (mechanically transplanted from `src/moneybin/testing/scenarios/`). One `test_*.py` per shipped YAML calls `run_scenario(scenario)` and asserts `result.passed is True`; failed assertions/expectations/evaluations surface as pytest failure messages. `make verify-scenarios` and CI both run `uv run pytest tests/scenarios/ -m scenarios --json-report --json-report-file=scenarios.json`. The `synthetic verify` Typer command and its E2E tests are deleted; `synthetic generate` and `synthetic reset` stay (legitimate user commands).

**Tech Stack:** pytest, pytest-xdist (already in use), pytest-json-report (new dev dep), DuckDB, SQLMesh, Pydantic v2, Typer.

---

## File Structure

**Created (new):**
- `tests/scenarios/__init__.py` — empty package marker
- `tests/scenarios/conftest.py` — re-exports the in-memory keyring fixture, registers `MONEYBIN_DATABASE__ENCRYPTION_KEY` env default for sandboxed runs
- `tests/scenarios/_runner/__init__.py` — re-exports `run_scenario`, `ScenarioResult`, `load_scenario`, `load_shipped_scenario`, `list_shipped_scenarios`
- `tests/scenarios/_runner/runner.py` — moved from `src/moneybin/testing/scenarios/runner.py` with `ResponseEnvelope` swapped for a local `ScenarioResult` dataclass
- `tests/scenarios/_runner/loader.py` — moved verbatim from `src/moneybin/testing/scenarios/loader.py` with the `FIXTURES_ROOT` / `SHIPPED_SCENARIOS_DIR` paths re-anchored to `tests/scenarios/data/`
- `tests/scenarios/_runner/steps.py` — moved verbatim, imports updated
- `tests/scenarios/_runner/expectations.py` — moved verbatim, imports updated
- `tests/scenarios/_runner/fixture_loader.py` — moved verbatim, imports updated
- `tests/scenarios/_runner/seed_merchants.py` — moved verbatim, imports updated
- `tests/scenarios/_runner/result.py` — new `ScenarioResult` dataclass (replaces `ResponseEnvelope` payload)
- `tests/scenarios/data/*.yaml` — moved (six files)
- `tests/scenarios/data/fixtures/dedup/*` — moved (CSV + OFX + expectations YAML)
- `tests/scenarios/test_basic_full_pipeline.py`
- `tests/scenarios/test_family_full_pipeline.py`
- `tests/scenarios/test_dedup_cross_source.py`
- `tests/scenarios/test_encryption_key_propagation.py`
- `tests/scenarios/test_migration_roundtrip.py`
- `tests/scenarios/test_transfer_detection.py`

**Modified:**
- `pyproject.toml` — add `pytest-json-report` to dev deps; add `scenarios` marker; drop the `package-data` entries pointing at `testing/scenarios/data/...`
- `Makefile` — replace `test-scenarios` target with `verify-scenarios` running pytest
- `.github/workflows/scenarios.yml` — replace `moneybin synthetic verify --all --output=json` step with pytest invocation
- `src/moneybin/cli/commands/synthetic.py` — delete `verify_cmd` (lines 269–359) and its imports of `cast`, `json`, `ResponseEnvelope`-related plumbing
- `docs/specs/testing-scenario-runner.md` — banner noting the runner now lives under `tests/scenarios/_runner/`
- `docs/specs/testing-overview.md` — update the scenarios section to reference `pytest tests/scenarios/`
- `docs/specs/INDEX.md` — flip `testing-scenario-comprehensive.md` from `draft` to `in-progress`

**Deleted:**
- `src/moneybin/testing/scenarios/` (entire directory) — once the new layout passes
- `tests/integration/test_scenario_runner.py` — covered by the new per-YAML pytest tests
- `tests/e2e/test_e2e_synthetic_verify.py` — command no longer exists
- `tests/moneybin/test_testing/test_scenarios_loader.py`, `tests/moneybin/test_testing/test_scenarios_steps.py`, `tests/moneybin/test_testing/test_scenarios_expectations.py` — re-add equivalents under `tests/scenarios/_runner/test_*.py` only if they cover behavior the per-YAML tests don't (see Task 12)

---

## Task 1: Add pytest-json-report dependency and `scenarios` marker

**Files:**
- Modify: `pyproject.toml` (markers block at line 254; dependency-groups dev section)

- [ ] **Step 1: Add `pytest-json-report` to dev dependencies**

Edit `pyproject.toml`. Find the existing `[dependency-groups]` `dev = [...]` (or `[tool.uv]` dev section — match whichever pattern this repo already uses; `uv tree --dev | head` will show it). Add the line `"pytest-json-report>=1.5.0",` to that list, alphabetically grouped near other pytest plugins (`pytest-xdist`, `pytest-cov`).

- [ ] **Step 2: Add `scenarios` pytest marker**

Edit `pyproject.toml` line 254 (`markers = [...]`). Add a new entry:

```toml
markers = [
    "slow: marks tests as slow (deselect with '-m \"not slow\"')",
    "integration: marks tests as integration tests",
    "unit: marks tests as unit tests",
    "e2e: marks tests as end-to-end subprocess tests",
    "scenarios: marks whole-pipeline scenario tests (slow, real DB, real SQLMesh)",
]
```

- [ ] **Step 3: Sync and verify the marker is recognised**

Run: `uv sync --all-extras`
Expected: clean install, no resolver errors.

Run: `uv run pytest --markers | grep scenarios`
Expected: `@pytest.mark.scenarios: marks whole-pipeline scenario tests (slow, real DB, real SQLMesh)`

- [ ] **Step 4: Commit**

```bash
git add pyproject.toml uv.lock
git commit -m "Add pytest-json-report dev dep and scenarios marker"
```

---

## Task 2: Add internal `ScenarioResult` dataclass

**Files:**
- Create: `tests/scenarios/__init__.py`
- Create: `tests/scenarios/_runner/__init__.py`
- Create: `tests/scenarios/_runner/result.py`

- [ ] **Step 1: Create the directory skeleton**

Create empty `tests/scenarios/__init__.py` and `tests/scenarios/_runner/__init__.py` (single newline each — pytest collection works with or without them, but explicit packages keep `pyright` happy).

- [ ] **Step 2: Write `ScenarioResult`**

Create `tests/scenarios/_runner/result.py`:

```python
"""Lightweight result type returned by ``run_scenario``.

Replaces the bespoke ``ResponseEnvelope`` previously used by the
``moneybin synthetic verify`` CLI. Scenario tests assert on this
dataclass directly; pytest-json-report captures pass/fail in CI.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True, slots=True)
class ScenarioResult:
    """Outcome of running a single scenario end-to-end."""

    scenario: str
    passed: bool
    duration_seconds: float
    halted: str | None = None
    tmpdir: str | None = None
    assertions: list[dict[str, Any]] = field(default_factory=list)
    expectations: list[dict[str, Any]] = field(default_factory=list)
    evaluations: list[dict[str, Any]] = field(default_factory=list)

    def failure_summary(self) -> str:
        """Render a multi-line description of every failing check.

        Used as the ``assert`` failure message so pytest output points
        the reader directly at which assertion/expectation/evaluation
        broke and why, without dumping PII-bearing details.
        """
        lines: list[str] = [f"scenario {self.scenario!r} failed"]
        if self.halted:
            lines.append(f"  halted: {self.halted}")
        for a in self.assertions:
            if not a["passed"]:
                lines.append(f"  assertion {a['name']}: {a.get('error') or 'failed'}")
        for e in self.expectations:
            if not e["passed"]:
                lines.append(f"  expectation {e['name']}")
        for v in self.evaluations:
            if not v["passed"]:
                lines.append(
                    f"  evaluation {v['name']}: "
                    f"{v['metric']}={v['value']} < threshold={v['threshold']}"
                )
        return "\n".join(lines)
```

- [ ] **Step 3: Re-export from `_runner/__init__.py`**

Replace the content of `tests/scenarios/_runner/__init__.py` with:

```python
"""Scenario runner harness — moved from ``src/moneybin/testing/scenarios``.

Underscore-prefixed so pytest doesn't try to collect tests from it.
"""

from tests.scenarios._runner.result import ScenarioResult

__all__ = ["ScenarioResult"]
```

(Other names — `run_scenario`, `load_scenario`, etc. — get added in Task 4 after the runner is moved.)

- [ ] **Step 4: Verify imports**

Run: `uv run python -c "from tests.scenarios._runner.result import ScenarioResult; print(ScenarioResult)"`
Expected: `<class 'tests.scenarios._runner.result.ScenarioResult'>`

- [ ] **Step 5: Commit**

```bash
git add tests/scenarios/__init__.py tests/scenarios/_runner/__init__.py tests/scenarios/_runner/result.py
git commit -m "Add ScenarioResult dataclass for the relocated runner"
```

---

## Task 3: Move scenario YAML data and fixtures

**Files:**
- Create: `tests/scenarios/data/*.yaml` (six files, moved)
- Create: `tests/scenarios/data/fixtures/dedup/*` (three files, moved)
- Delete (later in Task 11): `src/moneybin/testing/scenarios/data/`

- [ ] **Step 1: Move the YAML scenarios and fixtures via `git mv`**

```bash
mkdir -p tests/scenarios/data/fixtures
git mv src/moneybin/testing/scenarios/data/basic-full-pipeline.yaml             tests/scenarios/data/basic-full-pipeline.yaml
git mv src/moneybin/testing/scenarios/data/family-full-pipeline.yaml            tests/scenarios/data/family-full-pipeline.yaml
git mv src/moneybin/testing/scenarios/data/dedup-cross-source.yaml              tests/scenarios/data/dedup-cross-source.yaml
git mv src/moneybin/testing/scenarios/data/encryption-key-propagation.yaml      tests/scenarios/data/encryption-key-propagation.yaml
git mv src/moneybin/testing/scenarios/data/migration-roundtrip.yaml             tests/scenarios/data/migration-roundtrip.yaml
git mv src/moneybin/testing/scenarios/data/transfer-detection-cross-account.yaml tests/scenarios/data/transfer-detection-cross-account.yaml
git mv src/moneybin/testing/scenarios/data/fixtures/dedup tests/scenarios/data/fixtures/dedup
```

`git mv` preserves history and makes the diff reviewable.

- [ ] **Step 2: Verify nothing else still references the old data path**

Run: `rg -n "testing/scenarios/data" --type py --type toml --type yaml --type md`
Expected: only matches inside `pyproject.toml` (the `package-data` glob that we'll drop in Task 11) and possibly docs that we'll update in Task 11. No Python imports.

- [ ] **Step 3: Commit**

```bash
git commit -m "Move scenario YAML and dedup fixtures to tests/scenarios/data"
```

(History shows these as renames — no content diff.)

---

## Task 4: Move runner, steps, loader, expectations, fixture_loader, seed_merchants

**Files:**
- Create: `tests/scenarios/_runner/runner.py` (moved + edited)
- Create: `tests/scenarios/_runner/steps.py` (moved + edited)
- Create: `tests/scenarios/_runner/loader.py` (moved + edited)
- Create: `tests/scenarios/_runner/expectations.py` (moved + edited)
- Create: `tests/scenarios/_runner/fixture_loader.py` (moved + edited)
- Create: `tests/scenarios/_runner/seed_merchants.py` (moved + edited)
- Modify: `tests/scenarios/_runner/__init__.py`

- [ ] **Step 1: `git mv` the six modules**

```bash
git mv src/moneybin/testing/scenarios/runner.py          tests/scenarios/_runner/runner.py
git mv src/moneybin/testing/scenarios/steps.py           tests/scenarios/_runner/steps.py
git mv src/moneybin/testing/scenarios/loader.py          tests/scenarios/_runner/loader.py
git mv src/moneybin/testing/scenarios/expectations.py    tests/scenarios/_runner/expectations.py
git mv src/moneybin/testing/scenarios/fixture_loader.py  tests/scenarios/_runner/fixture_loader.py
git mv src/moneybin/testing/scenarios/seed_merchants.py  tests/scenarios/_runner/seed_merchants.py
```

- [ ] **Step 2: Update `loader.py` paths and imports**

Edit `tests/scenarios/_runner/loader.py`:

Replace the constants block (around lines 24–28 in the current file):

```python
REPO_ROOT = Path(__file__).resolve().parents[4]
FIXTURES_ROOT = (Path(__file__).parent / "data" / "fixtures").resolve()
```

with:

```python
# Anchor at <repo>/tests/scenarios/data — scenario YAML and fixtures live
# under tests/, not the installed package.
_DATA_ROOT = Path(__file__).resolve().parent.parent / "data"
REPO_ROOT = Path(__file__).resolve().parents[3]
FIXTURES_ROOT = (_DATA_ROOT / "fixtures").resolve()
```

Replace the bottom-of-file constant:

```python
SHIPPED_SCENARIOS_DIR = Path(__file__).parent / "data"
```

with:

```python
SHIPPED_SCENARIOS_DIR = _DATA_ROOT
```

- [ ] **Step 3: Update `steps.py` imports**

Edit `tests/scenarios/_runner/steps.py`. Replace the three internal imports:

```python
from moneybin.testing.scenarios.loader import SetupSpec
from moneybin.testing.scenarios.seed_merchants import seed_merchants_from_persona
```

and (inside `_step_load_fixtures`):

```python
from moneybin.testing.scenarios.fixture_loader import load_fixture_into_db
```

with the relocated paths:

```python
from tests.scenarios._runner.loader import SetupSpec
from tests.scenarios._runner.seed_merchants import seed_merchants_from_persona
```

and:

```python
from tests.scenarios._runner.fixture_loader import load_fixture_into_db
```

- [ ] **Step 4: Update `expectations.py` imports**

Edit `tests/scenarios/_runner/expectations.py`. Replace:

```python
from moneybin.testing.scenarios.loader import ExpectationSpec
```

with:

```python
from tests.scenarios._runner.loader import ExpectationSpec
```

- [ ] **Step 5: Update `fixture_loader.py` imports**

Edit `tests/scenarios/_runner/fixture_loader.py`. Find any `from moneybin.testing.scenarios.loader import ...` and rewrite to `from tests.scenarios._runner.loader import ...`. (Run `rg -n "moneybin.testing.scenarios" tests/scenarios/_runner/fixture_loader.py` to confirm no stragglers.)

- [ ] **Step 6: Update `runner.py` — swap envelope for `ScenarioResult`**

Edit `tests/scenarios/_runner/runner.py`:

(a) Replace the imports block (lines 21–34 in the current file) with:

```python
from moneybin.database import Database, close_database, get_database
from moneybin.validation.assertions import assert_sqlmesh_catalog_matches
from moneybin.validation.result import AssertionResult, EvaluationResult
from tests.scenarios._runner.expectations import (
    ExpectationResult,
    verify_expectations,
)
from tests.scenarios._runner.loader import (
    AssertionSpec,
    EvaluationSpec,
    Scenario,
)
from tests.scenarios._runner.result import ScenarioResult
from tests.scenarios._runner.steps import run_step
```

(Note: the `from moneybin.mcp.envelope import ResponseEnvelope, build_envelope` line is removed.)

(b) Change the `run_scenario` return annotation:

```python
def run_scenario(scenario: Scenario, *, keep_tmpdir: bool = False) -> ScenarioResult:
```

(c) Replace `_build_envelope` (the function at lines 293–336) with `_build_result`:

```python
def _build_result(
    *,
    scenario: Scenario,
    started: float,
    tmpdir: str,
    keep_tmpdir: bool = False,
    assertions: list[AssertionResult],
    expectations: list[ExpectationResult],
    evaluations: list[EvaluationResult],
    halted: str | None = None,
) -> ScenarioResult:
    duration = round(time.perf_counter() - started, 2)
    all_a = all(a.passed for a in assertions)
    all_e = all(e.passed for e in expectations) if expectations else True
    all_v = all(e.passed for e in evaluations) if evaluations else True
    passed = all_a and all_e and all_v and halted is None

    return ScenarioResult(
        scenario=scenario.name,
        passed=passed,
        duration_seconds=duration,
        tmpdir=tmpdir if keep_tmpdir else None,
        halted=halted,
        assertions=[asdict(a) for a in assertions],
        expectations=[asdict(e) for e in expectations],
        evaluations=[asdict(e) for e in evaluations],
    )
```

(d) Replace every call site of `_build_envelope(...)` inside `run_scenario` with `_build_result(...)` — six call sites total: one in the preflight failure branch, one in the pipeline-crash branch, one in the expectations-crash branch, one in the success path. Function-rename only; argument lists stay identical.

(e) Drop the now-unused `from moneybin.mcp.envelope import ...` line if any survived. Drop `actions` construction (it was envelope-specific).

- [ ] **Step 7: Update `_runner/__init__.py` to re-export the moved API**

Replace `tests/scenarios/_runner/__init__.py`:

```python
"""Scenario runner harness — moved from ``src/moneybin/testing/scenarios``.

Underscore-prefixed so pytest doesn't try to collect tests from it.
"""

from tests.scenarios._runner.loader import (
    Scenario,
    ScenarioValidationError,
    list_shipped_scenarios,
    load_scenario,
    load_scenario_from_string,
    load_shipped_scenario,
)
from tests.scenarios._runner.result import ScenarioResult
from tests.scenarios._runner.runner import run_scenario

__all__ = [
    "Scenario",
    "ScenarioResult",
    "ScenarioValidationError",
    "list_shipped_scenarios",
    "load_scenario",
    "load_scenario_from_string",
    "load_shipped_scenario",
    "run_scenario",
]
```

- [ ] **Step 8: Verify the relocated package imports cleanly**

Run: `uv run python -c "from tests.scenarios._runner import run_scenario, list_shipped_scenarios; print(len(list_shipped_scenarios()))"`
Expected: `6` (the six shipped YAMLs are discoverable at the new path).

- [ ] **Step 9: Commit**

```bash
git add tests/scenarios/_runner/ src/moneybin/testing/scenarios/
git commit -m "Move scenario runner modules to tests/scenarios/_runner"
```

(Renames + small edits — diff should be tractable for review.)

---

## Task 5: Add `tests/scenarios/conftest.py`

**Files:**
- Create: `tests/scenarios/conftest.py`

- [ ] **Step 1: Write the conftest**

Create `tests/scenarios/conftest.py`:

```python
"""Shared fixtures for scenario tests.

Scenarios are slow, real-DB, real-SQLMesh checks. They run in CI under a
single concurrency group and locally via ``make verify-scenarios``. The
fixtures here provide:

- An in-memory keyring so ``SecretStore`` works without a system backend.
- An ephemeral encryption-key env var so ``Database`` can encrypt the
  scenario tempdir's DuckDB file without needing the real key.
"""

from __future__ import annotations

from collections.abc import Generator

import keyring
import pytest

from tests.e2e.memory_keyring import MemoryKeyring


@pytest.fixture(autouse=True)
def _scenario_keyring() -> Generator[None, None, None]:
    """Swap in the dict-backed keyring for every scenario test."""
    previous = keyring.get_keyring()
    keyring.set_keyring(MemoryKeyring())
    try:
        yield
    finally:
        MemoryKeyring.clear()
        keyring.set_keyring(previous)


@pytest.fixture(autouse=True)
def _scenario_encryption_key(monkeypatch: pytest.MonkeyPatch) -> None:
    """Provide an ephemeral encryption key when the system has none."""
    monkeypatch.setenv(
        "MONEYBIN_DATABASE__ENCRYPTION_KEY",
        "scenario-ephemeral-key-tmpdir-only",
    )
```

- [ ] **Step 2: Verify the conftest is collected**

Run: `uv run pytest tests/scenarios/ --collect-only -q`
Expected: zero items collected (no test files yet) and no collection errors.

- [ ] **Step 3: Commit**

```bash
git add tests/scenarios/conftest.py
git commit -m "Add scenarios conftest with in-memory keyring and ephemeral key"
```

---

## Task 6: Port `basic-full-pipeline` to pytest

**Files:**
- Create: `tests/scenarios/test_basic_full_pipeline.py`

- [ ] **Step 1: Write the test**

Create `tests/scenarios/test_basic_full_pipeline.py`:

```python
"""Scenario: end-to-end pipeline correctness for the basic persona."""

from __future__ import annotations

import pytest

from tests.scenarios._runner import load_shipped_scenario, run_scenario


@pytest.mark.scenarios
@pytest.mark.slow
def test_basic_full_pipeline() -> None:
    scenario = load_shipped_scenario("basic-full-pipeline")
    assert scenario is not None, "basic-full-pipeline.yaml not found"
    result = run_scenario(scenario)
    assert result.passed, result.failure_summary()
```

- [ ] **Step 2: Run it (expect PASS) — note the cost**

Run: `uv run pytest tests/scenarios/test_basic_full_pipeline.py -v -n0`
Expected: 1 passed in ~60–120s. (Disable xdist with `-n0` for clean output during the first port; CI runs it under default `-n auto`.)

If it fails: do NOT relax the assertion. Inspect `result.failure_summary()` against the YAML's expected behavior. The YAML hasn't changed — any failure is a real regression introduced by the relocation.

- [ ] **Step 3: Commit**

```bash
git add tests/scenarios/test_basic_full_pipeline.py
git commit -m "Port basic-full-pipeline scenario to pytest"
```

---

## Task 7: Port the remaining five scenarios

**Files:**
- Create: `tests/scenarios/test_family_full_pipeline.py`
- Create: `tests/scenarios/test_dedup_cross_source.py`
- Create: `tests/scenarios/test_encryption_key_propagation.py`
- Create: `tests/scenarios/test_migration_roundtrip.py`
- Create: `tests/scenarios/test_transfer_detection.py`

- [ ] **Step 1: Write `test_family_full_pipeline.py`**

```python
"""Scenario: end-to-end pipeline correctness for the family persona (3 years)."""

from __future__ import annotations

import pytest

from tests.scenarios._runner import load_shipped_scenario, run_scenario


@pytest.mark.scenarios
@pytest.mark.slow
def test_family_full_pipeline() -> None:
    scenario = load_shipped_scenario("family-full-pipeline")
    assert scenario is not None
    result = run_scenario(scenario)
    assert result.passed, result.failure_summary()
```

- [ ] **Step 2: Write `test_dedup_cross_source.py`**

```python
"""Scenario: cross-source dedup collapses 6 fixture rows into 3 gold records."""

from __future__ import annotations

import pytest

from tests.scenarios._runner import load_shipped_scenario, run_scenario


@pytest.mark.scenarios
@pytest.mark.slow
def test_dedup_cross_source() -> None:
    scenario = load_shipped_scenario("dedup-cross-source")
    assert scenario is not None
    result = run_scenario(scenario)
    assert result.passed, result.failure_summary()
```

- [ ] **Step 3: Write `test_encryption_key_propagation.py`**

```python
"""Scenario: subprocess transform opens the encrypted DB with the propagated key."""

from __future__ import annotations

import pytest

from tests.scenarios._runner import load_shipped_scenario, run_scenario


@pytest.mark.scenarios
@pytest.mark.slow
def test_encryption_key_propagation() -> None:
    scenario = load_shipped_scenario("encryption-key-propagation")
    assert scenario is not None
    result = run_scenario(scenario)
    assert result.passed, result.failure_summary()
```

- [ ] **Step 4: Write `test_migration_roundtrip.py`**

```python
"""Scenario: migrations apply to the right schema; populated columns survive."""

from __future__ import annotations

import pytest

from tests.scenarios._runner import load_shipped_scenario, run_scenario


@pytest.mark.scenarios
@pytest.mark.slow
def test_migration_roundtrip() -> None:
    scenario = load_shipped_scenario("migration-roundtrip")
    assert scenario is not None
    result = run_scenario(scenario)
    assert result.passed, result.failure_summary()
```

- [ ] **Step 5: Write `test_transfer_detection.py`**

```python
"""Scenario: cross-account transfer pairs detected; F1 vs ground truth."""

from __future__ import annotations

import pytest

from tests.scenarios._runner import load_shipped_scenario, run_scenario


@pytest.mark.scenarios
@pytest.mark.slow
def test_transfer_detection() -> None:
    scenario = load_shipped_scenario("transfer-detection-cross-account")
    assert scenario is not None
    result = run_scenario(scenario)
    assert result.passed, result.failure_summary()
```

- [ ] **Step 6: Run the full scenario suite**

Run: `uv run pytest tests/scenarios/ -m scenarios -v`
Expected: 6 passed. (Total wall-clock with `-n auto` and SQLMesh in the loop is typically 5–10 minutes; on CI's single-runner box it stays under the workflow's 10-minute timeout.)

If any scenario fails: inspect the failure_summary, then check whether the failure is a real regression in the relocated code (most likely path-resolution in `loader.py` or an import that didn't get rewritten). Fix the underlying bug — never edit the YAML.

- [ ] **Step 7: Commit**

```bash
git add tests/scenarios/test_family_full_pipeline.py \
        tests/scenarios/test_dedup_cross_source.py \
        tests/scenarios/test_encryption_key_propagation.py \
        tests/scenarios/test_migration_roundtrip.py \
        tests/scenarios/test_transfer_detection.py
git commit -m "Port remaining shipped scenarios to pytest"
```

---

## Task 8: Replace `make test-scenarios` with `make verify-scenarios`

**Files:**
- Modify: `Makefile` (line 4 and line 156)

- [ ] **Step 1: Update the `.PHONY` declaration**

Find line 4:

```
.PHONY: help setup clean install install-dev test test-cov lint format type-check pre-commit venv activate status install-uv test-e2e test-scenarios
```

Replace `test-scenarios` with `verify-scenarios`.

- [ ] **Step 2: Replace the target body**

Find the existing target (around line 156):

```
test-scenarios: venv ## Development: Run all synthetic scenarios via the scenario runner
	@echo "$(BLUE)🧪 Running all scenarios...$(RESET)"
	@uv run moneybin synthetic verify --all
```

Replace with:

```
verify-scenarios: venv ## Development: Run all whole-pipeline scenarios via pytest
	@echo "$(BLUE)🧪 Running all scenarios...$(RESET)"
	@uv run pytest tests/scenarios/ -m scenarios -v
```

- [ ] **Step 3: Verify the target works**

Run: `make verify-scenarios`
Expected: all 6 scenarios pass.

- [ ] **Step 4: Commit**

```bash
git add Makefile
git commit -m "Replace test-scenarios target with verify-scenarios pytest target"
```

---

## Task 9: Update CI workflow to use pytest

**Files:**
- Modify: `.github/workflows/scenarios.yml`

- [ ] **Step 1: Replace the run/summary steps**

Edit `.github/workflows/scenarios.yml`. Replace the `Run scenario suite` and `Per-scenario summary` steps with:

```yaml
      - name: Run scenario suite
        env:
          # GitHub Actions runners have no keyring backend; SecretStore
          # falls through to this env var for the encryption key. The
          # value is ephemeral — the runner is destroyed after each job.
          MONEYBIN_DATABASE__ENCRYPTION_KEY: ci-ephemeral-key-runner-destroyed-after-job
        run: |
          set -o pipefail
          uv run pytest tests/scenarios/ -m scenarios -v \
            --json-report --json-report-file=scenarios.json

      - name: Per-scenario summary
        if: always()
        run: |
          echo "## Scenario results" >> "$GITHUB_STEP_SUMMARY"
          jq -r '.tests[] | "- \(.nodeid | split("::") | last): \(.outcome | ascii_upcase) (\(.duration | tonumber | floor)s)"' \
            scenarios.json >> "$GITHUB_STEP_SUMMARY" || true
```

Update the `Upload artifact` step's `path:` from `scenarios.jsonl` to `scenarios.json`.

- [ ] **Step 2: Lint the YAML locally**

Run: `uv run python -c "import yaml; yaml.safe_load(open('.github/workflows/scenarios.yml'))"`
Expected: no output (clean parse).

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/scenarios.yml
git commit -m "Run scenarios via pytest + json-report in CI"
```

---

## Task 10: Remove `synthetic verify` CLI command

**Files:**
- Modify: `src/moneybin/cli/commands/synthetic.py`
- Delete: `tests/e2e/test_e2e_synthetic_verify.py`

- [ ] **Step 1: Delete `verify_cmd` and its imports**

Edit `src/moneybin/cli/commands/synthetic.py`:

- Remove the entire `@app.command("verify")` block — the `verify_cmd` function spanning lines 269–359 in the current file.
- Remove now-unused imports at the top of the file: `import json`, `from typing import cast` (if cast is used only by `verify_cmd`), and any `OutputFormat`/`output_option` references that only `verify_cmd` consumes. Run `uv run ruff check src/moneybin/cli/commands/synthetic.py` after the deletion — Ruff will flag the unused imports if any remain.

- [ ] **Step 2: Delete the E2E test file**

```bash
git rm tests/e2e/test_e2e_synthetic_verify.py
```

- [ ] **Step 3: Confirm no other test references `synthetic verify`**

Run: `rg -n "synthetic verify|synthetic_verify|verify_cmd" tests/ src/`
Expected: zero matches.

- [ ] **Step 4: Update the help-commands E2E list**

Find `tests/e2e/test_e2e_help.py`. If `_HELP_COMMANDS` (or equivalent) contains `("synthetic", "verify")` or similar, remove that entry.

Run: `rg -n '"verify"' tests/e2e/test_e2e_help.py`
Expected: no remaining references to a `synthetic verify` command path.

- [ ] **Step 5: Verify CLI still boots**

Run: `uv run moneybin synthetic --help`
Expected: help text lists only `generate` and `reset` (no `verify`).

Run: `uv run pytest tests/e2e/ -m e2e -v -k "synthetic or help"`
Expected: all selected tests pass.

- [ ] **Step 6: Commit**

```bash
git add src/moneybin/cli/commands/synthetic.py tests/e2e/
git commit -m "Remove moneybin synthetic verify CLI; scenarios run via pytest now"
```

---

## Task 11: Delete the old src/ scenario package and orphaned tests

**Files:**
- Delete: `src/moneybin/testing/scenarios/` (entire directory)
- Delete: `tests/integration/test_scenario_runner.py`
- Delete (or leave for Task 12): `tests/moneybin/test_testing/test_scenarios_loader.py`, `test_scenarios_steps.py`, `test_scenarios_expectations.py`
- Modify: `pyproject.toml` (drop `package-data` entries pointing at the old path)

- [ ] **Step 1: Confirm nothing in `src/` imports from the old location**

Run: `rg -n "moneybin\.testing\.scenarios" src/`
Expected: zero matches. (If any survive, fix them — they're either the runner's own internal imports we already rewrote in Task 4, or something we missed.)

- [ ] **Step 2: Delete the now-empty src package**

```bash
git rm -r src/moneybin/testing/scenarios
```

- [ ] **Step 3: Delete the integration test superseded by per-YAML pytest tests**

```bash
git rm tests/integration/test_scenario_runner.py
```

This file's two tests (`test_runner_returns_envelope_for_passing_scenario`, `test_runner_reports_failure_without_crashing`) used the inline `TINY` scenario to exercise the runner mechanics. The six per-YAML tests created in Tasks 6–7 now cover passing-path mechanics on real scenarios; the failure-path is exercised when any of them fails (and is more meaningfully checked by future Phase 3 work that adds `tests/scenarios/_runner/test_runner.py`).

- [ ] **Step 4: Move the per-module unit tests under `_runner/`**

```bash
mkdir -p tests/scenarios/_runner_tests
git mv tests/moneybin/test_testing/test_scenarios_loader.py        tests/scenarios/_runner_tests/test_loader.py
git mv tests/moneybin/test_testing/test_scenarios_steps.py         tests/scenarios/_runner_tests/test_steps.py
git mv tests/moneybin/test_testing/test_scenarios_expectations.py  tests/scenarios/_runner_tests/test_expectations.py
touch tests/scenarios/_runner_tests/__init__.py
```

Edit each moved file: rewrite every `from moneybin.testing.scenarios.X import Y` to `from tests.scenarios._runner.X import Y`. Run:

```bash
rg -n "moneybin\.testing\.scenarios" tests/scenarios/_runner_tests/
```

Expected: zero matches once edits are complete.

Run: `uv run pytest tests/scenarios/_runner_tests/ -v`
Expected: all tests pass.

- [ ] **Step 5: Drop `package-data` entries that pointed at the old YAML location**

Edit `pyproject.toml`. Find the `[tool.setuptools.package-data]` (or hatch equivalent — match what's there) block around line 70–78:

```toml
"testing/scenarios/data/*.yaml",
"testing/scenarios/data/fixtures/**/*.csv",
"testing/scenarios/data/fixtures/**/*.yaml",
```

Delete these three lines. The wheel no longer ships scenario YAMLs (they're test-only).

- [ ] **Step 6: Verify a clean install still works**

Run: `uv sync --all-extras && uv build`
Expected: build succeeds; the resulting wheel under `dist/` does NOT contain any `testing/scenarios/data/*` entries (verify with `unzip -l dist/moneybin-*.whl | grep scenarios` — expected: no output).

- [ ] **Step 7: Run the full test suite**

Run: `uv run pytest tests/ -v -m "not scenarios and not e2e and not integration"`
Expected: existing pass/fail count matches the pre-relocation baseline (one pre-existing failure in `test_profile_environment_variable` is unrelated and already documented).

Run: `uv run pytest tests/scenarios/ -v`
Expected: 6 scenarios pass + the relocated unit tests pass.

- [ ] **Step 8: Commit**

```bash
git add -A
git commit -m "Delete src/moneybin/testing/scenarios; relocate unit tests"
```

---

## Task 12: Update documentation references

**Files:**
- Modify: `docs/specs/testing-scenario-runner.md`
- Modify: `docs/specs/testing-overview.md`
- Modify: `docs/specs/INDEX.md`
- Modify: `README.md` (if it references `moneybin synthetic verify`)
- Modify: `CONTRIBUTING.md` (if it references the old path)

- [ ] **Step 1: Find every doc reference to the old layout**

Run: `rg -n "moneybin synthetic verify|src/moneybin/testing/scenarios|test-scenarios" docs/ README.md CONTRIBUTING.md`

Expected: a handful of hits. For each:
- If it says `moneybin synthetic verify --all` → replace with `make verify-scenarios` (or `uv run pytest tests/scenarios/ -m scenarios`).
- If it says `src/moneybin/testing/scenarios/` → replace with `tests/scenarios/`.
- If it says `make test-scenarios` → replace with `make verify-scenarios`.

- [ ] **Step 2: Add a banner to `testing-scenario-runner.md`**

At the top of `docs/specs/testing-scenario-runner.md`, add:

```markdown
> **Status update (2026-04-30):** The scenario runner has moved from
> `src/moneybin/testing/scenarios/` to `tests/scenarios/_runner/`, and
> scenarios now run via `pytest tests/scenarios/ -m scenarios` instead of
> the (now-removed) `moneybin synthetic verify` CLI. See
> [`testing-scenario-comprehensive.md`](testing-scenario-comprehensive.md)
> for the migration plan. The architecture and assertion vocabulary
> documented below are still accurate.
```

- [ ] **Step 3: Flip the comprehensive spec to `in-progress`**

Edit `docs/specs/INDEX.md`. Find the `testing-scenario-comprehensive.md` row and change its status column from `draft` (or whatever it currently shows) to `in-progress`. Edit `docs/specs/testing-scenario-comprehensive.md` frontmatter or status header similarly if it has one.

- [ ] **Step 4: Verify nothing still points at the old paths**

Run: `rg -n "moneybin synthetic verify|src/moneybin/testing/scenarios|make test-scenarios" docs/ README.md CONTRIBUTING.md`
Expected: zero matches.

- [ ] **Step 5: Commit**

```bash
git add docs/ README.md CONTRIBUTING.md
git commit -m "Update doc references for scenario relocation"
```

---

## Task 13: Pre-push quality pass

- [ ] **Step 1: Run the full pre-commit checklist**

Run: `make check test`
Expected: format, lint, type-check, unit/integration tests all green (excluding the pre-existing unrelated `test_profile_environment_variable` failure).

- [ ] **Step 2: Run the scenarios suite end-to-end**

Run: `make verify-scenarios`
Expected: 6 scenarios pass.

- [ ] **Step 3: Run `/simplify` per shipping.md**

Invoke the `/simplify` skill on the changes in this branch. Apply any duplication/quality fixes it surfaces. Re-run `make check test` after edits.

- [ ] **Step 4: Final commit (if simplify made changes)**

```bash
git add -A
git commit -m "Apply /simplify pass before merge"
```

- [ ] **Step 5: Open PR**

Use the `/commit-push-pr` skill. Branch is already `feat/scenario-testing-comprehensive`. Suggested PR title:

> Relocate scenario runner to tests/ and replace `synthetic verify` with pytest

Suggested PR body sections: Summary (R5 of `testing-scenario-comprehensive.md`), Impact (CI summary format changes from per-scenario JSON to pytest-json-report; `make test-scenarios` renamed to `make verify-scenarios`; `moneybin synthetic verify` removed), Changes (relocation, runner result type swap, CI workflow, CLI removal, doc updates), Testing (`make verify-scenarios`, full unit suite), Notes (Phase 2 — validation library extract — comes next as a separate PR).

---

## Self-Review Notes

- Spec coverage: R5 (relocation) is the only requirement targeted by this plan. R1/R2/R3/R4/R6 are deferred to PRs 2 and 3 per the user's explicit split.
- Placeholder scan: every step has either exact code or an exact command; no "TBD"/"add error handling"/"similar to above" left behind. The one place I name a target without showing every byte is `tests/e2e/test_e2e_help.py` — the file's exact `_HELP_COMMANDS` shape isn't in this plan because it's been edited recently and the executor should grep for the live structure rather than match a stale snippet.
- Type consistency: `ScenarioResult` defined in Task 2; consumed in Task 4 (`run_scenario` annotation, `_build_result` constructor) and Tasks 6–7 (test files use `result.passed` and `result.failure_summary()`). Field names match.
- Sequence: data moves first (Task 3), then runner code (Task 4 — `loader.py` rewrites `SHIPPED_SCENARIOS_DIR` to point at the already-moved data path), then conftest (Task 5), then tests (Tasks 6–7). Old src/ deletion happens last (Task 11) so the relocated tests have proven green before the safety net is removed.
