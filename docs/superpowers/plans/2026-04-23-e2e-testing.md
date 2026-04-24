# E2E Testing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add subprocess-based E2E tests that run `uv run moneybin` like a real user, catching boot/schema/init bugs that in-process tests miss.

**Architecture:** A `tests/e2e/` package with a `conftest.py` providing a `run_cli()` helper and session-scoped fixtures that create temporary profiles with fast argon2 params. Smoke tests cover every command group. Workflow tests cover multi-step user flows. Existing integration tests move from `tests/moneybin/test_integration/` to `tests/integration/`.

**Tech Stack:** pytest, subprocess, `tmp_path_factory`, existing test fixtures

---

## File Map

| Action | Path | Responsibility |
|--------|------|---------------|
| Create | `tests/e2e/__init__.py` | Package marker |
| Create | `tests/e2e/conftest.py` | `run_cli()` helper, `e2e_home`/`e2e_profile`/`e2e_env` fixtures |
| Create | `tests/e2e/test_e2e_smoke.py` | Parametrized smoke tests for all CLI commands |
| Create | `tests/e2e/test_e2e_workflows.py` | Multi-step workflow tests |
| Create | `tests/integration/__init__.py` | Package marker |
| Create | `tests/integration/conftest.py` | Moved from `tests/moneybin/test_integration/` (if fixtures needed) |
| Move | `tests/moneybin/test_integration/*.py` → `tests/integration/` | Relocate existing integration tests |
| Delete | `tests/moneybin/test_integration/` | Remove old directory |
| Modify | `pyproject.toml:245-249` | Add `e2e` marker |
| Modify | `Makefile:133-150` | Add `test-e2e` target, update `test-unit` to exclude `e2e` |
| Modify | `.claude/rules/testing.md` | Add "Test Coverage by Layer" section |
| Modify | `.claude/rules/shipping.md` | Add "Test Layer Check" item |
| Modify | `docs/specs/INDEX.md` | Add this spec |
| Modify | `docs/specs/e2e-testing.md` | Update status to `in-progress` |

---

### Task 1: Move integration tests to `tests/integration/`

**Files:**
- Move: `tests/moneybin/test_integration/test_integration_existing.py` → `tests/integration/test_integration_existing.py`
- Move: `tests/moneybin/test_integration/test_tabular_e2e.py` → `tests/integration/test_tabular_e2e.py`
- Create: `tests/integration/__init__.py`
- Delete: `tests/moneybin/test_integration/`

- [ ] **Step 1: Create integration directory and move files**

```bash
mkdir -p tests/integration
mv tests/moneybin/test_integration/test_integration_existing.py tests/integration/
mv tests/moneybin/test_integration/test_tabular_e2e.py tests/integration/
```

- [ ] **Step 2: Create `tests/integration/__init__.py`**

```python
"""Cross-subsystem integration tests (real DB, real loaders, still in-process)."""
```

- [ ] **Step 3: Fix fixture path references**

The moved files reference `FIXTURES_DIR = Path(__file__).parent.parent.parent / "fixtures"`. After the move, the relative path changes. Update both files:

In `tests/integration/test_integration_existing.py`, change:
```python
FIXTURES_DIR = Path(__file__).parent.parent.parent / "fixtures"
```
to:
```python
FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"
```

In `tests/integration/test_tabular_e2e.py`, change:
```python
FIXTURES = Path(__file__).parents[2] / "fixtures" / "tabular"
```
to:
```python
FIXTURES = Path(__file__).parent.parent / "fixtures" / "tabular"
```

- [ ] **Step 4: Remove old directory**

```bash
rm -rf tests/moneybin/test_integration/
```

- [ ] **Step 5: Verify integration tests still pass**

Run: `uv run pytest tests/integration/ -v -m "not integration"`
Expected: All non-integration-marked tests pass (the tabular e2e tests have no marker).

Run: `uv run pytest tests/integration/test_tabular_e2e.py -v`
Expected: PASS — fixture paths resolve correctly.

- [ ] **Step 6: Verify unit tests still pass**

Run: `uv run pytest tests/moneybin/ -m "not integration" -q`
Expected: Same pass count as before (no tests lost).

- [ ] **Step 7: Commit**

```bash
git add tests/integration/ && git rm -r tests/moneybin/test_integration/
git commit -m "Move integration tests to top-level tests/integration/ package

Separate cross-subsystem integration tests from unit tests to clarify
test directory structure: tests/moneybin/ for units, tests/integration/
for cross-subsystem, tests/e2e/ for subprocess tests (coming next)."
```

---

### Task 2: Add `e2e` marker and Makefile targets

**Files:**
- Modify: `pyproject.toml:245-249`
- Modify: `Makefile:133-150`

- [ ] **Step 1: Add `e2e` marker to `pyproject.toml`**

In the `[tool.pytest.ini_options]` section, add the `e2e` marker to the markers list:

```toml
markers = [
    "slow: marks tests as slow (deselect with '-m \"not slow\"')",
    "integration: marks tests as integration tests",
    "unit: marks tests as unit tests",
    "e2e: marks tests as end-to-end subprocess tests",
]
```

- [ ] **Step 2: Update `Makefile` test targets**

Update `test-unit` to exclude both `integration` and `e2e`:

```makefile
test-unit: venv ## Development: Run unit tests only (excludes integration and e2e tests)
	@echo "$(BLUE)🧪 Running unit tests (use 'make test-all' for all tests)...$(RESET)"
	@uv run pytest tests/ -m "not integration and not e2e"
```

Update `test-cov` similarly:

```makefile
test-cov: venv ## Development: Run tests with coverage report
	@echo "$(BLUE)🧪 Running tests with coverage...$(RESET)"
	@uv run pytest --cov=src tests/ -m "not integration and not e2e"
	@echo "$(BLUE)📊 Coverage report generated$(RESET)"
```

Add `test-e2e` target after `test-integration`:

```makefile
test-e2e: venv ## Development: Run end-to-end subprocess tests
	@echo "$(BLUE)🧪 Running end-to-end tests...$(RESET)"
	@uv run pytest tests/e2e/ -m "e2e" -v
```

- [ ] **Step 3: Verify Makefile targets parse correctly**

Run: `make help`
Expected: `test-e2e` appears in the Development section.

- [ ] **Step 4: Commit**

```bash
git add pyproject.toml Makefile
git commit -m "Add e2e marker and Makefile test-e2e target

New pytest marker for subprocess-based CLI tests. Update test-unit
and test-cov to exclude both integration and e2e. Add test-e2e
target for running E2E tests in isolation."
```

---

### Task 3: Create E2E test infrastructure (`conftest.py`)

**Files:**
- Create: `tests/e2e/__init__.py`
- Create: `tests/e2e/conftest.py`

- [ ] **Step 1: Create `tests/e2e/__init__.py`**

```python
"""End-to-end subprocess tests — run `uv run moneybin` like a real user."""
```

- [ ] **Step 2: Write `tests/e2e/conftest.py`**

```python
"""Shared fixtures for E2E subprocess tests.

These tests run `uv run moneybin ...` as a real subprocess to catch
boot, schema, and init wiring bugs that in-process tests miss.
"""

from __future__ import annotations

import os
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

# ---------------------------------------------------------------------------
# Result type
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CLIResult:
    """Result from a CLI subprocess invocation."""

    exit_code: int
    stdout: str
    stderr: str

    @property
    def output(self) -> str:
        """Combined stdout + stderr for simple assertions."""
        return self.stdout + self.stderr

    def assert_success(self) -> None:
        """Assert the command exited 0 with no Python tracebacks."""
        assert "Traceback (most recent call last)" not in self.stderr, (
            f"Python traceback in stderr:\n{self.stderr}"
        )
        assert self.exit_code == 0, (
            f"Expected exit code 0, got {self.exit_code}\n"
            f"stdout: {self.stdout}\nstderr: {self.stderr}"
        )


# ---------------------------------------------------------------------------
# CLI runner
# ---------------------------------------------------------------------------

_FAST_ARGON2_ENV = {
    "MONEYBIN_DATABASE__ARGON2_TIME_COST": "1",
    "MONEYBIN_DATABASE__ARGON2_MEMORY_COST": "1024",
    "MONEYBIN_DATABASE__ARGON2_PARALLELISM": "1",
}

_TEST_PASSPHRASE = "e2e-test-passphrase-1234"


def run_cli(
    *args: str,
    env: dict[str, str] | None = None,
    input_text: str | None = None,
    timeout: int = 120,
) -> CLIResult:
    """Run a moneybin CLI command as a subprocess.

    Args:
        *args: CLI arguments (e.g., "profile", "list").
        env: Environment variables (merged with os.environ).
        input_text: Text to pipe to stdin.
        timeout: Seconds before killing the process.

    Returns:
        CLIResult with exit_code, stdout, stderr.
    """
    cmd = ["uv", "run", "moneybin", *args]
    full_env = {**os.environ, **_FAST_ARGON2_ENV, **(env or {})}

    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        input=input_text,
        timeout=timeout,
        env=full_env,
    )
    return CLIResult(
        exit_code=result.returncode,
        stdout=result.stdout,
        stderr=result.stderr,
    )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def e2e_home(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Temporary MONEYBIN_HOME — auto-removed after the test session."""
    return tmp_path_factory.mktemp("e2e_home")


@pytest.fixture(scope="session")
def e2e_env(e2e_home: Path) -> dict[str, str]:
    """Base env dict pointing at the temp MONEYBIN_HOME."""
    return {"MONEYBIN_HOME": str(e2e_home)}


@pytest.fixture(scope="session")
def e2e_profile(e2e_env: dict[str, str]) -> dict[str, str]:
    """Create a test profile with an initialized, encrypted database.

    Returns the env dict with MONEYBIN_HOME set. The profile is named
    'e2e-test' and is ready for commands that need get_database().
    """
    profile_name = "e2e-test"
    env = {**e2e_env, "MONEYBIN_PROFILE": profile_name}

    # Create profile
    result = run_cli("profile", "create", profile_name, env=env)
    assert result.exit_code == 0, f"Failed to create profile: {result.stderr}"

    # Initialize database with passphrase
    passphrase_input = f"{_TEST_PASSPHRASE}\n{_TEST_PASSPHRASE}\n"
    result = run_cli(
        "db",
        "init",
        "--passphrase",
        "--yes",
        env=env,
        input_text=passphrase_input,
    )
    assert result.exit_code == 0, f"Failed to init database: {result.stderr}"

    return env


def make_workflow_env(
    e2e_home: Path,
    profile_name: str,
) -> dict[str, str]:
    """Create a fresh profile for a workflow test.

    Runs profile create + db init. Returns the env dict.
    Call this at the start of each workflow test for isolation.
    """
    env = {"MONEYBIN_HOME": str(e2e_home), "MONEYBIN_PROFILE": profile_name}

    result = run_cli("profile", "create", profile_name, env=env)
    assert result.exit_code == 0, (
        f"Failed to create profile '{profile_name}': {result.stderr}"
    )

    passphrase_input = f"{_TEST_PASSPHRASE}\n{_TEST_PASSPHRASE}\n"
    result = run_cli(
        "db",
        "init",
        "--passphrase",
        "--yes",
        env=env,
        input_text=passphrase_input,
    )
    assert result.exit_code == 0, (
        f"Failed to init DB for '{profile_name}': {result.stderr}"
    )

    return env


FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"
```

- [ ] **Step 3: Verify conftest imports cleanly**

Run: `uv run python -c "from tests.e2e.conftest import run_cli, CLIResult; print('OK')"`
Expected: `OK`

- [ ] **Step 4: Commit**

```bash
git add tests/e2e/
git commit -m "Add E2E test infrastructure: run_cli helper and fixtures

CLIResult dataclass, run_cli() subprocess wrapper with fast argon2
params, session-scoped e2e_profile fixture that creates a temp
profile and initializes an encrypted database."
```

---

### Task 4: Smoke tests — help commands (Tier 1)

**Files:**
- Create: `tests/e2e/test_e2e_smoke.py`

- [ ] **Step 1: Write Tier 1 help smoke tests**

```python
# ruff: noqa: S101
"""E2E smoke tests — verify every CLI command boots without errors."""

from __future__ import annotations

import pytest

from tests.e2e.conftest import run_cli

pytestmark = pytest.mark.e2e


# ---------------------------------------------------------------------------
# Tier 1: --help commands (no DB, no profile needed)
# ---------------------------------------------------------------------------

_HELP_COMMANDS: list[list[str]] = [
    [],  # moneybin --help
    ["profile"],
    ["import"],
    ["sync"],
    ["categorize"],
    ["matches"],
    ["transform"],
    ["synthetic"],
    ["db"],
    ["db", "migrate"],
    ["logs"],
    ["mcp"],
    ["stats"],
    ["track"],
    ["export"],
]


class TestHelpCommands:
    """Tier 1: every command group responds to --help without errors."""

    @pytest.mark.parametrize(
        "cmd",
        _HELP_COMMANDS,
        ids=[" ".join(c) if c else "top-level" for c in _HELP_COMMANDS],
    )
    def test_help_exits_cleanly(self, cmd: list[str]) -> None:
        result = run_cli(*cmd, "--help")
        result.assert_success()
        assert "Usage" in result.stdout or "usage" in result.stdout.lower()
```

- [ ] **Step 2: Run to verify help tests pass**

Run: `uv run pytest tests/e2e/test_e2e_smoke.py::TestHelpCommands -v`
Expected: 15 tests PASS.

- [ ] **Step 3: Commit**

```bash
git add tests/e2e/test_e2e_smoke.py
git commit -m "Add E2E smoke tests: Tier 1 help commands

Parametrized test runs --help for every command group. Verifies
the app boots and all commands register without import errors."
```

---

### Task 5: Smoke tests — no-DB commands (Tier 2)

**Files:**
- Modify: `tests/e2e/test_e2e_smoke.py`

- [ ] **Step 1: Add Tier 2 tests to `test_e2e_smoke.py`**

Append after the `TestHelpCommands` class:

```python
# ---------------------------------------------------------------------------
# Tier 2: commands that run without a database
# ---------------------------------------------------------------------------


class TestNoDBCommands:
    """Tier 2: commands that execute real logic but don't need get_database()."""

    def test_profile_list(self, e2e_env: dict[str, str]) -> None:
        result = run_cli("profile", "list", env=e2e_env)
        result.assert_success()

    def test_profile_show(self, e2e_env: dict[str, str]) -> None:
        # May show "no active profile" — that's fine, just no crash
        result = run_cli("profile", "show", env=e2e_env)
        # exit_code may be 0 or 1 depending on whether a profile is set
        assert "Traceback" not in result.stderr

    def test_import_list_formats(self) -> None:
        result = run_cli("import", "list-formats")
        result.assert_success()

    def test_import_preview(self) -> None:
        fixture = FIXTURES_DIR / "tabular" / "standard.csv"
        result = run_cli("import", "preview", str(fixture))
        result.assert_success()

    def test_logs_path(self, e2e_env: dict[str, str]) -> None:
        result = run_cli("logs", "path", env=e2e_env)
        result.assert_success()

    def test_mcp_list_tools(self) -> None:
        result = run_cli("mcp", "list-tools")
        result.assert_success()

    def test_mcp_list_prompts(self) -> None:
        result = run_cli("mcp", "list-prompts")
        result.assert_success()

    def test_db_ps(self) -> None:
        result = run_cli("db", "ps")
        result.assert_success()
```

Also add the import at the top of the file, after the existing imports:

```python
from tests.e2e.conftest import FIXTURES_DIR
```

- [ ] **Step 2: Run to verify Tier 2 tests pass**

Run: `uv run pytest tests/e2e/test_e2e_smoke.py::TestNoDBCommands -v`
Expected: 8 tests PASS.

- [ ] **Step 3: Commit**

```bash
git add tests/e2e/test_e2e_smoke.py
git commit -m "Add E2E smoke tests: Tier 2 no-DB commands

Tests for profile list, import preview, logs path, mcp list-tools,
and other commands that run without a database connection."
```

---

### Task 6: Smoke tests — DB-dependent commands (Tier 3)

**Files:**
- Modify: `tests/e2e/test_e2e_smoke.py`

- [ ] **Step 1: Add Tier 3 tests to `test_e2e_smoke.py`**

Append after the `TestNoDBCommands` class:

```python
# ---------------------------------------------------------------------------
# Tier 3: commands that need an initialized database
# ---------------------------------------------------------------------------


class TestDBCommands:
    """Tier 3: commands that go through get_database() → init_schemas."""

    def test_db_info(self, e2e_profile: dict[str, str]) -> None:
        result = run_cli("db", "info", env=e2e_profile)
        result.assert_success()

    def test_db_query(self, e2e_profile: dict[str, str]) -> None:
        result = run_cli("db", "query", "SELECT 1 AS ok", env=e2e_profile)
        result.assert_success()

    def test_db_migrate_status(self, e2e_profile: dict[str, str]) -> None:
        result = run_cli("db", "migrate", "status", env=e2e_profile)
        result.assert_success()

    def test_transform_status(self, e2e_profile: dict[str, str]) -> None:
        result = run_cli("transform", "status", env=e2e_profile)
        result.assert_success()

    def test_transform_validate(self, e2e_profile: dict[str, str]) -> None:
        result = run_cli("transform", "validate", env=e2e_profile)
        result.assert_success()

    def test_import_status(self, e2e_profile: dict[str, str]) -> None:
        result = run_cli("import", "status", env=e2e_profile)
        result.assert_success()

    def test_import_history(self, e2e_profile: dict[str, str]) -> None:
        result = run_cli("import", "history", env=e2e_profile)
        result.assert_success()

    def test_categorize_stats(self, e2e_profile: dict[str, str]) -> None:
        result = run_cli("categorize", "stats", env=e2e_profile)
        result.assert_success()

    def test_categorize_list_rules(self, e2e_profile: dict[str, str]) -> None:
        result = run_cli("categorize", "list-rules", env=e2e_profile)
        result.assert_success()

    def test_matches_history(self, e2e_profile: dict[str, str]) -> None:
        result = run_cli("matches", "history", env=e2e_profile)
        result.assert_success()

    def test_stats_show(self, e2e_profile: dict[str, str]) -> None:
        result = run_cli("stats", "show", env=e2e_profile)
        result.assert_success()
```

- [ ] **Step 2: Run to verify Tier 3 tests pass**

Run: `uv run pytest tests/e2e/test_e2e_smoke.py::TestDBCommands -v`
Expected: 11 tests PASS. If any fail, they reveal real wiring bugs (like the `source_transaction_id` error).

- [ ] **Step 3: Commit**

```bash
git add tests/e2e/test_e2e_smoke.py
git commit -m "Add E2E smoke tests: Tier 3 DB-dependent commands

Tests for db info, db query, transform status, import history,
categorize stats, matches history, and other commands that go
through get_database() and init_schemas. These catch schema
wiring bugs that in-process tests miss."
```

---

### Task 7: Workflow tests

**Files:**
- Create: `tests/e2e/test_e2e_workflows.py`

- [ ] **Step 1: Write workflow tests**

```python
# ruff: noqa: S101
"""E2E workflow tests — multi-step user flows run as subprocesses."""

from __future__ import annotations

from pathlib import Path

import pytest

from tests.e2e.conftest import FIXTURES_DIR, make_workflow_env, run_cli

pytestmark = pytest.mark.e2e


class TestSyntheticPipeline:
    """Workflow 1: profile create → db init → synthetic generate → transform → query."""

    def test_synthetic_generate_and_transform(
        self,
        e2e_home: Path,
    ) -> None:
        home = e2e_home
        env = make_workflow_env(home, "wf-synthetic")

        # Generate synthetic data (skip transform — we'll run it separately)
        result = run_cli(
            "synthetic",
            "generate",
            "--persona",
            "basic",
            "--profile",
            "wf-synthetic",
            "--skip-transform",
            "--seed",
            "42",
            env=env,
            timeout=120,
        )
        result.assert_success()
        assert "Created" in result.stderr or "Generated" in result.stderr

        # Run transforms
        result = run_cli("transform", "apply", env=env, timeout=180)
        result.assert_success()

        # Verify core tables have data
        result = run_cli(
            "db",
            "query",
            "SELECT COUNT(*) AS n FROM core.fct_transactions",
            env=env,
        )
        result.assert_success()
        # Output should contain a number > 0
        assert "0" != result.stdout.strip().split("\n")[-1].strip()


class TestCSVImportPipeline:
    """Workflow 2: profile create → db init → import CSV → transform → query."""

    def test_csv_import_and_transform(self, e2e_home: Path) -> None:
        home = e2e_home
        env = make_workflow_env(home, "wf-csv")

        fixture = FIXTURES_DIR / "tabular" / "standard.csv"

        # Import CSV
        result = run_cli(
            "import",
            "file",
            str(fixture),
            "--account-id",
            "e2e-test-acct",
            "--skip-transform",
            env=env,
        )
        result.assert_success()

        # Run transforms
        result = run_cli("transform", "apply", env=env, timeout=180)
        result.assert_success()

        # Verify core tables have data
        result = run_cli(
            "db",
            "query",
            "SELECT COUNT(*) AS n FROM core.fct_transactions",
            env=env,
        )
        result.assert_success()


class TestOFXImportPipeline:
    """Workflow 3: profile create → db init → import OFX → transform → query."""

    def test_ofx_import_and_transform(self, e2e_home: Path) -> None:
        home = e2e_home
        env = make_workflow_env(home, "wf-ofx")

        fixture = FIXTURES_DIR / "sample_statement.qfx"

        # Import OFX
        result = run_cli(
            "import",
            "file",
            str(fixture),
            "--skip-transform",
            env=env,
        )
        result.assert_success()

        # Run transforms
        result = run_cli("transform", "apply", env=env, timeout=180)
        result.assert_success()

        # Verify core tables have data
        result = run_cli(
            "db",
            "query",
            "SELECT COUNT(*) AS n FROM core.fct_transactions",
            env=env,
        )
        result.assert_success()


class TestLockUnlockCycle:
    """Workflow 4: profile create → db init → query → lock → unlock → query."""

    def test_lock_unlock_preserves_access(self, e2e_home: Path) -> None:
        from tests.e2e.conftest import _TEST_PASSPHRASE

        home = e2e_home
        env = make_workflow_env(home, "wf-lock")

        # Verify DB works before locking
        result = run_cli("db", "query", "SELECT 1 AS ok", env=env)
        result.assert_success()

        # Lock
        result = run_cli("db", "lock", env=env)
        result.assert_success()

        # Unlock with passphrase
        result = run_cli(
            "db",
            "unlock",
            env=env,
            input_text=f"{_TEST_PASSPHRASE}\n",
        )
        result.assert_success()

        # Verify DB still works
        result = run_cli("db", "query", "SELECT 1 AS ok", env=env)
        result.assert_success()


class TestCategorizationPipeline:
    """Workflow 5: import → transform → seed categories → apply rules → stats."""

    def test_categorize_after_import(self, e2e_home: Path) -> None:
        home = e2e_home
        env = make_workflow_env(home, "wf-categorize")

        fixture = FIXTURES_DIR / "tabular" / "standard.csv"

        # Import
        result = run_cli(
            "import",
            "file",
            str(fixture),
            "--account-id",
            "e2e-cat-acct",
            "--skip-transform",
            env=env,
        )
        result.assert_success()

        # Transform
        result = run_cli("transform", "apply", env=env, timeout=180)
        result.assert_success()

        # Seed categories
        result = run_cli("categorize", "seed", env=env)
        result.assert_success()

        # Apply rules
        result = run_cli("categorize", "apply-rules", env=env)
        result.assert_success()

        # Stats should work
        result = run_cli("categorize", "stats", env=env)
        result.assert_success()
```

- [ ] **Step 2: Run workflow tests**

Run: `uv run pytest tests/e2e/test_e2e_workflows.py -v --timeout=300`
Expected: 5 tests PASS. Some may reveal real bugs (like the `source_transaction_id` error) — that's the point.

- [ ] **Step 3: Commit**

```bash
git add tests/e2e/test_e2e_workflows.py
git commit -m "Add E2E workflow tests: 5 multi-step user flows

Synthetic pipeline, CSV import, OFX import, lock/unlock cycle,
and categorization pipeline. Each creates a fresh profile and
runs the full command sequence as subprocesses."
```

---

### Task 8: Update rules and documentation

**Files:**
- Modify: `.claude/rules/testing.md`
- Modify: `.claude/rules/shipping.md`
- Modify: `docs/specs/INDEX.md`
- Modify: `docs/specs/e2e-testing.md`

- [ ] **Step 1: Add "Test Coverage by Layer" section to `.claude/rules/testing.md`**

Append after the "Coverage Goals" section (after line 41):

```markdown
## Test Coverage by Layer

Every shipped feature must have tests at the appropriate layers:

| Layer | What it catches | Required when |
|---|---|---|
| Unit (`tests/moneybin/`) | Logic bugs, edge cases | Always |
| Integration (`tests/integration/`) | Cross-subsystem wiring | Feature touches >1 subsystem |
| E2E smoke (`tests/e2e/test_e2e_smoke.py`) | Boot/schema/init/registration errors | Feature adds or modifies a CLI command |
| E2E workflow (`tests/e2e/test_e2e_workflows.py`) | Multi-step pipeline breakage | Feature adds a user-facing workflow |

- New CLI commands: add to the smoke test parametrize list in `tests/e2e/test_e2e_smoke.py`
- New import formats or data sources: add an E2E workflow test that imports a fixture file
- New DB schema changes: covered automatically by existing smoke tests (they exercise `init_schemas`)
- Unit tests alone are not sufficient for shipped features that add CLI commands or cross subsystem boundaries
```

- [ ] **Step 2: Update the Markers section in `.claude/rules/testing.md`**

Update the markers section to include `e2e`:

```python
@pytest.mark.unit         # Fast unit tests (default)
@pytest.mark.integration  # Requires external systems
@pytest.mark.e2e          # End-to-end subprocess tests
@pytest.mark.slow         # Long-running
```

- [ ] **Step 3: Update the Commands section in `.claude/rules/testing.md`**

Add E2E commands:

```bash
uv run pytest tests/ -v                                    # All tests
uv run pytest tests/ -v -m "not integration and not e2e"   # Unit only
uv run pytest tests/e2e/ -v -m "e2e"                       # E2E only
uv run pytest tests/test_file.py -v                        # Specific file
uv run pytest tests/ --cov=src/moneybin --cov-report=html  # Coverage
```

- [ ] **Step 4: Add "Test Layer Check" to `.claude/rules/shipping.md`**

Append before the "## Principle" section:

```markdown
### Test Layer Check

Before marking a spec as `implemented`, verify the feature has tests at every applicable layer (see testing.md "Test Coverage by Layer"). Unit tests alone are not sufficient for features that add CLI commands or cross subsystem boundaries.
```

- [ ] **Step 5: Update `docs/specs/e2e-testing.md` status**

Change `**Status:** draft` to `**Status:** in-progress`.

- [ ] **Step 6: Add E2E testing spec to `docs/specs/INDEX.md`**

Add a row to the "Testing & Validation" table:

```markdown
| [E2E Testing](e2e-testing.md) | Feature | in-progress | Subprocess-based CLI smoke tests and workflow tests; test directory reorganization |
```

- [ ] **Step 7: Commit**

```bash
git add .claude/rules/testing.md .claude/rules/shipping.md docs/specs/INDEX.md docs/specs/e2e-testing.md
git commit -m "Update rules and docs for E2E test coverage requirements

Add 'Test Coverage by Layer' section to testing rules, add pre-ship
test layer check to shipping rules, update spec index and status."
```

---

### Task 9: Fix any failures and finalize

- [ ] **Step 1: Run the full E2E suite**

Run: `uv run pytest tests/e2e/ -v -m "e2e"`
Expected: All tests PASS. If any Tier 3 or workflow tests fail, they indicate real bugs.

- [ ] **Step 2: Run the full test suite to verify no regressions**

Run: `uv run pytest tests/ -m "not integration and not e2e" -q`
Expected: Same pass count as baseline. The integration test move should not break anything.

- [ ] **Step 3: Run format and lint**

Run: `make format && make lint`
Expected: Clean.

- [ ] **Step 4: Run type check on new files**

Run: `uv run pyright tests/e2e/`
Expected: No errors. If pyright complains about `e2e_home` type annotations, add explicit `Path` types.

- [ ] **Step 5: Fix any issues discovered by the E2E tests**

If any E2E tests reveal real bugs (like the `source_transaction_id` schema error), fix them. These are exactly the bugs E2E tests are designed to catch. Fix the root cause, don't skip the test.

- [ ] **Step 6: Run E2E suite again to confirm all green**

Run: `uv run pytest tests/e2e/ -v -m "e2e"`
Expected: All PASS.

- [ ] **Step 7: Final commit if fixes were needed**

```bash
git add -A
git commit -m "Fix issues discovered by E2E tests

<describe what was fixed>"
```

- [ ] **Step 8: Update spec status to `implemented`**

In `docs/specs/e2e-testing.md`, change status to `implemented`.
In `docs/specs/INDEX.md`, change the E2E Testing row status to `implemented`.

```bash
git add docs/specs/e2e-testing.md docs/specs/INDEX.md
git commit -m "Mark E2E testing spec as implemented"
```
