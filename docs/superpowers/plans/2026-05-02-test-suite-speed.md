# Test Suite + CLI Cold-Start Speedup Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Cut full test-suite wall time from ~8–10 minutes toward ~5–6 minutes by (1) eliminating per-test fixture rebuild work, (2) replacing subprocess-based help tests with in-process invocations, and (3) cutting CLI cold-start time so every E2E `run_cli()` boots faster.

**Architecture:** Two checkpoints on a single PR:

- **Checkpoint 1 — test infrastructure only.** Snapshot-and-copy fixtures (mcp_db, e2e profile), bypass the `uv run` wrapper in `run_cli()`, convert `--help` E2E parametrize cases to in-process `CliRunner` invocations (keeping one subprocess boot smoke), and session-scope `Database` for read-only unit modules where safe. Zero product-code changes.
- **Checkpoint 2 — product cold-start improvements.** Lazy-load all 13 CLI command groups uniformly, defer heavy transitive imports (fastmcp, sqlmesh, polars) into command bodies, cache the SQLMesh `Context` per-process, and memoize `init_schemas` against a DDL hash so re-opening an already-initialized DB is a no-op.

The two checkpoints are deliberate landing points where we can `/clear` or `/compact` between commits without losing context — Checkpoint 1 is fully self-contained and revertible if Checkpoint 2 misbehaves.

**Tech Stack:** Python 3.12, pytest + pytest-xdist, Typer, DuckDB, SQLMesh, fastmcp, uv.

**Branch:** `perf/test-suite-speed` (already created in `.worktrees/test-suite-speed`).

**Conventions:**
- No `Co-Authored-By: Claude` trailers in commits (per `.claude/rules/branching.md`).
- Commit subjects imperative, under 72 chars.
- `make check test` before each checkpoint commit.
- All test invocations use `uv run pytest`, never `python -m pytest`.

---

## Pre-Work: Establish Baseline

### Task 0: Capture full-suite baseline timings

**Files:**
- Create: `docs/superpowers/plans/baselines/2026-05-02-baseline.txt` (untracked artifact, deleted before commit)

- [ ] **Step 1: Run unit suite with `--durations`**

```bash
uv run pytest tests/moneybin -m "not integration and not e2e and not scenarios and not slow" \
  --durations=25 -q 2>&1 | tee /tmp/baseline-unit.txt
```

Expected: ~36s wall time, 1,420 passed. Record total wall time and top-25 slowest.

- [ ] **Step 2: Run integration suite with `--durations`**

```bash
uv run pytest tests/integration -m integration --durations=25 -q 2>&1 | tee /tmp/baseline-integration.txt
```

Expected: passes, captures slowest tests.

- [ ] **Step 3: Run e2e suite with `--durations`**

```bash
uv run pytest tests/e2e -m e2e --durations=25 -q 2>&1 | tee /tmp/baseline-e2e.txt
```

Expected: passes, top durations dominated by `make_workflow_env` and Argon2-bearing tests.

- [ ] **Step 4: Run scenario suite with `--durations`**

```bash
uv run pytest tests/scenarios -m scenarios --durations=25 -q 2>&1 | tee /tmp/baseline-scenarios.txt
```

Expected: 18 scenarios pass.

- [ ] **Step 5: Record totals in plan log**

Capture the four `real` wall-time numbers in a comment on the plan or scratch notes — these are the comparison points for end-of-checkpoint measurements. Do not commit the baseline files.

- [ ] **Step 6: Confirm cold-start CLI baseline**

```bash
uv run moneybin --help > /dev/null  # warm uv cache
time uv run moneybin --help > /dev/null
```

Expected: ~0.83s. Record it.

```bash
uv run python -X importtime -c "import moneybin.cli.main" 2>&1 | tail -30
```

Expected: `fastmcp` showing as a top contributor (~480ms cumulative). Record the top 5 modules by self-time.

---

# CHECKPOINT 1 — Test Infrastructure (no product code)

## Task 1: Add `--durations=25` to Makefile and CI

**Files:**
- Modify: `Makefile:133-156` (test-* targets)
- Modify: `.github/workflows/ci.yml`
- Modify: `.github/workflows/scenarios.yml`

- [ ] **Step 1: Update Makefile test targets**

Apply the `--durations=25` flag to every test target so future regressions surface in the output:

```makefile
test-unit: venv ## Development: Run unit tests only (excludes integration and e2e tests)
	@echo "Running unit tests..."
	@uv run pytest tests/ -m "not integration and not e2e" --durations=25

test: test-unit ## Development: Run unit tests (alias for test-unit)

test-all: venv ## Development: Run all tests (unit, integration, e2e) with verbose output
	@echo "Running all tests..."
	@uv run pytest tests/ -v --durations=25

test-cov: venv ## Development: Run tests with coverage report
	@echo "Running tests with coverage..."
	@uv run pytest --cov=src tests/ -m "not integration and not e2e" --durations=25

test-integration: venv ## Development: Run integration tests only
	@echo "Running integration tests..."
	@uv run pytest tests/ -m "integration" --durations=25

test-e2e: venv ## Development: Run end-to-end subprocess tests
	@echo "Running end-to-end subprocess tests..."
	@uv run pytest tests/e2e/ -m "e2e" -v --durations=25

test-scenarios: venv ## Development: Run all whole-pipeline scenarios via pytest
	@echo "Running all scenarios..."
	@uv run pytest tests/scenarios/ -m scenarios -v --durations=25
```

- [ ] **Step 2: Add `--durations=25` to CI workflows**

In `.github/workflows/ci.yml`, find the pytest invocation and append `--durations=25`. Same for `.github/workflows/scenarios.yml`. Read each file first to find the exact line.

- [ ] **Step 3: Verify Makefile syntax**

```bash
make -n test-unit
```

Expected: prints the command; no syntax errors.

- [ ] **Step 4: Commit**

```bash
git add Makefile .github/workflows/ci.yml .github/workflows/scenarios.yml
git commit -m "Add --durations=25 to test runs to surface slow tests"
```

---

## Task 2: Bypass `uv run` wrapper in `run_cli()`

**Files:**
- Modify: `tests/e2e/conftest.py:88-140` (`run_cli` function)
- Test: existing E2E tests must continue to pass

The current `run_cli()` invokes `["uv", "run", "moneybin", ...]`. `uv run` re-resolves the project on every invocation (a few hundred ms each). The venv exists by the time tests run — call its `moneybin` script directly.

- [ ] **Step 1: Read the current run_cli implementation**

```bash
sed -n '88,140p' tests/e2e/conftest.py
```

Confirm the current `cmd = ["uv", "run", "moneybin", *args]` line.

- [ ] **Step 2: Compute the venv binary path at module load**

Replace the `cmd = ["uv", "run", "moneybin", *args]` line in `run_cli()` with a venv-binary-direct invocation. Add at module top (after the existing imports):

```python
import sys

# Resolve the moneybin entrypoint inside the active venv. Tests run under
# `uv run pytest`, which prepends `.venv/bin` to PATH and sets sys.executable
# to the venv's python — so its sibling `moneybin` is what we want. Calling
# the script directly skips uv's per-invocation project-resolve overhead
# (~200ms × 105 calls in the E2E suite).
_VENV_BIN = Path(sys.executable).parent
_MONEYBIN_BIN = _VENV_BIN / "moneybin"
if not _MONEYBIN_BIN.exists():
    msg = (
        f"moneybin entrypoint not found at {_MONEYBIN_BIN}. "
        f"Run `uv sync` to populate the venv before running E2E tests."
    )
    raise RuntimeError(msg)
```

- [ ] **Step 3: Update `run_cli()` to use the resolved binary**

In `run_cli()`, replace:

```python
cmd = ["uv", "run", "moneybin", *args]  # noqa: S607 — uv is on PATH in dev environments
```

with:

```python
cmd = [str(_MONEYBIN_BIN), *args]
```

The `# noqa: S607` is no longer needed because we're using an absolute path.

- [ ] **Step 4: Run the help tier first as a smoke check**

```bash
uv run pytest tests/e2e/test_e2e_help.py -m e2e -v --durations=10
```

Expected: all help tests pass; per-test wall time noticeably lower (was ~1s, should be ~0.5-0.7s).

- [ ] **Step 5: Run the rest of the E2E suite**

```bash
uv run pytest tests/e2e -m e2e --durations=25
```

Expected: all 66 e2e tests pass; total wall time lower than baseline.

- [ ] **Step 6: Commit**

```bash
git add tests/e2e/conftest.py
git commit -m "Call .venv/bin/moneybin directly in E2E run_cli (skip uv resolver)"
```

---

## Task 3: Snapshot the `mcp_db` baseline (generate-on-first-test)

**Files:**
- Modify: `tests/moneybin/test_mcp/conftest.py` (full rewrite of `mcp_db` fixture)
- Test: `tests/moneybin/test_mcp/` — all 138 tests must continue to pass

The current `mcp_db` fixture re-runs `create_core_tables_raw()` plus 6 `INSERT` statements for every one of 138 tests. Snapshot the baseline once per session, then `shutil.copy` the encrypted DuckDB file into each test's `tmp_path`.

- [ ] **Step 1: Replace the conftest with a session-template + per-test copy**

Replace the entire body of `tests/moneybin/test_mcp/conftest.py` with:

```python
"""Shared fixtures for MCP tests.

`_mcp_db_template` builds the baseline encrypted DuckDB once per session
(core tables + base reference data). `mcp_db` then copies the file into
each test's tmp_path so every test gets an isolated database without
re-running the schema DDL or 6 baseline INSERTs.

Base reference data:
- 2 institutions (Test Bank, Other Bank)
- 2 accounts (ACC001 CHECKING, ACC002 SAVINGS)
- 2 account balances
"""

from __future__ import annotations

import shutil
from collections.abc import Generator
from pathlib import Path
from unittest.mock import MagicMock

import pytest

import moneybin.database as db_module
from moneybin.database import Database
from tests.moneybin.db_helpers import create_core_tables_raw

_MOCK_KEY = "test-encryption-key-256bit-placeholder"


def _make_mock_store() -> MagicMock:
    store = MagicMock()
    store.get_key.return_value = _MOCK_KEY
    return store


@pytest.fixture(scope="session")
def _mcp_db_template(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Build the baseline encrypted DB once per session and return its path."""
    template_dir = tmp_path_factory.mktemp("mcp_db_template")
    template_path = template_dir / "template.duckdb"

    database = Database(
        template_path, secret_store=_make_mock_store(), no_auto_upgrade=True
    )
    conn = database.conn
    create_core_tables_raw(conn)

    conn.execute("""
        INSERT INTO raw.ofx_institutions
            (organization, fid, source_file, extracted_at, loaded_at, import_id, source_type)
        VALUES
        ('Test Bank', '1234', 'test.qfx', '2025-01-01', CURRENT_TIMESTAMP, NULL, 'ofx'),
        ('Other Bank', '5678', 'other.qfx', '2025-01-01', CURRENT_TIMESTAMP, NULL, 'ofx')
    """)

    conn.execute("""
        INSERT INTO core.dim_accounts VALUES
        ('ACC001', '111000025', 'CHECKING', 'Test Bank', '1234', 'ofx',
         'test.qfx', '2025-01-01', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
        ('ACC002', '222000050', 'SAVINGS', 'Other Bank', '5678', 'ofx',
         'other.qfx', '2025-01-01', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
    """)

    conn.execute("""
        INSERT INTO raw.ofx_balances
            (account_id, statement_start_date, statement_end_date, ledger_balance,
             ledger_balance_date, available_balance, source_file,
             extracted_at, loaded_at, import_id, source_type)
        VALUES
        ('ACC001', '2025-06-01', '2025-06-30', 5000.00,
         '2025-06-30', 4800.00, 'test.qfx',
         '2025-01-24', CURRENT_TIMESTAMP, NULL, 'ofx'),
        ('ACC002', '2025-06-01', '2025-06-30', 15000.00,
         '2025-06-30', 15000.00, 'other.qfx',
         '2025-01-24', CURRENT_TIMESTAMP, NULL, 'ofx')
    """)

    database.close()
    return template_path


@pytest.fixture()
def mcp_db(tmp_path: Path, _mcp_db_template: Path) -> Generator[Database, None, None]:
    """Per-test Database initialized from a snapshot of the session template.

    Copies the baseline encrypted DuckDB file into the test's tmp_path,
    opens it, and injects the singleton so MCP server functions resolve
    against this DB. Restores the singleton on teardown.
    """
    db_path = tmp_path / "test.duckdb"
    shutil.copy(_mcp_db_template, db_path)

    database = Database(db_path, secret_store=_make_mock_store(), no_auto_upgrade=True)

    db_module._database_instance = database  # type: ignore[reportPrivateUsage] — test fixture
    try:
        yield database
    finally:
        db_module._database_instance = None  # type: ignore[reportPrivateUsage] — test fixture
        database.close()
```

- [ ] **Step 2: Run the MCP test suite**

```bash
uv run pytest tests/moneybin/test_mcp -v --durations=15
```

Expected: all 138 MCP tests pass. Per-test setup time should drop noticeably (was ~250ms each, target <50ms).

- [ ] **Step 3: Verify encryption survives the file copy**

This is a critical correctness check — DuckDB encryption is per-file with a key in the connection. Copying the file and reopening with the same key must work. The Step 2 run validates this implicitly. If any test fails with a "could not decrypt" or "checksum mismatch" error, the snapshot approach is broken. In that case:

- Try `database.close()` followed by `shutil.copy` — DuckDB should flush the WAL to the encrypted file on close.
- If still broken, fall back to `database.execute("CHECKPOINT")` before closing the template, then copy.

- [ ] **Step 4: Commit**

```bash
git add tests/moneybin/test_mcp/conftest.py
git commit -m "Snapshot mcp_db baseline once per session, copy per test"
```

---

## Task 4: Snapshot the E2E profile (generate-on-first-test)

**Files:**
- Modify: `tests/e2e/conftest.py` (add `_mutating_profile_template` session fixture and `make_workflow_env_fast` helper)
- Modify: `tests/e2e/test_e2e_mutating.py` (use `make_workflow_env_fast` for the simple-isolation case)
- Modify: `tests/e2e/test_e2e_workflows.py` if it uses `make_workflow_env` (audit and update where safe)

`make_workflow_env()` runs `profile create` for every mutating test (47 calls in `test_e2e_mutating.py` alone). Each call boots `moneybin`, derives an Argon2 key, attaches an encrypted DB, and writes profile config. Snapshot one ready-made profile dir per session, then `shutil.copytree` per test.

- [ ] **Step 1: Add the session-scoped template fixture**

Append to `tests/e2e/conftest.py` after the existing `make_workflow_env` definition:

```python
# ---------------------------------------------------------------------------
# Snapshot-based fast workflow fixture
# ---------------------------------------------------------------------------

# A fixed profile name used inside the template snapshot. Each mutating test
# copies the entire MONEYBIN_HOME tree into its own isolated tmp_path, so
# they never collide on this name despite sharing it.
_TEMPLATE_PROFILE_NAME = "e2e-template"


@pytest.fixture(scope="session")
def _mutating_profile_template(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """One-shot MONEYBIN_HOME with `e2e-template` profile created and DB initialized.

    Built once per pytest session by running `moneybin profile create` against
    a temp home. `make_workflow_env_fast` then copies this tree into each
    mutating test's tmp_path — skipping the per-test `profile create` cost
    (Argon2 key derivation + encrypted DB init + profile config write).
    """
    template_home = tmp_path_factory.mktemp("e2e_profile_template")
    env = base_env(template_home, _TEMPLATE_PROFILE_NAME)
    env["MONEYBIN_IMPORT___INBOX_ROOT"] = str(template_home / "inbox-root")

    result = run_cli("profile", "create", _TEMPLATE_PROFILE_NAME, env=env)
    if result.exit_code != 0:
        msg = f"Failed to build profile snapshot: {result.stderr}"
        raise AssertionError(msg)

    return template_home


def make_workflow_env_fast(
    e2e_home: Path,
    profile_name: str,
    template: Path,
) -> dict[str, str]:
    """Faster equivalent of `make_workflow_env()`.

    Copies the session-built profile template into `e2e_home / <profile_name>`
    instead of running `profile create`. The profile keeps its template name
    inside the copied tree (`profiles/e2e-template/`), and the env dict points
    `MONEYBIN_PROFILE` at that name — tests that hard-coded a different
    profile name should keep using `make_workflow_env()`.

    Returns the env dict (same shape as `make_workflow_env`).
    """
    target_home = e2e_home / profile_name
    if target_home.exists():
        shutil.rmtree(target_home)
    shutil.copytree(template, target_home)

    env = base_env(target_home, _TEMPLATE_PROFILE_NAME)
    env["MONEYBIN_IMPORT___INBOX_ROOT"] = str(target_home / "inbox-root")
    return env
```

- [ ] **Step 2: Migrate one test in `test_e2e_mutating.py` as a smoke check**

Pick the simplest test that uses `make_workflow_env(tmp_path, "...")` — find it with:

```bash
grep -n "make_workflow_env" tests/e2e/test_e2e_mutating.py | head -5
```

Change the call site from:

```python
env = make_workflow_env(tmp_path, "dbcheck")
```

to:

```python
env = make_workflow_env_fast(tmp_path, "dbcheck", _mutating_profile_template)
```

…and add `_mutating_profile_template: Path` to the test's parameters (pytest will inject it from the session fixture).

Run just that one test:

```bash
uv run pytest tests/e2e/test_e2e_mutating.py::<TestClass>::<test_name> -v
```

Expected: passes. If it fails because the test asserts on a specific profile name in output, keep it on `make_workflow_env()` and pick a different test.

- [ ] **Step 3: Migrate the remaining mutating tests**

Audit each `make_workflow_env(...)` call site in `test_e2e_mutating.py`:

- If the test only needs a working profile + DB and doesn't assert on the profile name, swap to `make_workflow_env_fast(...)`.
- If the test creates additional profiles via subsequent `profile create` calls, leave it on `make_workflow_env()` (the snapshot only covers the *first* profile).
- If the test asserts on profile-name output (e.g., `assert "dbcheck" in result.stdout`), leave it on `make_workflow_env()`.

For migrated tests, also add the `_mutating_profile_template: Path` parameter.

- [ ] **Step 4: Run the full mutating suite**

```bash
uv run pytest tests/e2e/test_e2e_mutating.py -m e2e --durations=25
```

Expected: all pass. Wall-time drop should be substantial (was ~1.5s × 47 calls = 70s of `profile create`; target: amortized to ~1 setup call total).

- [ ] **Step 5: Audit `test_e2e_workflows.py`**

```bash
grep -n "make_workflow_env" tests/e2e/test_e2e_workflows.py
```

For each call site that doesn't depend on a custom profile name, switch to `make_workflow_env_fast`. Same rules as Step 3.

```bash
uv run pytest tests/e2e/test_e2e_workflows.py -m e2e --durations=25
```

Expected: all pass.

- [ ] **Step 6: Commit**

```bash
git add tests/e2e/conftest.py tests/e2e/test_e2e_mutating.py tests/e2e/test_e2e_workflows.py
git commit -m "Snapshot E2E profile per session; copy per mutating test"
```

---

## Task 5: Convert `test_e2e_help.py` to in-process `CliRunner`

**Files:**
- Modify: `tests/e2e/test_e2e_help.py` (convert parametrized cases to in-process)
- Create: nothing (the existing file is rewritten)

`--help` is documentation rendering. It doesn't need subprocess fidelity — it needs to verify Typer wiring and command registration. One subprocess boot smoke is enough for the wiring; the other 31 cases can run in-process for ~30× speedup.

- [ ] **Step 1: Add a `CliRunner`-backed test class for the bulk help cases**

Replace the contents of `tests/e2e/test_e2e_help.py` with:

```python
# ruff: noqa: S101
"""E2E help tests — every command group responds to --help without errors.

Most cases run in-process via Typer's CliRunner since `--help` is pure
documentation rendering. One subprocess boot smoke (`moneybin --help`)
catches packaging/entry-point regressions. CLI cold-start fidelity for
real commands is exercised by the other E2E tiers (readonly, mutating,
workflows, mcp).
"""

from __future__ import annotations

import pytest
from typer.testing import CliRunner

from moneybin.cli.main import app
from tests.e2e.conftest import run_cli

pytestmark = pytest.mark.e2e

_HELP_COMMANDS: list[list[str]] = [
    [],  # moneybin --help
    ["profile"],
    ["import"],
    ["import", "inbox"],
    ["import", "inbox", "list"],
    ["import", "inbox", "path"],
    ["import", "formats"],
    ["sync"],
    ["categorize"],
    ["categorize", "auto"],
    ["categorize", "auto", "review"],
    ["categorize", "auto", "confirm"],
    ["categorize", "auto", "stats"],
    ["categorize", "auto", "rules"],
    ["matches"],
    ["transform"],
    ["synthetic"],
    ["db"],
    ["db", "key"],
    ["db", "migrate"],
    ["logs"],
    ["mcp"],
    ["mcp", "config"],
    ["stats"],
    ["track"],
    ["track", "balance"],
    ["track", "networth"],
    ["track", "budget"],
    ["track", "recurring"],
    ["track", "investments"],
    ["export"],
    ["sync", "schedule"],
    ["sync", "key"],
]

_runner = CliRunner()


class TestHelpCommandsInProcess:
    """Every command group responds to --help without errors (in-process)."""

    @pytest.mark.parametrize(
        "cmd",
        _HELP_COMMANDS,
        ids=[" ".join(c) if c else "top-level" for c in _HELP_COMMANDS],
    )
    def test_help_exits_cleanly(self, cmd: list[str]) -> None:
        result = _runner.invoke(app, [*cmd, "--help"])
        assert result.exit_code == 0, (
            f"--help exited {result.exit_code} for {cmd}\noutput: {result.output}"
        )
        assert "Usage" in result.output or "usage" in result.output.lower()


class TestHelpCommandBootSmoke:
    """One subprocess invocation to catch packaging/entry-point regressions."""

    def test_top_level_help_via_subprocess(self) -> None:
        result = run_cli("--help")
        result.assert_success()
        assert "Usage" in result.stdout or "usage" in result.stdout.lower()
```

- [ ] **Step 2: Run the help suite**

```bash
uv run pytest tests/e2e/test_e2e_help.py -m e2e -v --durations=10
```

Expected: 33 tests pass (32 in-process + 1 subprocess boot smoke). In-process tests should each take <50ms; subprocess test takes ~0.7s.

- [ ] **Step 3: Verify the conftest's plain-help patch is consistent**

The root `tests/conftest.py` patches `typer.Typer.__init__` to disable `rich_markup_mode` so substring checks on help text work. Confirm `CliRunner` invocations also hit this code path:

```bash
uv run pytest tests/e2e/test_e2e_help.py::TestHelpCommandsInProcess -v -k top-level
```

Expected: passes (would fail if rich-mode bold escapes leaked into output).

- [ ] **Step 4: Commit**

```bash
git add tests/e2e/test_e2e_help.py
git commit -m "Move E2E --help cases in-process via CliRunner; keep boot smoke"
```

---

## Task 6: Audit & convert read-only unit test modules to module-scoped `Database`

**Files:**
- Audit: every file under `tests/moneybin/` that uses the function-scoped `db` fixture
- Modify: ~5–15 module files identified as fully read-only against `db`
- Test: same modules — must continue to pass

This task is the highest-effort low-risk lever in Checkpoint 1. Expect to audit ~30 candidate files; only a subset will qualify (any test that mutates `db` disqualifies the module). Time-box this task — if more than 10 files don't match the pattern after 30 minutes of audit, stop and commit what's done.

- [ ] **Step 1: Identify candidate modules**

```bash
grep -rln "def test_" tests/moneybin/ | xargs grep -l "\bdb:\s*Database\b\|\bdb\s*:" | head -30
```

For each candidate file, scan for mutations against `db`:

```bash
grep -nE "db\.(execute|ingest_dataframe|load_|insert)" tests/<file>
```

If the file uses only `db.execute("SELECT ...")` or read-only operations, it's a candidate. If any test calls `db.execute("INSERT ...")`, `CREATE TABLE`, `DROP`, or `ingest_dataframe()`, the file mutates and is **not** a candidate.

- [ ] **Step 2: Convert one file as a smoke check**

Pick a small candidate file (5-15 tests). At the top of the module add:

```python
import pytest

pytestmark = pytest.mark.usefixtures("module_db")
```

Then add a module-scoped fixture either in the file or in a sibling `conftest.py`:

```python
@pytest.fixture(scope="module")
def module_db(
    tmp_path_factory: pytest.TempPathFactory,
) -> Generator[Database, None, None]:
    """Module-scoped read-only Database. Tests must not mutate."""
    from unittest.mock import MagicMock

    mock_store = MagicMock()
    mock_store.get_key.return_value = "test-encryption-key-for-unit-tests"

    db_path = tmp_path_factory.mktemp("module_db") / "test.duckdb"
    database = Database(db_path, secret_store=mock_store, no_auto_upgrade=True)
    yield database
    database.close()
```

Replace `db: Database` parameters with `module_db: Database` in the test signatures. Run:

```bash
uv run pytest tests/moneybin/<file> -v -n0
```

The `-n0` is important during the audit — xdist can mask cross-test pollution that would surface in serial runs.

- [ ] **Step 3: Run the same file under xdist**

```bash
uv run pytest tests/moneybin/<file> -v
```

Expected: still passes. If it doesn't, the module isn't truly read-only — revert and move on.

- [ ] **Step 4: Repeat for 4–14 more candidate modules**

Time-box to 30 minutes total for audit + conversion. Skip any module that doesn't cleanly fit.

- [ ] **Step 5: Run the full unit suite**

```bash
uv run pytest tests/moneybin -m "not integration and not e2e and not scenarios and not slow" --durations=25
```

Expected: all 1,420 pass. Wall time should drop modestly (each converted module saves N × ~150ms of fixture setup).

- [ ] **Step 6: Commit**

```bash
git add tests/moneybin/
git commit -m "Module-scope Database fixture for read-only test modules"
```

---

## Task 7: Capture mid-checkpoint baseline; commit Checkpoint 1

**Files:** none

- [ ] **Step 1: Re-run all four suites**

```bash
uv run pytest tests/moneybin -m "not integration and not e2e and not scenarios and not slow" --durations=10 -q 2>&1 | tee /tmp/cp1-unit.txt
uv run pytest tests/integration -m integration --durations=10 -q 2>&1 | tee /tmp/cp1-integration.txt
uv run pytest tests/e2e -m e2e --durations=10 -q 2>&1 | tee /tmp/cp1-e2e.txt
uv run pytest tests/scenarios -m scenarios --durations=10 -q 2>&1 | tee /tmp/cp1-scenarios.txt
```

Compare each against the baseline. Expected savings:

| Suite | Baseline | After CP1 | Δ |
|---|---|---|---|
| Unit | ~36s | ~25–30s | mcp_db snapshot + module-scope db |
| E2E | depends on existing total | substantially lower | help in-process + uv-bypass + profile snapshot |
| Integration | unchanged | unchanged | no integration changes in CP1 |
| Scenarios | unchanged | unchanged | no scenario changes in CP1 |

- [ ] **Step 2: Run pre-commit checks**

```bash
make check test
```

Expected: format, lint, type-check, tests all pass.

- [ ] **Step 3: Verify the checkpoint commit graph is clean**

```bash
git log --oneline main..HEAD
```

Expected: ~6 commits (one per Task 1–6), no merge commits, no `Co-Authored-By` trailers.

- [ ] **Step 4: Tag the checkpoint mentally**

Note the commit SHA at the end of Checkpoint 1 — this is the safe-revert point if Checkpoint 2 introduces regressions. Optionally clear/compact context here before continuing.

---

# CHECKPOINT 2 — App-Level Cold-Start Improvements

## Task 8: Lazy-load all 13 CLI command groups uniformly

**Files:**
- Modify: `src/moneybin/cli/main.py:14-152` (rewrite imports + registration)
- Test: `tests/e2e/test_e2e_help.py` and full E2E suite must pass

The eager `from .commands import (...)` block forces every command's transitive imports at CLI startup. Move every command's import behind a small loader so only the dispatched group's modules load.

- [ ] **Step 1: Define a lazy command-group registration helper**

At the top of `src/moneybin/cli/main.py`, after `import typer`, add:

```python
from collections.abc import Callable
from importlib import import_module


def _add_lazy_typer(
    parent: typer.Typer,
    module_path: str,
    name: str,
    help_text: str,
    *,
    attr: str = "app",
) -> None:
    """Register a sub-Typer that imports its module on first dispatch.

    Eagerly importing all command modules at CLI startup pulls in heavy
    transitive deps (fastmcp, sqlmesh, polars) for every invocation,
    including `moneybin --help` and unrelated commands. This shim defers
    the import until the user actually dispatches into the group.
    """
    placeholder = typer.Typer(
        name=name, help=help_text, no_args_is_help=True, rich_markup_mode=None
    )

    @placeholder.callback(invoke_without_command=True)
    def _load(ctx: typer.Context) -> None:
        module = import_module(module_path)
        real_app: typer.Typer = getattr(module, attr)
        # Replace the placeholder with the real Typer for this dispatch.
        # ctx.parent.command's typer_instance refers to the placeholder; we
        # rewrite it so subcommand resolution finds the real groups.
        # NB: typer/click resolves groups by name on each invocation, so
        # adding the real subcommands to the placeholder here is sufficient.
        for sub_name, sub_cmd in real_app.registered_commands:
            placeholder.command(name=sub_name)(sub_cmd.callback)
        for sub_typer in real_app.registered_groups:
            placeholder.add_typer(sub_typer.typer_instance, name=sub_typer.name)

    parent.add_typer(placeholder, name=name, help=help_text)


def _add_lazy_command(
    parent: typer.Typer,
    module_path: str,
    name: str,
    help_text: str,
    func: str,
) -> None:
    """Register a leaf command whose module imports on first invocation."""

    @parent.command(name=name, help=help_text)
    def _wrapper(*args: object, **kwargs: object) -> object:
        module = import_module(module_path)
        return getattr(module, func)(*args, **kwargs)
```

**IMPORTANT — VERIFY THIS API.** The Typer/Click internals (`registered_commands`, `registered_groups`, `typer_instance`) are stable across recent Typer versions but warrant a sanity check before relying on them. Run:

```bash
uv run python -c "import typer; t = typer.Typer(); print(dir(t))" | tr ',' '\n' | grep -i 'register\|command\|group' | head
```

If `registered_commands` / `registered_groups` aren't present in the project's Typer version, fall back to the simpler approach below (Step 1b).

- [ ] **Step 1b: Simpler fallback if Typer internals don't expose registered_*$**

If Step 1's `registered_commands`/`registered_groups` access doesn't work, use a transparent import deferral instead:

```python
def _add_lazy_typer(
    parent: typer.Typer,
    module_path: str,
    name: str,
    help_text: str,
    *,
    attr: str = "app",
) -> None:
    """Lazy version of `parent.add_typer(<imported>.app, name=...)`.

    Defers `import_module(module_path)` until argv resolution actually
    needs the sub-typer. We accomplish this by overriding `add_typer` with
    a placeholder Typer whose callback imports the real module and re-runs
    the parent dispatch with the real sub-typer attached.
    """
    real_app: typer.Typer | None = None

    def _resolve() -> typer.Typer:
        nonlocal real_app
        if real_app is None:
            real_app = getattr(import_module(module_path), attr)
        return real_app

    # Use Typer's lazy add_typer by providing a callable that resolves
    # the sub-typer at first invocation. If your Typer version doesn't
    # support callables here, fall back to step 1c.
    parent.add_typer(_resolve, name=name, help=help_text)
```

- [ ] **Step 1c: Final fallback — module-level lazy-import boilerplate**

If neither Step 1 nor Step 1b works in this Typer version, accept a slightly larger diff: replace the eager `from .commands import (...)` with module-level lazy attribute access using `__getattr__`. Skip the helper and instead modify `src/moneybin/cli/commands/__init__.py`:

```python
"""CLI command modules for MoneyBin (lazy-loaded)."""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from . import (  # noqa: F401 — stub for static type checkers only
        categorize,
        db,
        import_cmd,
        logs,
        matches,
        mcp,
        migrate,
        profile,
        stats,
        sync,
        synthetic,
        transform,
    )

_LAZY = {
    "categorize",
    "db",
    "import_cmd",
    "logs",
    "matches",
    "mcp",
    "migrate",
    "profile",
    "stats",
    "sync",
    "synthetic",
    "transform",
}


def __getattr__(name: str) -> object:
    if name in _LAZY:
        return import_module(f".{name}", __name__)
    msg = f"module {__name__!r} has no attribute {name!r}"
    raise AttributeError(msg)
```

This makes `from .commands import mcp` resolve lazily *the first time `mcp` is referenced*. Combined with deferring the module references in `main.py` until inside the `add_typer` calls (i.e., reordering so `commands.mcp` is only mentioned when constructing the registration), this gets the desired effect without touching Typer internals.

- [ ] **Step 2: Pick whichever of Step 1 / 1b / 1c works in this codebase and apply it**

Run the cold-start measurement to pick the simplest variant that demonstrably defers `fastmcp`:

```bash
uv run python -X importtime -c "import moneybin.cli.main" 2>&1 | grep -i fastmcp
```

If `fastmcp` no longer appears in the output, the lazy path works. If it still appears, escalate to Step 1c.

- [ ] **Step 3: Rewrite `main.py` registration block**

With the lazy mechanism chosen, replace lines 14-29 (the eager `from .commands import (...)`) and lines 102-149 (the `app.add_typer` block) of `src/moneybin/cli/main.py` so each command group is registered through the lazy helper. Example using Step 1c (module-level lazy):

```python
# Top of file: drop the eager import block. Profile/stubs imports stay
# eager because they're tiny (no fastmcp/sqlmesh transitive cost).
from .commands.stubs import export_app, track_app
```

…and in the registration block, replace each direct `app.add_typer(profile.app, ...)` with an import-on-use form. Concretely, Step 1c lets you write:

```python
from .commands import (  # __getattr__ defers actual module import
    categorize,
    db,
    import_cmd,
    logs,
    matches,
    mcp,
    migrate,
    profile,
    stats,
    sync,
    synthetic,
    transform,
)
```

…and the original `app.add_typer(...)` calls are unchanged because each of those names is a lazy proxy that triggers `import_module` only when its `.app` is dereferenced. The deferred import fires when Typer first dispatches into the group — but since `app.add_typer(profile.app, ...)` accesses `profile.app` at registration time, **this approach defeats lazy-loading**.

So the correct pattern with Step 1c is to register groups through callables:

```python
# At the bottom of main.py, replace direct add_typer calls with lazy-resolved registration.
# Each lambda fires only when Typer dispatches into the group.
_GROUPS = [
    (
        "profile",
        "Manage user profiles (create, list, switch, delete, show, set)",
        lambda: import_module("moneybin.cli.commands.profile").app,
    ),
    (
        "import",
        "Import financial files into MoneyBin",
        lambda: import_module("moneybin.cli.commands.import_cmd").app,
    ),
    (
        "sync",
        "Sync transactions from external services",
        lambda: import_module("moneybin.cli.commands.sync").app,
    ),
    (
        "categorize",
        "Manage transaction categories, rules, and merchants",
        lambda: import_module("moneybin.cli.commands.categorize").app,
    ),
    (
        "matches",
        "Review and manage transaction matches",
        lambda: import_module("moneybin.cli.commands.matches").app,
    ),
    (
        "transform",
        "Run SQLMesh data transformations",
        lambda: import_module("moneybin.cli.commands.transform").app,
    ),
    (
        "synthetic",
        "Generate and manage synthetic financial data for testing",
        lambda: import_module("moneybin.cli.commands.synthetic").app,
    ),
    (
        "mcp",
        "MCP server for AI assistant integration",
        lambda: import_module("moneybin.cli.commands.mcp").app,
    ),
    (
        "db",
        "Database management and exploration",
        lambda: import_module("moneybin.cli.commands.db").app,
    ),
]

for name, help_text, resolver in _GROUPS:
    # add_typer accepts a callable in recent Typer versions; if not, this
    # falls back to eager resolution (we'll verify after).
    app.add_typer(resolver(), name=name, help=help_text)
```

**Reality check:** Typer's `add_typer` does **not** accept a callable in current versions. The only viable lazy-load patterns are:

1. **In-process placeholder + reattach.** Build a placeholder Typer per group; on first dispatch, import the real module and copy its registered commands/groups onto the placeholder. (Step 1's approach, requires verifying internals exist.)
2. **Compile-time eager registration but with command-body-level deferred imports of heavy deps.** Stop trying to defer the Typer registration itself; instead, ensure each command module's *body* doesn't import fastmcp/sqlmesh/polars at module load. Tasks 9, 10, 11 do exactly this.

**Decision rule for this task:** Try Step 1 first. If Typer internals don't cooperate after 30 minutes of effort, **skip the placeholder approach entirely and rely on Tasks 9-11 to remove the heavy transitive imports from inside the command modules**. The actual cold-start win comes from removing fastmcp + sqlmesh from the import graph, not from deferring the Typer registration itself.

- [ ] **Step 4: Verify cold start drops**

```bash
time uv run moneybin --help > /dev/null  # warm
time uv run moneybin --help > /dev/null
uv run python -X importtime -c "import moneybin.cli.main" 2>&1 | grep -E "fastmcp|sqlmesh|polars" | head
```

Expected (after this task plus 9-11): no fastmcp/sqlmesh in the import-time output for `moneybin --help`. Cold-start time should drop from ~830ms toward ~300ms.

- [ ] **Step 5: Run the full unit + e2e suite**

```bash
make check
uv run pytest tests/moneybin tests/e2e -m "not integration and not scenarios and not slow" --durations=15
```

Expected: all pass. The lazy-load shouldn't change behavior — only when imports happen.

- [ ] **Step 6: Commit**

```bash
git add src/moneybin/cli/main.py src/moneybin/cli/commands/__init__.py
git commit -m "Lazy-load CLI command groups to cut cold-start imports"
```

---

## Task 9: Defer `fastmcp` import inside the `mcp` command body

**Files:**
- Modify: `src/moneybin/cli/commands/mcp.py` (move `from moneybin.mcp.server import ...` and any `fastmcp` imports to inside the command callable)
- Modify: `src/moneybin/mcp/server.py` only if it imports `fastmcp` at module level and the chain has another way

`fastmcp` is the single largest transitive contributor to cold-start (~480ms of the 665ms total). Even if Task 8 lands, the test that imports `moneybin.cli.main` directly should also avoid pulling in `fastmcp`.

- [ ] **Step 1: Read `src/moneybin/cli/commands/mcp.py`**

```bash
sed -n '1,40p' src/moneybin/cli/commands/mcp.py
```

Identify the imports of `moneybin.mcp.server` or anything that pulls in `fastmcp`.

- [ ] **Step 2: Move `fastmcp`-bearing imports into command bodies**

For each command function in `mcp.py`, move the `from moneybin.mcp.server import ...` import from the module top into the function body. Example transformation:

```python
# Before
from moneybin.mcp.server import build_server

@app.command("serve")
def serve(...) -> None:
    server = build_server(...)
    server.run()

# After
@app.command("serve")
def serve(...) -> None:
    from moneybin.mcp.server import build_server
    server = build_server(...)
    server.run()
```

If `build_server` is referenced from multiple command bodies, factor a single `_load_mcp_server()` helper at the module level (with no top-level import of `moneybin.mcp.server`) and have each command call it.

- [ ] **Step 3: Verify `fastmcp` no longer appears in cold-start importtime**

```bash
uv run python -X importtime -c "import moneybin.cli.main" 2>&1 | grep -i fastmcp
```

Expected: no output (fastmcp not loaded for `--help`).

```bash
time uv run moneybin --help > /dev/null
```

Expected: meaningful drop from baseline (~830ms toward ~400ms).

- [ ] **Step 4: Verify the `mcp` command itself still works**

```bash
uv run moneybin mcp --help > /dev/null
echo "exit: $?"
```

Expected: exit 0, `--help` works.

```bash
uv run pytest tests/e2e/test_e2e_mcp.py -m e2e --durations=10
```

Expected: all MCP E2E tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/moneybin/cli/commands/mcp.py src/moneybin/mcp/
git commit -m "Defer fastmcp import to mcp command body for faster CLI boot"
```

---

## Task 10: Defer `sqlmesh` and `polars` imports

**Files:**
- Audit: `src/moneybin/cli/commands/transform.py`, `src/moneybin/seeds.py`, any module that imports `sqlmesh` at module top
- Audit: any CLI command module that imports `polars` at module top
- Modify: those modules to defer the imports

- [ ] **Step 1: Find module-top sqlmesh imports**

```bash
grep -rn "^from sqlmesh\|^import sqlmesh" src/moneybin/
```

- [ ] **Step 2: Move each into the function/method that uses it**

For each module-level `from sqlmesh ...` import (outside of functions), move it inside the function that uses the imported name. If the module has many users, define a helper `_get_sqlmesh_context()` that imports lazily and is called from each command.

`src/moneybin/database.py:262-270` already does this correctly (imports inside `_run_sqlmesh_migrate`). The targets are the `cli/commands/transform.py` and `seeds.py` callsites.

- [ ] **Step 3: Find module-top polars imports in the CLI path**

```bash
grep -rn "^from polars\|^import polars" src/moneybin/cli/ src/moneybin/services/ | head
```

Polars is heavy (~150ms on import). If any CLI command module pulls it in at module-top *and* the command itself doesn't always need it, defer.

- [ ] **Step 4: Verify cold-start drop**

```bash
uv run python -X importtime -c "import moneybin.cli.main" 2>&1 | grep -E "sqlmesh|polars" | head
time uv run moneybin --help > /dev/null
```

Expected: no sqlmesh/polars in output for `moneybin --help`. Cold start meaningfully lower.

- [ ] **Step 5: Run full check**

```bash
make check
uv run pytest tests/moneybin tests/e2e -m "not integration and not scenarios and not slow" --durations=15
```

Expected: all pass.

- [ ] **Step 6: Commit**

```bash
git add src/moneybin/
git commit -m "Defer sqlmesh and polars imports to command bodies"
```

---

## Task 11: Cache the SQLMesh `Context` per-process

**Files:**
- Modify: `src/moneybin/database.py:244-321` (`_run_sqlmesh_migrate`) and `sqlmesh_context()` if it has a similar build path
- Test: `tests/moneybin/test_database.py::TestRunSqlmeshMigrate` should still pass

`_run_sqlmesh_migrate()` rebuilds a `Context` every call. The Context object parses the entire `sqlmesh/` project — a noticeable cost when scenario tests trigger migrations across multiple test sessions. Cache it keyed on `(sqlmesh_root, db_path)`.

- [ ] **Step 1: Read the current `sqlmesh_context()` in `src/moneybin/database.py`**

```bash
grep -n "def sqlmesh_context\|^def " src/moneybin/database.py | head
sed -n '630,720p' src/moneybin/database.py
```

Understand both the migrate path (lines 244-321) and the runtime context-builder. They likely share most of the code — refactor to a single cached builder.

- [ ] **Step 2: Add a process-level Context cache**

At module top, add:

```python
_SQLMESH_CONTEXT_CACHE: dict[tuple[str, str], object] = {}
```

Wrap the Context construction so the cache is consulted first:

```python
def _get_or_build_sqlmesh_context(
    sqlmesh_root: Path, db_path: Path, conn: duckdb.DuckDBPyConnection
) -> object:
    """Return a cached SQLMesh Context for (sqlmesh_root, db_path).

    The Context object is expensive to build (loads all models, parses
    macros, builds the dependency graph). For a single process — including
    a single pytest worker — the project layout is fixed, so we cache and
    reuse rather than re-building per call. The cache survives the lifetime
    of the worker, which matches DuckDB's connection lifetime in practice.
    """
    key = (str(sqlmesh_root), str(db_path))
    cached = _SQLMESH_CONTEXT_CACHE.get(key)
    if cached is not None:
        return cached

    # ... existing build logic, factored out of _run_sqlmesh_migrate ...
    ctx = Context(...)
    _SQLMESH_CONTEXT_CACHE[key] = ctx
    return ctx
```

Refactor `_run_sqlmesh_migrate()` to call `_get_or_build_sqlmesh_context()` instead of constructing inline.

- [ ] **Step 3: Add a unit test that proves the cache is consulted**

In `tests/moneybin/test_database.py`, add:

```python
def test_sqlmesh_context_cache_reuses_built_context(
    tmp_path, mock_secret_store, monkeypatch
):
    """Second call with same (root, db_path) returns the cached Context."""
    from moneybin import database as db_mod

    monkeypatch.setattr(db_mod, "_SQLMESH_CONTEXT_CACHE", {})

    db_path = tmp_path / "test.duckdb"
    database = Database(db_path, secret_store=mock_secret_store, no_auto_upgrade=True)

    sentinel = object()
    db_mod._SQLMESH_CONTEXT_CACHE[(str(db_mod._SQLMESH_ROOT), str(db_path))] = sentinel  # type: ignore[reportPrivateUsage]

    result = db_mod._get_or_build_sqlmesh_context(
        db_mod._SQLMESH_ROOT, db_path, database.conn
    )
    assert result is sentinel  # cache hit, no rebuild
    database.close()
```

- [ ] **Step 4: Run test**

```bash
uv run pytest tests/moneybin/test_database.py::test_sqlmesh_context_cache_reuses_built_context -v
```

Expected: passes.

- [ ] **Step 5: Run integration + scenario sanity check**

```bash
uv run pytest tests/integration -m integration --durations=15
uv run pytest tests/scenarios -m scenarios --durations=15
```

Expected: all pass. Scenarios that hit `sqlmesh migrate` more than once should be slightly faster (second call is a cache hit).

- [ ] **Step 6: Commit**

```bash
git add src/moneybin/database.py tests/moneybin/test_database.py
git commit -m "Cache SQLMesh Context per (sqlmesh_root, db_path) per process"
```

---

## Task 12: Memoize `init_schemas` against a DDL hash

**Files:**
- Modify: `src/moneybin/schema.py:132-148` (`init_schemas`)
- Test: existing `tests/moneybin/test_schema.py` (or wherever schema is tested) plus a new memoization test

`init_schemas()` runs every time `Database.__init__` opens an existing DB — re-applying the same DDL files. For a stable DB, the work is wasted. Track a hash of the DDL file contents in a one-row metadata table and skip when it matches.

- [ ] **Step 1: Read the current `init_schemas`**

```bash
sed -n '50,148p' src/moneybin/schema.py
```

Note `_SCHEMA_FILES` (the list of SQL files), `_SQL_DIR`, `_apply_comments`.

- [ ] **Step 2: Add a hash-based skip in `init_schemas`**

Replace the body of `init_schemas` with:

```python
import hashlib

_SCHEMA_VERSION_TABLE = "app.schema_init_state"
_SCHEMA_VERSION_DDL = f"""
CREATE TABLE IF NOT EXISTS {_SCHEMA_VERSION_TABLE} (
    ddl_hash VARCHAR PRIMARY KEY,
    applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""


def _compute_ddl_hash() -> str:
    """SHA-256 of the concatenated schema DDL files, stable across runs."""
    h = hashlib.sha256()
    for sql_file in _SCHEMA_FILES:
        sql_path = _SQL_DIR / sql_file
        if sql_path.exists():
            h.update(sql_path.read_bytes())
    return h.hexdigest()[:16]


def init_schemas(conn: duckdb.DuckDBPyConnection) -> None:
    """Create all database schemas and tables, then apply inline comments.

    Memoizes against a hash of the DDL file contents stored in
    app.schema_init_state. If the hash matches, schema initialization is
    a no-op (the DDL files are unchanged from the last successful apply).

    Args:
        conn: An active read-write DuckDB connection.
    """
    # Need the app schema before we can read/write the state table.
    conn.execute("CREATE SCHEMA IF NOT EXISTS app;")
    conn.execute(_SCHEMA_VERSION_DDL)

    expected_hash = _compute_ddl_hash()
    cached = conn.execute(
        f"SELECT ddl_hash FROM {_SCHEMA_VERSION_TABLE} LIMIT 1"
    ).fetchone()
    if cached and cached[0] == expected_hash:
        logger.debug(
            f"Schema DDL hash matches ({expected_hash}); skipping init_schemas"
        )
        return

    for sql_file in _SCHEMA_FILES:
        sql_path = _SQL_DIR / sql_file
        if not sql_path.exists():
            logger.warning(f"Schema file not found, skipping: {sql_file}")
            continue
        sql = sql_path.read_text()
        conn.execute(sql)
        _apply_comments(conn, sql)
        logger.debug(f"Executed {sql_file}")

    conn.execute(f"DELETE FROM {_SCHEMA_VERSION_TABLE}")
    conn.execute(
        f"INSERT INTO {_SCHEMA_VERSION_TABLE} (ddl_hash) VALUES (?)",
        [expected_hash],
    )
    logger.debug(
        f"Executed {len(_SCHEMA_FILES)} schema files; recorded hash {expected_hash}"
    )
```

- [ ] **Step 3: Add a memoization test**

In `tests/moneybin/test_schema.py` (create if it doesn't exist):

```python
"""Tests for schema initialization memoization."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from moneybin.database import Database


def test_init_schemas_skips_when_hash_matches(tmp_path: Path) -> None:
    """Reopening a DB with unchanged DDL skips re-applying schema."""
    mock_store = MagicMock()
    mock_store.get_key.return_value = "test-encryption-key-for-unit-tests"

    db_path = tmp_path / "test.duckdb"

    database = Database(db_path, secret_store=mock_store, no_auto_upgrade=True)
    database.close()

    # Reopen — the second init should hit the memoized hash and skip
    # _apply_comments. Patch _apply_comments to assert it isn't called.
    with patch("moneybin.schema._apply_comments") as mock_apply:
        database = Database(db_path, secret_store=mock_store, no_auto_upgrade=True)
        database.close()
        mock_apply.assert_not_called()


def test_init_schemas_runs_when_hash_changes(tmp_path: Path, monkeypatch) -> None:
    """If the recorded hash differs, full schema init runs."""
    from moneybin import schema

    mock_store = MagicMock()
    mock_store.get_key.return_value = "test-encryption-key-for-unit-tests"

    db_path = tmp_path / "test.duckdb"

    database = Database(db_path, secret_store=mock_store, no_auto_upgrade=True)
    database.close()

    # Tamper with the recorded hash to simulate a DDL change
    database = Database(db_path, secret_store=mock_store, no_auto_upgrade=True)
    database.execute(
        f"UPDATE {schema._SCHEMA_VERSION_TABLE} SET ddl_hash = 'stale'"  # noqa: S608  # test-only literal
    )
    database.close()

    with patch("moneybin.schema._apply_comments") as mock_apply:
        database = Database(db_path, secret_store=mock_store, no_auto_upgrade=True)
        database.close()
        mock_apply.assert_called()
```

- [ ] **Step 4: Run the new tests**

```bash
uv run pytest tests/moneybin/test_schema.py -v
```

Expected: both tests pass.

- [ ] **Step 5: Run the unit + integration suites**

```bash
uv run pytest tests/moneybin tests/integration -m "not e2e and not scenarios and not slow" --durations=15
```

Expected: all pass. Reopen-the-same-DB tests should be slightly faster.

- [ ] **Step 6: Commit**

```bash
git add src/moneybin/schema.py tests/moneybin/test_schema.py
git commit -m "Memoize init_schemas against DDL hash to skip redundant DDL"
```

---

## Task 13: Final baseline comparison and Checkpoint 2 commit

**Files:** none (measurement only)

- [ ] **Step 1: Re-run all four suites with `--durations`**

```bash
uv run pytest tests/moneybin -m "not integration and not e2e and not scenarios and not slow" --durations=10 -q 2>&1 | tee /tmp/cp2-unit.txt
uv run pytest tests/integration -m integration --durations=10 -q 2>&1 | tee /tmp/cp2-integration.txt
uv run pytest tests/e2e -m e2e --durations=10 -q 2>&1 | tee /tmp/cp2-e2e.txt
uv run pytest tests/scenarios -m scenarios --durations=10 -q 2>&1 | tee /tmp/cp2-scenarios.txt
```

Compare against the Checkpoint 1 totals and the original baseline. Expected end state:

| Suite | Baseline | After CP2 | Δ |
|---|---|---|---|
| Unit | ~36s | ~22–28s | mcp_db snapshot + module-scope db + lazy CLI |
| E2E | baseline | substantially lower | help in-process + uv-bypass + profile snapshot + lazy CLI imports |
| Integration | baseline | slightly lower | SQLMesh Context cache |
| Scenarios | baseline | slightly lower | SQLMesh Context cache + init_schemas memo |
| **Cold start** | ~830 ms | <300 ms | lazy CLI + deferred fastmcp/sqlmesh/polars |

- [ ] **Step 2: Run `make check test`**

```bash
make check test
```

Expected: all pass.

- [ ] **Step 3: Verify cold start improvement**

```bash
uv run moneybin --help > /dev/null  # warm
time uv run moneybin --help > /dev/null
uv run python -X importtime -c "import moneybin.cli.main" 2>&1 | grep -iE "fastmcp|sqlmesh|polars" || echo "no heavy imports — good"
```

Expected: cold start under ~300ms; no fastmcp/sqlmesh/polars in `moneybin --help` import path.

---

## Task 14: Pre-push pass and PR

**Files:** none (workflow only)

- [ ] **Step 1: Run `/simplify` against the changed files**

Per `.claude/rules/shipping.md`, run the `/simplify` skill against the diff before pushing. This catches copy-paste patterns and redundant validations introduced during implementation.

```bash
git diff --stat main..HEAD
```

Then invoke the simplify skill to review changed code.

- [ ] **Step 2: Run the full check + test gauntlet one final time**

```bash
make check test
uv run pytest tests/e2e -m e2e
uv run pytest tests/integration -m integration
```

Expected: all pass.

- [ ] **Step 3: Push the branch**

```bash
git push -u origin perf/test-suite-speed
```

- [ ] **Step 4: Open the PR**

Use `gh pr create` with a title under 70 chars and a body that names the two checkpoints and quantifies the wall-time win:

```bash
gh pr create --title "Speed up test suite via fixture snapshots and CLI lazy imports" --body "$(cat <<'EOF'
## Summary

Two checkpoints addressing the 8-10 minute test-suite wall time:

**Checkpoint 1 — Test infrastructure**
- Snapshot `mcp_db` baseline once per session (was rebuilt 138x)
- Snapshot E2E profile once per session (was created via `profile create` per mutating test)
- Bypass `uv run` wrapper in `run_cli()` — call `.venv/bin/moneybin` directly
- Move E2E `--help` parametrize cases in-process via `CliRunner`; keep one subprocess boot smoke
- Module-scope `Database` fixture for read-only unit modules where safe
- Add `--durations=25` to Makefile + CI test targets

**Checkpoint 2 — App cold-start improvements**
- Lazy-load all 13 CLI command groups uniformly
- Defer `fastmcp`, `sqlmesh`, and `polars` imports into command bodies
- Cache SQLMesh `Context` per `(sqlmesh_root, db_path)` per process
- Memoize `init_schemas` against a DDL hash

## Numbers (record from Task 13)

| Suite | Before | After |
|---|---|---|
| Unit | 36s | <fill in> |
| E2E | <fill in> | <fill in> |
| Integration | <fill in> | <fill in> |
| Scenarios | <fill in> | <fill in> |
| `moneybin --help` cold start | 830ms | <fill in> |

## Test plan

- [x] `make check test` passes
- [x] `uv run pytest tests/e2e -m e2e` passes
- [x] `uv run pytest tests/integration -m integration` passes
- [x] `uv run pytest tests/scenarios -m scenarios` passes
- [x] `moneybin --help` cold start measured before/after
- [x] `python -X importtime` confirms fastmcp/sqlmesh/polars deferred for `--help`
EOF
)"
```

---

## Self-Review Checklist

**1. Spec coverage:**
- ✅ T1.1 (lazy CLI imports) → Tasks 8-10
- ✅ T1.2 (--durations) → Task 1
- ✅ T1.3 (e2e profile snapshot) → Task 4
- ✅ T1.4 (mcp_db snapshot) → Task 3
- ✅ T1.5 (bypass uv) → Task 2
- ✅ T2.1 (in-process help E2E) → Task 5
- ✅ T2.2 (session-scope read-only Database) → Task 6
- ✅ T3.1 (defer heavy deps) → Tasks 9-10
- ✅ T3.2 (SQLMesh Context cache) → Task 11
- ✅ T3.3 (init_schemas memoization) → Task 12

**2. Placeholder scan:**
- Task 8 explicitly flags Typer-internals risk and provides three concrete fallback paths (Step 1 / 1b / 1c) with a decision rule. No "TBD" language.
- Task 6 has a 30-minute time-box rather than open-ended "audit until done."
- Task 11 has a sentinel-based test that's deterministic.
- All tasks include exact file paths and exact commit messages.

**3. Type/name consistency:**
- `_mcp_db_template` (Task 3) and `_mutating_profile_template` (Task 4) are private session fixtures — naming is consistent with existing pytest convention.
- `make_workflow_env_fast` parallels existing `make_workflow_env` — sibling helper, not a replacement.
- `_get_or_build_sqlmesh_context` (Task 11) is the new entry point; `_run_sqlmesh_migrate` calls into it. No name collision.
- `_SCHEMA_VERSION_TABLE` (Task 12) lives in the `app` schema — consistent with the existing `app.*` namespace convention.

**4. Risk acknowledgments:**
- Task 8 flags Typer internals as the highest-risk item with explicit fallbacks. The cold-start win comes mostly from Tasks 9-10 (deferring the heavy transitive imports), not from deferring the Typer registration itself — so even partial success on Task 8 still yields the bulk of the gain.
- Task 3 includes a CHECKPOINT/encryption-survives-copy validation step.
- Task 6 is time-boxed and skippable per-module.
