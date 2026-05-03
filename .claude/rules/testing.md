---
description: "Testing standards: pytest patterns, fixtures, mocking strategy, database test helpers"
paths: ["tests/**", "**/conftest.py", "src/moneybin/testing/**"]
---

# Testing Standards

## Framework

- pytest with `conftest.py` fixtures. Naming: `test_*.py`, `test_*()`, `TestClassName`.
- Type-annotate fixtures: `tmp_path: Path`, `mocker: MockerFixture`, `caplog`.

## Markers

```python
@pytest.mark.unit         # Fast unit tests (default)
@pytest.mark.integration  # Requires external systems
@pytest.mark.e2e          # End-to-end subprocess tests
@pytest.mark.slow         # Long-running
```

## Commands

```bash
uv run pytest tests/ -v                                       # All tests
uv run pytest tests/ -v -m "not integration and not e2e"      # Unit only
uv run pytest tests/test_file.py -v                           # Specific file
uv run pytest tests/e2e/ -m "e2e" -v                          # E2E only
uv run pytest tests/ --cov=src/moneybin --cov-report=html     # Coverage
uv run pytest tests/path/to/test.py -n0 -v                    # Disable xdist (for pdb / clean output)
```

Tests run in parallel via `pytest-xdist` (`-n auto` in `pyproject.toml`).
Pass `-n0` to disable parallelism when you need `pdb`, ordered output,
or are debugging a flaky test that may have inter-test state leaks.

## Mocking Strategy

- **Mock external dependencies**: APIs, databases, file systems.
- **Use real objects** for internal business logic.
- **CLI tests**: Mock business logic classes (tested separately). Test argument parsing, exit codes, error messages -- not business logic.

## Coverage Goals

- Business logic: 90%+
- CLI commands: CLI-specific paths only (argument parsing, exit codes, error display)
- Integration: Critical user workflows end-to-end

## Mock Boundaries

When a function delegates to an external system (SQLMesh, DuckDB CLI, keyring, subprocess), test the delegation itself — not just the caller with the delegation mocked out.

- **Test the real call shape**: argument order, config types, exception types. `assert flag in args` misses ordering bugs — assert position or use exact-match.
- **Mocks must raise real library exceptions**: if keyring raises `PasswordDeleteError`, the mock must too — not the project wrapper the code is supposed to produce.
- **Integration tests for subsystem boundaries** (`@pytest.mark.integration`, `make test-all`): one test per boundary that exercises the real interaction (encrypted DB + SQLMesh, passphrase lock/unlock cycle, key rotation round-trip).

## Database Fixtures

- **Always pass `no_auto_upgrade=True`** when creating `Database` instances in tests, unless the test is specifically verifying migration behavior. Without this, each test creates a SQLMesh `Context` and runs migration checks — slow (~1.5s per test) and requires the full sqlmesh project directory to be resolvable.
- **Use `mock_secret_store`** from the root `conftest.py` (or create a local `MagicMock` with `get_key.return_value = "test-key"`) — never hit the real keyring.
- **Avoid `autouse=True` on expensive fixtures.** Use `pytestmark = pytest.mark.usefixtures("fixture_name")` at module level, and add the fixture as an explicit parameter to any inner fixtures that depend on it (e.g., `_insert_data(self, mcp_db: object)`).
- **Use `module_db` for read-only modules.** `tests/moneybin/conftest.py` exposes a module-scoped `module_db` fixture that amortizes one `Database()` open across every test in the module. Eligible if **every** test in the module is read-only against `db` — no `INSERT`, `UPDATE`, `DELETE`, `CREATE`, `DROP`, `ALTER`, no `db.ingest_dataframe()`, no helper that mutates the DB. A single mutating test in the module disqualifies it. When in doubt, stay on the function-scoped `db` fixture.

```python
# CORRECT — fast test database
Database(tmp_path / "test.duckdb", secret_store=mock_store, no_auto_upgrade=True)

# WRONG — spawns sqlmesh subprocess, runs migrations on every test
Database(tmp_path / "test.duckdb", secret_store=mock_store)
```

## Performance Patterns

When fixture build cost × test count adds up to seconds of suite time, prefer **snapshot-and-copy** over re-building per test:

1. Build the baseline once in a session-scoped fixture (template DB file, template MONEYBIN_HOME tree, etc.) and return its path.
2. Per-test fixture does `shutil.copy` (single file) or `shutil.copytree` (directory) into `tmp_path`, then opens it.

Existing examples:
- `tests/moneybin/test_mcp/conftest.py:_mcp_db_template` — encrypted DuckDB with core tables + base reference data, copied into each MCP test's `tmp_path`.
- `tests/e2e/conftest.py:_mutating_profile_template` — initialized profile dir built once via `moneybin profile create`, copytree'd into each mutating E2E test's `tmp_path`.

Caveats:
- Each xdist worker rebuilds the template once per session. Wins are largest when the per-test cost dominates the per-worker build cost.
- Copying an actively-attached encrypted DuckDB file is unsafe. Close the template `Database` before returning the path (or run `CHECKPOINT` before close) so the WAL is flushed.
- `os.link` (hardlink) is faster than `shutil.copy` but corrupts the template — DuckDB writes through the inode. Always use `shutil.copy`.

For mutating E2E tests, prefer **`make_workflow_env_fast(e2e_home, subdir, _mutating_profile_template)`** over `make_workflow_env()`. Stay on `make_workflow_env()` when the test:
- Passes `--profile <name>` directly to a CLI command (the fast path's active profile is always `e2e-template`)
- Asserts on the profile-name string in CLI output
- Creates additional profiles via subsequent `profile create` calls
- Otherwise needs a fresh profile state the snapshot doesn't preserve

## Test Fixture Factories

When a dataclass or model requires 3+ fields and appears in multiple tests, write a module-level `_make_thing()` factory with sensible defaults. Tests override only the fields they care about, keeping the focus on the behavior under test rather than construction boilerplate.

```python
# Good — factory with defaults, tests override what matters
def _make_migration(version: int = 1, filename: str = "V001__test.sql", **kw):
    return Migration(version=version, filename=filename, content=b"SELECT 1;", ...)

mock_runner.pending.return_value = [_make_migration(version=2, filename="V002__new.sql")]

# Bad — full constructor repeated in every test
Migration(version=2, name="new", filename="V002__new.sql", checksum="def456",
          content=b"SELECT 1;", path=Path("/tmp/V002__new.sql"), file_type="sql")
```

## Golden-Case Fixtures

For pure functions whose correctness is best expressed as input → output pairs,
keep cases in a YAML fixture file under `tests/.../fixtures/` and write a
parametrized test asserting exact equality.

**When to add:** Real-world input that should produce a specific output, and no
existing case covers it.

**How to add:**

1. Append a row to the fixture YAML with a unique, kebab-case `id` naming the
   *behavior under test*, not the input.
2. Run the test. Fix the function until it passes — do NOT relax `expected` to
   match incorrect output.

**Why exact equality:** Loose assertions hide subtle regressions like extra
whitespace or partial strips. Goldens force every character intentional.

## Test Coverage by Layer

Every shipped feature must have tests at the appropriate layers:

| Layer | What it catches | Required when |
|---|---|---|
| Unit (`tests/moneybin/`) | Logic bugs, edge cases | Always |
| Integration (`tests/integration/`) | Cross-subsystem wiring | Feature touches >1 subsystem |
| E2E (`tests/e2e/`) | Boot, wiring, schema, subprocess errors | Every CLI command (see below) |
| E2E workflow (`tests/e2e/test_e2e_workflows.py`) | Multi-step pipeline breakage | Feature adds a user-facing workflow |
| Scenario (`tests/scenarios/` + `make test-scenarios`) | Whole-pipeline correctness against synthetic + labeled fixtures | When changing data shapes, matching/categorization heuristics, or migrations |

- New import formats or data sources: add an E2E workflow test that imports a fixture file
- New DB schema changes: covered automatically by existing E2E tests (they exercise `init_schemas`)
- Unit tests alone are not sufficient for shipped features that add CLI commands or cross subsystem boundaries

## E2E Test Coverage Requirement

**Every CLI command must have an E2E subprocess test.** The only exceptions are `db shell` and `db ui` (interactive-only commands that cannot be driven via subprocess).

E2E tests are organized into tiers by what they need:

| Tier | File | Scope | Fixture |
|---|---|---|---|
| Help | `test_e2e_help.py` | `--help` for every command group — **in-process via `CliRunner`** plus one subprocess boot smoke | None (no profile/DB) |
| Read-only | `test_e2e_readonly.py` | Commands that query but don't mutate | `e2e_env` or `e2e_profile` (shared) |
| Mutating | `test_e2e_mutating.py` | Commands that write state | `tmp_path` + `make_workflow_env_fast()` (snapshot-copy) or `make_workflow_env()` (fresh `profile create`) |
| Workflows | `test_e2e_workflows.py` | Multi-step user flows | `e2e_home` + `make_workflow_env()` |
| MCP | `test_e2e_mcp.py` | MCP server boot, tool invocation | `make_workflow_env()` |
| Stubs | `test_e2e_readonly.py::TestStubCommands` | Placeholder commands | None |

When adding a new CLI command:

1. Add a `--help` entry to `_HELP_COMMANDS` in `test_e2e_help.py` (if it's a new command group)
2. Add a test to the appropriate tier file based on whether the command reads or writes
3. If the command is a stub, add it to the `TestStubCommands` parametrize list
4. Mutating tests must use `tmp_path` + `make_workflow_env()` for isolation — never share DB state

## Best Practices

- Arrange-Act-Assert structure.
- Each test verifies a single behavior.
- No shared mutable state between tests.
- Use `monkeypatch` for env vars.
- Descriptive test names that explain the scenario.

## Scenario Expectations Must Be Independently Derived

Scenario assertions, expectations, and tolerances must be derived **independently of the program's output**. A test that codifies "what the code currently produces" only proves the code is consistent with itself — it does not prove the code is correct.

When authoring or modifying a scenario:

1. **Allowed derivation paths.** Expected row counts, match outcomes, category labels, and tolerances must come from one of:
   - **The input fixture** — count the rows yourself; label outcomes by hand before running the pipeline.
   - **The persona / generator config** — derive expected values via a deterministic formula over declared parameters (e.g., `years × accounts × mean_txns_per_month × 12`).
   - **Hand-authored ground truth** written *before* running the pipeline.
2. **Forbidden: observe-and-paste.** Running the scenario, observing the output, and pasting the resulting number into the YAML is not acceptable, even if the output "looks right."
3. **Tolerances require a formula.** A bare `±15%` is not acceptable. Any tolerance must accompany the formula it absorbs and a comment explaining the source of variance (e.g., "seeded RNG produces ±5% per year over 3 years → ~15%").
4. **When code change breaks an expectation, fix the code first.** The default response to a failing scenario expectation is to investigate the code, not to update the expectation. Updating the expectation requires a written justification in the PR explaining why the new value is correct in itself — not "what the new code produces."
5. **Negative expectations are required where applicable.** If a scenario asserts "these N records should match," it must also include cases that should *not* match. Otherwise the test only catches under-matching, not over-matching.

This rule applies to YAML scenario expectations, pytest assertions in `tests/scenarios/`, and any future bug-report-driven scenario. See [`docs/specs/testing-scenario-comprehensive.md`](../../docs/specs/testing-scenario-comprehensive.md) for the full taxonomy and contributor recipe.
