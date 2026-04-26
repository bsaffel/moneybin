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
```

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

```python
# CORRECT — fast test database
Database(tmp_path / "test.duckdb", secret_store=mock_store, no_auto_upgrade=True)

# WRONG — spawns sqlmesh subprocess, runs migrations on every test
Database(tmp_path / "test.duckdb", secret_store=mock_store)
```

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

## Test Coverage by Layer

Every shipped feature must have tests at the appropriate layers:

| Layer | What it catches | Required when |
|---|---|---|
| Unit (`tests/moneybin/`) | Logic bugs, edge cases | Always |
| Integration (`tests/integration/`) | Cross-subsystem wiring | Feature touches >1 subsystem |
| E2E (`tests/e2e/`) | Boot, wiring, schema, subprocess errors | Every CLI command (see below) |
| E2E workflow (`tests/e2e/test_e2e_workflows.py`) | Multi-step pipeline breakage | Feature adds a user-facing workflow |

- New import formats or data sources: add an E2E workflow test that imports a fixture file
- New DB schema changes: covered automatically by existing E2E tests (they exercise `init_schemas`)
- Unit tests alone are not sufficient for shipped features that add CLI commands or cross subsystem boundaries

## E2E Test Coverage Requirement

**Every CLI command must have an E2E subprocess test.** The only exceptions are `db shell` and `db ui` (interactive-only commands that cannot be driven via subprocess).

E2E tests are organized into tiers by what they need:

| Tier | File | Scope | Fixture |
|---|---|---|---|
| Help | `test_e2e_help.py` | `--help` for every command group | None (no profile/DB) |
| Read-only | `test_e2e_readonly.py` | Commands that query but don't mutate | `e2e_env` or `e2e_profile` (shared) |
| Mutating | `test_e2e_mutating.py` | Commands that write state | `tmp_path` + `make_workflow_env()` (isolated) |
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
