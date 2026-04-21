---
description: "When writing or modifying tests"
---

# Testing Standards

## Framework

- pytest with `conftest.py` fixtures. Naming: `test_*.py`, `test_*()`, `TestClassName`.
- Type-annotate fixtures: `tmp_path: Path`, `mocker: MockerFixture`, `caplog`.

## Markers

```python
@pytest.mark.unit         # Fast unit tests (default)
@pytest.mark.integration  # Requires external systems
@pytest.mark.slow         # Long-running
```

## Commands

```bash
uv run pytest tests/ -v                                    # All tests
uv run pytest tests/ -v -m "not integration"               # Unit only
uv run pytest tests/test_file.py -v                        # Specific file
uv run pytest tests/ --cov=src/moneybin --cov-report=html  # Coverage
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

- **Always pass `no_auto_upgrade=True`** when creating `Database` instances in tests, unless the test is specifically verifying migration behavior. Without this, each test spawns a `sqlmesh migrate` subprocess (~1.5s per test).
- **Use `mock_secret_store`** from the root `conftest.py` (or create a local `MagicMock` with `get_key.return_value = "test-key"`) — never hit the real keyring.
- **Avoid `autouse=True` on expensive fixtures.** Use `pytestmark = pytest.mark.usefixtures("fixture_name")` at module level, and add the fixture as an explicit parameter to any inner fixtures that depend on it (e.g., `_insert_data(self, mcp_db: object)`).

```python
# CORRECT — fast test database
Database(tmp_path / "test.duckdb", secret_store=mock_store, no_auto_upgrade=True)

# WRONG — spawns sqlmesh subprocess, runs migrations on every test
Database(tmp_path / "test.duckdb", secret_store=mock_store)
```

## Best Practices

- Arrange-Act-Assert structure.
- Each test verifies a single behavior.
- No shared mutable state between tests.
- Use `monkeypatch` for env vars.
- Descriptive test names that explain the scenario.
