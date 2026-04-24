# End-to-End Testing

**Status:** implemented
**Created:** 2026-04-23

## Problem

All existing tests run in-process — CLI tests use `typer.testing.CliRunner`, integration tests call Python methods directly. No test exercises the `uv run moneybin ...` path that users actually run. This means boot/schema/init wiring bugs (like `_apply_comments` referencing a renamed column) pass all tests but crash on real invocation.

## Goals

1. Catch startup, schema initialization, and command registration errors by running CLI commands as subprocesses.
2. Validate multi-step user workflows end-to-end (profile → import → transform → query).
3. Ensure every non-stub CLI command has at least one E2E test.
4. Reorganize the test directory so unit, integration, and E2E tests are clearly separated.

## Non-Goals

- Replacing unit or integration tests. E2E tests complement them.
- Testing MCP server protocol (that's a separate concern).
- Performance benchmarking.

## Design

### Test Directory Reorganization

```
tests/
├── moneybin/           # Unit tests (in-process, mocked boundaries)
│   ├── test_cli/
│   ├── test_extractors/
│   ├── test_loaders/
│   └── ...
├── integration/        # Cross-subsystem, real DB, still in-process
│   ├── conftest.py
│   ├── test_integration_existing.py  (moved from moneybin/test_integration/)
│   └── test_tabular_e2e.py           (moved from moneybin/test_integration/)
├── e2e/                # Subprocess, runs `uv run moneybin` like a user
│   ├── conftest.py
│   ├── test_e2e_smoke.py
│   └── test_e2e_workflows.py
└── fixtures/           # Shared test fixtures (unchanged)
```

E2E tests live at the top level because they test the installed CLI, not `moneybin` internals. Integration tests are similarly cross-cutting and don't mirror a single source module.

### Infrastructure (`e2e/conftest.py`)

#### `run_cli()` helper

Wraps `subprocess.run(["uv", "run", "moneybin", ...])` with `capture_output=True, text=True`. Returns a result object with `exit_code`, `stdout`, `stderr`. Accepts `env` overrides for `MONEYBIN_HOME`, argon2 tuning, etc.

#### `e2e_home` fixture (session-scoped)

Creates a temp directory via `tmp_path_factory` to serve as `MONEYBIN_HOME`. Pytest auto-removes it after the session — no manual teardown, no leftover databases. The user's real `~/.moneybin/` is never touched.

#### `e2e_profile` fixture (session-scoped)

Depends on `e2e_home`. Creates a profile and initializes the database:

1. `moneybin profile create e2e-test`
2. `moneybin db init --passphrase --yes` with a piped test passphrase

Uses fast argon2 params via env vars (`MONEYBIN_DATABASE__ARGON2_TIME_COST=1`, `MONEYBIN_DATABASE__ARGON2_MEMORY_COST=1024`, `MONEYBIN_DATABASE__ARGON2_PARALLELISM=1`) to keep test runtime low.

Yields a dict with the profile name and env vars for use by tests.

#### `e2e_env` fixture

Merges `os.environ` with the profile-specific overrides from `e2e_profile`. Passed to every `run_cli()` call that needs a database.

### Smoke Tests (`e2e/test_e2e_smoke.py`)

Three tiers, all marked `@pytest.mark.e2e`:

#### Tier 1 — Help commands (no DB needed)

One parametrized test running `moneybin <group> --help` for every command group. Verifies the app boots and commands register without errors.

Groups: top-level, `profile`, `import`, `sync`, `categorize`, `matches`, `transform`, `synthetic`, `db`, `db migrate`, `logs`, `mcp`, `stats`, `track`, `export`.

#### Tier 2 — Commands that run without a DB

Commands that execute real logic but don't call `get_database()`:

- `profile list`, `profile create <name>`, `profile show`
- `import list-formats`, `import preview <fixture.csv>`
- `logs path`, `logs tail`
- `mcp list-tools`, `mcp list-prompts`
- `db ps`

#### Tier 3 — Commands that need an initialized DB

Use the `e2e_profile` fixture. These go through the full `get_database()` → `init_schemas` → `_apply_comments` path — the path that catches schema wiring bugs.

- `db info`, `db query "SELECT 1"`, `db migrate status`
- `transform status`, `transform validate`
- `import status`, `import history`
- `categorize stats`, `categorize list-rules`
- `matches history`
- `stats show`

All smoke tests assert: exit code 0, no Python tracebacks in stderr.

### Workflow Tests (`e2e/test_e2e_workflows.py`)

Each workflow is a single test function that creates its own fresh profile, runs steps sequentially, and verifies the final state. All marked `@pytest.mark.e2e`.

#### Workflow 1: Synthetic data pipeline

```
profile create → db init --passphrase --yes
→ synthetic generate --persona basic --skip-transform
→ transform apply
→ db query "SELECT COUNT(*) FROM core.fct_transactions"
```

Verifies: profile creation, DB init, synthetic generation, SQLMesh transforms, core table population.

#### Workflow 2: CSV import pipeline

```
profile create → db init
→ import file <fixture.csv> --account-id test-acct
→ transform apply
→ db query "SELECT COUNT(*) FROM core.fct_transactions"
```

Verifies: tabular import through the full pipeline to core tables.

#### Workflow 3: OFX import pipeline

```
profile create → db init
→ import file <fixture.qfx>
→ transform apply
→ db query "SELECT COUNT(*) FROM core.fct_transactions"
```

Verifies: OFX import through the full pipeline.

#### Workflow 4: Lock/unlock cycle

```
profile create → db init --passphrase --yes
→ db query "SELECT 1"
→ db lock
→ db unlock (pipe passphrase)
→ db query "SELECT 1"
```

Verifies: database remains accessible after lock/unlock.

#### Workflow 5: Categorization pipeline

```
profile create → db init
→ import file <fixture>
→ transform apply
→ categorize seed
→ categorize apply-rules
→ categorize stats
```

Verifies: categorization wiring end-to-end.

### Marker and Makefile Integration

**`pyproject.toml`** — new marker:

```toml
"e2e: marks tests as end-to-end subprocess tests"
```

**`Makefile`** — new target and updated targets:

```makefile
test-e2e: venv
    @uv run pytest tests/e2e/ -m "e2e" -v

test-all: venv  # updated to include e2e
    @uv run pytest tests/ -v
```

`make test` (fast feedback) excludes both `integration` and `e2e`. `make test-all` runs everything.

### Rule Updates

#### `.claude/rules/testing.md` — new section

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

#### `.claude/rules/shipping.md` — new checklist item

```markdown
### Test Layer Check
Before marking a spec as `implemented`, verify the feature has tests at every
applicable layer (see testing.md "Test Coverage by Layer"). Unit tests alone
are not sufficient for features that add CLI commands or cross subsystem
boundaries.
```

## Deliverables

1. Move `tests/moneybin/test_integration/` → `tests/integration/`
2. Create `tests/e2e/conftest.py` — `run_cli()` helper, `e2e_home`, `e2e_profile`, `e2e_env` fixtures
3. Create `tests/e2e/test_e2e_smoke.py` — ~30 parametrized tests across tiers 1–3
4. Create `tests/e2e/test_e2e_workflows.py` — 5 workflow tests
5. Update `pyproject.toml` — add `e2e` marker
6. Update `Makefile` — add `test-e2e` target, update `test-all`
7. Update `.claude/rules/testing.md` — test coverage by layer
8. Update `.claude/rules/shipping.md` — test layer check
9. Update `docs/specs/INDEX.md` — add this spec
