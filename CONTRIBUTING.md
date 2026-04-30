# Contributing to MoneyBin

## Setup

```bash
git clone https://github.com/bsaffel/moneybin.git
cd moneybin
make setup
```

Requires Python 3.11+ and [uv](https://docs.astral.sh/uv/).

## Development workflow

```bash
make check        # Format + lint + type-check (Ruff + Pyright)
make test         # Unit tests
make test-all     # Unit + integration
make test-cov     # With coverage report
```

`make check test` is the pre-commit gate. Run it before pushing.

SQL formatting: `uv run sqlmesh -p sqlmesh format`.

## Project structure

```
moneybin/
├── src/moneybin/
│   ├── mcp/                # MCP server (FastMCP, tools, resources, prompts)
│   ├── cli/                # Typer CLI (thin wrappers over service layer)
│   ├── services/           # Business logic (shared by MCP + CLI)
│   ├── extractors/         # File parsers (OFX, PDF, tabular)
│   ├── loaders/            # DuckDB data loaders
│   ├── matching/           # Cross-source dedup + transfer detection engine
│   ├── validation/         # Reusable assertions + evaluations
│   ├── testing/
│   │   ├── synthetic/      # Persona-based synthetic data generator
│   │   └── scenarios/      # Whole-pipeline scenario runner
│   ├── metrics/            # prometheus_client registry + DuckDB persistence
│   ├── logging/            # Stream-based logging config + sanitization
│   ├── database.py         # Connection factory (encryption, schemas)
│   ├── migrations.py       # Dual-path migration runner (SQL + Python)
│   ├── config.py           # Pydantic Settings (single source of truth)
│   └── log_sanitizer.py    # PII detection and masking
├── sqlmesh/models/         # SQL transformations (prep/, core/)
├── tests/                  # pytest (unit + integration + e2e)
└── docs/
    ├── specs/              # Feature specs (status tracking in INDEX.md)
    ├── decisions/          # ADRs
    └── guides/             # User-facing feature docs
```

## Pipeline verification

Every PR runs the scenario suite via [`.github/workflows/scenarios.yml`](.github/workflows/scenarios.yml). Each scenario boots an empty encrypted DuckDB, runs `generate → transform → match → categorize`, and reports three correctness signals:

- **Assertions** — invariants like FK integrity, sign convention, balanced transfers.
- **Expectations** — per-record claims on hand-labeled fixtures.
- **Evaluations** — aggregate scores like categorization accuracy and transfer-detection F1.

Run locally:

```bash
moneybin synthetic verify --list                       # Show shipped scenarios
moneybin synthetic verify --scenario basic-full-pipeline
moneybin synthetic verify --all --output json          # CI mode
```

Scenarios live at `src/moneybin/testing/scenarios/data/*.yaml`. The runner is documented in [`docs/specs/testing-scenario-runner.md`](docs/specs/testing-scenario-runner.md).

## Working with synthetic data

The synthetic data generator (see [`docs/guides/synthetic-data.md`](docs/guides/synthetic-data.md)) is the primary source of test data for development. Three personas (`basic`, `family`, `freelancer`), ~200 real merchants, deterministic seeds.

```bash
moneybin synthetic generate --persona family --profile dev-family
```

Ground-truth labels live in `synthetic.ground_truth` and feed scenario evaluations.

## Commit and branch conventions

See [`.claude/rules/branching.md`](.claude/rules/branching.md). Branches are `{type}/{kebab-summary}` where type maps to a PR label (`feat/`, `fix/`, `docs/`, `refactor/`, `chore/`, `deps/`, `ci/`, `security/`, `test/`, `perf/`).

Commit messages: imperative mood, < 72-char subject, body explains *why*.

## Specs and shipping

Feature work is tracked through specs in `docs/specs/`. The [Spec Index](docs/specs/INDEX.md) is the single source of truth for status. When a feature ships, update the spec status, the index, and the README roadmap. See [`.claude/rules/shipping.md`](.claude/rules/shipping.md) for the full checklist.
