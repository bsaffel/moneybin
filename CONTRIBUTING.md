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

PRs that touch code run the scenario suite via [`.github/workflows/scenarios.yml`](.github/workflows/scenarios.yml) — docs-only changes (`**/*.md`, `docs/**`) are skipped via `paths-ignore`. Each scenario boots an empty encrypted DuckDB, runs `generate → transform → match → categorize`, and reports three correctness signals:

- **Assertions** — invariants like FK integrity, sign convention, balanced transfers.
- **Expectations** — per-record claims on hand-labeled fixtures.
- **Evaluations** — aggregate scores like categorization accuracy and transfer-detection F1.

Run locally:

```bash
make test-scenarios                                    # Run all shipped scenarios
uv run pytest tests/scenarios/ -m scenarios -v         # Same, via pytest directly
uv run pytest tests/scenarios/test_basic_full_pipeline.py -v  # Single scenario
```

Scenarios live at `tests/scenarios/data/*.yaml`. The runner is documented in [`docs/specs/testing-scenario-runner.md`](docs/specs/testing-scenario-runner.md). Per [`docs/specs/testing-scenario-comprehensive.md`](docs/specs/testing-scenario-comprehensive.md), scenarios run as pytest tests under `tests/scenarios/`.

### Authoring a new scenario (especially for a user bug report)

The prescribed recipe is in [`docs/guides/scenario-authoring.md`](docs/guides/scenario-authoring.md). Summary:

1. Get the bug report and an anonymized DB snapshot ([`testing-anonymized-data.md`](docs/specs/testing-anonymized-data.md)) — never commit raw user data.
2. Build a fixture under `tests/scenarios/data/fixtures/<bug-id>/`.
3. Write the expectation **independently of program output** — derive it from the fixture, the persona config, or hand-authored ground truth. Never observe-and-paste. See [`.claude/rules/testing.md`](.claude/rules/testing.md) "Scenario Expectations Must Be Independently Derived."
4. Verify the scenario fails on the broken code, then passes on the fix.
5. Cover Tier 1 invariants (always) plus relevant Tier 2–4 checks per [`docs/specs/testing-scenario-comprehensive.md`](docs/specs/testing-scenario-comprehensive.md).

Every new scenario must declare its **tier coverage** (in the test
docstring as `tiers: T1, T2-...`). The five-tier taxonomy is defined in
[`docs/specs/testing-scenario-comprehensive.md`](docs/specs/testing-scenario-comprehensive.md):
T1 (structural invariants) is required everywhere; T2 (semantic
correctness), T3 (pipeline behavior), T4 (distribution / quality), and
T5 (operational) apply where relevant.

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

## Where the strategy lives

The public planning artifacts are:

- [`docs/specs/`](docs/specs/INDEX.md) — design specs for every feature (status: `draft` → `ready` → `in-progress` → `implemented`).
- [`docs/decisions/`](docs/decisions/) — architecture decision records (ADRs) for the load-bearing choices.
- [`docs/guides/`](docs/guides/) — user-facing how-tos for shipped capabilities.
- [`docs/roadmap.md`](docs/roadmap.md) — milestone status (M0 through M3E + post-launch).
- [`docs/features.md`](docs/features.md) — capability snapshot (what works today).
- [`README.md`](README.md) — storefront pointing to the rest.
- [`CHANGELOG.md`](CHANGELOG.md) — version history.

If you're contributing and want context on *why* a spec was written the way it was, the spec's `Background` section links to the relevant ADRs and references — the answer almost always lives in the public artifacts above.

**User-visible changes update `CHANGELOG.md`'s `Unreleased`.** Refactors, internal-only docs, and CI tweaks don't need entries. See `.claude/rules/shipping.md` for the full post-implementation checklist.
