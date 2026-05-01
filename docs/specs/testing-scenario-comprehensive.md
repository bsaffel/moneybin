# Feature: Comprehensive Scenario Testing

## Status
in-progress

## Goal

Make the scenario suite an exhaustive, regression-proof check on the data pipeline, with a contributor recipe (executable by an AI coding agent) for converting any user bug report into a permanent scenario. This spec ships concrete enhancements to the existing scenarios and runner — but it also functions as the **architectural authority for all future scenario work**: every new scenario, every new assertion, and every bug-report reproduction must conform to the taxonomy and rules defined here.

## Background

### Why this spec

The audit summarized in [the recent code-review thread] found that the existing six scenarios (in `src/moneybin/testing/scenarios/data/`) are closer to smoke tests than the kind of audit a data engineer would run. They check sign convention, FK integrity, and a handful of evaluations — but they miss obvious data-engineering invariants like:

- Idempotency (re-running the pipeline doesn't duplicate rows)
- Source attribution (`source_system` populated and matches input)
- Schema drift detection
- Negative expectations (records that should *not* match)
- Empty / malformed input handling
- Date continuity per account
- Ground-truth coverage (evaluations gaming a tiny labeled subset)

Worse, one expectation (`family-full-pipeline` row count `~2,900 ±15%`) appears to have been set by running the pipeline once and observing the result. That pattern — observe-and-paste — produces tests that prove the code is consistent with itself, not that it is correct.

### Scope boundary with `data-reconciliation.md`

[`data-reconciliation.md`](data-reconciliation.md) is **runtime + user-facing**: SQLMesh views (`core.reconciliation_*`), a `moneybin reconciliation check` CLI, MCP tools, metrics. It runs against live user data and produces investigation reports.

This spec is **pre-merge + contributor-facing**: scenario YAML / pytest assertions, fixture authoring, AI-agent recipes for translating bug reports into reproductions. It runs in CI and gates merges.

The two share primitives (row accounting, orphan detection, temporal coverage). This spec formalizes that sharing by extracting a common assertion library at `src/moneybin/validation/` that both consumers depend on.

### Related specs

- [`testing-overview.md`](testing-overview.md) — umbrella; this spec is a child.
- [`testing-scenario-runner.md`](testing-scenario-runner.md) — host of the assertion/expectation/evaluation primitives this spec extends.
- [`testing-synthetic-data.md`](testing-synthetic-data.md) — produces ground-truth labels consumed here.
- [`testing-anonymized-data.md`](testing-anonymized-data.md) — soft dependency; the prescribed tool for producing fixtures from real user databases when reproducing bug reports without leaking PII.
- [`data-reconciliation.md`](data-reconciliation.md) — peer; shares assertion primitives but targets runtime user data.

## Design Principles

1. **Independent expectations.** Tests prove correctness only when the expected output is derived from something other than the code under test. See `.claude/rules/testing.md` ("Scenario Expectations Must Be Independently Derived").
2. **Comprehensive by taxonomy.** Every scenario is evaluated against a five-tier checklist (Section: The Five Tiers). Coverage gaps are explicit, not implicit.
3. **Pytest-native.** Scenarios are pytest tests, not a parallel runner. We get fixtures, parametrization, parallelism, IDE integration, and JSON output for free.
4. **Architectural authority for future agents.** This spec is normative. Future scenarios MUST cite which tier each assertion covers. The bug-report recipe is the only sanctioned path for translating a user report into a scenario.

## Requirements

### R1 — Five-tier assertion taxonomy

Every scenario MUST be evaluated against the following tiers. The scenario YAML (or pytest test docstring) MUST declare which tiers it covers and explain any tier it intentionally skips.

#### Tier 1 — Structural Invariants (every scenario)

| Check | Primitive |
|---|---|
| Row counts in `raw.* → prep.stg_* → core.fct_*` match an independently-derived expected value | `assert_row_count` |
| Schema snapshot: column set + types per core table | `assert_schema_snapshot` |
| FK integrity: `fct_transactions.account_id ∈ dim_accounts.account_id` | `assert_valid_foreign_keys` (existing) |
| No-NULLs on required columns: `amount`, `transaction_date`, `account_id`, `source_system` | `assert_no_nulls` (existing) |
| No-duplicates on natural keys (`transaction_id`, `account_id`) | `assert_no_duplicates` (existing) |
| `source_system` populated and ∈ expected set per scenario | `assert_source_system_populated` |
| Provenance completeness: every `transaction_id` has ≥1 provenance row | `assert_no_orphans` (existing) |
| Sign convention: expense<0, income>0, transfers exempt | `assert_sign_convention` (existing) |
| Amount precision: `DECIMAL(18,2)`, no truncation | `assert_amount_precision` |
| Date bounds: all `transaction_date` within scenario's declared date range | `assert_date_bounds` |

#### Tier 2 — Semantic Correctness (where applicable)

| Check | Primitive |
|---|---|
| Balanced transfers: confirmed transfer pairs sum to zero | `assert_balanced_transfers` (existing) |
| Categorization accuracy + per-category precision/recall vs ground truth | `score_categorization` (existing — extend with P/R breakdown) |
| Transfer detection: F1 + raw precision and recall (separately, to catch one-sided bias) | `score_transfer_detection` (existing — extend) |
| Match confidence distribution within expected bounds | `assert_distribution_within_bounds` (existing — wire in) |
| **Negative expectations**: labeled non-matches that must NOT collapse | `assert_negative_match` |

#### Tier 3 — Pipeline Behavior

| Check | Primitive |
|---|---|
| Idempotency: re-run `transform` / re-load same fixture → row counts unchanged | `assert_idempotent` |
| Incremental safety: load A then B with overlap → only new rows added; A's matches not reprocessed | `assert_incremental_safe` |
| Empty-input handling: empty CSV → empty raw, no crash, downstream tables empty | `assert_empty_input_safe` |
| Malformed-input handling: missing column / bad header → loader rejects with clear error, no partial load | `assert_malformed_input_rejected` |
| Subprocess parity: same input via subprocess vs. in-process produces identical output | `assert_subprocess_parity` |

#### Tier 4 — Distribution / Quality

| Check | Primitive |
|---|---|
| Amount distribution bounds (min/max/mean within plausible range) | `assert_amount_distribution` |
| Date continuity: no month-long gaps per account in multi-year scenarios | `assert_date_continuity` |
| Ground-truth coverage: ≥90% of `fct_transactions` labeled in `synthetic.ground_truth` | `assert_ground_truth_coverage` |
| Category distribution sanity (no single category swallows >X% of rows) | `assert_category_distribution` |

#### Tier 5 — Operational

| Check | Primitive |
|---|---|
| Step duration thresholds (perf regression detection) | pytest `--durations`, slow-marker gating |
| Memory ceiling | `pytest-memray` or equivalent, optional |

### R2 — Per-scenario tier matrix

This spec ships with a binding matrix declaring what each existing scenario covers and what it must add. Future agents MUST update this matrix when adding a scenario.

| Scenario | T1 (all 10) | T2 | T3 | T4 | T5 |
|---|---|---|---|---|---|
| `basic-full-pipeline` | required | categorization P/R | **idempotency** (new) | — | — |
| `family-full-pipeline` | required (replace `±15%` with derived formula) | balanced transfers, categorization P/R, transfer F1 + P + R | **idempotency** (new) | **date continuity, ground-truth coverage** (new) | — |
| `dedup-cross-source` | required | match confidence dist, **negative expectations** (new) | **incremental safety** (new) | — | — |
| `transfer-detection-cross-account` | required | transfer F1 + P + R, **negative pairs** (new) | — | date continuity | — |
| `migration-roundtrip` | required + **pre/post row count parity** (new) | — | — | — | — |
| `encryption-key-propagation` | required (replace `min_rows ≥ 100` with derived count) | — | subprocess parity | — | — |
| `idempotency-rerun` *(new)* | required | — | idempotency, incremental safety | — | — |
| `dedup-negative-fixture` *(new)* | required | negative expectations | — | — | — |
| `empty-input-handling` *(new)* | required (zero-row variants) | — | empty-input safe | — | — |
| `malformed-input-rejection` *(new)* | required | — | malformed-input rejected | — | — |

### R3 — Independent-expectations rule

The rule lives in [`.claude/rules/testing.md`](../../.claude/rules/testing.md) under "Scenario Expectations Must Be Independently Derived." Summary of the contract:

- Allowed derivation paths: input fixture (count by hand), persona/generator config (deterministic formula), or hand-authored ground truth written before running.
- Forbidden: observe-and-paste.
- Tolerances require an accompanying formula and a variance comment.
- Failing expectations default to "fix the code"; updating the expectation requires PR justification.
- Negative expectations are required wherever positive expectations exist.

### R4 — Bug-report recipe

A contributor (or their coding agent) MUST follow [`docs/guides/scenario-authoring.md`](../guides/scenario-authoring.md) when translating a user bug report into a permanent scenario. The recipe:

1. Capture the bug report and (ideally) an anonymized DB snapshot via `testing-anonymized-data.md` tooling.
2. Reproduce by extracting an isolated fixture under `tests/scenarios/data/fixtures/<bug-id>/`.
3. Express the bug as an expectation derived independently per R3.
4. Verify the scenario fails on the broken code.
5. Fix the code → verify the scenario passes.
6. Land the scenario as permanent regression coverage.

### R5 — Relocation to `tests/scenarios/`

The scenario runner, steps, loader, fixture loader, expectations module, and YAML data move from `src/moneybin/testing/scenarios/` to `tests/scenarios/`. The synthetic data **generator** (`src/moneybin/testing/synthetic/`) stays in `src/` because `moneybin synthetic generate` remains a supported user command. The `moneybin synthetic verify` CLI is removed; scenarios run via `pytest tests/scenarios/ -m scenarios` and the `make test-scenarios` target. The bespoke `ResponseEnvelope` is dropped in favor of `pytest-json-report`.

### R6 — Shared validation library

Reusable check primitives (the `assert_*` functions in the tier tables) live at `src/moneybin/validation/` and are consumed by both the scenario suite and `data-reconciliation.md`'s runtime views. This is the only validation code that ships with the package — scenario fixtures and runner code are test-only.

## Data Model

No schema changes. This spec adds tests and assertion primitives, and relocates existing test infrastructure.

## Implementation Plan

### Files to Create

- `src/moneybin/validation/structural.py` — `assert_row_count`, `assert_schema_snapshot`, `assert_source_system_populated`, `assert_amount_precision`, `assert_date_bounds`
- `src/moneybin/validation/behavioral.py` — `assert_idempotent`, `assert_incremental_safe`, `assert_empty_input_safe`, `assert_malformed_input_rejected`, `assert_subprocess_parity`
- `src/moneybin/validation/quality.py` — `assert_amount_distribution`, `assert_date_continuity`, `assert_ground_truth_coverage`, `assert_category_distribution`
- `src/moneybin/validation/semantic.py` — `assert_negative_match`, P/R breakdown helpers
- `tests/scenarios/conftest.py` — shared fixtures: encrypted DB bootstrap, MONEYBIN_HOME isolation, persona generator helpers
- `tests/scenarios/test_basic_full_pipeline.py` — port + add idempotency
- `tests/scenarios/test_family_full_pipeline.py` — port + replace `±15%`, add date continuity, ground-truth coverage
- `tests/scenarios/test_dedup_cross_source.py` — port + add negative-fixture cases, incremental safety
- `tests/scenarios/test_transfer_detection.py` — port + add negative pairs
- `tests/scenarios/test_migration_roundtrip.py` — port + add pre/post row-count parity
- `tests/scenarios/test_encryption_key_propagation.py` — port + replace `min_rows ≥ 100` with derived count
- `tests/scenarios/test_idempotency_rerun.py` *(new)*
- `tests/scenarios/test_dedup_negative_fixture.py` *(new)*
- `tests/scenarios/test_empty_input_handling.py` *(new)*
- `tests/scenarios/test_malformed_input_rejection.py` *(new)*
- `tests/scenarios/data/fixtures/dedup-negative/...` — hand-authored fixture: same date/amount, different merchants, must NOT collapse
- `tests/scenarios/data/fixtures/empty-input/...` — empty CSV / OFX
- `tests/scenarios/data/fixtures/malformed/...` — missing-header CSV, truncated OFX
- `docs/guides/scenario-authoring.md` — full bug-report recipe

### Files to Modify

- `.claude/rules/testing.md` — add "Scenario Expectations Must Be Independently Derived" (done in this branch)
- `CONTRIBUTING.md` — link to `docs/guides/scenario-authoring.md` and add a one-paragraph summary of the recipe
- `docs/specs/INDEX.md` — add this spec
- `docs/specs/testing-overview.md` — reference this spec; note relocation to `tests/`
- `docs/specs/testing-scenario-runner.md` — note that scenarios now live in `tests/scenarios/`, link to this spec for taxonomy
- `Makefile` — repoint `test-scenarios` target at pytest
- `.github/workflows/scenarios.yml` — replace `moneybin synthetic verify --all --output=json` with `uv run pytest tests/scenarios/ -m scenarios --json-report`

### Files to Delete

- `src/moneybin/testing/scenarios/` (entire directory — migrated to `tests/scenarios/`)
- `src/moneybin/cli/commands/synthetic.py::verify` and its E2E tests (the only `src/` consumer)

### Sequencing

1. **Phase 1 — Relocation.** Move `src/moneybin/testing/scenarios/` to `tests/scenarios/`, port to pytest, drop ResponseEnvelope, remove `synthetic verify` CLI, update CI workflow. No new behavior; existing assertions/expectations preserved 1:1.
2. **Phase 2 — Validation library.** Extract shared primitives to `src/moneybin/validation/`. Update existing scenarios to use them. No new assertions yet.
3. **Phase 3 — Tier 1 backfill.** Add the missing Tier 1 assertions to every existing scenario (source attribution, schema snapshot, amount precision, date bounds). Replace `±15%` and `min_rows ≥ 100` with derived formulas.
4. **Phase 4 — New scenarios.** Author the four new scenarios (`idempotency-rerun`, `dedup-negative-fixture`, `empty-input-handling`, `malformed-input-rejection`).
5. **Phase 5 — Tier 2/4 enrichment.** Add P/R breakdowns, ground-truth coverage, date continuity to applicable scenarios.
6. **Phase 6 — Recipe + governance.** Ship `docs/guides/scenario-authoring.md` and CONTRIBUTING.md updates. Update `testing-overview.md` and `testing-scenario-runner.md` references.

### PR Grouping (chosen 2026-04-30)

The six phases ship as **three** PRs, not six — grouped by review-coherence rather than phase boundaries:

| PR | Phases | Plan | Why grouped this way |
|---|---|---|---|
| **PR 1** | Phase 1 | _shipped_ | Pure relocation — runner moved to `tests/scenarios/_runner/`, scenarios driven via pytest. |
| **PR 2** | Phases 2 + 3 + 4 | _written after PR 1 merges_ | Validation-library extract (P2) is the input to Tier 1 backfill (P3); the four new scenarios (P4) need both to be authored cleanly. Splitting them produces churn. |
| **PR 3** | Phases 5 + 6 | _written after PR 2 merges_ | Tier 2/4 enrichment (P5) and the contributor recipe (P6) are the documentation/quality polish layer; they don't gate each other but neither blocks anything downstream. |

Each plan is written only after the prior PR merges, so it grounds in the real post-merge file layout instead of a predicted one.

After PR 3 merges, this spec moves to `implemented` and becomes the binding architectural reference for all future scenario work.

### Key Decisions

- **Drop ResponseEnvelope.** No installed base; pytest expresses step-level halting via fixture failure context and mixed result types via separate test functions / parametrize cases. `pytest-json-report` covers the CI artifact need. If a future agent loop needs richer metadata, design that on real requirements.
- **Synthetic generator stays in `src/`.** `moneybin synthetic generate` is a legitimate user command for trying the tool with sample data. Only the *verifier* (a contributor tool) moves to `tests/`.
- **Validation primitives in `src/moneybin/validation/`.** Both this spec and `data-reconciliation.md` consume them. Tests-only code stays in `tests/`.

## CLI Interface

`moneybin synthetic verify` is **removed**.

Replacement:

```bash
make test-scenarios                                            # Run all scenario tests
uv run pytest tests/scenarios/ -m scenarios -v                 # Same, manual
uv run pytest tests/scenarios/test_dedup_cross_source.py -v    # Single scenario
uv run pytest tests/scenarios/ -m scenarios --json-report      # CI artifact
```

The `synthetic generate` command is unchanged.

## MCP Interface

None. Scenarios are contributor tooling.

## Testing Strategy

Self-applying. The deliverables of this spec *are* the testing strategy. Two meta-checks:

1. **Tier matrix lint.** A small pytest-collection-time check verifies every test in `tests/scenarios/` declares (via marker or docstring tag) which tier(s) it covers. CI fails on missing declarations.
2. **Independent-derivation review.** Code review enforces R3. PRs touching `tests/scenarios/` that update an expected value without justification get flagged.

## Synthetic Data Requirements

No new synthetic data shapes required for the existing personas. New scenarios reuse `basic` and `family`. The empty-input and malformed-input scenarios use hand-authored fixtures (not generator output).

## Dependencies

- `pytest-json-report` (new dev dependency) — replaces ResponseEnvelope's JSON output role.
- [`testing-scenario-runner.md`](testing-scenario-runner.md) — provides the existing assertion library this spec extends.
- [`testing-synthetic-data.md`](testing-synthetic-data.md) — provides ground-truth labels consumed by Tier 2/4 evaluations.
- [`testing-anonymized-data.md`](testing-anonymized-data.md) — *soft* dependency. The bug-report recipe references the anonymizer as the prescribed tool for producing PII-free fixtures from real user databases. Until the anonymizer ships, contributors hand-author fixtures or use synthetic personas.

## Out of Scope

- **Runtime user-data reconciliation.** Lives in [`data-reconciliation.md`](data-reconciliation.md). This spec shares primitives with that one but targets pre-merge CI, not runtime user reports.
- **Performance / load testing.** Tier 5 (Operational) sketches durations and memory ceilings as future work; this spec does not implement them.
- **Anonymizer implementation.** [`testing-anonymized-data.md`](testing-anonymized-data.md) owns the anonymization engine. This spec only consumes its output.
- **MCP exposure of scenarios.** Scenarios are contributor tooling. Exposing them via MCP would require a separate spec.
