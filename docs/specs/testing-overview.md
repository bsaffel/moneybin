# Testing & Validation — Overview

> Last updated: 2026-09-01
> Status: Ready — umbrella doc for the testing & validation initiative. Child specs listed in [Child Specs](#child-specs) are written separately. Four child specs (`testing-synthetic-data.md`, `testing-scenario-runner.md`, `testing-scenario-comprehensive.md`, `testing-normalize-description-fixtures.md`) are implemented; three (`testing-csv-fixtures.md`, `testing-format-compat.md`, `testing-migration-safety.md`) and `testing-anonymized-data.md` remain planned.
> Companions: `CLAUDE.md` "Architecture: Data Layers", [`sync-overview.md`](sync-overview.md) (owns Plaid Sandbox testing)

## Purpose

Testing & Validation is MoneyBin's umbrella spec for verification infrastructure. It defines independently useful testing capabilities that compose into end-to-end verification scenarios. Two first-class goals:

1. **Autonomous verification.** Agents completing features can validate correctness end-to-end without human review. The same capabilities that a developer uses to check their work are available programmatically — assertions, scenarios, and scored evaluations that return structured pass/fail/score results.

2. **Realistic demo environments.** Generate plausible, multi-year financial histories that look like real people's data. A demo environment should be indistinguishable from a real user's database at a glance — recurring merchants, seasonal spending patterns, salary deposits, realistic balances. The generator produces *life-like data*; the verification tiers validate properties of that data, not the other way around. There should never be "just enough data to pass the assertions."

## Vision

> **Any feature can be built, tested, and verified without a human in the loop. The testing infrastructure provides the data, the assertions, and the scoring to make autonomous development reliable.**

Three commitments:

1. **Independently useful capabilities.** The generator, audit catalog, fixture library, and scenario format each deliver value on their own. You can generate demo data without running scenarios. You can run assertions without generating data. The scenario format composes them, but doesn't gate them.
2. **One definition, multiple execution contexts.** A data-quality check is written once as SQLMesh audit SQL and read the same way during development, in CI, and at runtime — `make test-scenarios` and `moneybin system doctor` both go through `moneybin.audits.runner`, so neither can report a verdict the other would not.
3. **Life-like data, not test data.** Synthetic data models real financial lives — not minimal fixtures designed to exercise code paths. Realistic data catches realistic bugs.

## Verification Tiers

Three tiers, each solving a different problem. A scenario can use any combination.

### Tier 1 — Golden Snapshots (regression)

Deterministic output from a seeded generator, compared against committed baselines. Catches unintended changes to pipeline behavior. Brittle by design — when a snapshot breaks, you either fix the regression or update the baseline. Best suited for stable, well-understood paths.

### Tier 2 — Property Assertions (pipeline integrity)

Invariants that must hold regardless of the specific data. These are the workhorse for agentic verification — an agent doesn't need to know expected row counts, just that referential integrity holds, amounts are well-formed, and no duplicates exist. Properties survive schema evolution better than snapshots.

Example properties:

- Every `fct_transactions.account_id` exists in `dim_accounts`
- No duplicate `transaction_id` within a `source_type`
- All amounts are non-NULL `DECIMAL(18,2)`
- Date ranges are contiguous (no gaps in monthly coverage for a given account)
- Sign convention: expenses negative, income positive

### Tier 3 — Scored Evaluation (AI/ML quality)

Ground-truth labels in the synthetic data enable precision/recall/accuracy scoring for categorization, transfer detection, merchant normalization, and future ML features. Returns metrics, not pass/fail — though scenarios can set threshold gates.

### Testing Domains

The tiers apply across multiple testing domains. Each domain uses the tiers differently:

| Domain | Tier 1 (Snapshots) | Tier 2 (Properties) | Tier 3 (Scored) |
|---|---|---|---|
| **Pipeline integrity** | Expected row counts after full run | Referential integrity, no dupes, sign convention | Categorization accuracy, transfer detection F1 |
| **Format compatibility** | Known CSV/OFX files produce exact expected parse output | All required columns present, types correct, no parse errors | Column-mapping accuracy for smart detection |
| **Migration safety** | Row counts and checksums unchanged post-migration | No orphaned foreign keys, no NULL-ed previously-populated fields | N/A |
| **Idempotency** | Second run produces identical output | No new rows, no changed values | N/A |
| **Resilience** | N/A | Corrupt/truncated input produces graceful error + zero data loss | N/A |
| **Security** | N/A | No PII in logs, parameterized queries only | N/A |

The first three domains (pipeline, format compatibility, migration safety) each get their own child spec. The bottom three (idempotency, resilience, security) are cross-cutting concerns woven into scenarios as needed.

## Data Quality & Verification Infrastructure

One definition, different execution contexts. From the perspective of a user, agent, or CI pipeline, you ask one question: "Is my data correct?" The answer comes from one place — the audit SQL — whether you asked through `moneybin system doctor`, a scenario run, or a `sqlmesh run`.

### SQLMesh audits — the canonical home for data-quality checks

Location: `src/moneybin/sqlmesh/audits/*.sql`

A check on a `core.*` relation is written once, as audit SQL, and every
surface reads that one definition. `src/moneybin/audits/runner.py` renders and
executes them; `DoctorService` folds the outcomes into `moneybin system
doctor`, and scenario YAML asserts on them through `assert_transform_audit`.
Nothing else re-expresses a check in Python.

Each audit projects the offending entity's ID in column 0, carries its
semantics in a header comment, and declares `standalone TRUE` so it runs
against every materialized model rather than only where a model references it.
Standalone audits are non-blocking in SQLMesh — a violation warns rather than
halting a transform — so the outcome has to be read deliberately, which is
what doctor and the scenario assertion do.

| Audit | Checks |
|---|---|
| `fct_transactions_sign_convention` | `amount` is classifiable, and `transaction_direction` / `amount_absolute` agree with its sign |
| `fct_transactions_fk_integrity` | Every transaction's `account_id` resolves in `dim_accounts` |
| `bridge_transfers_balanced` | Both legs of a confirmed transfer pair exist and cancel exactly |
| `fct_investment_transactions_sign_convention` | Buys and reinvests are cash out, sells are cash in |
| `fct_investment_transactions_uniqueness` | `investment_transaction_id` holds its declared grain |
| `fct_investment_transactions_fk_integrity` | Every investment transaction's `account_id` resolves in `dim_accounts` |

The set grows organically — when a feature needs a check that doesn't exist,
add an audit file. Two rules bound what belongs here: an audit polices what the
ledger can prove, never what a label implies (a refund carries a positive
amount under an expense category, so category-vs-sign reports correct data as a
defect), and a diverged Python twin is never the answer — fix the SQL.

### Scenario-support primitives

Location: `tests/validation/` (assertions, expectations, evaluations)

Test-only scaffolding for the scenario runner, not a second home for
data-quality truth. Every consumer is under `tests/`, which is why the package
lives there. Three things an audit cannot express keep it earning its place:

| Kind | Example | Why not an audit |
|---|---|---|
| Parameterized shape checks | `assert_no_duplicates(db, table, columns)`, `assert_row_count_delta(db, table, expected, tolerance_pct)` | Table, columns, and bounds come from the scenario, not the model |
| Environment checks | `assert_migrations_at_head`, `assert_no_unencrypted_db_files`, `assert_sqlmesh_catalog_matches` | Assert about the profile and the filesystem, not about rows |
| Ground-truth scoring | `score_categorization`, `score_transfer_detection` | Compare against `synthetic.ground_truth`, which no production database has |

`assert_transform_audit(db, audit=...)` is the bridge: it runs one canonical
audit and reports it as an `AssertionResult`, so a scenario and a doctor run
cannot disagree about what a check means.

### Execution Contexts

| Context | When | Who triggers | What runs | What you see |
|---|---|---|---|---|
| **Development** | Agent or human building a feature | `make test-scenarios` (pytest scenarios) or `moneybin system doctor` | Everything — unit tests, audits, assertions, evaluations | Structured pass/fail/score report |
| **Pipeline** | `sqlmesh run` transforms data | Automatic | SQLMesh audits + unit tests fire as part of the run | Pipeline halts or warns on failure |
| **CI** | PR or commit | GitHub Actions (future) | Full scenario suite against synthetic data | Pass/fail gate on the PR |
| **Runtime** | After a real data load or import | `moneybin system doctor` | Every standalone audit plus doctor's own `app.*` invariants, against the live database | User-facing health report |

### The rest of the SQLMesh surface

- **Model-attached audits** — SQLMesh's built-ins (`not_null`, `unique`, `unique_combination_of_columns`, `forall`) declared in a model's `audits (...)` property. These are blocking, and appropriate for a constraint a model must never violate. A check that should report rather than halt belongs in `src/moneybin/sqlmesh/audits/` as a standalone audit.
- **SQLMesh unit tests** — YAML-defined fixture tests in `src/moneybin/sqlmesh/tests/` that validate transformation logic with controlled inputs and expected outputs. These test the *code*, not the data — "given these raw rows, does the staging model produce the right output?"

## Persona Catalog

Five personas representing distinct financial lives. The umbrella defines *what* each persona represents and *which code paths it exercises*. The generator child spec owns *how* to produce life-like data for each.

Each persona uses a named profile to keep its data isolated. The existing profile system (`MoneyBinSettings.profile`) already supports this — each profile gets its own database, data directory, logs, and env file. The persona-to-profile name mapping (`basic` → `alice`, etc.) is a recommended convention, not enforced; any persona can be generated into any profile name.

### V1 Personas (existing schema)

| Persona | Profile | Financial profile | Key code paths exercised |
|---|---|---|---|
| `basic` | `alice` | Single income, 1 checking + 1 credit card, simple spending, ~300 txns/yr | Core pipeline, basic categorization, OFX import |
| `family` | `bob` | Dual income, joint + individual accounts, child-related expenses, shared bills, ~1,500 txns/yr | Multi-account, transfer detection, split categorization |
| `freelancer` | `charlie` | Irregular income (invoices + 1099), business + personal accounts, quarterly tax payments, ~800 txns/yr | Irregular income patterns, business categorization, tax-relevant transactions |
| `international` | `eve` | Five banks in five countries, one account and one local income per currency (EUR, GBP, CAD, AED, USD), ~690 txns/yr | Per-account currency through both the OFX and tabular writers, per-currency net worth segmentation, the unpriced-pair path (AED sits outside the FX provider's published set) |

### Future Personas (gated on data model)

| Persona | Profile | Gates on | Key additions |
|---|---|---|---|
| `investor` | `david` | Investment schema | Brokerage + 401k + IRA, dividends, trades, capital gains |

`international` ships without cross-currency transfers or forex fees: both need
a conversion the generator does not yet perform, and they arrive with M1K.3.

### Anonymized Generation Mode

Generate synthetic data that preserves the statistical properties and structure of the user's real database — transaction distributions, account relationships, spending patterns — while applying industry-standard anonymization (merchant name substitution, amount perturbation, date shifting, account ID replacement). This is a **peer child spec** (`testing-anonymized-data.md`), not part of the persona-based generator. Different problem (data masking pipeline vs. financial life simulator), same output layer (`synthetic` schema, raw table writes). The anonymized dataset preserves existing categorizations as ground truth.

## Scenario Runner

Scenarios are pinned, reproducible test plans that go from an empty encrypted DuckDB through the full pipeline (`generate → transform → match → categorize`) and run assertions, expectations, and evaluations against the resulting data. The scenario file format, orchestration model, assertion/evaluation libraries, fixture-expectation contract, pytest harness (`make test-scenarios` / `pytest tests/scenarios/`), and v1 scenario catalog are owned by [`testing-scenario-runner.md`](testing-scenario-runner.md).

The runner is the missing test layer above unit, integration, and E2E: those check their own slice in isolation; the runner asserts that whole-pipeline output is correct.

Scenario authoring rules — taxonomy, independent-expectations rule, and
contributor recipe — are the responsibility of
[`testing-scenario-comprehensive.md`](testing-scenario-comprehensive.md),
which is the architectural authority for all scenario work.

## Child Specs

Child specs under this umbrella. Each is independently useful, designed knowing how it feeds into the scenario format.

| Child spec | Purpose | V1 scope | Key design concerns |
|---|---|---|---|
| `testing-synthetic-data.md` (implemented) | Produce life-like financial histories | Four fictional personas (`basic`, `family`, `freelancer`, `international`); deterministic seeding; ground-truth labels; YAML-driven personas and merchant catalogs; Level 2 realism | Declarative YAML architecture, merchant catalogs with real brand names, spending distributions, temporal realism, income patterns. Anonymized mode is a separate child spec (`testing-anonymized-data.md`). |
| `testing-scenario-runner.md` (implemented) | Whole-pipeline correctness with structured assertions, expectations, and evaluations | YAML scenario format, orchestrator with fresh encrypted DB per run, validation/evaluation primitive libraries, pytest harness (`make test-scenarios`), six shipped scenarios | Database isolation via `MONEYBIN_HOME` override; in-process service-layer execution; fixture expectations as first-class signal |
| `testing-scenario-comprehensive.md` (implemented) | Five-tier assertion taxonomy; bug-report recipe; architectural authority for all future scenario work | Tier matrix; independent-expectations rule; relocation of scenarios to `tests/scenarios/`; scenario-support primitives at `tests/validation/` | Pytest-native, contributor recipe, stable validation contract |
| `testing-normalize-description-fixtures.md` (implemented) | YAML golden cases for `normalize_description()` | Parametrized exact-equality tests; contributor surface for adding cases | Pure-function regression coverage; duplicate-id detection |
| `testing-csv-fixtures.md` (planned) | Curated bank export samples for format compatibility testing | Directory convention (`tests/fixtures/csv_formats/`), naming schema (`<institution>_<account_type>_<year>.csv` + `.expected.json`), initial fixtures from anonymized real exports | Anonymization checklist, contribution path, expected-result format for scoring smart detection |
| `testing-format-compat.md` (planned) | Verify parsers handle all known file formats correctly | Test harness that runs each extractor against its fixtures, compares to expected output | Assertion integration, how to add a new format test, failure reporting |
| `testing-migration-safety.md` (planned) | Verify schema migrations preserve data integrity | Pre/post migration assertions (row counts, checksums, no orphaned FKs, no NULLed fields) | Requires synthetic data to populate a DB before migration; depends on generator |

### Sequencing

1. Synthetic data generator ships first — everything else benefits from having realistic data
2. CSV fixture library can proceed in parallel (no dependency on generator)
3. Format compatibility and migration safety depend on having fixtures and/or generated data

### Deferred (with pointers)

- **Plaid Sandbox testing** — addressed in sync spec (`sync-overview.md`)
- **`investor` persona** — added when investment schema lands
- **`international` persona** — added when multi-currency schema lands

## Cross-Cutting Concerns

These aren't child specs — they're properties enforced across all scenarios via the assertion library.

| Concern | How it's handled |
|---|---|
| **Idempotency** | `assert_idempotent(conn, operation_fn)` — run any load/transform twice, assert identical state |
| **Resilience** | Dedicated assertions for error paths: truncated/corrupt input produces graceful errors + zero data corruption. Exercised by specific scenario steps that feed bad input. |
| **Security** | `assert_no_pii_in_output(log_capture)` — verify logs and error messages don't leak PII. `assert_parameterized_queries()` — static analysis or runtime check that no string-interpolated SQL reaches DuckDB. |
| **Performance** | `assert_completes_within(operation_fn, max_seconds)` — baseline timing assertions. Not a hard gate in v1, but establishes baselines for future CI enforcement. |

These grow as needed. No upfront framework — add an assertion when a new cross-cutting property matters.

## CLI Interface

| Command | Purpose |
|---|---|
| `moneybin synthetic generate --persona=family --profile=bob --years=3 --seed=42` | Generate persona-based synthetic data into a named profile |
| `moneybin synthetic anonymize --profile=anon --seed=42` | Generate anonymized synthetic data from current profile into a named profile (see `testing-anonymized-data.md`, separate child spec) — planned |
| `moneybin synthetic reset --persona=family --seed=42` | Wipe a generated profile and regenerate to clean state |
| `uv run pytest tests/scenarios/test_family_full_pipeline.py -v` | Run a pinned scenario (generate + pipeline + assertions + evaluation) |
| `make test-scenarios` | Run the full scenario suite (pytest-driven; replaces the retired `moneybin synthetic verify`) |
| `moneybin data verify` | User-facing health check — core assertions against the active profile (planned; not yet shipped) |

## Dependencies

- **Profiles** — named profile system must support multiple concurrent databases
- **SQLMesh** — audit and unit test infrastructure for pipeline-time checks
- **DuckDB** — assertion library queries run directly against DuckDB connections
- **Investment schema** (future) — gates `investor` persona
- **Multi-currency schema** (future) — gates `international` persona

## Out of Scope

- CI/CD pipeline configuration — implementation detail for later
- MCP tool testing — covered by MCP specs' own test plans
- Plaid Sandbox testing — deferred to sync spec (`sync-overview.md`)
- UI/visual testing — not applicable to current CLI/MCP architecture
