# Scenario Authoring Guide

This guide is the prescribed recipe for adding a scenario to MoneyBin's pipeline test suite — most commonly to reproduce a user bug report and lock in regression coverage. It is written so a coding agent can execute it end-to-end given only a bug report and (ideally) an anonymized DB snapshot.

**Architectural authority:** The taxonomy and rules below come from [`docs/specs/testing-scenario-comprehensive.md`](../specs/testing-scenario-comprehensive.md) and [`.claude/rules/testing.md`](../../.claude/rules/testing.md). Read both before authoring. They are normative — deviations need explicit PR justification.

---

## When to Author a Scenario

| Situation | Author a scenario? |
|---|---|
| User bug report involving the data pipeline (raw → prep → core) | **Yes** |
| User bug report involving matching, categorization, transfers, or reconciliation | **Yes** |
| User bug report in pure CLI / argument parsing | No — add an E2E test instead |
| Internal refactor with no behavior change | No — existing scenarios cover regressions |
| New pipeline stage or new data source | **Yes** — add a scenario alongside the feature |

---

## The Recipe

### Step 1 — Capture the bug

Collect from the user (or the report intake flow):

1. **Symptom**: what they expected vs. what happened (e.g., "I imported X, ran transform, and saw 17 transactions in core; should have been 20").
2. **Reproducer data**: an anonymized snapshot of the relevant slice of their database, produced via [`testing-anonymized-data.md`](../specs/testing-anonymized-data.md) tooling. If the anonymizer is not yet available, hand-author a minimal fixture — see Step 2.
3. **Pipeline stage where the bug manifests**: import? transform? match? categorize?
4. **Affected tables / fields**: which `core.*` table shows the wrong value.

Do **not** copy un-anonymized real user data into the repo under any circumstance. If you cannot produce an anonymized fixture, stop and route the report through the privacy-aware intake.

### Step 2 — Build the fixture

Place the fixture under `tests/scenarios/data/fixtures/<bug-id>/`. Choose `<bug-id>` to be a stable, descriptive slug (`csv-amazon-trailing-comma`, `transfer-same-day-collision`).

Fixture types:

- **CSV / OFX / PDF input file**: drop the anonymized export at `<bug-id>/input.csv` (or `.ofx`, etc.).
- **Pre-loaded raw rows**: a YAML file describing rows to insert directly into raw tables (used when the bug is in `transform` or downstream and you don't need to re-exercise the loader).
- **Pre-loaded core state**: rare; only when reproducing a bug in a downstream stage that depends on existing state.

Add a `<bug-id>/README.md` with:

- The original symptom (anonymized — no user names, no real merchants).
- Why the fixture has the shape it does (which row triggers the bug and why).
- The expected behavior, derived independently of the broken code.

### Step 3 — Express the bug as an expectation (independently derived)

This is the most important step. The expectation must be derivable from the fixture alone — not from running the broken code, not from running the *fixed* code, not from "looks right."

Allowed derivation paths (per `.claude/rules/testing.md`):

1. **Count the input fixture by hand.** "I authored 20 rows in `input.csv`. After dedup, 3 are duplicates of one other. Expected: 17 rows in `fct_transactions`."
2. **Derive from persona / generator config.** "Family persona, 3 accounts × 3 years × 80 txns/account/month × 12 months = 8,640 rows."
3. **Hand-author ground truth.** "I labeled rows 4 and 7 as a transfer pair. Expected: `fct_transactions` shows them sharing a `transfer_pair_id`."

Forbidden:

- Running the scenario, observing the row count, and pasting it into the YAML.
- Bare tolerance bands (`±15%`) without a formula for the variance source.
- "It currently produces this, so that's the expected value."

If your scenario asserts positive matching, **also include negative cases** — records that should *not* match — under the same fixture. Otherwise you only catch under-matching, not over-matching.

### Step 4 — Pick the tier coverage

Per [`testing-scenario-comprehensive.md`](../specs/testing-scenario-comprehensive.md), every scenario covers:

- **Tier 1 (Structural Invariants) — required.** Row counts, schema, FK integrity, no-NULLs, no-dupes, source attribution, provenance, sign convention, amount precision, date bounds.
- **Tier 2 (Semantic Correctness) — when applicable.** Balanced transfers, P/R for categorization/transfers, match confidence, negative expectations.
- **Tier 3 (Pipeline Behavior) — for behavior bugs.** Idempotency, incremental safety, empty-input, malformed-input, subprocess parity.
- **Tier 4 (Distribution / Quality) — for multi-account or multi-year fixtures.** Amount distribution, date continuity, ground-truth coverage, category distribution.
- **Tier 5 (Operational) — opt-in.** Duration, memory.

Declare the tier coverage in the test docstring (e.g., `tiers: T1, T2-balanced-transfers, T3-incremental`).

### Step 5 — Write the test

Tests live in `tests/scenarios/test_<bug-id>.py`. Use the shared fixtures from `tests/scenarios/conftest.py` to bootstrap an encrypted DB and isolated `MONEYBIN_HOME`.

Use the assertion primitives from `src/moneybin/validation/`:

```python
import pytest
from moneybin.validation import (
    assert_row_count,
    assert_no_duplicates,
    assert_source_system_populated,
    assert_negative_match,
)


@pytest.mark.scenarios
def test_csv_amazon_trailing_comma(scenario_db, load_fixture, run_pipeline):
    """Reproduce bug #142: trailing comma in Amazon CSV row dropped from raw.

    tiers: T1, T3-malformed-input
    derivation: 20 rows in input.csv counted by hand; 3 are intentional dupes.
    """
    load_fixture("csv-amazon-trailing-comma/input.csv")
    run_pipeline(["transform", "match"])

    assert_row_count(scenario_db, "raw.tabular_transactions", expected=20)
    assert_row_count(scenario_db, "core.fct_transactions", expected=17)
    assert_no_duplicates(scenario_db, "core.fct_transactions", "transaction_id")
    assert_source_system_populated(scenario_db, expected={"csv"})
```

### Step 6 — Verify failure first

Before fixing the bug, **run the new scenario against the broken code**. It must fail with the same symptom the user reported. If it doesn't, your fixture or expectation doesn't actually capture the bug — go back to Step 2 or 3.

```bash
uv run pytest tests/scenarios/test_<bug-id>.py -v
```

### Step 7 — Fix the code → verify pass

Now fix the underlying bug. Re-run:

```bash
uv run pytest tests/scenarios/test_<bug-id>.py -v
make verify-scenarios   # confirm no regressions in other scenarios
```

Both must pass. If the fix breaks another scenario, that's signal — investigate before updating any expectations. Per the independent-expectations rule, "the new code produces a different number" is not justification.

### Step 8 — Land

Open a PR with:

- The fixture
- The new test
- The fix
- A note in the PR description linking to the original bug report

The scenario is now permanent regression coverage. Future code changes that re-introduce the bug will fail this test in CI.

---

## Common Mistakes

| Mistake | Fix |
|---|---|
| Pasted observed row count into expected | Re-derive from fixture or persona config |
| Tolerance band with no formula | Add a comment explaining the variance source, or replace with exact value |
| Only positive expectations | Add a negative fixture: records that should *not* match |
| Skipped Tier 1 because "the bug isn't structural" | Tier 1 is always required — they're cheap and catch unrelated regressions |
| Used real user data because anonymizer wasn't ready | Hand-author a minimal fixture instead — never commit raw user data |
| Test passes against broken code | Fixture or expectation is wrong — not the code |

---

## Reference

- [`docs/specs/testing-scenario-comprehensive.md`](../specs/testing-scenario-comprehensive.md) — full taxonomy and architectural rules
- [`.claude/rules/testing.md`](../../.claude/rules/testing.md) — independent-expectations rule
- [`docs/specs/testing-anonymized-data.md`](../specs/testing-anonymized-data.md) — anonymizer (the prescribed fixture-production tool)
- [`docs/specs/testing-scenario-runner.md`](../specs/testing-scenario-runner.md) — pre-existing assertion / expectation / evaluation primitives
- [`docs/specs/data-reconciliation.md`](../specs/data-reconciliation.md) — runtime user-facing checks that share assertion primitives
