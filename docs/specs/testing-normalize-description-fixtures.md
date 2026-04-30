# Feature: Normalize-Description Golden Fixtures

## Status
implemented

## Goal

Provide a YAML-driven golden-case fixture for `normalize_description()` so
contributors can add real-world transaction descriptions and their expected
normalized form, and so the test suite asserts exact equality rather than
loose substring checks.

## Background

`moneybin.services._text.normalize_description` is the deterministic regex
cleanup applied to raw transaction descriptions before merchant-pattern
matching and auto-rule pattern extraction. Call sites:

- `services/auto_rule_service.py::_extract_pattern` — fallback when no
  merchant is linked to a transaction.
- `services/auto_rule_service.py::apply_rules` — used during rule evaluation
  alongside `LOWER(description)` SQL matching.

Existing coverage in
`tests/moneybin/test_services/test_categorization_service.py::TestNormalizeDescription`
has two gaps:

1. Several assertions use `"X" not in result` rather than exact equality, so
   regressions that subtly change output (extra whitespace, partial strip)
   pass undetected.
2. There is no documented surface for contributors to add cases when they
   encounter a real description that should normalize a specific way.

Related specs:
- [Auto-Rule Generation](categorization-auto-rules.md) — primary consumer of
  `normalize_description` via merchant-first pattern extraction.
- [Categorization Overview](categorization-overview.md) — broader context.

## Requirements

1. A YAML fixture file enumerates `(raw, expected)` pairs with a stable `id`
   per case.
2. A parametrized pytest test loads every case and asserts
   `normalize_description(raw) == expected` exactly.
3. Pytest test ids match the fixture `id` so failures point at the offending
   row.
4. Loader rejects duplicate `id` values at collection time.
5. All ten existing `TestNormalizeDescription` cases are migrated into the
   fixture with exact-equality expectations. Where the current regex output
   does not match the intended normalized form, the regex in `_text.py` is
   fixed in the same change.
6. Contributor-facing documentation explains when, where, and how to add a
   case, and what to do when the test fails.
7. Existing call sites of `normalize_description`
   (`auto_rule_service._extract_pattern`, `auto_rule_service.apply_rules`)
   continue to pass their own tests after any regex changes.

## Data Model

No database changes. Fixture file only:

`tests/moneybin/test_services/fixtures/normalize_description_cases.yaml`

```yaml
cases:
  - id: square-prefix-and-store-id
    raw: "SQ *STARBUCKS #1234"
    expected: "STARBUCKS"
    note: "Square POS prefix + trailing store id"
  - id: trailing-city-state
    raw: "STARBUCKS SEATTLE WA"
    expected: "STARBUCKS"
```

Schema per case:
- `id` (string, required, unique) — used as pytest parametrize id.
- `raw` (string, required) — input passed verbatim to
  `normalize_description`.
- `expected` (string, required) — exact expected output.
- `note` (string, optional) — short reviewer-facing context.

## Implementation Plan

### Files to Create

- `tests/moneybin/test_services/fixtures/normalize_description_cases.yaml` —
  golden cases, seeded with the migrated existing tests.

### Files to Modify

- `tests/moneybin/test_services/test_categorization_service.py` — replace
  `TestNormalizeDescription` with `TestNormalizeDescriptionGoldens` plus a
  module-level `_load_normalize_cases()` helper.
- `src/moneybin/services/_text.py` — adjust regexes as needed so migrated
  cases pass on the intended exact output (no behavior change beyond making
  current cases produce clean results).
- `.claude/rules/testing.md` — add a "Golden-case fixtures" subsection
  documenting the contribution workflow.

### Key Decisions

- **YAML, not JSON or Python.** Aligns with existing project convention for
  contributor-facing fixtures (synthetic-data personas/merchants, scenario
  runner). See memory: project standardizes on YAML to support a future
  community contribution model.
- **Single fixture file per pure function.** No directory-of-cases pattern —
  the function is small and the case count is small.
- **Migrate the existing class wholesale.** No reason to keep
  `TestNormalizeDescription` alongside the new parametrized class; the new
  class subsumes it.
- **Fix the regex in the same PR when migration reveals dirty output.** The
  user explicitly chose this over capturing-current-behavior; the rationale is
  that loose assertions hid actual cleanup gaps and we should fix them now
  while the change is in flight.
- **No layer-2 fixture in this spec.** Merchant-pattern matching is covered by
  the scenario runner (see `testing-scenario-runner.md`); adding a parallel
  fixture format here would duplicate that surface.

## CLI Interface

None.

## MCP Interface

None.

## Testing Strategy

- **Unit:** Every YAML row exercises `normalize_description` directly via
  `pytest.mark.parametrize`. Failures surface with the case `id` in the test
  output.
- **Loader sanity:** A separate unit test verifies the loader raises on
  duplicate `id` values.
- **Regression safety:** Run the full categorization-service and
  auto-rule-service test suites after regex changes; both depend on
  `normalize_description` and would catch behavioral drift in real call sites.
- **No integration or E2E layer.** `normalize_description` is a pure function
  with no I/O; layered tests would add no signal.

## Synthetic Data Requirements

None.

## Dependencies

None beyond existing project deps (PyYAML is already used by the synthetic
data generator and scenario runner).

## Out of Scope

- Layer-2 (raw description → merchant_id / category) golden cases.
- Changes to `auto_rule_service` pattern-extraction logic.
- New normalization features (e.g., handling additional POS prefixes beyond
  the current set) — those should arrive as new cases plus targeted regex
  changes after this spec ships.
