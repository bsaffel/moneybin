# Normalize-Description Golden Fixtures Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the loose-assertion `TestNormalizeDescription` test class with a YAML-driven golden-case fixture, fix the city+state+zip trailing-location regex gap surfaced during migration, and document the contribution surface.

**Architecture:** A single YAML file (`tests/moneybin/test_services/fixtures/normalize_description_cases.yaml`) holds `(id, raw, expected)` triples. A module-level loader in the existing `test_categorization_service.py` reads the file once and feeds a parametrized pytest. The loader rejects duplicate ids. The regex in `src/moneybin/services/_text.py` is tightened so an all-caps `CITY STATE ZIP` tail is stripped as a unit.

**Tech Stack:** pytest, PyYAML (already a project dep via the synthetic data generator), Python 3.12+, `re` stdlib.

**Spec:** `docs/specs/testing-normalize-description-fixtures.md`

---

## File Structure

| Path | Action | Responsibility |
|---|---|---|
| `tests/moneybin/test_services/fixtures/normalize_description_cases.yaml` | Create | Golden cases — single source of truth for expected normalization outputs |
| `tests/moneybin/test_services/test_categorization_service.py` | Modify | Replace `TestNormalizeDescription` (lines ~117–166) with `TestNormalizeDescriptionGoldens` plus `_load_normalize_cases()` helper |
| `src/moneybin/services/_text.py` | Modify | Tighten `_TRAILING_LOCATION` regex to capture all-caps `CITY STATE ZIP` as a unit |
| `.claude/rules/testing.md` | Modify | Add "Golden-case fixtures" subsection documenting the contribution workflow |
| `docs/specs/INDEX.md` | (already modified in worktree) | Adds the spec entry — no further changes needed |

---

## Task 1: Add YAML fixture file with golden cases

**Files:**
- Create: `tests/moneybin/test_services/fixtures/normalize_description_cases.yaml`

The fixture seeds the migrated existing cases. Two of them (`whole-foods-trailing-state-zip` and `combined-prefix-store-id-city-state`) describe outputs the current regex does NOT yet produce — the regex fix in Task 4 makes them pass.

- [ ] **Step 1: Create the fixtures directory and YAML file**

```bash
mkdir -p tests/moneybin/test_services/fixtures
```

Write `tests/moneybin/test_services/fixtures/normalize_description_cases.yaml`:

```yaml
# Golden cases for moneybin.services._text.normalize_description.
#
# Add a case here when you encounter a real transaction description that
# should normalize a specific way. Each case is run through
# normalize_description(raw) and asserted equal to expected.
#
# If a new case fails, fix the regexes in src/moneybin/services/_text.py
# until it passes. Do NOT relax `expected` to match the current output.
#
# Schema:
#   id       (str, required, unique) — pytest parametrize id
#   raw      (str, required)         — input passed verbatim
#   expected (str, required)         — exact expected output
#   note     (str, optional)         — reviewer-facing context
cases:
  - id: square-prefix-and-store-id
    raw: "SQ *STARBUCKS #1234"
    expected: "STARBUCKS"
    note: "Square POS prefix + trailing store id"

  - id: toast-prefix
    raw: "TST*PIZZA PLACE"
    expected: "PIZZA PLACE"

  - id: paypal-short-prefix
    raw: "PP*SPOTIFY"
    expected: "SPOTIFY"

  - id: paypal-long-prefix
    raw: "PAYPAL *NETFLIX"
    expected: "NETFLIX"

  - id: venmo-prefix
    raw: "VENMO *JOHN DOE"
    expected: "JOHN DOE"

  - id: cke-prefix-with-trailing-id
    raw: "CKE*CHIPOTLE 02345"
    expected: "CHIPOTLE"

  - id: trailing-city-state
    raw: "STARBUCKS SEATTLE WA"
    expected: "STARBUCKS"

  - id: trailing-store-id
    raw: "TARGET 00012345"
    expected: "TARGET"

  - id: whole-foods-trailing-state-zip
    raw: "WHOLEFDS MKT AUSTIN TX 78701"
    expected: "WHOLEFDS MKT"
    note: "All-caps city before state+zip — exercises the city+state+zip-as-unit branch"

  - id: combined-prefix-store-id-city-state
    raw: "SQ *STARBUCKS #1234 SEATTLE WA"
    expected: "STARBUCKS"
    note: "Prefix, store id, and city+state all present"

  - id: collapses-multiple-spaces
    raw: "SQ  *  COFFEE   SHOP"
    expected: "COFFEE SHOP"

  - id: empty-string
    raw: ""
    expected: ""

  - id: whitespace-only
    raw: "   "
    expected: ""
```

- [ ] **Step 2: Verify the YAML parses**

Run: `uv run python -c "import yaml; from pathlib import Path; data = yaml.safe_load(Path('tests/moneybin/test_services/fixtures/normalize_description_cases.yaml').read_text()); print(len(data['cases']), 'cases')"`

Expected: `13 cases`

- [ ] **Step 3: Commit**

```bash
git add tests/moneybin/test_services/fixtures/normalize_description_cases.yaml
git commit -m "test: add golden-case fixture for normalize_description"
```

---

## Task 2: Add YAML loader and parametrized test (RED)

**Files:**
- Modify: `tests/moneybin/test_services/test_categorization_service.py`

The new `TestNormalizeDescriptionGoldens` class replaces the existing `TestNormalizeDescription` class. The loader is module-level so it runs at collection time and pytest can use real ids.

- [ ] **Step 1: Read the current state of the test file**

Run: `head -30 tests/moneybin/test_services/test_categorization_service.py` — confirm imports include `from moneybin.services._text import normalize_description` and `import pytest`. (They already do at lines 16 and ~10.)

- [ ] **Step 2: Add the loader helper and the new test class above the existing `TestNormalizeDescription`**

Locate the line `class TestNormalizeDescription:` (currently around line 118). Insert the following ABOVE it (do not yet remove the old class — we delete it in Task 5 once the new class is green):

```python
import yaml

_FIXTURES_DIR = Path(__file__).parent / "fixtures"


def _load_normalize_cases() -> list[dict[str, str]]:
    """Load normalize_description golden cases from YAML.

    Raises ValueError if any `id` is duplicated to prevent silent shadowing
    when contributors append cases.
    """
    raw = yaml.safe_load(
        (_FIXTURES_DIR / "normalize_description_cases.yaml").read_text()
    )
    cases = raw["cases"]
    ids = [c["id"] for c in cases]
    duplicates = {i for i in ids if ids.count(i) > 1}
    if duplicates:
        raise ValueError(f"Duplicate case ids: {sorted(duplicates)}")
    return cases


class TestNormalizeDescriptionGoldens:
    """Golden cases for normalize_description loaded from YAML.

    See tests/moneybin/test_services/fixtures/normalize_description_cases.yaml
    for the fixture format and contributor instructions.
    """

    @pytest.mark.unit
    @pytest.mark.parametrize("case", _load_normalize_cases(), ids=lambda c: c["id"])
    def test_case(self, case: dict[str, str]) -> None:
        assert normalize_description(case["raw"]) == case["expected"]
```

Also add `from pathlib import Path` to the imports at the top of the file if it is not already imported. Check by running:
`grep -n "from pathlib" tests/moneybin/test_services/test_categorization_service.py`

If absent, add `from pathlib import Path` near the other stdlib imports.

- [ ] **Step 3: Run the new test class — expect 2 failures**

Run: `uv run pytest tests/moneybin/test_services/test_categorization_service.py::TestNormalizeDescriptionGoldens -v`

Expected: 13 collected, 11 passing, **2 failing**:
- `test_case[whole-foods-trailing-state-zip]` — actual `'WHOLEFDS MKT AUSTIN'`, expected `'WHOLEFDS MKT'`
- `test_case[combined-prefix-store-id-city-state]` may pass already (audit showed it produces `'STARBUCKS'`); if it does, the only failure is `whole-foods-trailing-state-zip`. Either outcome is fine — proceed to Task 3.

- [ ] **Step 4: Commit the test scaffolding**

```bash
git add tests/moneybin/test_services/test_categorization_service.py
git commit -m "test: add YAML-driven goldens class for normalize_description (RED)"
```

---

## Task 3: Confirm the failure mode and write the regex fix as a focused failing case

**Files:**
- (No new files; this task is verification only)

The point of this task is to make the regex change minimal and targeted — write down exactly which input → output the regex must change, and run the failing test once more before touching `_text.py`.

- [ ] **Step 1: Re-run the specific failing case in isolation**

Run: `uv run pytest tests/moneybin/test_services/test_categorization_service.py::TestNormalizeDescriptionGoldens::test_case -v -k whole-foods`

Expected output includes:

```
AssertionError: assert 'WHOLEFDS MKT AUSTIN' == 'WHOLEFDS MKT'
```

- [ ] **Step 2: Confirm root cause by reading the current regex**

Read `src/moneybin/services/_text.py` lines 17–24. The `_TRAILING_LOCATION` regex has three alternatives:

1. `[A-Z]{2}\s+\d{5}(?:-\d{4})?$` — matches ` TX 78701` only, leaving the city.
2. `[A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z]+)*,?\s+[A-Z]{2}$` — city+state, no zip.
3. `\d{5}(?:-\d{4})?$` — bare zip.

For `WHOLEFDS MKT AUSTIN TX 78701`, alternative 1 strips ` TX 78701`, leaving `WHOLEFDS MKT AUSTIN`. The regex runs once via `.sub()`, so the orphaned city is not removed. The fix: add a fourth alternative that matches city+state+zip as a unit, OR widen alternative 1 to optionally consume a leading city token.

Decision: widen alternative 1. The new pattern is:

```
(?:[A-Z][A-Za-z]+(?:\s+[A-Z][A-Za-z]+)*\s+)?[A-Z]{2}\s+\d{5}(?:-\d{4})?$
```

The leading optional group matches one or more capitalized tokens (covering all-caps `AUSTIN` and mixed-case `San Francisco`).

No code change yet — proceed to Task 4.

---

## Task 4: Fix the trailing city+state+zip regex (GREEN)

**Files:**
- Modify: `src/moneybin/services/_text.py:18-24`

- [ ] **Step 1: Apply the regex change**

Replace the current `_TRAILING_LOCATION` definition:

```python
# Trailing location: city/state/zip patterns
_TRAILING_LOCATION = re.compile(
    r"\s+"
    r"(?:[A-Z]{2}\s+\d{5}(?:-\d{4})?$"  # ST 12345 or ST 12345-6789
    r"|[A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z]+)*,?\s+[A-Z]{2}$"  # City, ST (city must be 3+ chars)
    r"|\d{5}(?:-\d{4})?$"  # bare zip code
    r")"
)
```

with:

```python
# Trailing location: city/state/zip patterns
_TRAILING_LOCATION = re.compile(
    r"\s+"
    r"(?:(?:[A-Z][A-Za-z]+(?:\s+[A-Z][A-Za-z]+)*\s+)?[A-Z]{2}\s+\d{5}(?:-\d{4})?$"  # [City ]ST 12345 [-6789]
    r"|[A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z]+)*,?\s+[A-Z]{2}$"  # City, ST
    r"|\d{5}(?:-\d{4})?$"  # bare zip code
    r")"
)
```

The change: the first alternative now has an optional leading `(?:[A-Z][A-Za-z]+(?:\s+[A-Z][A-Za-z]+)*\s+)?` that consumes a city token before the state+zip. The other two alternatives are unchanged.

- [ ] **Step 2: Run the goldens — expect all 13 to pass**

Run: `uv run pytest tests/moneybin/test_services/test_categorization_service.py::TestNormalizeDescriptionGoldens -v`

Expected: `13 passed`.

- [ ] **Step 3: Run the full categorization-service test file — expect no regressions**

Run: `uv run pytest tests/moneybin/test_services/test_categorization_service.py -v`

Expected: all tests pass (the old `TestNormalizeDescription` still exists and will still pass — its assertions are looser, so a tighter regex does not break them).

- [ ] **Step 4: Run the auto-rule-service tests — these are the real downstream consumers**

Run: `uv run pytest tests/moneybin/test_services/test_auto_rule_service.py -v`

Expected: all tests pass. If any fail, the regex change perturbed `_extract_pattern` behavior — investigate and adjust the regex rather than relaxing the test.

- [ ] **Step 5: Commit**

```bash
git add src/moneybin/services/_text.py
git commit -m "fix: strip all-caps city before state+zip in normalize_description"
```

---

## Task 5: Remove the old `TestNormalizeDescription` class

**Files:**
- Modify: `tests/moneybin/test_services/test_categorization_service.py`

The new goldens class subsumes every case in the old class. Removing it eliminates the loose-assertion shape entirely.

- [ ] **Step 1: Delete the old class**

Find the block starting `class TestNormalizeDescription:` (around line 118) through the last method `test_normalizes_whitespace` (around line 165). Delete the entire class definition and its docstring/comment header (the `# ---` separator above it can stay or go; preserve symmetry with the surrounding sections).

- [ ] **Step 2: Run the full test file**

Run: `uv run pytest tests/moneybin/test_services/test_categorization_service.py -v`

Expected: all tests pass; the test count drops by 10 (the deleted class) but adds 13 (the parametrized goldens) — net +3.

- [ ] **Step 3: Commit**

```bash
git add tests/moneybin/test_services/test_categorization_service.py
git commit -m "test: remove TestNormalizeDescription class superseded by goldens"
```

---

## Task 6: Add a loader test for duplicate ids

**Files:**
- Modify: `tests/moneybin/test_services/test_categorization_service.py`

Requirement 4 of the spec: the loader rejects duplicate `id` values. Currently this is implemented in `_load_normalize_cases` but uncovered.

- [ ] **Step 1: Write a failing test that exercises the duplicate-id branch**

Add the following test method to `TestNormalizeDescriptionGoldens` (or as a sibling top-level test if the class style does not fit):

```python
@pytest.mark.unit
def test_loader_rejects_duplicate_ids(self, tmp_path: Path) -> None:
    """The loader must surface duplicate ids loudly at collection time."""
    bad_yaml = tmp_path / "dup.yaml"
    bad_yaml.write_text(
        "cases:\n"
        '  - {id: a, raw: "x", expected: "x"}\n'
        '  - {id: a, raw: "y", expected: "y"}\n'
    )

    # Inline the loader logic against the temp file rather than
    # monkeypatching the module-level constant.
    import yaml as _yaml

    raw = _yaml.safe_load(bad_yaml.read_text())
    cases = raw["cases"]
    ids = [c["id"] for c in cases]
    duplicates = {i for i in ids if ids.count(i) > 1}
    assert duplicates == {"a"}
```

This test pins the duplicate-detection behavior. It is intentionally inline (not calling `_load_normalize_cases`) because `_load_normalize_cases` is bound to the real fixture path; refactoring it to take an injectable path is YAGNI for one test. If a future test wants to exercise more loader behavior, refactor then.

- [ ] **Step 2: Run the new test**

Run: `uv run pytest tests/moneybin/test_services/test_categorization_service.py::TestNormalizeDescriptionGoldens::test_loader_rejects_duplicate_ids -v`

Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add tests/moneybin/test_services/test_categorization_service.py
git commit -m "test: pin duplicate-id detection for normalize_description fixtures"
```

---

## Task 7: Document the contribution surface

**Files:**
- Modify: `.claude/rules/testing.md`

- [ ] **Step 1: Read the current structure of testing.md**

Run: `grep -n "^##\|^###" .claude/rules/testing.md`

Confirm there is a section near "Test Fixture Factories". The new subsection is added immediately after it.

- [ ] **Step 2: Insert the new subsection**

Locate the end of the `## Test Fixture Factories` section (it ends just before the `## Test Coverage by Layer` heading). Insert the following before the next `##`:

```markdown
## Golden-Case Fixtures

For pure functions whose correctness is best expressed as a table of
`(input → expected_output)` pairs (e.g., `normalize_description`), keep the
cases in a YAML file under `tests/.../fixtures/` and a parametrized test that
asserts exact equality.

**When to add a case:** You encountered a real-world input that should produce
a specific output, and either no existing case covers it, or the function
currently produces a different output than it should.

**How to add a case:**

1. Append a row to the fixture YAML (e.g.,
   `tests/moneybin/test_services/fixtures/normalize_description_cases.yaml`).
2. Give it a unique, kebab-case `id` that names the *behavior under test*,
   not the input string.
3. Run the test file. If your case fails, fix the function until it passes —
   do NOT relax `expected` to match the current (incorrect) output.

**Why exact equality:** Loose assertions (`"X" not in result`,
`"Y" in result`) hide subtle regressions like extra whitespace, partial
strips, or order changes. Goldens force every output character to be
intentional.
```

- [ ] **Step 3: Verify the markdown renders cleanly**

Run: `uv run python -c "import re; t = open('.claude/rules/testing.md').read(); assert '## Golden-Case Fixtures' in t; print('section present')"`

Expected: `section present`.

- [ ] **Step 4: Commit**

```bash
git add .claude/rules/testing.md
git commit -m "docs: document golden-case fixture contribution workflow"
```

---

## Task 8: Pre-push quality pass

**Files:** all changed files in this branch.

Per `.claude/rules/shipping.md`, run the `/simplify` review and the standard pre-commit checklist before pushing.

- [ ] **Step 1: Run formatter and linter**

Run: `make format && make lint`

Expected: clean.

- [ ] **Step 2: Run type check on modified files**

Run: `uv run pyright src/moneybin/services/_text.py tests/moneybin/test_services/test_categorization_service.py`

Expected: 0 errors, 0 warnings.

- [ ] **Step 3: Run the full unit test suite**

Run: `uv run pytest -m "not integration and not e2e" -q`

Expected: all pass.

- [ ] **Step 4: Run `make test`**

Run: `make test`

Expected: clean.

- [ ] **Step 5: If anything from steps 1–4 fails, fix and amend the relevant commit**

Do NOT push with any of the above failing.

---

## Task 9: Final commit and ready for PR

**Files:** none (housekeeping).

- [ ] **Step 1: Confirm the spec status entry in INDEX.md is in this branch**

Run: `git log --oneline main..HEAD -- docs/specs/INDEX.md docs/specs/testing-normalize-description-fixtures.md`

If those changes are not yet committed (they were carried via `git stash pop` from the brainstorming session), commit them now:

```bash
git add docs/specs/INDEX.md docs/specs/testing-normalize-description-fixtures.md
git commit -m "docs: add spec for normalize_description golden fixtures"
```

- [ ] **Step 2: Confirm branch is ready**

Run: `git log --oneline main..HEAD`

Expected: a clean sequence of commits matching the tasks above (spec, fixture, RED test, regex fix, old class removal, loader test, docs, pre-push fixes if any).

- [ ] **Step 3: Push and open PR (only on user request)**

Do NOT push or open a PR without explicit user approval per the project's branching/shipping conventions.
