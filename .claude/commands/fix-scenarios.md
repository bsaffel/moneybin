Find and fix failures in the latest `Scenarios` workflow run for a branch.

The `Scenarios` workflow runs `moneybin synthetic verify --all` against every shipped
scenario YAML in `src/moneybin/testing/scenarios/data/`. Failures are surfaced both as
non-zero CI exit and as per-scenario PASS/FAIL lines in the JSONL artifact.

## Usage

- `/fix-scenarios` — fix the current branch
- `/fix-scenarios <branch>` — fix a specific branch
- `/fix-scenarios --list` — list all branches with recent scenario failures and stop (no fixes)

## Steps

1. **Determine the target branch**:
   - If `$ARGUMENTS` is `--list`: run step 2 in list mode, then stop.
   - If `$ARGUMENTS` is a branch name, use that.
   - If `$ARGUMENTS` is empty, use `git branch --show-current`.

2. **Find the latest failed Scenarios run** on the target branch:
   ```
   gh run list --branch <branch> --workflow scenarios.yml --json databaseId,conclusion,createdAt,headBranch --jq '[.[] | select(.conclusion=="failure")][0]'
   ```
   In `--list` mode, instead run without `--branch` to show all branches, filter to
   failures, and print a summary table (branch, run ID, age, failed scenario names)
   — then stop.

   If no failed run exists for the target branch, report that scenarios are passing
   and stop.

3. **Pull the structured scenario results**. The workflow uploads a `scenarios-results`
   artifact containing `scenarios.jsonl` — one JSON envelope per scenario. This is
   far more useful than raw logs because each line carries the failing assertion
   names, expectation diffs, and evaluation breakdowns.
   ```
   mkdir -p /tmp/fix-scenarios-<run-id>
   gh run download <run-id> --name scenarios-results --dir /tmp/fix-scenarios-<run-id>
   ```
   If the artifact is unavailable (job timed out or crashed before upload), fall
   back to `gh run view <run-id> --log-failed` and parse the verify output.

4. **Identify the failing scenarios** from the JSONL. Each line is a
   `ResponseEnvelope`; failures have `data.passed=false`. For each failed scenario,
   extract:
   - `data.scenario` — name (matches the YAML filename in
     `src/moneybin/testing/scenarios/data/`)
   - `data.halted` — non-null means a pipeline step crashed before assertions ran
   - `data.assertions[]` — entries with `passed=false` carry `details` and `error`
   - `data.expectations[]` — failures carry the expected vs. actual diff
   - `data.evaluations[]` — failures carry `metric`, `value`, `threshold`,
     `breakdown`

   ```
   jq -r 'select(.data.passed==false) | "\(.data.scenario): halted=\(.data.halted)"' \
     /tmp/fix-scenarios-<run-id>/scenarios.jsonl
   jq 'select(.data.passed==false) | {scenario: .data.scenario, failed_assertions: [.data.assertions[] | select(.passed==false)], failed_expectations: [.data.expectations[] | select(.passed==false)], failed_evaluations: [.data.evaluations[] | select(.passed==false)]}' \
     /tmp/fix-scenarios-<run-id>/scenarios.jsonl
   ```

5. **If the target branch is not the current branch**, warn the user and ask whether
   to check it out before fixing or stop. Do not modify files on a branch you are not
   currently on.

6. **Categorize each failure** before touching anything. Scenario failures fall into
   three buckets:

   | Symptom | Likely cause | Where to look |
   |---|---|---|
   | `halted` non-null, no assertions ran | Pipeline step crashed (loader, transform, match, etc.) | `src/moneybin/testing/scenarios/steps.py` and the called service |
   | Assertion failed with `error` | Assertion fn raised | `src/moneybin/validation/assertions/` |
   | Assertion failed with `details` | Pipeline output diverged from spec | The pipeline step that owns the data, **or** the scenario YAML if the expectation is wrong |
   | Expectation failed | Per-record claim doesn't match | The fixture YAML, the expectation engine, or the categorize/match step |
   | Evaluation below threshold | Score regressed | The pipeline + the threshold itself — was the threshold realistic? |

   For each failure, decide whether the **code is wrong** (fix code) or the
   **scenario is wrong** (fix YAML / fixture / threshold). Prefer fixing code unless
   the scenario clearly encodes an out-of-date expectation.

7. **Read the affected files** in full before making any changes:
   - The scenario YAML at `src/moneybin/testing/scenarios/data/<name>.yaml`
   - Any referenced fixtures under `tests/fixtures/`
   - The pipeline step or assertion implicated by the failure

8. **Fix the issues**, following project conventions (see `CLAUDE.md` and
   `.claude/rules/`):
   - Pipeline crashes: fix the underlying service or step. Add a focused unit test
     for the regression in `tests/moneybin/test_testing/` or the relevant subsystem.
   - Assertion failures from real data divergence: fix the producing code, not the
     assertion — assertions encode invariants, not expectations.
   - Expectation failures: fix the categorize/match logic if the expectation is
     correct; update the fixture YAML if the expectation is stale.
   - Evaluation regressions: investigate the breakdown. Score drops are usually code
     issues — only adjust thresholds with a clear justification (and note in
     `docs/followups.md` if appropriate).

9. **Reproduce locally** before claiming the fix works. The full suite is slow, so
   target the failing scenario:
   ```
   uv run moneybin synthetic verify --scenario=<name> --output=json
   ```
   For the integration tests that exercise the runner directly:
   ```
   uv run pytest tests/integration/test_scenario_runner.py -v
   ```

10. **Verify** with the full pre-commit checklist:
    ```
    make check test
    ```
    Plus the scenario suite end-to-end:
    ```
    uv run moneybin synthetic verify --all
    ```
    If anything still fails, fix it before proceeding.

11. **Show a summary** organized by failing scenario:
    - Root cause and which file owned the fix
    - Whether the fix was code or scenario YAML
    - Any thresholds adjusted, with justification
    - Any items deferred to `docs/followups.md`

    Ask the user to confirm before committing.

12. **After confirmation**, stage only the files changed to fix scenarios (do not
    stage unrelated unstaged files), commit, and push:
    - Commit message: `Fix scenario failures: <brief description>` with a bulleted
      body listing each scenario fixed and how
    - Push to the current branch's remote tracking branch
