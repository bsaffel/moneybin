Find and fix failures in the latest CI run for a branch.

The `CI` workflow (`ci.yml`) runs these parallel jobs on each PR:

- `checks` — `ruff format --check`, `ruff check`, `pyright`
- `test-unit` — `pytest tests/moneybin/ -m "not integration and not e2e and not slow"`
- `test-integration` — `pytest tests/integration/`
- `test-e2e` — `pytest tests/e2e/`

Any of them can fail independently. This command handles all of them. For `Scenarios` workflow failures use `/fix-scenarios`; for `Security` workflow findings use `/fix-security`.

## Usage

- `/fix-ci` — fix the current branch
- `/fix-ci <branch>` — fix a specific branch
- `/fix-ci --list` — list all branches with recent CI failures and stop (no fixes)

## Steps

1. **Determine the target branch**:
   - If `$ARGUMENTS` is `--list`: run step 2 in list mode, then stop.
   - If `$ARGUMENTS` is a branch name, use that.
   - If `$ARGUMENTS` is empty, use `git branch --show-current`.

2. **Find the latest failed run** on the target branch:
   ```
   gh run list --branch <branch> --workflow ci.yml --json databaseId,conclusion,createdAt,headBranch --jq '[.[] | select(.conclusion=="failure")][0].databaseId'
   ```
   In `--list` mode, instead run without `--branch` to show all branches, filter to failures, and print a summary table (branch, run ID, age, failed job names) — then stop.

   If no failed run exists for the target branch, report that CI is passing and stop.

3. **Identify which jobs failed** (a single run can have multiple failed jobs):
   ```
   gh run view <id> --json jobs --jq '.jobs[] | select(.conclusion=="failure") | .name'
   ```

4. **Pull the failure logs** (returns logs for every failed job):
   ```
   gh run view <id> --log-failed
   ```

5. **Classify each failure** by the failing job and the tool/output in the logs:
   - `checks` job:
     - `ruff format --check` step → formatting issue
     - `ruff check` step → lint error
     - `pyright` step → type error
   - `test-unit`, `test-integration`, `test-e2e` jobs → pytest failure in that suite

   Track failures per job so the fix and the local verification can be scoped.

6. **If the target branch is not the current branch**, warn the user and ask whether to check it out before fixing or stop. Do not modify files on a branch you are not currently on.

7. **Read the affected files** before making any changes. Understand the context fully.

8. **Fix the issues**:
   - Formatting: run `uv run ruff format .` and review what changed
   - Lint: fix each flagged line; prefer code changes over `# noqa` (only use `# noqa` with a justification comment when the rule is a false positive)
   - Type errors: fix the type issue; do not use `# type: ignore` unless the error is a known library stub gap — always add a comment explaining why
   - Test failures: read the test, understand what it's asserting, fix the source code (not the test) unless the test itself is wrong

9. **Verify locally**, scoping to what changed:
   - If only `checks` failed: `make check`
   - If a single test suite failed: re-run just that suite — e.g. `uv run pytest tests/integration/ -v` or `uv run pytest tests/e2e/ -v` — then `make check`
   - If multiple jobs failed or you're not sure: `make check test-all`

   If anything still fails, fix it before proceeding.

10. **Show a summary** of every change made — files modified, what was wrong, and how it was fixed. Ask the user to confirm before committing.

11. **After confirmation**, stage only the files changed to fix CI (do not stage unrelated unstaged files), commit, and push:
    - Commit message: `Fix CI failures: <brief description>` with a bulleted body listing each fix
    - Push to the current branch's remote tracking branch
