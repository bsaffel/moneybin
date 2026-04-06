Find and fix failures in the latest CI run for a branch.

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

3. **Pull the failure logs**:
   ```
   gh run view <id> --log-failed
   ```

4. **Identify the failure type** from the logs:
   - `ruff format --check` → formatting issue
   - `ruff check` → lint error
   - `pyright` → type error
   - `pytest` → test failure

5. **If the target branch is not the current branch**, warn the user and ask whether to check it out before fixing or stop. Do not modify files on a branch you are not currently on.

6. **Read the affected files** before making any changes. Understand the context fully.

7. **Fix the issues**:
   - Formatting: run `uv run ruff format .` and review what changed
   - Lint: fix each flagged line; prefer code changes over `# noqa` (only use `# noqa` with a justification comment when the rule is a false positive)
   - Type errors: fix the type issue; do not use `# type: ignore` unless the error is a known library stub gap — always add a comment explaining why
   - Test failures: read the test, understand what it's asserting, fix the source code (not the test) unless the test itself is wrong

8. **Verify locally** by running the full pre-commit checklist:
   ```
   make check test-all
   ```
   If anything still fails, fix it before proceeding.

9. **Show a summary** of every change made — files modified, what was wrong, and how it was fixed. Ask the user to confirm before committing.

10. **After confirmation**, stage only the files changed to fix CI (do not stage unrelated unstaged files), commit, and push:
    - Commit message: `Fix CI failures: <brief description>` with a bulleted body listing each fix
    - Include `Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>`
    - Push to the current branch's remote tracking branch
