Find and fix open security findings from CodeQL and pip-audit.

## Usage

- `/fix-security` — fix findings on the current branch / default branch
- `/fix-security --list` — list all open findings and stop (no fixes)

## Steps

1. **Fetch open CodeQL alerts** from the GitHub code scanning API:
   ```
   gh api /repos/{owner}/{repo}/code-scanning/alerts?state=open&per_page=50
   ```
   Replace `{owner}/{repo}` with the actual repo from `gh repo view --json nameWithOwner`.

2. **Fetch pip-audit output** from the latest completed security workflow run:
   ```
   gh run list --workflow security.yml --json databaseId,conclusion,createdAt --jq '[.[] | select(.conclusion != "cancelled")][0].databaseId'
   gh run view <id> --log
   ```
   Extract the `dependency-audit` job output and parse any CVEs reported.

3. In `--list` mode: display a summary table of all open CodeQL alerts (rule, severity, file, line) and all pip-audit CVEs (package, CVE ID, severity, fixed version) — then stop.

4. **Triage each finding** before touching any code:
   - For CodeQL: read the flagged file and line, understand whether it is a true positive or false positive.
   - For pip-audit: check whether a non-vulnerable version of the package is available and compatible.

5. **Fix true positives**:
   - CodeQL vulnerabilities: fix the code. Common patterns in this project — parameterized SQL (never string interpolation), subprocess as list not shell=True, path traversal validation. Do not suppress with `# noqa` or `# type: ignore` without a clear explanation of why it is a false positive.
   - pip-audit CVEs: run `uv add <package>@<safe-version>` to upgrade. Check that tests still pass after the upgrade.

6. **For false positives only**: add a targeted suppression with a comment explaining why it is not exploitable in this context. Do not bulk-suppress.

7. **Verify**:
   ```
   uv run ruff format . && uv run ruff check . && uv run pyright && uv run pytest tests/ -v
   ```

8. **Show a summary** of every finding — what it was, whether it was fixed or suppressed as a false positive and why, and any findings that require human judgment (e.g. a CVE with no available fix). Ask the user to confirm before committing.

9. **After confirmation**, stage only the files changed to address security findings, commit, and push:
   - Commit message: `Fix security findings: <brief description>` with a bulleted body listing each fix
   - Include `Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>`
   - Push to the current branch's remote tracking branch
