Fetch open code review comments on the current branch's PR and address them.

## Usage

- `/fix-review` — address review comments on the current branch's PR
- `/fix-review --list` — list all open review comments and stop (no fixes)

## Steps

1. **Find the open PR** for the current branch:
   ```
   gh pr view --json number,title,url,reviewDecision
   ```
   If no open PR exists, report it and stop.

2. **Fetch all review comments** (inline and top-level):
   ```
   gh api /repos/{owner}/{repo}/pulls/{number}/comments --jq '[.[] | {id, path, line, body, user: .user.login, resolved: (has("in_reply_to_id") | not)}]'
   gh api /repos/{owner}/{repo}/pulls/{number}/reviews --jq '[.[] | select(.state != "APPROVED") | {id, state, body, user: .user.login}]'
   ```
   Replace `{owner}/{repo}` with the actual repo from `gh repo view --json nameWithOwner`.

3. In `--list` mode: print a summary of all open comments grouped by file, including reviewer, line, and comment text — then stop.

4. **Group comments by file** and read each affected file in full before making any changes.

5. **Address each comment**:
   - Apply the change if the reviewer's suggestion is clear and correct.
   - If the comment is a question or discussion point rather than a change request, note it in the final summary for the user to respond to — do not silently skip it.
   - If you disagree with a suggestion or it conflicts with project conventions (see CLAUDE.md and `.claude/rules/`), note it in the summary and do not apply it — let the user decide.
   - Do not make changes beyond what the comment requests. Do not refactor surrounding code opportunistically.

6. **Verify**:
   ```
   uv run ruff format . && uv run ruff check . && uv run pyright && uv run pytest tests/ -v
   ```

7. **Show a summary** organized by comment:
   - Changes applied and why
   - Comments that need a human response (questions, discussions)
   - Suggestions skipped and why (conflict with conventions, disagreement)

   Ask the user to confirm before committing.

8. **After confirmation**, stage only the files changed to address review comments, commit, and push:
   - Commit message: `Address PR review comments` with a bulleted body listing what was changed
   - Include `Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>`
   - Push to the current branch's remote tracking branch
