Fetch open code review comments on the current branch's PR and address them.

## Usage

- `/fix-review` — address open review comments on the current branch's PR
- `/fix-review --list` — list all open review comments and stop (no fixes)

## Principles

- **Latest first.** A reviewer's most recent comment in a thread is the current ask — older comments in the same thread may already be superseded. Always process threads ordered by their latest activity, newest first, and treat the latest comment as authoritative.
- **Resolution status comes from GraphQL.** REST's `/pulls/{n}/comments` cannot tell you if a thread is resolved. Use GraphQL `reviewThreads { isResolved }` — only address threads where `isResolved == false`.
- **Three sources, all required.** Inline review threads, review-level summaries, AND top-level PR conversation comments (the `/issues/{n}/comments` endpoint). Skipping any one of these silently drops feedback.
- **Read full review bodies, never excerpts.** A review's actionable item is often a "New finding" / "New issue" / "One new issue" section *after* a "Re-review complete" preamble. Truncating to the first N characters or printing only metadata (`state`, `submitted_at`) hides those findings. Process every non-`APPROVED` review by reading the entire `body` field, not a slice.
- **Pending re-reviews mean not-yet-clean.** If the most recent activity in the PR is your own `@reviewer` ping requesting a re-review, the PR is not clean — it's waiting. Do not declare "0 unresolved" until at least one new review, thread, or conversation comment has landed *after* the timestamp of your ping. If nothing newer exists, report the wait state and stop without committing.

## Steps

1. **Find the open PR** for the current branch:
   ```
   gh pr view --json number,title,url,reviewDecision,headRefOid
   ```
   If no open PR exists, report it and stop. Capture `headRefOid` — it's the current HEAD SHA, used below to flag stale comments.

2. **Fetch unresolved review threads via GraphQL** (this is the authoritative source for inline comments + resolution state):
   ```
   gh api graphql -f query='
     query($owner:String!, $repo:String!, $number:Int!) {
       repository(owner:$owner, name:$repo) {
         pullRequest(number:$number) {
           reviewThreads(first:100) {
             nodes {
               id
               isResolved
               isOutdated
               path
               line
               originalLine
               comments(first:50) {
                 nodes {
                   id
                   databaseId
                   author { login }
                   body
                   createdAt
                   originalCommit { oid }
                   commit { oid }
                 }
               }
             }
           }
         }
       }
     }' -F owner={owner} -F repo={repo} -F number={number}
   ```
   Replace `{owner}`, `{repo}`, `{number}` from `gh repo view --json nameWithOwner` and step 1.

   Filter to `isResolved == false`. For each thread, the **latest comment** (max `createdAt`) is the current ask.

3. **Fetch review-level summaries** (top-level review bodies that aren't `APPROVED`):
   ```
   gh api --paginate /repos/{owner}/{repo}/pulls/{number}/reviews \
     --jq '[.[] | select(.state != "APPROVED" and (.body // "") != "") | {id, state, body, user: .user.login, submitted_at}]'
   ```

   **Read every `body` in full.** Do not slice, truncate, or print only metadata. A review whose first paragraph says "Re-review complete — N threads resolved" can still contain a "New finding" / "New issue" section below it. That section is the actionable item.

4. **Fetch top-level PR conversation comments** (these are NOT in `/pulls/{n}/comments`):
   ```
   gh api --paginate /repos/{owner}/{repo}/issues/{number}/comments \
     --jq '[.[] | {id, body, user: .user.login, created_at, updated_at}]'
   ```
   Filter out comments authored by the current user (`gh api /user --jq .login`) and bot comments that aren't actionable (e.g. coverage bots) — but when in doubt, include it.

5. **Sort everything newest-first** by latest activity:
   - Threads: by max `createdAt` of their comments
   - Reviews: by `submitted_at`
   - Issue comments: by `updated_at`

   Merge into a single list and process in that order. The most recent unresolved feedback gets read and addressed first.

   **Pending re-review check.** Before declaring the PR clean, scan the merged list for the most recent conversation comment authored by you (the current user, from `gh api /user --jq .login`) that contains a reviewer mention (`@claude`, `@codex`, etc.) requesting a re-review. If such a ping exists *and* no review/thread/comment from anyone else has been posted after that ping's `updated_at`, the PR is **not** clean — it's waiting on the requested re-review. Report the wait state and stop. Do not commit or declare done.

6. **In `--list` mode**: print the merged list (newest first), grouped by source (Thread / Review / Conversation), showing for each: reviewer, file:line (if applicable), latest-comment timestamp, body, and `[OUTDATED]` if `isOutdated` or if `originalCommit.oid != headRefOid`. Then stop.

7. **Read each affected file in full** before making changes. For inline threads, also note if the thread is `isOutdated` — the line numbers may no longer match; locate the relevant code by content, not line number.

8. **Address each item**:
   - Apply the change if the reviewer's latest comment is clear and correct.
   - If the comment is a question or discussion point rather than a change request, note it in the final summary for the user to respond to — do not silently skip it.
   - If you disagree with a suggestion or it conflicts with project conventions (see CLAUDE.md and `.claude/rules/`), note it in the summary and do not apply it — let the user decide.
   - Do not make changes beyond what the comment requests. Do not refactor surrounding code opportunistically.

9. **Verify**:
   ```
   make check test-all
   ```

10. **Show a summary** organized newest-first, grouped by source, listing every item fetched in step 5 with its disposition:
    - **Applied** — what changed and why
    - **Needs human response** — questions, discussion points
    - **Skipped** — with reason (convention conflict, disagreement, outdated/already-fixed)

    Every unresolved thread, non-approved review, and conversation comment from steps 2–4 must appear in this summary with one of those three dispositions. Do not drop items.

    Ask the user to confirm before committing.

11. **After confirmation**, stage only the files changed to address review comments, commit, and push:
    - Commit message: `Address PR review comments` with a bulleted body listing what was changed
    - Push to the current branch's remote tracking branch
