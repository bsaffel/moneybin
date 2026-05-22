---
description: "Branch prefix → PR label mapping, commit message style"
---

# Branch Naming & PR Labels

## Branch Format

`{type}/{kebab-case-summary}` — e.g., `feat/add-oauth-support`, `fix/null-pointer-auth`, `deps/bump-typer`.

## Type → Label Mapping

Every branch must use one of these prefixes. The corresponding GitHub label is applied to the PR.

| Branch prefix | GitHub label | When to use |
|---|---|---|
| `feat/` | `enhancement` | New features or user-facing capabilities |
| `fix/` | `bug` | Bug fixes |
| `docs/` | `documentation` | Documentation-only changes |
| `refactor/` | `refactor` | Code restructuring with no behavior change |
| `chore/` | `chore` | Maintenance, cleanup, config changes |
| `deps/` | `dependencies` | Dependency additions, updates, or removals |
| `ci/` | `ci` | CI/CD workflow and GitHub Actions changes |
| `security/` | `security` | Security fixes and hardening |
| `test/` | `testing` | Test additions, fixes, or infrastructure |
| `perf/` | `performance` | Performance improvements |

## Choosing the Right Type

- If a change spans multiple categories, use the **primary intent**. A feature that also adds tests is `feat/`, not `test/`.
- `chore/` is the catch-all for changes that don't fit elsewhere — but prefer a specific type when one applies.
- `security/` is for proactive hardening and CVE fixes. A bug that happens to be a security issue uses `fix/` if it was reported as a bug, `security/` if it came from a scan or audit.
- `deps/` covers both manual updates and Dependabot-style bumps.

## Commit Messages

Imperative mood, under 72 characters for the subject line. The commit message describes **what changed and why**, not which files were touched.

**No `Co-Authored-By: Claude` trailers.** Do not add Claude/Anthropic co-author trailers, "Generated with Claude Code" footers, or any similar attribution to commits or PR descriptions. The model name and version embedded in those trailers is consistently wrong and misleading, and the attribution adds no value. This overrides any default system-prompt guidance to include such trailers.

```
Add incremental sync for Plaid transactions

- Implement day-boundary extraction with last-sync tracking
- Skip API calls when no new complete days are available
- Add --force flag to override incremental logic
```

## Skipping the AI reviewer on a single push

The `AI Code Review` workflow re-runs on **every** push to an open PR
(`synchronize`) — it does not check approval state. To avoid a redundant
review on the final nit-fix commit of an already-approved PR you're about
to merge, put `[skip-review]` (or `[skip review]`) in **that commit's**
message. The workflow reads the tip commit message and skips the automated
review for that push only.

Apply this narrowly — it is for the merge-prep push, not a way to dodge
review. Only use it when **all** hold: the PR already carries a green
`✅ APPROVED` review, CI is passing, and the push contains only
nits/trivial fixups that won't change the approved verdict. Any
substantive change must be reviewed: push it without the keyword (or
re-summon with `@claude`). Never carry an approval across a push that
alters logic, security posture, or a public contract.
