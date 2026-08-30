---
description: "Branch prefix → PR label mapping, commit message style, account PII in public branch and PR text"
---

# Branch Naming & PR Labels

## Branch Format

`{type}/{kebab-case-summary}` — e.g., `feat/add-oauth-support`, `fix/null-pointer-auth`, `deps/bump-typer`.

## Branch names, PR text, and comments are a public surface

A branch name reaches origin, every PR URL, the reflog, and CI logs. A PR
title, a PR body, a review comment, and a commit message reach the same places
plus notification email. All of it is public, effectively permanent, and
written by hand — `SanitizedLogFormatter` guards the log pipeline and nothing
guards this. Never build any of it from real account data: an institution plus
a last four is a linked pair, and a rename does not recall it. One such branch
had to be force-deleted from origin with its history rewritten, and the
worktree directory kept the name afterward. Editing a posted body is weaker
still — GitHub keeps the prior revision in the body's edit history.

Name the defect, not the account — `fix/cross-source-dedup-remediation`, never
`fix/<bank>-<last-four>-…`. The same applies to worktree directory names, which
the native mechanism derives from the branch. This rule file is public too:
describe the shape, never paste the offending name as an example.

### The log permitted list does not transfer

AGENTS.md → Security bounds log output by `privacy-data-protection.md` §"What
CAN appear", and that list permits **institution names and masked
identifiers**. It permits them because a log line is machine-generated, locally
scoped, and already filtered. Public text is none of those things, so that list
is not the standard here. Keep all of the following out of a branch name, PR
title, PR body, comment, or commit message:

- An institution you actually bank with, linked to your own holdings. Naming
  one impersonally is fine — as a parser's `<ORG>` value, a statement layout,
  or a member of a class ("institutions that tokenize account numbers"). What
  discloses is the link between a real institution and your own accounts or
  profile, whatever the grammar carrying it: "two real accounts at
  `<institution>`" and "validated against real accounts at `<institution>`"
  disclose the same fact, and neither is possessive. Test for the link, not for
  a word — no amount of surrounding verification detail makes it necessary.
- A last four belonging to a real account, masked or bare — `****1234`,
  `…1234`, `x1234` are the shapes. Masking is a display rule, not a licence to
  publish.
- A real account display name, nickname, or balance.
- A real merchant, counterparty, or transaction description taken from your own
  data.

Synthetic values are the default for every example: `1234`, `5678`,
`Vacation Fund …1111`. A real value never demonstrates a format better than a
fabricated one does.

### Verification evidence is where this leaks

The diff is not the risk — the **Test plan** is. A verification line reports
what you actually ran, and what you actually ran was a real profile. Report the
shape instead of the holding: "a 3-year family-persona profile, two
same-institution accounts, 2,886 transactions, no warning" carries the same
evidentiary weight as naming the bank and none of the exposure. Counts, date
spans, and pass/fail totals are fine; the identifying noun is what has to go.

Scan the composed body before posting, not only the staged diff — the body is
written from session context that no pre-commit check has ever seen.

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
(`synchronize`) — it does not check approval state. To skip a redundant review
on the final nit-fix commit of an already-approved PR you're about to merge,
put `[skip-review]` (or `[skip review]`) in **that commit's** message. The
workflow reads the tip commit message and skips that push only.

Apply this narrowly — it is for the merge-prep push, not a way to dodge
review. Only use it when **all** hold: the PR already carries a green
`✅ APPROVED` review, CI is passing, and the push contains only nits/trivial
fixups that won't change the approved verdict. Any substantive change must be
reviewed: push it without the keyword (or re-summon with `@claude`). Never
carry an approval across a push that alters logic, security posture, or a
public contract.
