# Feature: Claude Worktree Branch Policy

## Status

in-progress

## Goal

Make Claude-created worktrees obey Moneybin's existing
`{type}/{kebab-case-summary}` branch contract at creation time. Native
worktree names remain safe directory tokens, while Git branches remain the
canonical inputs to PR-label selection.

## Background

Moneybin's [branching rule](../../.claude/rules/branching.md) requires a typed
Git branch such as `fix/sqlmesh-console-noise`. Claude Code's default worktree
implementation instead derives `worktree-<name>` as its branch, so a native
worktree named `fix+sqlmesh-console-noise` needs a later rename. That rename
can write shared `.git/config` branch state, which is unavailable to sandboxed
sessions.

Claude Code's `WorktreeCreate` hook receives the worktree `name`, replaces the
default creation logic, and returns the absolute worktree path. This is the
right integration boundary: create the correct branch once, rather than
renaming it after the session has entered the worktree.

On 2026-07-26, the adapter passed its 21-case disposable-Git regression suite
and Moneybin's local quality gate under Claude Code 2.1.220. The real
`--worktree` and `EnterWorktree` canaries remain pending explicit authorization
to send this private repository context to Claude Code.

## Requirements

1. The native worktree-name contract is
   `{type}+{kebab-case-summary}`. For example,
   `fix+sqlmesh-console-noise` maps exactly to the Git branch
   `fix/sqlmesh-console-noise`.
2. `type` must be one of the prefixes in `.claude/rules/branching.md`; the
   summary must be lowercase kebab case. A malformed or unsupported name must
   fail before creating a ref or directory, with an error that gives the
   required form.
3. A tracked `WorktreeCreate` hook in `.claude/settings.json` must invoke a
   standard-library Python helper. It must read the hook JSON from stdin and
   write only the absolute worktree path to stdout; diagnostics go to stderr.
4. The helper must resolve the repository root from the hook's current
   directory, reject path traversal and existing branch/worktree collisions,
   and create the worktree under `.claude/worktrees/<native-name>` on the
   canonical branch. It must use `origin/HEAD` when available and fall back to
   `HEAD`, matching Claude's fresh-worktree behavior without requiring branch
   upstream configuration.
5. The helper must refuse creation if a `.worktreeinclude` file exists. A
   custom creation hook bypasses Claude's automatic include processing, so
   failing explicitly is safer than silently omitting gitignored setup files.
   Support for that file is a future, separately-tested extension.
6. `.claude/rules/branching.md` must distinguish the native `+` worktree name
   from the canonical `/` Git branch, prohibit post-entry branch renames, and
   prescribe `git push origin HEAD:refs/heads/<current-branch>` for a new
   sandboxed worktree branch. That command does not require a local upstream
   configuration.
7. The worktree hook must be verified against both `claude --worktree` and the
   native `EnterWorktree` flow on the supported Claude Code version. If either
   bypasses the hook, this feature must not claim universal enforcement until
   the adapter is extended or that entry point is excluded explicitly.

## Data Model

None.

## Implementation Plan

### Files to Create

- `.claude/hooks/create_worktree.py` — validates the native name, chooses the
  base ref, runs `git worktree add -b`, and prints the resulting path.
- `tests/test_claude_worktree_hook.py` — unit and disposable-repository tests
  for the adapter's contract.

### Files to Modify

- `.claude/settings.json` — registers the repository-owned `WorktreeCreate`
  command.
- `.claude/rules/branching.md` — documents the two-name contract and explicit
  first-push form.
- `docs/specs/INDEX.md` — indexes this specification.

### Key Decisions

- `/` is the Git branch separator because it is the existing general Git and
  Moneybin convention. `+` is only a reversible transport encoding for
  Claude's single path-name token.
- The helper owns a fixed set of accepted types. Its tests compare that set
  with the table in `branching.md`, so a label-policy edit cannot silently
  diverge from creation policy.
- This change does not add a GitHub workflow to apply labels automatically.
  It preserves the existing PR-label workflow while guaranteeing that native
  worktree branches satisfy its input contract.
- No `WorktreeRemove` hook is needed: the hook creates an ordinary Git
  worktree, so Claude's normal Git worktree cleanup remains applicable.

## CLI Interface

No Moneybin CLI changes. The Claude Code invocation becomes:

```text
claude --worktree fix+sqlmesh-console-noise
```

and its resulting Git branch is `fix/sqlmesh-console-noise`.

## MCP Interface

None.

## Testing Strategy

- Unit-test every allowed `type+summary` to `type/summary` conversion.
- Reject unknown types, uppercase names, extra separators, path-like input,
  and pre-existing branches or directories without mutating the repository.
- In a temporary Git repository, assert that a valid invocation creates a
  linked worktree whose checked-out branch is exactly the canonical branch and
  that the helper's sole stdout line is its absolute path.
- Assert the helper prefers `origin/HEAD` and falls back to local `HEAD`.
- Parse the table in `branching.md` in a parity test so the accepted types and
  documented labels remain synchronized.
- Verify the settings registration names `WorktreeCreate` and invokes the
  tracked helper.
- Perform the two native Claude Code creation canaries from requirement 7;
  record the installed Claude Code version and outcomes in the PR description.

## Synthetic Data Requirements

None.

## Dependencies

- Claude Code with `WorktreeCreate` hook support.
- Git and Python 3; the helper deliberately has no third-party dependency.

## Out of Scope

- Changing Moneybin's type-to-label mapping.
- Automatically applying GitHub labels or enforcing branches in GitHub Actions.
- Global Claude settings, desktop-only branch-prefix settings, and Codex
  worktree behavior.
- Copying `.worktreeinclude` files; the hook explicitly protects against
  silently losing that future behavior.
