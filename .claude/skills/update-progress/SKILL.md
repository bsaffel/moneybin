---
name: update-progress
description: Reconcile the private/ tracking docs — design.md, implementation.md, followups.md, findings.md — with the team's current state of work. Removes items that shipped or are no longer relevant (PR + CHANGELOG carry the durable record), promotes in-flight items to their new status, and adds new items discovered in the in-scope work. Aggressively prunes — these are working memory, not history. Use this after shipping a PR, before opening a new one, alongside /update-specs and /update-docs, or whenever the user says "update progress" / "sweep the tracking docs" / "what's the state of the private docs."
---

# /update-progress

Keep `private/` tracking docs lean and reflective of where the team actually
is. These docs are working memory — they answer "what's next?" and "what's
still open?" The PR list, git log, and CHANGELOG are the durable record.
Anything that bloats these files makes them slower to scan and erodes their
value as a planning surface.

## Files in scope

Exactly these four:

- `private/design.md` — Spec quality/readiness tracker (formerly `spec_implementation.md`)
- `private/implementation.md` — Next actions, sequencing
- `private/followups.md` — Deferred PR-review followups by tier
- `private/findings.md` — UX questions / product ideas surfaced live

**Out of scope** for this skill:

- `private/strategy/` — durable strategic positioning; edit by hand.
- `private/plans/` — ephemeral per-task scaffolding; let plans live and die with their tasks.
- `private/reviews/` — per-PR deep-dive artifacts.
- `private/simplify.md`, `private/testing.md`, `private/sandboxing.md` — reference notes, not active tracking.

## Important path note

These files live at the **main repo root**, not inside any worktree.
`private/` is gitignored and shared across worktrees by project convention
(see CLAUDE.md and AGENTS.md `../../private/` reference).

When invoking from a worktree, paths look like `../../private/followups.md`
or the absolute `/Users/bsaffel/Workspace/moneybin/private/followups.md`.
Pass absolute paths to subagents to remove ambiguity.

## Pruning rule (the core discipline)

**Remove resolved items entirely.** When something has shipped or is no
longer relevant:

- Delete the bullet. Do not move it to a "Resolved" appendix.
- Do not preserve the rationale in-line. The PR and CHANGELOG carry the why.
- **Single exception:** if an item resolved with a *decision not to do it*,
  move it to `findings.md`'s "Won't Do" section with the reopen trigger.
  That's a different kind of durable: it stops the same idea from
  re-surfacing in a future planning pass.

The goal is that each tier / section fits in one screen. If a section is
bloating, prune harder — don't add subheadings. The current state of
`followups.md` (79K) is the anti-pattern this skill exists to reverse.

## Scope detection

Default scope is the current branch/PR/changes, not a sweep of all work
ever. Trying to reconcile everything at once produces noise; reconciling
against the in-flight change finds what genuinely moved.

Resolve scope in this order; first match wins:

1. **Explicit arguments.** If the user passed paths, a file name, or "everything," use those.
2. **Branch diff.** If HEAD is ahead of main: `git diff main...HEAD --name-only`.
3. **PR.** Else if `gh pr view --json files -q '.files[].path'` returns a PR's file list, use that.
4. **Uncommitted.** Else `git status --porcelain | awk '{print $2}'`.
5. **Empty.** If still nothing, ask what scope to use — don't default to "everything."

Also gather:

- Recent commit subjects on the branch: `git log main..HEAD --oneline`
- Recently merged PRs (for resolution detection): `gh pr list --state merged --limit 20 --json number,title,mergedAt`

Print a one-line scope summary before doing anything:

> Scope: 7 files changed on this branch, 4 commits, 3 recently-merged PRs (#152, #154, #156)

## Process

### 1. Dispatch one subagent per file, in parallel

Four files → process in two waves of two (or one wave of three + one solo)
to respect the **3-in-flight cap** from CLAUDE.md.

Each subagent receives a self-contained prompt with:

- The **absolute path** of the single tracking file it owns.
- The in-scope diff summary.
- Recent commit subjects.
- Recently merged PR numbers (with titles).
- The pruning rule above, verbatim.
- The expected report format.

Use `subagent_type: general-purpose`. These edits are not read-only.

### 2. Each subagent does, for its file

- **Identify resolved entries.** An entry is resolved if its work shipped in
  an in-scope or recently-merged PR, or if it referenced an issue / file /
  follow-up that is now gone. Delete the entry. Do not move it.
- **Promote in-flight entries.** If something moved from `ready` →
  `in-progress` or `in-progress` → `implemented`, reflect the new state.
- **Add new entries.** If the in-scope work surfaced a new followup, design
  question, or product idea, add it to the appropriate file in the
  appropriate tier. Be brief — entries should be a sentence or two with a
  pointer, not a mini-spec.
- **Update the "Last updated" header.** Each tracking file opens with a
  date-stamped header summarizing the most recent pass. Replace it with
  today's date + a one-line description of what changed in this pass.

### 3. Report

Per-file summary:

- **Removed** — count + a one-line sample
- **Promoted/updated** — count + one-line sample
- **Added** — count + one-line sample
- **Won't-Do moved** (findings.md only) — if any

Plus a top-level note: total bytes removed across the four files. The
metric the user cares about is whether these files got *leaner*.

## What this skill will NOT do

- Touch any public-facing docs (that's `/update-docs`).
- Touch specs in `docs/specs/` (that's `/update-specs`).
- Touch `private/strategy/`, `private/plans/`, or other private/ subdirs.
- Bloat tracking docs with "Resolved" appendices, status histories, or per-PR sections.
- Expand scope beyond what was detected without explicit user approval.
- Reference `private/` paths in any public artifact — these stay in private/.
