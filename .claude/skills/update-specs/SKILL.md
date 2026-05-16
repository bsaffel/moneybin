---
name: update-specs
description: Reconcile docs/specs/ with the implementation that actually shipped in the current scope. Reviews recent code changes, identifies which specs the changes touch via docs/specs/INDEX.md, and updates each spec to reflect design evolutions, refactors, or follow-up work that didn't get its own spec. Auto-edits small drift; surfaces design-intent changes for review. Use this whenever finishing a feature branch, after a follow-up PR adds behavior the original spec didn't describe, after a refactor renames or moves primitives referenced in specs, or whenever the user asks "are the specs still accurate?" / "update the specs" / "sync the specs to the code." If you've just finished implementation work that drifted from the spec it was based on, suggest running it without waiting to be asked.
---

# /update-specs

Keep `docs/specs/` honest. Specs are durable design contracts; implementation
inevitably drifts as constraints surface during the build. This skill detects
that drift in the **current scope** (not the whole spec set) and patches it
back in.

## When this skill is the right tool

- After implementation of a spec is largely done and the result differs from what the spec described
- After a follow-up PR added behavior that didn't get its own spec
- After a refactor renamed/moved primitives or modules that specs reference
- When the user asks whether the specs are still accurate

This skill is **not** for writing new specs. Use `docs/specs/_template.md` by
hand for that — the design discipline matters too much to automate.

## Scope detection

Default scope is whatever's in flight, not the whole codebase. Trying to
audit everything at once produces sprawling diffs that are hard to review.

Resolve scope in this order; first match wins:

1. **Explicit arguments.** If the user passed paths, a spec name, or
   "everything," use those.
2. **Branch diff.** If HEAD is ahead of main, use `git diff main...HEAD --name-only`.
3. **PR.** Else if `gh pr view --json files -q '.files[].path'` returns a
   PR's file list, use that.
4. **Uncommitted.** Else `git status --porcelain | awk '{print $2}'`.
5. **Empty.** If still nothing, ask the user what scope they want — don't
   default to the whole repo.

Before doing any updates, print a one-line scope summary:

> Scope: 7 files changed on this branch (3 in src/moneybin/extractors/, 2 in tests/, 2 in docs/specs/)

This gives the user a chance to interrupt if the inferred scope is wrong.

## Process

### 1. Map files to specs

Read `docs/specs/INDEX.md` — it lists every spec with status and topic. For
each in-scope source file, infer which specs it touches:

- First pass: match on INDEX descriptions and topic groupings.
- If INDEX is ambiguous, `grep -l '<module-path>' docs/specs/*.md` to find
  specs that reference the file directly.
- A file that maps to **zero specs** is fine — many changes (CI, internal
  refactors, fixtures) don't warrant a spec touch. Note it in the report,
  don't force a mapping.
- A file that maps to **multiple specs** spawns multiple subagents — one
  per matched spec.

### 2. Dispatch one subagent per spec, in parallel

Cap at **3 in flight** to avoid overwhelming Claude Code's subagent
capacity. If more than 3 specs are touched, process in waves.

Each subagent receives a self-contained prompt with:

- The path of the **single spec** it owns.
- The list of in-scope files relevant to that spec.
- A short summary of what changed (commit subjects from `git log main..HEAD --oneline` are usually enough).
- The auto-edit policy below — verbatim, not summarized.
- Instructions to report what it changed (or proposed) in a structured way.

Use `subagent_type: general-purpose` unless the analysis is read-only, in
which case `Explore` is cheaper.

### 3. Auto-edit policy (give this to every subagent verbatim)

**Auto-edit** (no review needed):

- Refresh stale file paths, function names, class names, line counts.
- Update "shipped via PR #N" footnotes when an in-scope PR closes the work.
- Promote status (`ready` → `in-progress`, `in-progress` → `implemented`).
- Add a short bullet to a Notes column reflecting a follow-up that landed.
- Mermaid diagrams: refresh box labels / arrow targets when names changed.

**Propose for review** (do not auto-edit — surface a diff):

- Adding or removing sections.
- Changing the design intent or the promised behavior.
- Renaming concepts that consumers of the spec rely on.
- Marking previously-spec'd behavior as removed.
- Anything that changes what the spec **promises** vs. what it **describes**.

If unsure: propose. Specs are public contracts; silent rewrites of intent
are the kind of thing that hurts later.

### 4. Update INDEX.md

If any subagent changed a spec's status, update the corresponding row in
`docs/specs/INDEX.md` in the same pass. Don't leave INDEX stale.

### 5. Report

After all subagents complete, summarize for Brandon:

- **Auto-edited** — one line per spec, e.g. `categorization-overview.md: refreshed module paths, bumped status to implemented (PR #142)`.
- **Awaiting your call** — proposals inline, one block per spec, showing the proposed diff and the reason it wasn't auto-applied.
- **Unmapped scope** — in-scope files that didn't match any spec. Flag, don't write. The user decides whether they warrant a new spec.

## What this skill will NOT do

- Write new specs (manual; use `_template.md`).
- Touch ADRs in `docs/decisions/` — durable design records, edit by hand.
- Touch `private/` tracking docs — that's `/update-progress`.
- Touch public guides, README, CHANGELOG, roadmap — that's `/update-docs`.
- Expand scope beyond what was detected without explicit user approval.
- Add new `private/` path references in specs. Existing references should be **removed when encountered** in the pass, matching the stored "no private/ refs in public docs" guidance — do not preserve them just because they were there before.
