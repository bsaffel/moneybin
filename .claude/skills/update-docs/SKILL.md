---
name: update-docs
description: Update the public-facing documentation surface — README, CHANGELOG, CONTRIBUTING, top-level docs (architecture, audience, comparison, features, licensing, roadmap), docs/guides/, docs/reference/, docs/architecture/, docs/tech/ — to reflect what the in-scope code change means for users. Persona-aware writing per docs/audience.md; assumes the reader is not an expert on internals. Excludes specs and ADRs (those are /update-specs and by-hand respectively). Use this when finishing user-visible work, before opening a PR that adds capability, when a milestone closes, or whenever the user says "update the docs" / "make sure the docs reflect this." Don't wait to be asked — if user-visible work just shipped, suggest running it.
---

# /update-docs

The public doc surface is a storefront, not a manual. Each file has a
purpose and an audience. This skill updates that surface for in-scope
changes, with **persona-aware writing** — never as a dumping ground for
whatever came out of implementation. The goal is that a reader who isn't an
expert on MoneyBin's internals gets exactly the answer they came for, in the
voice and density appropriate to where they landed.

## When this skill is the right tool

- Finishing user-visible work (new CLI command, new MCP tool, new import format, behavior change)
- Before opening a PR that adds capability — so the PR ships with its docs
- When a milestone closes and the README status callout / roadmap needs to move
- When the user asks whether the public docs reflect current state

This skill is **not** for writing brand-new guides from scratch — the
information architecture matters too much to automate. Use it to update,
prune, and reconcile.

## Files in scope

**IN scope:**

- `README.md`
- `CHANGELOG.md`
- `CONTRIBUTING.md`
- `docs/*.md` — top-level: `architecture.md`, `audience.md`, `comparison.md`, `features.md`, `licensing.md`, `roadmap.md`
- `docs/guides/`
- `docs/reference/`
- `docs/architecture/`
- `docs/tech/`

**OUT of scope:**

- `docs/specs/` — that's `/update-specs`
- `docs/decisions/` — ADRs are durable, edited by hand only
- `private/` — that's `/update-progress`

## Personas (read docs/audience.md as the canonical source)

Voice and density depend on who's reading. The current personas:

- **Power-user finance migrant** — coming from Tiller / Lunch Money / Beancount / hledger; comfortable with CLIs; wants the AI + database story.
- **AI-native developer** — lives in Claude Code / Cursor / VS Code; wants MCP for every domain; reads code fluently.
- **Self-hoster** — runs Vaultwarden / NextCloud / Photoprism; has a personal-finance todo; wants clean install + good defaults.
- **Tracker** (post-launch) — wants polished visual dashboard for spending and net worth.
- **FIRE / wealth-builder** (post-launch) — net worth + investments + cost basis.

For "not yet for you" cases, point honestly to alternatives (`docs/audience.md` has the table). Honest framing is part of the brand.

## Per-file voice + length budget

| File | Primary persona | Tone | Length budget |
|---|---|---|---|
| `README.md` | Power-user migrant + AI-native dev | Storefront — what is it, why care, how to start | ~220–260 lines (per stored feedback) |
| `CHANGELOG.md` | All users | Factual, one bullet per user-visible change, cite PR | One section per release / Unreleased |
| `CONTRIBUTING.md` | Self-hoster, OSS contributor | Concrete steps to land a change | As short as possible |
| `docs/architecture.md` | AI-native dev, technical migrant | Guarantees → diagram → contract → negative space | One page |
| `docs/audience.md` | Anyone evaluating | Honest "this is and isn't for you" | Tight |
| `docs/comparison.md` | Migrant from cloud PFM | 7-wide table with ✅/❌/🟡; no text-heavy cells | Tight |
| `docs/features.md` | Anyone evaluating | Capability snapshot, link out to guides | Bullets |
| `docs/roadmap.md` | Anyone evaluating | Milestone tables (📐 designed / 🗓️ planned / ✅ shipped) | One page |
| `docs/guides/` | Power-user migrant | How-to, one task per guide, CLI examples | One screen per concept |
| `docs/reference/` | AI-native dev | Complete and dense — table-shaped | As long as needed |
| `docs/architecture/` | AI-native dev | Deep technical context; **Mermaid over ASCII** for diagrams | As long as needed |
| `docs/tech/` | AI-native dev | Implementation-specific notes | As long as needed |

## Stored anti-patterns (do not reintroduce)

These come from prior feedback memories and should be passed to every
subagent verbatim. They keep accidentally regrowing:

- **README anti-patterns**: in-README roadmap matrix; text-heavy comparison cells; feature inventories; License essays; "Wave"/"Level" terminology; 9-node pipeline Mermaid in the README. Comparison stays 7-wide with ✅/❌/🟡 emoji. How-It-Works Mermaid stays 3–4 nodes (sales-pitch, not architecture).
- **No private/ references in public docs.** Never cite `private/...` paths or phrasing like "the strategic review" / "the strategic audit named X" in README, CHANGELOG, CONTRIBUTING, or any `docs/` file. Restate substance directly. If you find existing violations, fix them in-pass.
- **No `Co-Authored-By: Claude` trailers in any commit message produced by this skill.**
- **Tagline preserved**: `Your finances, understood by AI.` — do not change unless Brandon explicitly asks.
- **Milestone names**: `M0`, `M1`, `M2A`, `M2B`, `M2C`, `M3A` (Plaid), `M3B` (investments), `M3C` (multi-currency + budgets), `M3D` (Web UI + Streamable HTTP), `M3E` (hosted launch). Don't invent new ones.

## Scope detection

Default scope is the in-flight change, not a doc sweep. Auditing all docs at
once produces sprawling diffs and overwrites things that were intentional.

Resolve scope in this order; first match wins:

1. **Explicit arguments.** If the user passed paths, a doc name, or "everything," use those.
2. **Branch diff.** If HEAD is ahead of main: `git diff main...HEAD --name-only`.
3. **PR.** Else if `gh pr view --json files -q '.files[].path'` returns a PR's file list, use that.
4. **Uncommitted.** Else `git status --porcelain | awk '{print $2}'`.
5. **Empty.** If still nothing, ask what scope to use — don't default to "all docs."

Print a one-line scope summary before doing anything:

> Scope: 4 source files changed (1 new MCP tool, 1 new CLI command); evaluating 3 docs: README.md, docs/guides/cli.md, CHANGELOG.md.

## Process

### 1. Map code changes to user-visible effect

For each in-scope code change, identify what a *user* (not a developer)
would notice. Common mappings:

- New CLI command → README "What works today" reference + relevant guide + CHANGELOG `Added`
- New MCP tool → MCP guide + features.md + CHANGELOG `Added`
- New import format → import guide + features.md + CHANGELOG `Added`
- Behavior change in existing command → relevant guide + CHANGELOG `Changed`
- Bug fix users will notice → CHANGELOG `Fixed`
- Internal refactor with no user-visible effect → no doc change; do not invent one

If a change has no user-visible effect, say so in the report and move on.

### 2. Dispatch one subagent per affected doc file, in parallel

Cap at **3 in flight** per CLAUDE.md. Larger sets process in waves.

Each subagent receives a self-contained prompt with:

- Path of the **single doc file** it owns.
- The persona, tone, and length budget for that file (from the table above) — paste the row verbatim, don't summarize.
- The in-scope diff and a short summary of the user-visible effect.
- The anti-patterns block above — verbatim.
- The Mermaid-over-ASCII rule (from `.claude/rules/documentation.md`) if diagrams are involved.

Use `subagent_type: general-purpose`.

### 3. Each subagent does, for its file

- Update only what the in-scope change requires.
- Write in the persona voice for that file.
- Cut content that's no longer accurate.
- Respect the length budget — if you're adding, find something to remove or compact. Don't grow indefinitely.
- Apply the anti-patterns rules. If the file currently violates one (e.g., a private/ ref), fix it in-pass.
- For new diagrams, use Mermaid code blocks.

### 4. Run the shipping.md checklist for any newly-shipped feature

Per `.claude/rules/shipping.md`:

- **CHANGELOG entry** under `Unreleased` in the correct category (Added / Changed / Deprecated / Removed / Fixed / Security). Cite the PR. Skip if the change is internal-only (refactors, CI tweaks, style, test-only PRs, ADR additions, private/ changes).
- **`docs/roadmap.md`** — move the feature row from 📐 designed / 🗓️ planned to ✅ shipped. Update milestone status if a sub-milestone just closed.
- **`docs/features.md`** — add or update the entry if it's a user-facing capability.
- **`README.md`** status callout — update only if a milestone closed or a previously-promised feature now exists. Do not re-add an in-README roadmap matrix.
- **Per-feature guides** — extend existing guides; only add new ones for substantial new capability.

### 5. Report

Per-file summary of edits, plus:

- **Touched** — list of files modified, with one-line summary each.
- **Considered but skipped** — list of files where the in-scope change didn't warrant an edit, with the reason (e.g., "MCP guide: internal refactor, no user-visible change").
- **Anti-patterns fixed** — any pre-existing violations cleaned up in-pass.

## What this skill will NOT do

- Touch `docs/specs/` — that's `/update-specs`.
- Touch `docs/decisions/` — ADRs are by hand.
- Touch `private/` — that's `/update-progress`.
- Expand scope beyond what's needed for the in-scope change.
- Reintroduce stored anti-patterns (in-README roadmap, feature inventories, License essays, Wave/Level terminology, private/ refs).
- Change the README tagline `Your finances, understood by AI.` without explicit user direction.
- Add `Co-Authored-By: Claude` trailers to any commit.
