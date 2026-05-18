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
- When the user asks for a full doc reconciliation against current state ("clean from-scratch update," "everything")

This skill has **two modes** — see [Modes](#modes) below. Default is
incremental update against an in-flight code change. From-scratch
reconciliation mode is opt-in and treats existing docs as reference,
not as the starting point.

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

## "Last reviewed" datestamp convention

Every in-scope doc opens with an HTML comment naming the date a reviewer
last confirmed the doc matches the code:

```markdown
<!-- Last reviewed: YYYY-MM-DD -->
```

This is **not** git's last-modified date — that bumps for typo fixes
without anyone rechecking content. The stamp's contract is: "on this
date, someone read this doc end-to-end against the current state of
the code and asserted it was accurate."

Two uses:

1. **Scope detection.** Future runs can identify docs whose stamp
   predates the newest in-scope `src/` change — those are review
   candidates. Grep: `grep -rln '<!-- Last reviewed:' README.md CHANGELOG.md CONTRIBUTING.md docs/`.
2. **Reader signal.** A doc stamped six months ago has earned skepticism;
   a doc stamped last week is fresh.

**Bump rules** — the subagent doing the writing pass MUST update the
stamp to today's date as the final step. A subagent that decided no
edits were warranted MUST NOT bump the stamp — absence of an edit
means the doc wasn't re-read against code. If a doc lacks the stamp
entirely, add it on first edit using today's date.

## Modes

### Incremental (default)

- Trigger: branch diff, PR file list, or uncommitted changes (see Scope detection).
- Behavior: read existing doc; identify what the in-scope code change requires; edit only those parts; preserve everything else.
- Use when: a single PR shipped a feature, fix, or behavior change.

### From-scratch reconciliation

- Trigger: explicit user request — `everything`, `from-scratch`, `clean update`, or a named list of docs paired with framing language ("clean update," "from scratch," "as if writing fresh").
- Behavior: read current code state (CLI surface, MCP tools, schema, CHANGELOG `Unreleased`); treat the existing doc as **reference only** — structural inspiration, voice, length budget, anti-pattern list — but write what the doc should say from current truth. Don't be bound by the existing wording when current state contradicts it.
- Use when: a milestone closes; many features have shipped since the doc was last touched; the doc's "Last reviewed" stamp predates a tide of merged PRs; the user explicitly asks for a clean pass.
- Heavier: a full from-scratch sweep across the doc surface is a multi-wave, multi-hour exercise. **Confirm scope with the user before launching** and prefer wave-by-wave execution with checkpoint review between waves.

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

- **README anti-patterns**: in-README roadmap matrix; text-heavy comparison cells; feature inventories; License essays; "Wave"/"Level" terminology; 9-node pipeline Mermaid in the README. Comparison cells are ✅/❌/🟡 emoji (no prose). How-It-Works Mermaid stays 3–4 nodes (sales-pitch, not architecture).
- **Comparison table width** — prefer 7-wide for the README teaser and the `docs/comparison.md` table; the goal is scannable density on a phone, not exhaustive completeness. **Break the 7-wide guideline when honesty demands it**: if a major option for the page's primary persona is missing (precedent: Firefly III for the self-hoster persona, added as an 8th column with a one-line note above the table), go 8-wide. Don't break to 9+ without a written exception; if a 9th option is fighting to get in, retire a less-relevant column instead.
- **No private/ references in public docs.** Never cite `private/...` paths or phrasing like "the strategic review" / "the strategic audit named X" in README, CHANGELOG, CONTRIBUTING, or any `docs/` file. Restate substance directly. If you find existing violations, fix them in-pass.
- **No `Co-Authored-By: Claude` trailers in any commit message produced by this skill.**
- **Tagline preserved**: `Your finances, understood by AI.` — do not change unless the developer explicitly asks.
- **No milestone or version codes in user-facing prose — pre-v1.** Milestone codes (`M2A`, `M3B`, etc.) are insider jargon. A new reader who lands on the docs doesn't know what they mean, and shouldn't have to. **They'll care about version numbers once there's a usable v1** — until then, the words "shipped," "planned," and "coming with X" carry every meaning a reader needs. Replace milestone codes with the user-visible thing they describe:
  - "M3A shipped" → "Plaid sync is live"
  - "blocked on M2B" → "lands with the architecture-reference write-up"
  - "M3D persona" → "for users who want a polished web UI (planned)"
  - "After M3E" → "once the hosted tier is live"
  - "Pre-launch. M0/M1 shipped, M2A/B/C in flight, M3A Phase 1 shipped..." (status callout) → "Pre-v1. The CLI, MCP server, encrypted storage, and Plaid sync work today; web UI and the hosted tier are planned." Link to `docs/roadmap.md` for the milestone breakdown.

  **Exceptions — milestone codes ARE allowed and expected in:**
  - `docs/roadmap.md` (its structure IS milestones)
  - `CHANGELOG.md` (sections are tagged by milestone)
  - `docs/specs/`, `docs/decisions/` (internal — out of scope anyway)

  When used in the exception files, the naming is fixed: `M0`, `M1`, `M2A`, `M2B`, `M2C`, `M3A` (Plaid), `M3B` (investments), `M3C` (multi-currency + budgets), `M3D` (Web UI + Streamable HTTP), `M3E` (hosted launch). Don't invent new ones.

  **Post-v1:** this rule relaxes. Semantic versions (`v1.2`, `v2.0`) become meaningful to readers and can appear in user-facing prose ("new in v1.2"). Revisit this rule once v1 ships.

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

Cap at **3 in flight** to avoid overwhelming Claude Code's subagent
capacity. Larger sets process in waves.

Each subagent receives a self-contained prompt with:

- Path of the **single doc file** it owns.
- The persona, tone, and length budget for that file (from the table above) — paste the row verbatim, don't summarize.
- The in-scope diff and a short summary of the user-visible effect.
- The anti-patterns block above — verbatim.
- The Mermaid-over-ASCII rule (from `.claude/rules/documentation.md`) if diagrams are involved.

Use `subagent_type: general-purpose`.

### 3. Each subagent does, for its file

- **Incremental mode:** update only what the in-scope change requires; preserve everything else.
- **From-scratch mode:** read current code state and re-author the doc from current truth. Existing doc is reference for structure, voice, and length budget — not the baseline content.
- Write in the persona voice for that file (see [Per-file voice + length budget](#per-file-voice--length-budget)).
- Cut content that's no longer accurate.
- Respect the length budget — if you're adding, find something to remove or compact. Don't grow indefinitely.
- Apply the anti-patterns rules. If the file currently violates one (e.g., a private/ ref), fix it in-pass.
- For new diagrams, use Mermaid code blocks.
- **Bump the `<!-- Last reviewed: -->` stamp** to today's date as the final step. Add the stamp if missing.

### 4. Persona-review pass (mandatory for from-scratch mode, optional for incremental)

After the writing pass for a doc completes, dispatch **2–3 persona-review
subagents** in parallel to critique the freshly-edited file. This catches
the failure modes the author subagent can't see from inside its own pass:
unfamiliar jargon, missing context, jobs-to-be-done that aren't answered,
information present that the persona doesn't care about.

**Skip for:** `CHANGELOG.md` (mechanical), trivial diff-only edits in
incremental mode (one bullet added, no narrative change).

For each touched doc, pick 2–3 personas from `docs/audience.md` weighted
toward the file's primary audience but **always include at least one
adjacent persona** to surface unstated assumptions. Example weightings:

| File | Personas to review as |
|---|---|
| `README.md` | Power-user migrant + AI-native dev + Self-hoster |
| `docs/architecture.md` | AI-native dev + Power-user migrant |
| `docs/audience.md` | Tracker (post-launch) + FIRE / wealth-builder (post-launch) + Power-user migrant |
| `docs/comparison.md` | Power-user migrant + Self-hoster |
| `docs/features.md` | Power-user migrant + AI-native dev |
| `docs/guides/*` | Power-user migrant + AI-native dev |
| `docs/reference/*` | AI-native dev + Power-user migrant |
| `docs/architecture/*` | AI-native dev + AI-native dev (different framing: "new to project" + "deep in the stack") |
| `docs/tech/*` | AI-native dev + Self-hoster |

Each persona-review subagent receives:

- Path of the **single freshly-written doc**.
- The persona description verbatim from `docs/audience.md` (or the persona table above for stable definitions).
- This prompt template:

  > Read [DOC PATH] as [PERSONA NAME — paragraph description]. You are not editing — you are critiquing.
  >
  > Report in five sections (be concrete; cite line numbers or quote sentences when possible):
  >
  > 1. **Understandability** — what made sense; what didn't; jargon that needs definition or removal.
  > 2. **Information present that you don't need** — content that's noise for this persona.
  > 3. **Information missing that you came looking for** — questions you arrived with that this doc didn't answer.
  > 4. **Jobs-to-be-done** — list the JTBD this doc *does* answer for you; list adjacent JTBDs it *should* answer but doesn't.
  > 5. **One-sentence verdict** — would you keep reading the docs after this page, bounce, or feel oriented to act?
  >
  > Keep the report under 400 words. Do not propose specific rewrites — that's the next subagent's job. Surface gaps and concerns; don't author replacements.

Use `subagent_type: general-purpose`.

### 5. Revision pass — integrate persona critique

Re-dispatch the author subagent (or a fresh one with the same prompt
plus the critique reports) to revise the doc based on persona feedback.

The author subagent's prompt for this pass includes:

- The current state of the doc (post-writing-pass).
- All 2–3 persona critiques verbatim.
- This guidance:

  > Reconcile the critiques. Apply the ones that converge across personas or that clearly improve understandability. Push back on critiques that conflict with the file's primary persona or length budget — note them in the report but don't act.
  >
  > Do NOT chase every comment. The goal is a doc that serves its primary persona well, with the adjacent persona used as a sanity check on missing context — not as an equal voice.

The revision subagent bumps the `<!-- Last reviewed: -->` stamp again
only if it actually edits. If the critiques produced no actionable
revisions, leave the stamp from step 3.

### 6. Run the shipping.md checklist for any newly-shipped feature

Per `.claude/rules/shipping.md`:

- **CHANGELOG entry** under `Unreleased` in the correct category (Added / Changed / Deprecated / Removed / Fixed / Security). Cite the PR. Skip if the change is internal-only (refactors, CI tweaks, style, test-only PRs, ADR additions, private/ changes).
- **`docs/roadmap.md`** — move the feature row from 📐 designed / 🗓️ planned to ✅ shipped. Update milestone status if a sub-milestone just closed.
- **`docs/features.md`** — add or update the entry if it's a user-facing capability.
- **`README.md`** status callout — update only if a milestone closed or a previously-promised feature now exists. Do not re-add an in-README roadmap matrix.
- **Per-feature guides** — extend existing guides; only add new ones for substantial new capability.

### 7. Report

Per-file summary of edits, plus:

- **Touched** — list of files modified, with one-line summary each.
- **Considered but skipped** — list of files where the in-scope change didn't warrant an edit, with the reason (e.g., "MCP guide: internal refactor, no user-visible change").
- **Anti-patterns fixed** — any pre-existing violations cleaned up in-pass.
- **Persona-review highlights** — for each persona-reviewed doc: the top 1–2 actionable critiques applied, plus any critiques deliberately not applied (with rationale). Helps the user see what the second pass changed and why.
- **Stamps bumped** — list of docs whose `<!-- Last reviewed: -->` was updated and the new date. Surfaces drift if a doc was expected to be touched but wasn't.

## What this skill will NOT do

- Touch `docs/specs/` — that's `/update-specs`.
- Touch `docs/decisions/` — ADRs are by hand.
- Touch `private/` — that's `/update-progress`.
- Expand scope beyond what's needed for the in-scope change.
- Reintroduce stored anti-patterns (in-README roadmap, feature inventories, License essays, Wave/Level terminology, private/ refs).
- Change the README tagline `Your finances, understood by AI.` without explicit user direction.
- Add `Co-Authored-By: Claude` trailers to any commit.
