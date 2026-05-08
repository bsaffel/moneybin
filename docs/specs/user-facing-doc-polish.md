# Feature: User-Facing Documentation Polish

## Status
ready

## Goal

Bring the user-facing documentation surface (README, CHANGELOG, threat model, database-security guide, License framing, comparison table) up to a bar that survives ruthless scrutiny from the technical personas MoneyBin can credibly serve today and at M2C close — Sam (curator-engineer), Devon (MCP-native developer), and Priya (self-hosted privacy refugee). Most of the work ships *before* M2C and doesn't depend on product changes; the parts that do (architecture distillation, brew install instructions, demo asset) flip cleanly when their gating milestone closes.

The tagline `Your finances, understood by AI.` stays as the aspirational vision statement. The honesty and substance layer lives immediately below it.

## Background

- [`docs/specs/privacy-data-protection.md`](privacy-data-protection.md) — source for the user-facing threat model.
- [`docs/decisions/009-encryption-key-management.md`](../decisions/009-encryption-key-management.md) — KDF + key-storage decisions referenced from the threat model.
- Existing user-facing assets that this spec extends: [`README.md`](../../README.md), [`SECURITY.md`](../../SECURITY.md) (already strong, no change), [`CONTRIBUTING.md`](../../CONTRIBUTING.md) (one minor addition), [`docs/guides/database-security.md`](../guides/database-security.md).

This spec is purely user-facing documentation work. It does not change product behavior. It does not introduce new schemas, services, MCP tools, or CLI commands. The only "code" change is `pyproject.toml` metadata polish (already on the M2C distribution work).

## Requirements

Numbered for traceability. Each requirement is testable by inspection.

1. **Tagline preserved.** The README masthead retains `Your finances, understood by AI.` as the aspirational vision tagline. Honesty and substance follow below it.

2. **Sub-line carries the honesty + substance framing.** The line immediately below the tagline reads (or paraphrases): *The local-first, AI-native financial data platform you actually own. Encrypted by default. Queryable with SQL. Extensible with MCP.* Use "data platform" — not "ledger" (which has Beancount/hledger double-entry connotations MoneyBin's `dim_accounts` + `fct_transactions` star schema doesn't match), not "data warehouse" (technically accurate but tonally cold).

3. **Status block names the pre-launch state honestly.** The status paragraph explicitly references M2 (curator state, brew install, first-run wizard) and M3 (Plaid sync, investments, multi-currency, Web UI, hosted) — not "coming soon" hand-waves.

4. **"Why MoneyBin" bullets lead with lineage.** The first bullet is *Lineage you can audit*, framing every number as traceable from `core.fct_transactions` → SQLMesh model → `raw` row → source file. Encryption follows. AI-native + client-agnostic third. Local + hosted choice fourth.

5. **Quick Start frames the developer install honestly.** A one-line preface acknowledges that today's install path is `git clone` + `uv` (developer install) and that `brew install moneybin` ships in M2C. Active repulsion of personas who can't yet use it (Mark, Casey) is a feature, not a bug.

6. **"Who this is for / not yet for" candor block exists.** A pre-Quick-Start section names today's fits (curator-engineers, MCP developers, self-hosters) and today's not-yet-fits (one-click bank sync, polished mobile, investment tracking, pure envelope budgeting).

7. **Comparison table expands beyond the original four.** Era / BankSync, Lunch Money, Wealthfolio added as columns. Rows include encrypted-at-rest, AI/MCP integration, SQL access, license. Honest about what each competitor does and doesn't do; no "first" or "only" claims.

8. **Roadmap section adopts milestone terminology.** The ✅/📐/🗓️ icons stay. A `Milestone` column maps each row to M0–M1 (shipped), M2A/2B/2C (pre-launch), M3A–M3E (launch), or post-launch.

9. **License section explains AGPL with substance.** Replaces the badge-only treatment. Names the four implications (free use, free fork, network-service-must-publish-source, hosted-server-runs-the-same-code). References the Bitwarden / Plausible / Element / Sentry / Ghost peer set.

10. **`CHANGELOG.md` exists at repo root.** Backfilled from recent PRs in Keep-A-Changelog format. Groups entries by version (or by milestone — to be decided in implementation). Devon checks for this; absence reads as "not serious."

11. **`docs/guides/threat-model.md` exists.** One-page user-facing distillation of [`privacy-data-protection.md`](privacy-data-protection.md): what the encryption protects against (stolen laptop, synced folder, shared machine), what it doesn't (forgotten passphrase + lost recovery codes = data loss; AI vendor data flow when you ask Claude/ChatGPT a question; an attacker with both DB file and live keychain session). References ADR-009 for KDF rationale.

12. **`docs/guides/database-security.md` adds a threat-model summary paragraph.** One-paragraph summary near the top + link to the new threat-model guide + ADR-009 reference + explicit "passphrase loss = data loss" note with the auto-key + `db key show` mitigation pattern.

13. **`docs/architecture.md` placeholder exists, gated on M2B.** A short placeholder file links forward to `architecture-shared-primitives.md` once it lands. The full distillation (one-page user-facing version of the spec) ships in a follow-up PR after M2B closes — this spec doesn't block on that.

14. **Demo asset placeholder exists, gated on M2C.** Acknowledged in the README's `Documentation` or `Quick Start` section as "demo coming with brew install in M2C." Don't fake it; don't pretend it exists yet.

15. **Documentation section links surface the decision log.** README's `Documentation` section adds an explicit link to `docs/decisions/` (ADRs) — most projects bury these; surfacing them is a credibility signal.

16. **No false claims of "first" or "only."** Every superlative claim ("the first AI-native…", "the only local-first…") is removed or replaced with a non-superlative conjunction ("local-first AND AI-native AND open-source AND encrypted-by-default" — defensible without claiming primacy).

17. **MCP client list is precise about transport.** "Connect Claude, ChatGPT, Cursor…" stays, but a clarifying note distinguishes local-stdio support today (works for Claude Desktop, Claude Code, Cursor, Windsurf, VS Code, Gemini CLI, Codex CLI/Desktop/IDE, ChatGPT Desktop) from Streamable HTTP support arriving with hosted in M3D + M3E (which unlocks ChatGPT web/mobile and other remote clients).

18. **CONTRIBUTING.md gains a "where the strategy lives" pointer.** One short paragraph noting that `docs/specs/` and `docs/decisions/` are the public planning artifacts; project-internal strategy is private. Helps Sam understand what's open and what isn't.

## Data Model

None. This spec does not introduce or modify any database schemas, migrations, or data models.

## Implementation Plan

### Files to Create

| File | Purpose | Dependency |
|---|---|---|
| `CHANGELOG.md` | Keep-A-Changelog format. Backfill from recent PRs (M0–M1 cumulative entries grouped by milestone, then per-PR going forward). | None |
| `docs/guides/threat-model.md` | One-page user-facing threat model. Pulls from [`privacy-data-protection.md`](privacy-data-protection.md) and ADR-009. | None |
| `docs/architecture.md` | Placeholder with forward-pointer. Real content ships post-M2B once `architecture-shared-primitives.md` lands. | None for placeholder; M2B for full distillation |

### Files to Modify

| File | Change | Dependency |
|---|---|---|
| `README.md` | Tagline preserved. Sub-line refreshed. Status block names M2 / M3. "Who this is for / not yet for" block added. "Why MoneyBin" bullets reordered (lineage first). Quick Start gets honest preface. Comparison table expanded (Era/BankSync, Lunch Money, Wealthfolio rows). Roadmap table adds `Milestone` column. License section gets substance. Documentation section adds ADR link. | None for most. Quick Start "brew install" line goes from forward-pointer to live instruction at M2C close. |
| `docs/guides/database-security.md` | Add threat-model summary paragraph + link to `docs/guides/threat-model.md` + ADR-009 link + "passphrase loss = data loss" pattern. | None |
| `CONTRIBUTING.md` | One paragraph: "where the strategy lives." | None |
| `pyproject.toml` | Polish `[project]` metadata (author, license, homepage, classifiers, keywords, license-file inclusion) for PyPI publish readiness. | None for polish; PyPI publish workflow itself is M2C distribution-roadmap.md scope. |

### Key Decisions

- **Tagline stays.** User explicitly affirmed `Your finances, understood by AI.` is the aspirational vision and should be preserved. Substance and honesty layer below it.
- **No superlative claims.** "First," "only," "the best" do not appear in user-facing copy. Replaced by descriptive conjunctions.
- **Honesty disarms scrutiny.** The "who this isn't for yet" block is load-bearing. It actively repels personas who'd bounce in frustration; it earns trust from those who'd otherwise scrutinize harder.
- **Milestone terminology in user-facing roadmap.** Public README references milestones directly with their codes (M0, M1, M2A–C, M3A–E). Sam/Devon/Priya can plan around named milestones; they can't plan around "soon."
- **Demo asset and architecture distillation are forward-pointers in this spec.** They land in follow-up work tied to M2C and M2B respectively. This spec doesn't block on either.
- **MCP transport clarity over generality.** "Connect Claude, ChatGPT, Cursor" is too broad; the list explicitly distinguishes today's local-stdio coverage from M3D's Streamable HTTP coverage. Devon notices precision and rewards it.
- **No archiving of existing material.** Per project convention, implemented specs and existing guides stay where they are. This spec adds and refreshes; it does not move or delete.

### Sequencing

The work splits cleanly into "ship now (no product dependencies)" and "ship at milestone close":

**Now (single docs PR or two):**
1. README rewrite — tagline preserved, sub-line refreshed, Why bullets reordered, candor block added, comparison table expanded, License section with substance, milestone-aligned roadmap, Documentation section adds ADR link, MCP transport clarity. Quick Start preface acknowledges brew install ships at M2C without yet promising the command works today.
2. `CHANGELOG.md` backfilled from recent PRs.
3. `docs/guides/threat-model.md` written.
4. `docs/guides/database-security.md` polish pass.
5. `CONTRIBUTING.md` strategy-pointer paragraph.
6. `pyproject.toml` metadata polish.

**At M2B close (separate PR):**
7. `docs/architecture.md` becomes the user-facing distillation of `architecture-shared-primitives.md`. Placeholder is replaced with real content.

**At M2C close (separate PR):**
8. README Quick Start flips to brew-install-primary.
9. Demo asciinema cast or screen-recording asset added to `docs/assets/` and embedded in README.
10. README adds "demo profile preset" reference (`moneybin demo`).

The `now` batch is the bulk of this spec. The `M2B` and `M2C` items are explicitly out-of-scope for the initial implementation PR but tracked here so the spec is the single source of truth for the doc surface.

## CLI Interface

Not applicable. No CLI changes.

## MCP Interface

Not applicable. No MCP changes.

## Testing Strategy

This is documentation work; testing is by inspection and review.

- **Markdown linting** — existing CI gates apply (markdownlint per project conventions).
- **Link integrity** — verify all internal links (relative paths to specs, ADRs, guides) resolve.
- **Persona walkthrough** — manually walk the README from each of Sam/Devon/Priya's first-30-seconds checking pattern. Each persona's named bounce triggers must be addressed.
- **Honesty audit** — grep the README for superlatives ("first," "only," "the best") and aspirational hand-waves ("coming soon," "powered by AI") that aren't immediately substantiated.
- **CHANGELOG accuracy** — entries cross-referenced against `git log --oneline` for the relevant range; PR numbers cited.
- **Threat model accuracy** — claims in `docs/guides/threat-model.md` cross-checked against [`privacy-data-protection.md`](privacy-data-protection.md) and ADR-009.

## Synthetic Data Requirements

None. This spec does not exercise the data pipeline.

## Dependencies

- **`privacy-data-protection.md`** (✅ implemented) — source material for the threat model guide.
- **ADR-009** (encryption key management, ✅ written) — referenced from the threat model.
- **No code dependencies.** This work does not require any product change to ship the `now` batch.
- **`architecture-shared-primitives.md`** (M2B, not yet written) — required only for the *full* `docs/architecture.md` distillation. The placeholder version ships without it.
- **M2C distribution work** (`brew install`, PyPI publish, demo profile) — required only for the M2C-close batch (Quick Start flip, demo asset). The `now` batch does not block on this.

## Out of Scope

- **Static landing page** at `moneybin.dev` (or chosen domain). Tracked separately in `distribution-roadmap.md` as a M2C deliverable; not an in-repo artifact.
- **The architecture-shared-primitives spec itself** (`architecture-shared-primitives.md` — M2B). This spec only consumes its output via the `docs/architecture.md` distillation.
- **`brew install` formula authoring and PyPI publish workflow.** Tracked in `distribution-roadmap.md` as M2C deliverables. This spec only updates the README's Quick Start framing to reflect them.
- **Demo asciinema cast production.** Acknowledged here as forward-pointer; the actual recording and embedding ship at M2C close.
- **Telemetry / version-check / opt-in metrics in the package.** Tracked in `distribution-roadmap.md` §6 as post-launch consideration.
- **Internationalization or translations.** No multi-language docs in scope at launch.
- **Marketing-site copy beyond the README.** Out of repo; covered by M2C landing-page work.
- **Mobile / responsive treatment of the in-repo docs.** README is read on GitHub, which handles this.
- **First-class splits, envelope budgeting, ML categorization documentation.** Those features are parked or post-launch; their docs land when (and if) they ship.
