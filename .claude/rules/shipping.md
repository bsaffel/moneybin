---
description: "Post-implementation checklist: CHANGELOG, roadmap, features, README updates, pre-push /code-review pass"
paths: ["CHANGELOG.md", "README.md", "docs/roadmap.md", "docs/features.md", "docs/specs/INDEX.md"]
---

# Shipping & Public Documentation

## When a Feature Ships

An `implemented` spec stays where it is. `docs/specs/archived/` is only for specs that were superseded or abandoned — shipping is not a reason to archive one, and moving it there hides the design record for a feature that now exists.

After marking a spec as `implemented` (in both the spec file and `INDEX.md`), update public-facing documentation in this order:

### 1. CHANGELOG.md (user-visible changes)

If the change is user-visible, add a bullet to `CHANGELOG.md`'s `Unreleased` section under the appropriate category:

- **Added** — new features, new commands, new tools, new import formats
- **Changed** — behavior changes users will notice (default flag changes, schema changes, breaking changes)
- **Deprecated** — features marked for future removal
- **Removed** — features removed in this version
- **Fixed** — bug fixes (only those that affect user behavior)
- **Security** — security-relevant fixes

Cite PR numbers. Keep entries to one or two sentences each.

**What does NOT need a CHANGELOG entry:**

- Internal refactors with no behavior change (`/code-review` cleanup passes)
- CI tweaks
- Code-style changes (formatting, lint rules)
- Test-only PRs (unless they unblock something)
- ADR additions (the ADR itself is the durable artifact)
- Changes scoped to `private/` docs

When in doubt: if a user reading the changelog would benefit from knowing about it, add an entry. If it's purely internal, skip it.

### 2. docs/roadmap.md (milestone status)

Move the feature row from `📐 designed` (or `🗓️ planned`) to `✅ shipped` in the milestone table. If the milestone itself just closed (e.g., M2A or M3B), update the milestone status. The roadmap is the canonical source of milestone state.

### 3. docs/features.md (capability snapshot)

If the feature adds a user-facing capability (CLI command, MCP tool, import format, etc.), add or update the relevant entry in `docs/features.md`. Link to the per-feature guide if one exists.

### 4. README.md (status callout only)

The README is a storefront — it points to `docs/roadmap.md` rather than carrying the milestone matrix itself. Only update the README if:

- The status callout near the top needs to reflect a milestone closing (e.g., M2A → in flight → shipped).
- The Why-MoneyBin bullets need a small adjustment because a previously-promised feature now exists.

**Do not** re-add an in-README roadmap matrix. **Do not** re-add a detailed "What Works Today" feature table. Those live in `docs/`.

Read the README before editing it, and check its length after: the storefront is 158 lines, and 200 is the ceiling. Growth past that means content that belongs in `docs/` is accumulating in the shopfront window.

See `docs/specs/user-facing-doc-polish.md` for the README structure rationale.

### 5. Per-feature guides

For shipped features that warrant a user-facing how-to, add a guide in `docs/guides/`:

- **New import format**: extend the existing data-import guide; show a CLI example.
- **New CLI command group**: representative commands in the CLI reference or a dedicated guide.
- **New MCP domain**: add to the MCP server guide with a one-line description.
- **Infrastructure** (encryption, migrations): a sentence or two in the relevant existing guide.

The goal is that someone reading the docs gets an accurate picture of what MoneyBin can do *today*, without digging through specs.

### 6. Milestone address reconciliation

`docs/roadmap.md` is the canonical list of milestone addresses. Any other copy of that list — a planning tracker, a working note — is a **mirror**, and mirrors drift silently because no other step in this checklist touches them.

Enumerate the roadmap's addresses and diff them against any mirror you keep:

```bash
grep -oE 'M[0-3][A-Z](\.[0-9]+)?' docs/roadmap.md | sort -u
```

When the two lists diverge, the discriminator is **whether real work backs the address** — not which list it happens to sit in:

- **Work exists** (shipped, in progress, or specced) but the roadmap has no row → the roadmap is wrong. Add it here; the roadmap is canonical.
- **No work backs it** (retired, renamed, or never started) → the mirror is wrong. Drop it there.

Do **not** re-mint a retired address. Before assigning a letter, confirm it is genuinely free — absence from one of the two lists is not evidence that it is.

## When a New Spec Is Written

- Give the spec an **address** — the next free increment letter under its milestone (e.g. `M2F`), per `.claude/references/design-principles-depth.md` → "Milestone addressing." Don't invent a new numbering scheme.
- Add a 📐 entry in the matching milestone row of `docs/roadmap.md`.
- Add the spec to `docs/specs/INDEX.md` with status `draft` or `ready`.
- Run the address diff (step 6 above). A newly minted address is exactly the kind that reaches a mirror late or never.

## When a Feature Is Planned (No Spec Yet)

- Attach it to a milestone/increment per the address scheme (`.claude/references/design-principles-depth.md` → "Milestone addressing") — append the next free increment letter; don't fork a parallel sequence.
- Add a 🗓️ entry under that milestone in `docs/roadmap.md` (or the Post-launch section if genuinely beyond M3).
- No `INDEX.md` entry until a spec exists.
- Run the address diff (step 6 above).

## When a Milestone Closes

When all sub-milestones in a tier close (e.g., M2A + M2B + M2C + M2D + M2E all ship → M2 closes; M3A through M3E all close → launch):

1. Move the `Unreleased` block in `CHANGELOG.md` into a new dated section: `## [M2A] — YYYY-MM-DD`. Reset `Unreleased` to a placeholder like `(no changes since M2A)`.
2. Update `docs/roadmap.md` milestone status row.
3. Update README status callout.
4. Tag the commit (`git tag M2A` or similar) for cross-reference.

## Test Layer Check

Before marking a spec as `implemented`, verify the feature has tests at every applicable layer (see testing.md "Test Coverage by Layer"). Unit tests alone are not sufficient for features that add CLI commands or cross subsystem boundaries.

## Deferral Check

Before marking a spec as `implemented`, file every item its "Out of scope" / "Deferred" / "resolve in M__" text hands to a future milestone. Trackers hold work; specs hold design, and nothing crosses that boundary — a spec marked `implemented` reads as closed, and no planning sweep re-reads spec bodies. An unfiled handoff goes invisible the moment the status flips. File it while you still know why it was deferred.

Two traps when checking whether a handoff is already tracked:

- **Grep the work, not the address.** Search the function, table, column, or a distinctive phrase. The milestone address itself is usually mentioned somewhere already, so grepping it returns a hit and the check passes while the item stays unfiled.
- **Re-read conditional deferrals.** Text like "for v1 this doesn't matter", "when X ships", or "if any merchant exceeds N" states a condition that expires silently. The spec keeps reading as though it still holds.

## Pre-Push Review Pass

After implementation is complete and documentation is updated, run `/code-review medium --fix` **before the final commit and push**. At `medium` effort this surfaces high-confidence findings — correctness bugs alongside reuse, simplification, and efficiency cleanups — and applies the fixes. The goal is to catch copy-paste patterns, redundant state, missing validations, and other issues that accumulate during implementation before they land on `main`.

Keep this pass at `medium`: it stays in the high-confidence band, so auto-applying its fixes unattended is safe. Broader, lower-confidence review (`high`/`max`, or `/code-review ultra` for a deep cloud sweep) belongs **post-push**, where findings are triaged rather than blindly applied.

## Principle

The user-facing surface — README, CHANGELOG, roadmap, features — must stay **honest** (never claim shipped status for designed-only features) and **current** (a shipped feature with no doc trail is invisible to users). The README defers to `docs/` for detail; `docs/` defers to per-feature guides for depth. Each layer's job is to point downstream, not to carry every detail.
