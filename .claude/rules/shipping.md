# Shipping & Public Documentation

## When a Feature Ships

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

- Internal refactors with no behavior change (`/simplify` passes)
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

See `docs/specs/user-facing-doc-polish.md` for the README structure rationale.

### 5. Per-feature guides

For shipped features that warrant a user-facing how-to, add a guide in `docs/guides/`:

- **New import format**: extend the existing data-import guide; show a CLI example.
- **New CLI command group**: representative commands in the CLI reference or a dedicated guide.
- **New MCP domain**: add to the MCP server guide with a one-line description.
- **Infrastructure** (encryption, migrations): a sentence or two in the relevant existing guide.

The goal is that someone reading the docs gets an accurate picture of what MoneyBin can do *today*, without digging through specs.

## When a New Spec Is Written

- Add a 📐 entry in the appropriate row of `docs/roadmap.md` (matched to the milestone the spec is gated on).
- Add the spec to `docs/specs/INDEX.md` with status `draft` or `ready`.

## When a Feature Is Planned (No Spec Yet)

- Add a 🗓️ entry to `docs/roadmap.md` in the post-launch section.
- No `INDEX.md` entry until a spec exists.

## When a Milestone Closes

When all sub-milestones in a tier close (e.g., M2A + M2B + M2C all ship → M2 closes; M3A through M3E all close → launch):

1. Move the `Unreleased` block in `CHANGELOG.md` into a new dated section: `## [M2A] — YYYY-MM-DD`. Reset `Unreleased` to a placeholder like `(no changes since M2A)`.
2. Update `docs/roadmap.md` milestone status row.
3. Update README status callout.
4. Tag the commit (`git tag M2A` or similar) for cross-reference.

## Test Layer Check

Before marking a spec as `implemented`, verify the feature has tests at every applicable layer (see testing.md "Test Coverage by Layer"). Unit tests alone are not sufficient for features that add CLI commands or cross subsystem boundaries.

## Pre-Push Quality Pass

After implementation is complete and documentation is updated, run `/simplify` **before the final commit and push**. This reviews the changed code for reuse opportunities, quality issues, and efficiency problems — then fixes what it finds. The goal is to catch copy-paste patterns, redundant state, missing validations, and other issues that accumulate during implementation before they land on `main`.

## Principle

The user-facing surface — README, CHANGELOG, roadmap, features — must stay **honest** (never claim shipped status for designed-only features) and **current** (a shipped feature with no doc trail is invisible to users). The README defers to `docs/` for detail; `docs/` defers to per-feature guides for depth. Each layer's job is to point downstream, not to carry every detail.
