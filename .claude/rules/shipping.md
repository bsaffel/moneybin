# Shipping & Public Documentation

## When a Feature Ships

After marking a spec as `implemented` (in both the spec file and `INDEX.md`), update public-facing documentation:

### README.md Updates

1. **Roadmap table**: Change the feature's icon from 📐 or 🗓️ to ✅.
2. **"What Works Today" section**: Add or expand content describing the shipped feature. For user-facing surfaces (MCP tools, CLI commands, import formats), include enough detail that a reader understands what they can do — command examples, supported formats, tool domains, etc. Keep it scannable.
3. **If the feature is large** (e.g., full MCP tool catalog, complete CLI reference), create a sub-page under `docs/` and link from the README rather than inlining everything.

### What "enough detail" means

- **New import format**: add to the import table, show a one-liner CLI example.
- **New CLI command group**: add representative commands to the CLI section or link to a dedicated reference page.
- **New MCP domain**: add to the MCP tools table with a one-line description.
- **Infrastructure** (encryption, migrations): a sentence or two in the relevant section is fine.

The goal is that someone reading the README gets an accurate picture of what MoneyBin can do *today*, without needing to dig through specs.

## When a New Spec Is Written

- Add a 📐 entry to the relevant roadmap table in `README.md`.

## When a Feature Is Planned (No Spec Yet)

- Add a 🗓️ entry to the relevant roadmap table in `README.md`.

### Test Layer Check

Before marking a spec as `implemented`, verify the feature has tests at every applicable layer (see testing.md "Test Coverage by Layer"). Unit tests alone are not sufficient for features that add CLI commands or cross subsystem boundaries.

## Pre-Push Quality Pass

After implementation is complete and documentation is updated, run `/simplify` **before the final commit and push**. This reviews the changed code for reuse opportunities, quality issues, and efficiency problems — then fixes what it finds. The goal is to catch copy-paste patterns, redundant state, missing validations, and other issues that accumulate during implementation before they land on `main`.

## Principle

The README is the project's storefront. It must stay honest — never claim shipped status for designed-only features — but it should also stay *current*. A shipped feature that isn't in the README is invisible to users.
