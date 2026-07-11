<!-- Last reviewed: 2026-05-17 -->
# MoneyBin Documentation

Documentation for MoneyBin. The pages below are organized by what you're trying to do.

## Start here

Three recommended paths. Pick the row that matches you.

| You are... | Read in this order |
|---|---|
| **First-time visitor, deciding whether MoneyBin fits** | [`audience.md`](audience.md) → [`features.md`](features.md) → [top-level README Quick Start](../README.md#quick-start) |
| **Power user setting up and running it daily** | [`guides/data-import.md`](guides/data-import.md) → [`guides/cli-reference.md`](guides/cli-reference.md) → [`guides/categorization.md`](guides/categorization.md) |
| **Building agents on top of MoneyBin** | [`guides/mcp-server.md`](guides/mcp-server.md) (envelope, tool catalog, sensitivity tiers) → [`reference/prompts/`](reference/prompts/) (example prompts and workflows) → [`guides/mcp-clients.md`](guides/mcp-clients.md) (per-client install) |

Use your browser's Ctrl/Cmd-F to search this index; every doc carries a `<!-- Last reviewed: YYYY-MM-DD -->` header at the top — if the date looks old relative to recent CHANGELOG entries, the doc may lag the code.

## By what you're trying to do

### Decide whether MoneyBin is for you

- **[Audience](audience.md)** — Personas MoneyBin already serves well, personas it's being built toward, and personas it isn't for.
- **[Comparison](comparison.md)** — How MoneyBin stacks up against Tiller, Lunch Money, Copilot, Monarch, Beancount, and other personal-finance tools.
- **[Features](features.md)** — Capability snapshot of what works today, surface by surface.
- **[Roadmap](roadmap.md)** — What's shipped, what's in flight, what's planned.

### Install and first run

- **[Top-level README quick start](../README.md#quick-start)** — Clone, `make setup`, first import.
- **[Data import](guides/data-import.md)** — All supported file formats (OFX/QFX/QBO, CSV, TSV, Excel, Parquet, Feather) and the inbox workflow. Covers migration from Tiller, Monarch, Copilot exports.
- **[Profiles](guides/profiles.md)** — Isolation boundaries, profile lifecycle, per-profile config.

### Daily use

- **[CLI reference](guides/cli-reference.md)** — Complete command tree, every option, every JSON envelope.
- **[Categorization](guides/categorization.md)** — Rule engine, merchant normalization, bulk edits, auto-rule learning.
- **[Data pipeline](guides/data-pipeline.md)** — How files become canonical tables; the raw/staging/core layers and SQLMesh transforms.
- **[Data model](reference/data-model.md)** — Every table you can read from — `core.*`, `reports.*`, `app.*`, `meta.*`, `seeds.*` — with grain, key columns, and meaning.

### AI and agent workflows

For builders. The first two links are the developer surface; the third is for end-user client setup.

- **[MCP server](guides/mcp-server.md)** — The builder doc. Tool catalog, response envelope, sensitivity tiers, latency and cost guidance for planning tool budgets.
- **[Example prompts and workflows](reference/prompts/)** — Ready-to-use prompts (monthly review, anomaly detection, tax prep, transaction search) you can adapt for your own agent.
- **[MCP clients](guides/mcp-clients.md)** — Per-client setup for Claude Desktop, Claude Code, Cursor, Windsurf, VS Code Copilot, Gemini CLI, Codex, and the ChatGPT desktop app (plus why ChatGPT on the web can't connect yet).
- **Extending the server** — See [CONTRIBUTING § Adding a new MCP tool](../CONTRIBUTING.md#adding-a-new-mcp-tool) for the recipe (service, decorator, CLI peer, tests).

### Security and operations

- **[Database security](guides/database-security.md)** — AES-256-GCM encryption, Argon2id key derivation, key management, schema migrations.
- **[Threat model](guides/threat-model.md)** — What MoneyBin defends against, what it doesn't, where the trust boundaries sit.
- **[Observability](guides/observability.md)** — Structured logging, the metrics registry, redaction.

### Query your data directly

- **[Direct SQL access](guides/sql-access.md)** — Open the encrypted DuckDB file from the DuckDB CLI, a UI, or your own scripts.
- **[Data model](reference/data-model.md)** — Schema reference (also linked under Daily use); use this when writing queries.
- **[Data sources](reference/data-sources.md)** — Every supported import format and integration, what it preserves, where it lands.
- **[Account matching](reference/account-matching.md)** — How records from different sources resolve to one canonical account: the identity signals used, where each comes from per format, and when MoneyBin asks you to confirm.

### Architecture orientation

- **[System overview](reference/system-overview.md)** — Component map: what each major piece does, what runs when. Start here for "what are the pieces."
- **[Architecture](architecture.md)** — The shared primitives — `Database`, `TableRef`, `SecretStore`, response envelopes, the medallion layers — and the invariants they enforce. Start here for "why does it work this way."

### Testing and contributing

- **[Synthetic data](guides/synthetic-data.md)** — Generated personas, merchants, and ground truth for end-to-end testing.
- **[Scenario authoring](guides/scenario-authoring.md)** — How to write YAML scenarios that drive integration tests.
- **[CONTRIBUTING](../CONTRIBUTING.md)** — How to file issues, propose changes, and run the dev loop.

## Internals

Deep mechanics — not required reading, but useful when you're debugging or extending.

- **[Account identifiers and PII handling](architecture/account-identifiers.md)** — The identifiers MoneyBin uses for accounts and where PII is masked.
- **[Server API contract](reference/server-api-contract.md)** — The HTTP surface the client expects from `moneybin-sync` (Plaid broker).
- **[Auto-rule pipeline](tech/auto-rule-pipeline.md)** — How edits become proposed rules, how proposals get promoted, how rollback works.
- **[CLI startup flow](tech/cli-startup-flow.md)** — What happens between `moneybin <cmd>` and your code running.

## Browse by directory

`guides/` (how-tos) · `reference/` (lookup material, including [`reference/prompts/`](reference/prompts/)) · `architecture/` (focused architectural notes) · `tech/` (internal-mechanics deep-dives) · `specs/` (feature specs, indexed by [`specs/INDEX.md`](specs/INDEX.md)) · `decisions/` (ADRs) · `assets/` (images).

## What changed when

History of user-visible changes: [`../CHANGELOG.md`](../CHANGELOG.md).

---

[Licensing](licensing.md) — AGPL-3.0, what it means for self-hosters and the hosted tier, walk-away guarantees on your data.
