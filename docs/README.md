<!-- Last reviewed: 2026-09-02 -->
# MoneyBin Documentation

Every page, ordered by what you are trying to do. Guides are how-tos, reference pages are lookup material, [`specs/`](specs/) holds design records, and [`decisions/`](decisions/) holds ADRs.

## Start here

Three recommended paths. Pick the row that matches you.

| You are... | Read in this order |
|---|---|
| **First-time visitor, deciding whether MoneyBin fits** | [`audience.md`](audience.md) → [`features.md`](features.md) → [top-level README demo](../README.md#sixty-seconds-on-synthetic-data) |
| **Power user setting up and running it daily** | [`guides/data-import.md`](guides/data-import.md) → [`guides/cli-reference.md`](guides/cli-reference.md) → [`guides/categorization.md`](guides/categorization.md) |
| **Building agents on top of MoneyBin** | [`guides/mcp-server.md`](guides/mcp-server.md) (tool contract and sensitivity tiers) → [`guides/mcp-clients.md`](guides/mcp-clients.md) (per-client install) |

Every page carries a `<!-- Last reviewed: YYYY-MM-DD -->` header at the top. If that date is older than the most recent [CHANGELOG](../CHANGELOG.md) entries, the page may lag the code.

## Decide whether MoneyBin fits

- **[Audience](audience.md)** — Personas MoneyBin already serves well, personas it's being built toward, and personas it isn't for.
- **[Comparison](comparison.md)** — The lane MoneyBin is built for, and the cases where another tool is the better answer.
- **[Features](features.md)** — Capability snapshot of what works today, surface by surface.
- **[Roadmap](roadmap.md)** — What's shipped, what's in flight, what's planned.
- **[Top-level README demo](../README.md#sixty-seconds-on-synthetic-data)** — Clone, `make setup`, and evaluate against synthetic data before connecting anything real.

## First hour

Getting data in and learning the command surface. Allow 30–60 minutes including your first import.

- **[Data import](guides/data-import.md)** — Every supported file format (OFX/QFX/QBO, PDF, CSV, TSV, Excel, Parquet, Feather), the watched-folder inbox, and migration paths from Tiller, Mint, YNAB, Monarch, and Copilot exports.
- **[Google Sheets](guides/connect-gsheet.md)** — Connect a workbook as a live tabular source, and publish exports back to one.
- **[CLI reference](guides/cli-reference.md)** — The full command tree, every option, every JSON envelope.
- **[Profiles](guides/profiles.md)** — Isolation boundaries, profile lifecycle, per-profile config, and moving a profile between machines.

## Day to day

Working with data that is already loaded — categorizing it, understanding the pipeline that produced it, querying it directly.

- **[Categorization](guides/categorization.md)** — Rule engine, merchant normalization, bulk edits, auto-rule learning, source precedence.
- **[Data pipeline](guides/data-pipeline.md)** — How files become canonical tables: the raw / prep / core / app / reports layers, and where consumers should read from.
- **[Direct SQL access](guides/sql-access.md)** — Open the encrypted DuckDB file from the DuckDB CLI, a UI, or your own scripts.
- **[Data model](reference/data-model.md)** — Every table you can read — `core.*`, `reports.*`, `app.*`, `meta.*`, `seeds.*` — with grain, key columns, and meaning.
- **[Data sources](reference/data-sources.md)** — Every supported import format and integration, what it preserves, where it lands.
- **[Account matching](reference/account-matching.md)** — How records from different sources resolve to one canonical account: the identity signals used, where each comes from per format, and when MoneyBin asks you to confirm.

## Operate and deploy

Running MoneyBin unattended — cron, containers, more than one machine.

- **[Database and security](guides/database-security.md)** — AES-256-GCM encryption, Argon2id key derivation, key lifecycle, backup automation and restore verification, schema migrations, and env-var key injection for headless and cron runs.
- **[Server API contract](reference/server-api-contract.md)** — The HTTP surface the client expects from `moneybin-sync`, the Plaid broker you self-host.

## AI integration

- **[Setting up Claude Desktop](guides/setting-up-claude-desktop.md)** — The end-user happy path: install, one `moneybin mcp install` command, restart, ask a first question.
- **[MCP server](guides/mcp-server.md)** — The builder doc: tool catalog, response envelope, sensitivity tiers, action-hint chaining, latency and cost guidance for planning tool budgets, and the [seven registered prompts](guides/mcp-server.md#prompts).
- **[MCP clients](guides/mcp-clients.md)** — Per-client setup for the eight clients MoneyBin is tested against, plus why ChatGPT on the web cannot connect yet.
- **Extending the server** — [CONTRIBUTING § Adding a new MCP tool](../CONTRIBUTING.md#adding-a-new-mcp-tool) carries the recipe: service, decorator, CLI peer, tests.

## Security and privacy

Read these before pointing MoneyBin at real data.

- **[Threat model](guides/threat-model.md)** — What MoneyBin defends against, what it doesn't, where the trust boundaries sit, and what the client talks to over the network.
- **[What the AI provider sees](guides/what-the-ai-sees.md)** — What a connected AI client can receive and send to its model provider: what's masked, what isn't, what's recorded, and how to run a fully local model.
- **[Account identifiers](reference/account-identifiers.md)** — The identifiers MoneyBin uses for accounts, and where PII is masked.

## Observability

- **[Observability](guides/observability.md)** — Structured logging, the persisted metrics registry, redaction, the `moneybin system doctor` integrity sweep, alerting recipes, and container health checks.

## Testing and tooling

Generators and harnesses for trying MoneyBin without real data, or for writing end-to-end tests.

- **[Synthetic data](guides/synthetic-data.md)** — Generate personas, merchants, and transactions with ground-truth labels.
- **[Scenario authoring](guides/scenario-authoring.md)** — Write YAML scenarios that drive the whole pipeline against synthetic or fixture data.

## Internals

Deep mechanics — not required reading, but useful when you're debugging or extending.

- **[System overview](reference/system-overview.md)** — Component map: what each major piece does, what runs when. Start here for "what are the pieces."
- **[Architecture](architecture.md)** — The shared primitives — `Database`, `TableRef`, `SecretStore`, response envelopes, the medallion layers — and the invariants they enforce. Start here for "why does it work this way."
- **[Auto-rule pipeline](reference/auto-rule-pipeline.md)** — How edits become proposed rules, how proposals get promoted, how rollback works.
- **[CLI startup flow](reference/cli-startup-flow.md)** — What happens between `moneybin <cmd>` and your code running.

## Project and process

- **[Glossary](../CONTEXT.md)** — The canonical word for each concept, and the resolution for the overloaded ones.
- **[Contributing](../CONTRIBUTING.md)** — How to file issues, propose changes, and run the dev loop.
- **[Changelog](../CHANGELOG.md)** — The dated record of user-visible changes.
- **[Licensing](licensing.md)** — AGPL-3.0, what it means for self-hosters and the hosted tier, walk-away guarantees on your data.
- **[Spec index](specs/INDEX.md)** — Every feature spec and its status.

## Browse by directory

`guides/` (how-tos) · `reference/` (lookup material) · `specs/` (feature specs) · `decisions/` (ADRs) · `assets/` (images).
