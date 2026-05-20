<!-- Last reviewed: 2026-05-17 -->
# MoneyBin Guides

How-to guides for MoneyBin. These walk you through specific tasks — import data, query SQL, configure profiles, run a headless install. For reference material (schemas, APIs), see [`docs/reference/`](../reference/). For the doc index, see [`docs/README.md`](../README.md).

## First hour

The path most people follow when they first install MoneyBin: get data in, learn the CLI, decide whether to run more than one profile. Allow 30–60 minutes including your first import.

- **[Data Import](data-import.md)** — Bring your first statements in from CSV, OFX/QFX, Excel, Parquet, Feather, or a Plaid-connected bank.
- **[CLI Reference](cli-reference.md)** — The full command tree, with JSON output and scripting notes.
- **[Profiles](profiles.md)** — When to create more than one profile, how to switch between them, what gets isolated.

Migrating from Mint, Monarch, Copilot, or Tiller? Start with [`data-import.md`](data-import.md) for getting history in, then [`categorization.md`](categorization.md#migrating-curated-categories) for carrying curated categories across.

## Day to day

Working with data that's already loaded — categorizing it, understanding the pipeline that produced it, querying it directly.

- **[Categorization](categorization.md)** — Rules, merchant normalization, LLM-assist, source precedence, and bulk operations.
- **[Data Pipeline](data-pipeline.md)** — The raw/staging/core/app/reports layers, which models live where, and where consumers should read from.
- **[Direct SQL Access](sql-access.md)** — Connect to the encrypted DuckDB file from your own scripts and clients; the stable read-only surface.

## Operate and deploy

Running MoneyBin unattended — cron, containers, multiple machines. No standalone deployment guide today; the operator-relevant material is surfaced from the guides below by task.

- **Headless and cron** — [`database-security.md`](database-security.md#headless-and-cron-deployments) covers env-var key injection and unattended unlock.
- **Container deployment** — [`database-security.md`](database-security.md#headless-and-cron-deployments) for the encryption side; [`observability.md`](observability.md#headless-and-container-deployment) for logs, metrics, and health checks.
- **Backup automation** — [`database-security.md`](database-security.md#backup-automation) for `db backup` scheduling and restore verification.
- **Multi-machine workflows** — [`profiles.md`](profiles.md#multi-machine-workflows) for moving a profile between hosts.
- **Network posture** — [`threat-model.md`](threat-model.md#client-egress-profile) for what the client talks to and when.

## AI integration

The MCP server and the chat clients that talk to it.

- **[MCP Server](mcp-server.md)** — Tool catalog, response envelope, sensitivity tiers, and the action-hint chaining model.
- **[MCP Clients](mcp-clients.md)** — Install and configure the chat clients MoneyBin is tested against.

## Security and privacy

What's protected and what's not. Read these before trusting MoneyBin with real data.

- **[Database & Security](database-security.md)** — Encryption model, key lifecycle, backup and restore.
- **[Threat Model](threat-model.md)** — What MoneyBin protects against and what it does not.

## Observability

- **[Observability](observability.md)** — Structured logs, persisted metrics, the `system doctor` integrity sweep, and alerting recipes.

## Testing and tooling

Generators and harnesses for trying MoneyBin without real data, or for writing end-to-end tests.

- **[Synthetic Data](synthetic-data.md)** — Generate realistic personas, merchants, and transactions with ground-truth labels.
- **[Scenario Authoring](scenario-authoring.md)** — Write end-to-end tests that exercise the full pipeline against synthetic or fixture data.

## Not here

- **Design and intent** — [`docs/specs/`](../specs/).
- **License terms** — [`docs/licensing.md`](../licensing.md).
