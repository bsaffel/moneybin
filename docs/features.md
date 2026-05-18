<!-- Last reviewed: 2026-05-17 -->
# What Works Today

What MoneyBin can do today. Each capability links to its guide; the [roadmap](roadmap.md) covers what's planned and the [CHANGELOG](../CHANGELOG.md) carries the dated record.

> Pre-v1. Capabilities below are shipped and exercised end-to-end. Anything in flight is called out under [What's planned](#whats-planned).

## Data ingestion

- **Smart tabular import** — CSV, TSV, Excel, Parquet, and Feather through one pipeline. Heuristic column detection, three-tier confidence model, multi-account support, and first-class migration profiles for Tiller, Mint, YNAB, and Maybe. -> [Data import guide](guides/data-import.md)
- **OFX / QFX / QBO import** — Same `import_log` infrastructure as tabular: re-import detection, `--force` override, institution-name auto-resolution, and batch revert via `moneybin import revert <id>`. OFX descriptions are HTML-entity-decoded at import. -> [Data import guide](guides/data-import.md)
- **W-2 PDF extraction** — Pulls wage and withholding values from W-2 PDFs into the raw layer. (No dedicated guide yet.)
- **Plaid bank sync** — Connect accounts through Plaid Hosted Link via moneybin-server (the Plaid integration backend you can self-host, or use the hosted instance of). Cursor-based incremental sync by default; `--force` for full re-fetch. Plaid data lands alongside OFX and CSV in the same canonical tables. Cash and credit-card accounts flow through the canonical pipeline today; investment, loan, mortgage, and HSA accounts get loaded if Plaid exposes them, but the holdings, cost-basis, and balance-sheet surfaces those account types deserve land with the investments milestone. -> [CLI reference](guides/cli-reference.md)
- **Batch imports** — `moneybin import files PATHS...` (and `import_files` on MCP) ingests multiple files in a single call; per-file failures don't abort the batch. -> [Data import guide](guides/data-import.md)
- **Watched-folder inbox** — Drop files into `~/Documents/MoneyBin/<profile>/inbox/`. `moneybin import inbox` drains successes to `processed/YYYY-MM/` and failures to `failed/YYYY-MM/` with YAML error sidecars. Per-profile lockfile with crash recovery. -> [Data import guide](guides/data-import.md)
- **Manual transaction entry** — Add transactions by hand via `moneybin transactions create` (CLI) or `transactions_create` (MCP), one at a time. For cash, gifts, and anything that doesn't come from a file or sync. Bulk paste / CSV-row append is not yet wired through this path — use a normal CSV import instead. -> [CLI reference](guides/cli-reference.md)

## Storage and security

- **Encrypted DuckDB at rest** — AES-256-GCM by default. Argon2id KDF for passphrase mode; OS keychain for auto-key mode. One encrypted DuckDB file per profile under `~/.moneybin/profiles/<name>/`. -> [Database and security guide](guides/database-security.md)
- **Threat model** — What encryption protects against, and what it doesn't (forgotten passphrase, malware on your machine, AI-vendor data flow). -> [Threat model](guides/threat-model.md)
- **Key management and lifecycle** — `moneybin db init / lock / unlock / rotate-key / backup / restore / key show`. Encryption CLI is symmetric with the rest of the surface. -> [Database and security guide](guides/database-security.md)
- **Backup and restore** — `moneybin db backup` produces a portable encrypted snapshot; `db restore` recovers it. Snapshots are point-in-time of when the command ran; automated schedules are not yet built — use cron or your platform's scheduler. -> [Database and security guide](guides/database-security.md)
- **Schema migrations** — Auto-upgrade on first invocation; details are operator-level and live in the [Database and security guide](guides/database-security.md). Capacity: supports years of multi-account history on a single laptop (DuckDB columnar storage).
- **Multi-profile isolation** — Per-profile DB, config, and logs. `moneybin profile create / list / switch / delete / show / set`. -> [Profiles guide](guides/profiles.md)

## Transformations and refresh

- **Layered SQLMesh pipeline** — `raw` → `prep` (staging) → `core` (canonical facts / dimensions / bridges). Plus `app.*` for user-managed state and `reports.*` for curated views. Consumers (CLI, MCP, SQL clients) read from `core.*` and `reports.*`; `prep` is internal. -> [Data pipeline guide](guides/data-pipeline.md)
- **Cross-source dedup** — SHA-256 content hashes with golden-record merge across CSV, OFX, and Plaid. Config-driven source priority. -> [Data pipeline guide](guides/data-pipeline.md)
- **Transfer detection** — Cross-account matching with a four-signal scoring engine (date distance, keyword, roundness, pair frequency); produces `core.bridge_transfers` and `is_transfer` / `transfer_pair_id` on `fct_transactions`. -> [Data pipeline guide](guides/data-pipeline.md)
- **Refresh umbrella** — `moneybin refresh` (CLI) and `refresh_run` (MCP) are the single entry point for matching → SQLMesh apply → categorization. Pass `--step` (CLI) or `steps=[...]` (MCP) to scope sub-operations. `sync pull` and `import files` invoke refresh automatically unless `--no-refresh`. -> [Data pipeline guide](guides/data-pipeline.md)
- **Reliable under load** — Timeouts, write coordination, and schema-drift recovery are handled automatically; see [architecture](architecture.md) if you want the mechanics.

## Categorization

- **Rule-based engine** — Exact / contains / regex / `oneOf` exemplars. Your manual categorizations are immune to subsequent auto-categorization (source precedence enforced on write: user beats rule, rule beats LLM-assist, and so on). -> [Categorization guide](guides/categorization.md)
- **Smart matcher** — Matches against description plus memo text, and uses structural signals (check number, transfer flag, payment channel, amount sign), so PayPal / Venmo / Zelle / generic-ACH wrappers categorize on the merchant identity that lives in memo. -> [Categorization guide](guides/categorization.md)
- **Auto-rule learning** — User edits propose rules; review and promote them through a queue. -> [Categorization guide](guides/categorization.md)
- **LLM-assist (opt-in)** — Propose → review → commit workflow. The redactor strips amounts, dates, and account identifiers before any prompt leaves the machine; structural fields are exposed as signals. Auto-created merchants accumulate `oneOf` exemplars instead of inventing over-general patterns. CLI: `moneybin transactions categorize assist`. -> [Categorization guide](guides/categorization.md)
- **Auto-apply on commit** — Newly created rules and merchants apply across the rest of the dataset automatically, so the LLM is meaningfully less involved by the third or fourth import. -> [Categorization guide](guides/categorization.md)
- **Merchant catalog** — User- and system-created (no seeded catalog). Plaid pass-through, migration imports, LLM-assist, and the auto-apply pass all populate it. -> [Categorization guide](guides/categorization.md)
- **Bulk operations** — CLI, MCP, and service-layer parity for batch categorize, rule create / delete, merchant operations. -> [Categorization guide](guides/categorization.md)

## Curation (transaction-level user state)

- **Notes** — Free-text notes on transactions.
- **Tags** — Multi-tag table with rename semantics.
- **Splits via annotation** — Annotation-based splits today; first-class split rows planned (see [roadmap](roadmap.md)).
- **Import-batch labels** — Group imported rows under a human label.
- **Edit-history audit log** — Per-row history of curation edits.

All on the `app.*` layer; zero changes to the upstream pipeline. (No dedicated guide yet — see [CLI reference](guides/cli-reference.md) and [MCP server guide](guides/mcp-server.md).)

## Accounts and balances

- **Account management** — `moneybin accounts list / get / resolve / set` with Plaid-parity metadata (subtype, holder category, currency, credit limit, last four). One unified `set` covers display name, include-in-net-worth, and archive state. Reversible account merging via bridge model. -> [CLI reference](guides/cli-reference.md)
- **Net-worth and balance tracking** — Per-account balance show / history / assert / reconcile and cross-account `moneybin reports networth / networth-history` with period-over-period change. Daily carry-forward of authoritative observations from OFX, tabular running balances, and user assertions. -> [CLI reference](guides/cli-reference.md)

## Reports

Curated `reports.*` SQLMesh views back both the CLI and MCP surfaces. Same query, same envelope on both. Reports accept date-range filters (`--from` / `--to` on time-windowed reports like `cashflow` and `spending`, `--as-of` for snapshots like `networth`, plus `--account` and `--category` where they apply); grains vary per report. -> [CLI reference](guides/cli-reference.md) · [MCP server guide](guides/mcp-server.md)

- **`reports.net_worth`** — Cross-account total with period-over-period change.
- **`reports.cash_flow`** — Income vs spending by month.
- **`reports.spending_trend`** — Category spending over time.
- **`reports.recurring_subscriptions`** — Recurring transactions with confidence scores, cadence, and annualized cost. (No "mark as cancelled" workflow yet — see [roadmap](roadmap.md).)
- **`reports.merchant_activity`** — Per-merchant spend rollup.
- **`reports.uncategorized_queue`** — What still needs categorizing.
- **`reports.large_transactions`** — Outlier filter for human review.
- **`reports.balance_drift`** — Drift between asserted and computed balances.

## MCP server

- **Wide tool catalog** — Around 30+ first-party tools across `accounts.*`, `transactions.*`, `reports.*`, `categorize.*`, `merchants.*`, `system.*`, `refresh`, `sync.*`, `transform.*`, and `import.*`. Full per-domain inventory: [MCP server guide](guides/mcp-server.md).
- **Transport** — stdio today. Streamable HTTP transport ships with the web UI milestone (see [roadmap](roadmap.md)).
- **Auth and session model** — Each MCP session inherits the profile unlocked by `moneybin db unlock`. Locking the profile (`moneybin db lock`) detaches all active sessions.
- **Concurrency** — Reads coexist freely; writes are serialized per profile (single-writer rule). Two agents can read concurrently; only one can mutate at a time.
- **Response envelope** — `{status, data, error, audit_id}` on every tool. Money fields are JSON numbers (not strings). Validation errors land as `invalid_arguments` envelopes with a hint listing accepted parameters. -> [MCP server guide](guides/mcp-server.md)
- **Tool annotations** — Protocol-standard `readOnlyHint` / `destructiveHint` / `idempotentHint` / `openWorldHint` so clients can render confirmation UI for destructive operations.
- **Sensitivity tiers** — Every tool is tagged `low` / `medium` / `high`. Today the tier drives logging and audit metadata; full consent enforcement (gating `high`-tier invocations on explicit OK) lands with the privacy-framework work. See [architecture](architecture.md) for the tier-by-domain breakdown.
- **Action hints** — Successful responses include an `actions[]` array suggesting next-step tool calls (e.g., after a successful import, an action hint points at `refresh_run`), so agents can chain without prompt-side instructions for common flows. -> [MCP server guide](guides/mcp-server.md)
- **Curated schema resource** — `moneybin://schema` MCP resource (and `sql_schema` tool mirror) exposes core + select app interface tables with column comments and example queries. -> [Data model reference](reference/data-model.md)
- **Read-only SQL tool** — `sql_query` runs arbitrary `SELECT` against `core.*` and `reports.*`. Writes to `core.*` are blocked at the middleware layer; DDL is rejected. App-state mutations (notes, tags, splits, rules) flow through dedicated tools, not raw SQL.
- **MCP install across nine clients** — Claude Desktop, Claude Code, Cursor, Windsurf, VS Code, Gemini CLI, Codex (CLI / Desktop / IDE), ChatGPT Desktop. `moneybin mcp install --client <name>` writes the client config. -> [MCP clients guide](guides/mcp-clients.md)
- **Stability promise** — Pre-v1. Tool names and envelope fields may change before the v1 launch; the CHANGELOG records every rename, and removed tools stay as deprecation-aliased shims for one minor release.

## CLI

- **Typer v2 taxonomy** — Path-prefix-verb-suffix naming; entity groups (`accounts`, `transactions`), reference-data groups (`categories`, `merchants`), `reports` for cross-domain rollups, `system` for orientation. -> [CLI reference](guides/cli-reference.md)
- **`--output json` parity with MCP** — Every read command exposes `--output json` and returns the same `{status, data, error, audit_id}` envelope as the corresponding MCP tool, redacted by the same middleware. Agents driving the shell are first-class. -> [CLI reference](guides/cli-reference.md)
- **Structured error envelopes** — Runtime errors emit a machine-readable envelope to stdout when `--output json` is active.
- **Field projection** — `--json-fields` on `moneybin transactions list` selects a subset of fields; other read-only commands will adopt progressively.
- **Shell completion** — `moneybin --install-completion` / `--show-completion`.

## SQL access

- **Read-only SQL** — Connect any DuckDB client to the encrypted profile file. `moneybin db shell` opens an interactive shell; DuckDB UI works on the same file. -> [SQL access guide](guides/sql-access.md)
- **Layered schemas** — Consumers read from `core.*` and `reports.*`. Full schema reference: [Data model reference](reference/data-model.md) · [Architecture](architecture.md).

## Observability

- **Structured logs** — `moneybin logs clean / path / tail`. PII and financial detail are stripped at the formatter layer; see [Threat model](guides/threat-model.md). -> [Observability guide](guides/observability.md)
- **Prometheus-style metrics** — Per-operation counters and durations, persisted to DuckDB. `moneybin stats`. -> [Observability guide](guides/observability.md)
- **`moneybin system doctor`** — Read-only pipeline integrity check (FK integrity, sign convention, transfer balance, staging coverage, categorization coverage). Exits 0 on pass / warn, 1 on fail. `--verbose` for affected IDs, `--output json` for agents. Registered as the `system_doctor` MCP tool. -> [CLI reference](guides/cli-reference.md)

## What's planned

These are visible gaps a migrant or agent author will notice. See [Roadmap](roadmap.md) for the full milestone view.

- **Plaintext export** — `moneybin export` (CSV / Excel / Sheets) for data exit. Planned, not shipped.
- **Budgeting** — Envelopes, rollovers, period-over-period burn. Planned.
- **Investment tracking** — Holdings, FIFO lots, cost basis, 1099-B reconciliation. Planned.
- **Multi-currency** — FX gain/loss and non-USD accounts. Planned.
- **Web UI dashboard** — Local web UI plus Streamable HTTP MCP transport (so remote clients like ChatGPT web can reach MoneyBin). Planned.
- **Hosted tier** — Same code, hosted. Planned.
- **First-class split rows** — Today splits are annotations on the parent row; first-class split lines arrive with the curation polish work. Planned.
- **Subscription-cancellation workflow** — `reports.recurring_subscriptions` surfaces the candidates; a "mark cancelled / paused" tracking surface is planned.
- **Tax-year reporting** — Schedule-D-adjacent outputs and per-tax-year summaries. Not on the near-term roadmap.
- **Native mobile apps** — Not on the roadmap.
- **Household / shared budgets** — Multi-user accounts within one profile. Not on the roadmap.

Post-launch candidates (native PDF parsing beyond W-2, ML-powered categorization, MCP Apps, mobile read-only viewer, expanded privacy tiers) live on the same page.
