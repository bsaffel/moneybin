<!-- Last reviewed: 2026-05-24 -->
# What Works Today

What MoneyBin can do today. Each capability links to its guide; the [roadmap](roadmap.md) covers what's planned and the [CHANGELOG](../CHANGELOG.md) carries the dated record.

> Pre-v1. Capabilities below are shipped and exercised end-to-end. Anything in flight is called out under [What's planned](#whats-planned).

## Data ingestion

- **Smart tabular import** — CSV, TSV, Excel, Parquet, and Feather through one pipeline. Heuristic column detection, three-tier confidence model, multi-account support, and first-class migration profiles for Tiller, Mint, and YNAB (other tools' exports import via the generic detector). -> [Data import guide](guides/data-import.md)
- **OFX / QFX / QBO import** — Same `import_log` infrastructure as tabular: re-import detection, `--force` override, institution-name auto-resolution, and batch revert via `moneybin import revert <id>`. OFX descriptions are HTML-entity-decoded at import. -> [Data import guide](guides/data-import.md)
- **Plaid bank sync** — Connect accounts through Plaid Hosted Link via moneybin-server (the Plaid integration backend you can self-host, or use the hosted instance of). Cursor-based incremental sync by default; `--force` for full re-fetch. Plaid data lands alongside OFX and CSV in the same canonical tables. Cash and credit-card accounts flow through the canonical pipeline today; investment, loan, mortgage, and HSA accounts get loaded if Plaid exposes them, but the holdings, cost-basis, and balance-sheet surfaces those account types deserve land with the investments milestone. -> [CLI reference](guides/cli-reference.md)
- **Google Sheets sync** — Connect a Google Sheet as a live tabular source via direct OAuth (no shared client secret). Two adapters: `transactions` (Tiller-style ledgers participate in the full matching and categorization pipeline) and `seed` (any other sheet lands in `raw.gsheet_seeds` as JSON plus an auto-generated typed view, queryable via SQL and MCP). Every `moneybin refresh` re-pulls the latest sheet state; soft-delete preserves audit history; per-connection drift detection refuses pulls on structural change until you reconnect. -> [Google Sheets guide](guides/connect-gsheet.md)
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
- **Cross-source dedup** — SHA-256 content hashes with golden-record merge across CSV, OFX, and Plaid. Config-driven source priority. Three or more copies of the same transaction collapse to one record even when duplicates span sources *and* overlapping files (N-way collapse via a union-find spanning forest). -> [Data pipeline guide](guides/data-pipeline.md)
- **Transfer detection** — Cross-account matching with a two-signal scoring engine (date distance, keyword); produces `core.bridge_transfers` and `is_transfer` / `transfer_pair_id` on `fct_transactions`. -> [Data pipeline guide](guides/data-pipeline.md)
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
- **Edit-history audit log** — Per-row history of every curation edit.
- **Reversible edits** — Every protected `app.*` mutation (notes, tags, splits, categories, rules, account settings) is audit-paired and undoable as a unit keyed on `operation_id`. `moneybin system audit undo|history|get` (and `system_audit_undo` / `system_audit_history` / `system_audit_get` on MCP) reverse a change from its full before/after image; the undo is itself audited and undoable. Undo refuses (rather than silently cascading) when a later operation touched the same rows. -> [CLI reference](guides/cli-reference.md)

All on the `app.*` layer; zero changes to the upstream pipeline. (No dedicated guide yet — see [CLI reference](guides/cli-reference.md) and [MCP server guide](guides/mcp-server.md).)

## Accounts and balances

- **Account management** — `moneybin accounts list / get / resolve / set` with Plaid-parity metadata (subtype, holder category, currency, credit limit, last four). One unified `set` covers display name, include-in-net-worth, and archive state. Reversible account merging via bridge model. -> [CLI reference](guides/cli-reference.md)
- **Net-worth and balance tracking** — Per-account balance show / history / assert / reconcile and cross-account `moneybin reports networth / networth-history` with period-over-period change. Daily carry-forward of authoritative observations from OFX, tabular running balances, and user assertions. -> [CLI reference](guides/cli-reference.md)

## Reports

Curated `reports.*` SQLMesh views back both the CLI and MCP surfaces. Same query, same envelope on both. Reports accept date-range filters (`--from-month` / `--to-month` on time-windowed reports like `cashflow` and `spending`, `--as-of` for snapshots like `networth`, plus `--account` and `--category` where they apply); grains vary per report. -> [CLI reference](guides/cli-reference.md) · [MCP server guide](guides/mcp-server.md)

Each report is backed by a curated view and exposed identically on the CLI and MCP. A declarative **report framework** — one `@report` runner per report, from which the CLI command, MCP tool, parameter flags, and column masking are all derived — is in flight; it's what will let analysis packages and agents add new reports onto both surfaces from a single definition. See [Extensibility](#extensibility).

- **`reports.net_worth`** — Cross-account total with period-over-period change.
- **`reports.cash_flow`** — Income vs spending by month.
- **`reports.spending_trend`** — Category spending over time.
- **`reports.recurring_subscriptions`** — Recurring transactions with confidence scores, cadence, and annualized cost. (No "mark as cancelled" workflow yet — see [roadmap](roadmap.md).)
- **`reports.merchant_activity`** — Per-merchant spend rollup.
- **`reports.uncategorized_queue`** — What still needs categorizing.
- **`reports.large_transactions`** — Outlier filter for human review.
- **`reports.balance_drift`** — Drift between asserted and computed balances.

## MCP server

- **Wide tool catalog** — Around seventy first-party tools across `accounts.*`, `transactions.*`, `reports.*`, `categorize.*`, `merchants.*`, `system.*`, `refresh`, `sync.*`, `transform.*`, and `import.*`. Full per-domain inventory: [MCP server guide](guides/mcp-server.md).
- **Transport** — stdio today. Streamable HTTP transport ships with the web UI milestone (see [roadmap](roadmap.md)).
- **Auth and session model** — Each MCP session inherits the profile unlocked by `moneybin db unlock`. Locking the profile (`moneybin db lock`) detaches all active sessions.
- **Concurrency** — Reads coexist freely; writes are serialized per profile (single-writer rule). Two agents can read concurrently; only one can mutate at a time.
- **Response envelope** — `{status, summary, data, actions, error?, next_cursor?}` on every tool. `summary` carries counts, sensitivity tier, and display currency; `actions` carries next-step hints (see below); `next_cursor` is the opaque pagination token. Money fields are JSON numbers (not strings). Validation errors land as `invalid_arguments` envelopes with a hint listing accepted parameters. -> [MCP server guide](guides/mcp-server.md)
- **Tool annotations** — Protocol-standard `readOnlyHint` / `destructiveHint` / `idempotentHint` / `openWorldHint` so clients can render confirmation UI for destructive operations.
- **Sensitivity tiers** — Every tool is tagged `low` / `medium` / `high`. Today the tier drives logging and audit metadata; full consent enforcement (gating `high`-tier invocations on explicit OK) lands with the privacy-framework work. See [architecture](architecture.md) for the tier-by-domain breakdown.
- **Action hints** — Successful responses include an `actions[]` array suggesting next-step tool calls (e.g., after a successful import, an action hint points at `refresh_run`), so agents can chain without prompt-side instructions for common flows. -> [MCP server guide](guides/mcp-server.md)
- **Curated schema resource** — `moneybin://schema` MCP resource (and `sql_schema` tool mirror) exposes core + select app interface tables with column comments and example queries. -> [Data model reference](reference/data-model.md)
- **Read-only SQL — privacy-safe on both surfaces** — `sql_query` (MCP) and `moneybin sql query` (CLI) run read-only `SELECT`/`WITH`/`DESCRIBE`/`SHOW`/`PRAGMA`/`EXPLAIN` against the `core` and `app` schemas, sharing one enforcement primitive: writes and file-access functions are blocked, and each output column is classified via sqlglot lineage so CRITICAL fields (account/routing numbers) are masked (`****<last4>`) — raw SQL is not a privacy bypass on either surface. App-state mutations (notes, tags, splits, rules) flow through dedicated tools, not raw SQL. (`moneybin db query`/`shell`/`ui` are raw, unmasked operator access.)
- **MCP install across nine clients** — Claude Desktop, Claude Code, Cursor, Windsurf, VS Code, Gemini CLI, Codex (CLI / Desktop / IDE), ChatGPT Desktop. `moneybin mcp install --client <name>` writes the client config. -> [MCP clients guide](guides/mcp-clients.md)
- **Stability promise** — Pre-v1. Tool names and envelope fields may change before the v1 launch; the CHANGELOG records every rename, and removed tools stay as deprecation-aliased shims for one minor release.

## CLI

- **Typer v2 taxonomy** — Path-prefix-verb-suffix naming; entity groups (`accounts`, `transactions`), reference-data groups (`categories`, `merchants`), `reports` for cross-domain rollups, `system` for orientation. -> [CLI reference](guides/cli-reference.md)
- **`--output json` parity with MCP** — Every read command exposes `--output json` and returns the same `{status, summary, data, actions, error?, next_cursor?}` envelope as the corresponding MCP tool, redacted by the same middleware. Agents driving the shell are first-class. -> [CLI reference](guides/cli-reference.md)
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

## Extensibility

MoneyBin is built on the assumption that you'll want to track your money your way — and that an AI agent is a first-class way to make that happen. The schema, the reports, and the import pipeline are stable contracts an agent can read and build against, so you (or Claude Code, or Cursor) can scaffold a custom report, importer, or tracker on top of your own data.

- **Declarative reports (in flight)** — Today's eight reports are hand-wired on the CLI and MCP. A report framework collapses that into one `@report` runner per report — from which the CLI command, MCP tool, parameter flags, and column masking are generated — so adding a report (yours, a package's, or one an agent scaffolds) becomes a single-definition task. Once it lands, the agent driving MoneyBin has everything it needs — the schema resource, the runner contract, and SQL access — to write a new one.
- **The extension contract (in flight)** — A contributor-facing surface for adding your own **reports**, **analysis packages**, and **data providers**, with a Quality Scale (Bronze → Platinum). Designed in [`extension-contracts.md`](specs/extension-contracts.md); v1 ships two reference packages (`assets`, `us_tax`) at Platinum quality as worked examples.

## What's planned

These are visible gaps a migrant or agent author will notice. See [Roadmap](roadmap.md) for the full milestone view.

- **Plaintext export** — `moneybin export` (CSV / Excel / Sheets) for data exit. Planned, not shipped.
- **Budgeting** — Envelopes, rollovers, period-over-period burn. Planned.
- **Investment tracking** — Holdings, FIFO lots, cost basis, 1099-B reconciliation. Planned (core, not a package).
- **Multi-currency** — FX gain/loss and non-USD accounts. Planned.
- **Web UI dashboard** — Local web UI plus Streamable HTTP MCP transport (so remote clients like ChatGPT web can reach MoneyBin). Planned.
- **Hosted tier** — Same code, hosted. Planned.
- **Drop-any-PDF import** — AI-assisted extraction of bank-statement PDFs: native-text statements extract locally and free, harder layouts escalate to the AI agent you're already driving MoneyBin with, and a learned recipe replays for free next time. Transaction-shaped rows route to `core`; everything else lands as queryable JSON seeds. **Phase 2a shipped (PR #233)** — auto-derived recipes persist to `app.pdf_formats` keyed by layout fingerprint, reconcile to within 1¢ of the statement's reported balance delta, and replay deterministically on subsequent imports. Phase 2b (LLM-bridge rung + auto-`bump_version` recovery) in flight. See [`smart-import-pdf.md`](specs/smart-import-pdf.md).
- **Import confirmation & confidence** — One trust step across every import channel (tabular, Sheets, PDF): nothing lands unconfirmed on first contact, a confirmed layout replays silently, and recovery from a wrong guess is one step away (`import_confirm`). In flight.
- **Extension contract** — The contributor-facing surface for reports, analysis packages, and providers (see [Extensibility](#extensibility)). In flight; ships at v1 with two reference packages at Platinum quality.
- **Reference package: `assets`** — Real estate, vehicles, and valuables. First reference package; demonstrates the package contract.
- **Reference package: `us_tax`** — Locale-specific tax reporting helpers (realized gain/loss summaries, cost-basis snapshots). Built on top of investment tracking; not Schedule D generation.
- **First-class split rows** — Today splits are annotations on the parent row; first-class split lines arrive with the curation polish work. Planned.
- **Subscription-cancellation workflow** — `reports.recurring_subscriptions` surfaces the candidates; a "mark cancelled / paused" tracking surface is planned.
- **Native mobile apps** — Not on the roadmap.
- **Household / shared budgets** — Multi-user accounts within one profile. Not on the roadmap.

Post-launch candidates (PDF data extraction, ML-powered categorization, mobile read-only viewer, expanded privacy tiers) live on the same page.
