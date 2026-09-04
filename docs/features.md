<!-- Last reviewed: 2026-09-04 -->
# What Works Today

What MoneyBin can do today. Each capability links to its guide; the [roadmap](roadmap.md) covers what's planned and the [CHANGELOG](../CHANGELOG.md) carries the dated record.

Capabilities below are shipped and exercised end-to-end. Work that has not shipped is listed under [What's planned](#whats-planned).

## Data ingestion

- **Smart tabular import** — CSV, TSV, Excel, Parquet, and Feather through one pipeline. Heuristic column detection, three-tier confidence model, multi-account support, and first-class migration profiles for Tiller, Mint, and YNAB (other tools' exports import via the generic detector). -> [Data import guide](guides/data-import.md)
- **OFX / QFX / QBO import** — Same `import_log` infrastructure as tabular: re-import detection, `--force` override, institution-name auto-resolution, and batch revert via `moneybin import revert <id>`. OFX descriptions are HTML-entity-decoded at import. -> [Data import guide](guides/data-import.md)
- **Cross-source account identity** — One real account = one canonical id even when it arrives from multiple sources (a `.qfx` statement and a `.csv` export of the same account). Strong signals (remembered binding, scoped account number, persistent token) adopt silently; a weak match (shared last four, name, or a reissued card) is surfaced, never silently merged. A shared last four is enough on its own — a source that names no institution, as a tabular export often doesn't, still matches — while two sources naming *different* banks veto the pair. For a duplicate no signal reaches, `accounts links run <id> <id>` proposes the pair you name; it queues for the same review, and merges nothing by itself. An import that could be an account you already have stops before loading anything and returns a confirmation you ratify with `--account-binding @0=ACCOUNT_ID|new` (adopt an existing account, or mint a distinct one), where `@0` labels the first account the file declares. Agents get the same stop, not a pass. A file matching nothing has one possible answer, so it loads and names the account it created instead of asking — except a bare Date/Description/Amount CSV, which states no account at all and is asked with a pick-list. Collapsing twins lets cross-source transaction dedup fire. -> [Data import guide](guides/data-import.md)
- **Cross-source merchant identity** — One real merchant = one canonical `merchant_id`, resolved by Plaid's stable `merchant_entity_id` before name matching. Strong signals (remembered binding, exact name) adopt silently; a fuzzy match is surfaced, never silently merged. Proposals queue under `merchants links pending` and are accepted or declined via `merchants links set`; `merchants links run` backfills over existing Plaid history. `moneybin review --type merchant-links --status` reports their pending count alongside the other review queues. -> [CLI reference](guides/cli-reference.md)
- **Plaid bank sync** — Connect accounts through Plaid Hosted Link via `moneybin-sync`, the Plaid integration backend you self-host. Cursor-based incremental sync by default; `--force` for a full re-fetch. Plaid data lands alongside OFX and CSV in the same canonical tables. Cash and credit-card accounts get full transaction history; investment, loan, mortgage, and HSA accounts load too, and their balances count toward net worth as soon as Plaid reports one. The same `sync pull` also pulls securities, investment transactions, and holdings straight into the investment ledger (see [Investments](#investments) below) — no separate command. -> [CLI reference](guides/cli-reference.md)
- **Google Sheets sync** — Connect a Google Sheet as a live tabular source via direct OAuth — no setup beyond approving the consent screen, with your own Google Desktop client optional for a private API quota. Two adapters: `transactions` (Tiller-style ledgers participate in the full matching and categorization pipeline) and `seed` (any other sheet lands in `raw.gsheet_seeds` as JSON plus an auto-generated typed view, queryable via SQL and MCP). Every `moneybin refresh` re-pulls the latest sheet state; soft-delete preserves audit history; per-connection drift detection refuses pulls on structural change until you reconnect. -> [Google Sheets guide](guides/connect-gsheet.md)
- **Batch imports** — `moneybin import files PATHS...` (and `import_files` on MCP) ingests multiple files in a single call; per-file failures don't abort the batch. -> [Data import guide](guides/data-import.md)
- **Watched-folder inbox** — Drop files into `~/Documents/MoneyBin/<profile>/inbox/`. `moneybin import inbox` drains successes to `processed/YYYY-MM/` and failures to `failed/YYYY-MM/` with YAML error sidecars. Per-profile lockfile with crash recovery. -> [Data import guide](guides/data-import.md)
- **Manual transaction entry** — Add transactions by hand via `moneybin transactions create` (CLI). On MCP, `transactions_create` accepts a validated batch of 1–100 transactions. Use manual entry for cash, gifts, and anything that doesn't come from a file or sync. -> [CLI reference](guides/cli-reference.md)

## Storage and security

- **Encrypted DuckDB at rest** — AES-256-GCM by default. Argon2id KDF for passphrase mode; OS keychain for auto-key mode. One encrypted DuckDB file per profile under `~/.moneybin/profiles/<name>/`. -> [Database and security guide](guides/database-security.md)
- **Threat model** — What encryption protects against, and what it doesn't (forgotten passphrase, malware on your machine, AI-vendor data flow). -> [Threat model](guides/threat-model.md)
- **Key management and lifecycle** — `moneybin db init / lock / unlock / backup / restore` and `moneybin db key show / rotate`. Encryption CLI is symmetric with the rest of the surface. -> [Database and security guide](guides/database-security.md)
- **Backup and restore** — `moneybin db backup` produces a portable encrypted snapshot; `db restore` recovers it. Snapshots are point-in-time of when the command ran; automated schedules are not yet built — use cron or your platform's scheduler. -> [Database and security guide](guides/database-security.md)
- **Schema migrations** — Auto-upgrade on first invocation; details are operator-level and live in the [Database and security guide](guides/database-security.md). Capacity: supports years of multi-account history on a single laptop (DuckDB columnar storage).
- **Multi-profile isolation** — Per-profile DB, config, and logs. `moneybin profile create / list / switch / delete / show / set`. -> [Profiles guide](guides/profiles.md)

## Transformations and refresh

- **Layered SQLMesh pipeline** — `raw` → `prep` (staging) → `core` (canonical facts / dimensions / bridges). Plus `app.*` for user-managed state and `reports.*` for curated views. Consumers (CLI, MCP, SQL clients) read from `core.*` and `reports.*` for analysis; the agent-safe SQL paths also read `raw.*` and `prep.*` for inspection, masked by value shape rather than by column declaration. -> [Data pipeline guide](guides/data-pipeline.md)
- **Cross-source dedup** — SHA-256 content hashes with golden-record merge across CSV, OFX, and Plaid. Config-driven source priority. Three or more copies of the same transaction collapse to one record even when duplicates span sources *and* overlapping files (N-way collapse via a union-find spanning forest). -> [Data pipeline guide](guides/data-pipeline.md)
- **Transfer detection** — Cross-account matching with a two-signal scoring engine (date distance, keyword); produces `core.bridge_transfers` and `is_transfer` / `transfer_pair_id` on `fct_transactions`. -> [Data pipeline guide](guides/data-pipeline.md)
- **Refresh umbrella** — `moneybin refresh` (CLI) and `refresh_run` (MCP) are the single entry point for the default `gsheet → match → transform → categorize → identity → rates` cascade. Pass `--step` (CLI) or `steps=[...]` (MCP) to scope sub-operations. `sync pull` and `import files` invoke refresh automatically unless the caller explicitly opts out. -> [Data pipeline guide](guides/data-pipeline.md)
- **Reliable under load** — Timeouts, write coordination, and schema-drift recovery are handled automatically; see [architecture](architecture.md) if you want the mechanics.

## Categorization

- **Rule-based engine** — Exact / contains / regex / `oneOf` exemplars. Your manual categorizations are immune to subsequent auto-categorization (source precedence enforced on write: user beats rule, rule beats LLM-assist, and so on). -> [Categorization guide](guides/categorization.md)
- **Provider categorization (Plaid)** — Transactions synced from Plaid are auto-categorized from Plaid's Personal Finance Category, mapped to your canonical categories through the category-source bridge and confidence-gated (assigns only when Plaid is confident). It runs after your rules and merchants — so it never overrides a deliberate choice, it just clears the long tail before the LLM is ever asked. -> [Categorization guide](guides/categorization.md)
- **Smart matcher** — Matches against description plus memo text, and uses structural signals (check number, transfer flag, payment channel, amount sign), so PayPal / Venmo / Zelle / generic-ACH wrappers categorize on the merchant identity that lives in memo. -> [Categorization guide](guides/categorization.md)
- **Auto-rule learning** — User edits propose rules; review and promote them through a queue. -> [Categorization guide](guides/categorization.md)
- **Curator-impact queue** — What still needs categorizing, ranked by `ABS(amount) × age_days` so the highest-impact gaps surface first. CLI: `moneybin transactions categorize pending`; MCP: `reviews(kind="categorization", status="pending")`. -> [Categorization guide](guides/categorization.md)
- **LLM-assist (opt-in)** — Propose → review → commit workflow. The redactor strips amounts, dates, and account identifiers before any prompt leaves the machine; structural fields are exposed as signals. Auto-created merchants accumulate `oneOf` exemplars instead of inventing over-general patterns. CLI: `moneybin transactions categorize assist`. -> [Categorization guide](guides/categorization.md)
- **Auto-apply on commit** — Newly created rules and merchants apply across the rest of the dataset automatically, so the LLM is meaningfully less involved by the third or fourth import. -> [Categorization guide](guides/categorization.md)
- **Merchant catalog** — User- and system-created (no seeded catalog). Plaid pass-through, migration imports, LLM-assist, and the auto-apply pass all populate it. -> [Categorization guide](guides/categorization.md)
- **Bulk operations** — CLI, MCP, and service-layer parity for batch categorize, rule create / delete, merchant operations. -> [Categorization guide](guides/categorization.md)

## Curation (transaction-level user state)

- **Notes** — Free-text notes on transactions.
- **Tags** — Multi-tag table with rename semantics.
- **Splits via annotation** — Annotation-based splits today; first-class split rows are parked (see [roadmap](roadmap.md)).
- **Import-batch labels** — Group imported rows under a human label.
- **Edit-history audit log** — Per-row history of every curation edit.
- **Reversible edits** — Every protected `app.*` mutation (notes, tags, splits, categories, rules, account settings) is audit-paired and undoable as a unit keyed on `operation_id`. `moneybin system audit undo|history|get`, `system_audit(view="history")`, `system_audit(view="detail", operation_id=...)`, and `system_audit_undo(operation_id=...)` expose and reverse a change from its full before/after image; the undo is itself audited and undoable. Undo refuses (rather than silently cascading) when a later operation touched the same rows. -> [CLI reference](guides/cli-reference.md)

All on the `app.*` layer; zero changes to the upstream pipeline. (No dedicated guide yet — see [CLI reference](guides/cli-reference.md) and [MCP server guide](guides/mcp-server.md).)

## Accounts and balances

- **Account management** — `moneybin accounts list / get / resolve / set` with Plaid-parity metadata (subtype, holder category, currency, credit limit, last four). One unified `set` covers display name, include-in-net-worth, and archive state. Reversible account merging via bridge model. -> [CLI reference](guides/cli-reference.md)
- **Net-worth and balance tracking** — Per-account balance show / history / assert / reconcile and cross-account `moneybin reports networth / networth-history` with period-over-period change. Daily carry-forward of authoritative observations from OFX, Plaid sync balances, tabular running balances, and user assertions. -> [CLI reference](guides/cli-reference.md)
- **Exchange rates and your own rate corrections** — `moneybin fx rate USD EUR 2026-03-13` answers one pair on one date and names where the number came from: your own correction first, then a rate already cached, then a live call to Frankfurter's ECB reference series (no credential, and only the pair and the date leave your machine). A weekend resolves to the last published business day and reports that day, rather than presenting it as the weekend's own rate. `fx set` records your own rate for one pair and date, outranking every provider rate for that date; `fx delete` returns that date to provider pricing; `fx list` shows the stored series newest first with the source that won each date, reading only what is already on disk. Rates are stored to 8 decimal places, and the first answer for a pair on a date is the one kept — once a date is cached MoneyBin does not ask the provider again, so a later revision to that date never arrives. Use `fx set` to change a rate you already hold. These are the rates `--display-currency` prices a report with — see [Reports](#reports). -> [CLI reference](guides/cli-reference.md)

## Investments

- **Securities catalog** — `moneybin investments securities add / set / list` maintains a manual catalog (equity, ETF, mutual fund, bond, crypto, cash, other) keyed on a stable id, with ticker, CUSIP, ISIN, FIGI, and CoinGecko-id as optional attributes. -> [CLI reference](guides/cli-reference.md)
- **Investment ledger** — `moneybin investments add` records buys, sells, reinvested dividends, interest, capital-gain distributions, transfers, splits, fees, and return of capital by hand; `investments list` reviews recorded events. -> [CLI reference](guides/cli-reference.md)
- **Cost basis, tax lots, and realized gain/loss** — Four cost-basis methods — FIFO (default), HIFO, specific identification, and average cost (funds/ETFs) — all compute over the same derived lot ledger. `investments holdings` shows current quantity, cost basis, and market value per position, valued from the close your broker already sends through `sync pull` — each row reports the date of the price it used and how many days old that price is, and a position with no usable price reports no value rather than zero. A security no connected broker prices values from an independent feed or your own mark (below); `investments gains` reports realized short-/long-term gain/loss — the 1099-B surface, checked against a hand-labeled full-tax-year fixture; `investments lots select` overrides which lots a sale draws from for tax-loss harvesting, once the security resolves to specific identification — by its own election or the account default — since under any other method the override would never be read, so the command refuses it rather than accepting a write nothing applies. The default method is set per-account (`accounts set --default-cost-basis-method`) or per-security (`investments securities set --method`). -> [CLI reference](guides/cli-reference.md)
- **Plaid Investments sync** — `moneybin sync pull` also loads securities, investment transactions, and dated holdings snapshots for connected brokerage/retirement accounts into the same ledger above — no separate command or import step. Security identity resolves automatically for exact matches (ticker, CUSIP, ISIN); anything ambiguous (a stripped ticker, a name that fuzzy-matches an existing security, a duplicate identifier) is never silently merged — it queues a proposed match under `investments securities links pending`, decided via `investments securities links set --accept/--reject`. A long-held position with no transaction history in Plaid's ~24-month window is seeded from its first holdings snapshot so a later sale doesn't realize a false zero-basis gain. Known v1 limits: stock splits are held for manual review rather than auto-applied, and reinvest/corporate-action pairing isn't linked yet — both surface through `moneybin system doctor`. -> [CLI reference](guides/cli-reference.md)
- **Independent price feeds and your own price marks** — `moneybin investments prices pull` refreshes closes from Tiingo (equities, ETFs, mutual-fund NAVs) and CoinGecko (crypto), so a position values from a source other than the broker reporting it. Fetch scope comes from open positions, not the whole catalog. A feed key binds silently only when the symbol names one catalog entry and the provider agrees about its exchange and issuer name; anything ambiguous queues under `investments securities links pending` instead — a ticker is not an identifier, since the same symbol names different securities across exchanges and gets recycled after a delisting. `investments prices set` records your own price for one security and date, outranking every provider close for that date; `investments prices delete` returns that date to provider-derived valuation; `investments prices list` shows the resolved series and which source won each date. `moneybin system doctor` gains three checks over the series: two feeds quoting the same security, date, and currency more than 2% apart; held positions carrying no usable price; and price rows whose source the pipeline cannot resolve. One source failing does not cost you the others — a missing Tiingo token still refreshes crypto, and the refresh names the source that failed. Store the Tiingo token with `investments prices token`; CoinGecko needs no credential. -> [CLI reference](guides/cli-reference.md)

## Reports

Eight registered report routes back both the CLI and MCP surfaces, and you can
save your own beside them (below). Six use SQL runners over curated `reports.*`
views; the two net-worth routes are service-backed and share `reports.net_worth`. Reports accept date-range filters (`--from-month` / `--to-month` on time-windowed reports like `cashflow` and `spending`, `--as-of` for snapshots like `networth`, plus `--account` and `--category` where they apply); grains vary per report. -> [CLI reference](guides/cli-reference.md) · [MCP server guide](guides/mcp-server.md)

The six SQL-runner routes use declarative `@report` definitions; the two service-backed net-worth routes keep their specialized execution path. The shared catalog derives parameters and masking without adding MCP tool slots. See [Extensibility](#extensibility).

- **`reports.net_worth`** — Cross-account total with period-over-period change.
- **`reports.cash_flow`** — Income vs spending by month.
- **`reports.spending_trend`** — Category spending over time.
- **`reports.recurring_subscriptions`** — Recurring transactions with confidence scores, cadence, and annualized cost. (No "mark as cancelled" workflow yet — see [roadmap](roadmap.md).)
- **`reports.merchant_activity`** — Per-merchant spend rollup.
- **`reports.large_transactions`** — Outlier filter for human review.
- **`reports.balance_drift`** — Drift between asserted and computed balances.

### Reading a report in one currency

`--display-currency EUR` — and `display_currency` on the `reports` MCP tool —
prices a report's amounts into one currency at read time, using the rates above.
Omit it and the target is the profile's home currency.
`summary.display_currency` names what the numbers are in.

Three reports convert, because each of their rows is one event on one date:
`large_transactions` at its transaction date, `balance_drift` at its assertion
date, and `networth` at its balance date. The other five aggregate with the
currency in their grouping key, so a row is already a per-currency subtotal;
pricing it would put two currencies behind one figure. Those stay sub-totalled
per currency, as does any report whose rates are not on disk — `moneybin
refresh` gathers them, since a read never fetches. Ask for a currency
explicitly and the reason appears in `summary.degraded_reason`; the
home-currency default falls back quietly, so a profile that has set one is not
warned on every report it cannot price.

Nothing converted is stored. The original amount and its currency stay
untouched in every table, and a converted figure is recomputed on each read —
so the original reading is always one command away, with the flag omitted.
`moneybin reports networth` prints one position per currency, and a single
combined position once conversion has priced them into the same one.

### Your own reports

`moneybin reports create <name> --sql "SELECT ..."` saves a query as a durable
report alongside the eight above. It appears in `reports list`, runs through
`reports run`, and exports through `moneybin export report` — the same catalog,
the same response envelope, the same masking. You never declare privacy classes:
MoneyBin derives them from the SQL at save time and stores them, so a routing
number in a column MoneyBin classifies is masked exactly as in a built-in one.
A report reading `raw.*` or `prep.*` is the one place that parity does not
hold: those columns are largely undeclared, so a routing number there comes
back as `****...NNNN` — last four retained — where a declared one returns
`*****`. The scan behind that covers strings and integers, so an account number
of 4 to 7 digits, one written with separators (`1234-5678`), or one stored as
`DECIMAL` or `FLOAT` passes through in full. `reports create` names those
columns when it saves, so which columns ride the scan is on the receipt rather
than something to work out later. If an upstream column is later reclassified
as more sensitive, the saved report masks that column instead of serving the
class it captured. `reports set` re-derives on any SQL or parameter change,
`reports delete` is undoable via `system audit undo`,
and `reports reclassify` lowers one column's masking floor on an explicit human
confirmation — audited, and the only path that does so.

Parameters are declared and bound by name: `--param month:str` at create,
`--param month=2026-01` at run. Reads are limited to `core.*`, `app.*`,
`reports.*`, `raw.*`, and `prep.*`, and only row-returning read-only SELECTs
are accepted. A report reading `raw.*` or `prep.*` saves and runs, but can
never be promoted to a materialized `reports.*` view — materialization derives
only from `core.*` and `app.*`, and `moneybin reports explain` names the
blocker.

- **`moneybin reports explain <handle>`** — Any report, any tier, states its
  work: the query in both an executed and a stored-template form, each output
  column's privacy class and the upstream column it descends from, the tables it
  reads, when its classification was derived, and whether it can be promoted to a
  materialized view. Runs nothing. -> [CLI reference](guides/cli-reference.md)

## Data export

- **Canonical export delivery** — `moneybin export bundle` publishes a closed
  13-table portability catalog to redacted CSV by default under
  `~/Documents/MoneyBin/<profile>/exports/`. Local CSV and Parquet bundles carry
  a manifest, checksums, and generated data dictionary; XLSX carries the same
  contract in one workbook. Each local run is immutable, ZIP is limited to CSV
  and Parquet, and `--unredacted` is an explicit per-run choice. `moneybin export
  report <report-id>` executes one complete catalog report once and retains its
  parameters and SQL provenance. Named local and output-only Sheets destinations are managed
  under `moneybin export destination`; Sheets replaces only MoneyBin-managed
  tabs, keeps bundle/report metadata separate, and preserves the latest good
  state on failure. MCP exposes the same
  outcomes through `export_run`, `exports_set`, and
  `system_status(sections=["exports"])`. -> [CLI reference](guides/cli-reference.md)
  · [MCP server guide](guides/mcp-server.md)

## MCP server

- **Bounded tool registry** — One 50-tool standard registry spans 13 user-facing domain groups across 17 literal tool-name prefixes. Registered reports run through the generic `reports` catalog and runner without consuming additional tool slots; 50 tools is the hard limit, and the registry now sits at it — admitting another tool means retiring one. Full per-domain inventory: [MCP registry](specs/moneybin-mcp.md); every tool's client-visible definition, generated from the code: [MCP tool reference](reference/mcp-tools.md).
- **Transport** — stdio today. Streamable HTTP transport ships with the web UI milestone (see [roadmap](roadmap.md)).
- **Auth and session model** — Each MCP session inherits the profile unlocked by `moneybin db unlock`. `moneybin db lock` clears the stored key so no new session can open the profile; sessions already running keep their in-memory key until they exit (`moneybin db kill` is the confirmation-gated command that terminates them).
- **Concurrency** — Reads coexist freely; writes are serialized per profile (single-writer rule). Two agents can read concurrently; only one can mutate at a time.
- **Response envelope** — `{status, summary, data, actions, error?, next_cursor?}` on every tool. `summary` carries counts, sensitivity tier, and display currency; `actions` carries next-step hints (see below); `next_cursor` is the opaque pagination token. Money fields are JSON numbers (not strings). Validation errors land as `invalid_arguments` envelopes with a hint listing accepted parameters. -> [MCP server guide](guides/mcp-server.md)
- **Tool annotations** — Protocol-standard `readOnlyHint` / `destructiveHint` / `idempotentHint` / `openWorldHint` so clients can render confirmation UI for destructive operations.
- **Sensitivity tiers** — MoneyBin uses `low` / `medium` / `high` / `critical`. Static tools derive classification from typed response payloads; variable projections classify dynamically under a declared maximum. Critical fields are masked today; global consent-based response gating remains deferred. See [architecture](architecture.md).
- **Action hints** — Successful responses include an `actions[]` array suggesting next-step tool calls (e.g., after a successful import, an action hint points at `refresh_run`), so agents can chain without prompt-side instructions for common flows. -> [MCP server guide](guides/mcp-server.md)
- **Curated schema resource** — `moneybin://schema` MCP resource (and `sql_schema` tool mirror) exposes core + select app interface tables with column comments and example queries. The tool also lists one schema's live relations — curated or not — via `sql_schema(table='<schema>.*')`, bounded by the same five schemas `sql_query` reads. -> [Data model reference](reference/data-model.md)
- **Read-only SQL — privacy-safe on both surfaces** — `sql_query` (MCP) and `moneybin sql query` (CLI) run read-only `SELECT`/`WITH`/`DESCRIBE`/`SHOW` against five schemas — `core`, `app`, `reports`, `raw`, and `prep`; `meta` and `seeds` are refused — sharing one enforcement primitive: writes, file-access functions, `PRAGMA`, and `EXPLAIN` are blocked, the schema limit binds catalog statements as well as `SELECT`, and each output column is classified via sqlglot lineage so CRITICAL fields (account/routing numbers) are masked (`****<last4>`). `raw` and `prep` carry no per-column declarations, so 34 columns across 17 tables are declared CRITICAL by hand and every other value is scanned for account and SSN shapes at execution. Neither surface is a way around the masking the typed tools apply — but both reach importer output no typed tool exposes, under the weaker of the two mechanisms. App-state mutations (notes, tags, splits, rules) flow through dedicated tools, not raw SQL. (`moneybin db query`/`shell`/`ui` are raw, unmasked operator access.)
- **MCP install across eight clients** — Claude Desktop, Claude Code, Cursor, Windsurf, VS Code, Gemini CLI, Codex (CLI / Desktop / IDE), and the ChatGPT desktop app (which hosts Codex and shares its config). `moneybin mcp install --client <name>` writes the client config. ChatGPT on the **web/mobile** cannot reach a local stdio server; remote MCP transport is planned on the [roadmap](roadmap.md). -> [MCP clients guide](guides/mcp-clients.md)
- **First-run setup, in session** — Connect before creating a profile and MoneyBin sets itself up on the first tool call instead of failing. Elicitation-capable clients (e.g. Claude Desktop) are prompted for a profile name and the encrypted profile is created in place — no terminal step, no restart; tools-only clients get one clear message pointing at `moneybin profile create`. -> [MCP server guide](guides/mcp-server.md)
- **Pre-v1 contract record** — Tool and envelope changes are recorded in the CHANGELOG. The current docs and checked-in surface snapshot define the 50-tool registry; no deprecated MCP aliases are advertised.

## CLI

- **`moneybin demo` — try it with no real data** — One command sets up an isolated `demo` profile with synthetic data, runs the pipeline to a clean `system doctor`, and shows net worth plus next steps. `--persona basic|family|freelancer|international` picks the data shape; re-running rebuilds the demo profile from scratch. The `international` profile holds five currencies, so `demo` reports net worth per currency rather than one total. It only ever touches the dedicated `demo` profile, so it can never write into a real one. The fastest way to see what MoneyBin does before connecting anything real.
- **Typer v2 taxonomy** — Path-prefix-verb-suffix naming; entity groups (`accounts`, `transactions`), reference-data groups (`categories`, `merchants`), `reports` for cross-domain rollups, `system` for orientation. -> [CLI reference](guides/cli-reference.md)
- **`--output json` parity with MCP** — Every read command exposes `--output json` and returns the same `{status, summary, data, actions, error?, next_cursor?}` envelope as the corresponding MCP tool, redacted by the same middleware. Agents driving the shell are first-class. Six commands are deliberate exceptions — the `db query` operator bypass, the `db info` / `db ps` reads that describe the database file rather than its contents, and the `stats` / `logs` / `migrate status` operations-metadata reads — each named in the CLI reference. -> [CLI reference](guides/cli-reference.md)
- **Structured error envelopes** — Runtime errors emit a machine-readable envelope to stdout when `--output json` is active.
- **Field projection** — `--json-fields` narrows `--output json` to a named subset of fields on `moneybin sql query` and `moneybin sync status`. No other command declares it yet.
- **Shell completion** — `moneybin --install-completion` / `--show-completion`.
- **Version** — `moneybin --version` prints the installed MoneyBin version.

## SQL access

- **Read-only SQL** — Connect any DuckDB client to the encrypted profile file. `moneybin db shell` opens an interactive shell; DuckDB UI works on the same file. -> [SQL access guide](guides/sql-access.md)
- **Layered schemas** — Consumers read from `core.*` and `reports.*` for analysis, plus `raw.*` and `prep.*` for inspection through the agent-safe SQL paths. Full schema reference: [Data model reference](reference/data-model.md) · [Architecture](architecture.md).

## Observability

- **Structured logs** — `moneybin logs cli|mcp|sqlmesh` tails one stream, filtered by `--level`, `--since`, `--until`, and `--grep`; `moneybin logs --print-path` locates the log directory and `moneybin logs --prune --older-than 30d` deletes old files. PII and financial detail are stripped at the formatter layer; see [Threat model](guides/threat-model.md). -> [Observability guide](guides/observability.md)
- **Prometheus-style metrics** — Per-operation counters and durations, persisted to DuckDB. `moneybin stats`. -> [Observability guide](guides/observability.md)
- **`moneybin system doctor`** — Read-only pipeline integrity check: SQLMesh audits for FK integrity, sign convention, and transfer-pair balance; transform model presence; dedup reconciliation, one account imported under two identities, and cross-source duplicates without a merge proposal; categorization coverage; currency integrity; `app.*` audit coverage and orphaned app state; and 13 investment checks. Exits 0 on pass / warn, 1 on fail. `--verbose` for affected IDs, `--full` for a whole-table scan, `--output json` for agents. MCP exposes the same outcome through `system_status(sections=["doctor"])`. -> [CLI reference](guides/cli-reference.md)

## Extensibility

MoneyBin is built on the assumption that you'll want to track your money your way — and that an AI agent is a first-class way to make that happen. The schema, the reports, and the import pipeline are stable contracts an agent can read and build against, so you (or Claude Code, or Cursor) can scaffold a custom report, importer, or tracker on top of your own data.

- **Declarative reports (implemented)** — Eight registered report routes share one catalog. Six use `@report` SQL runners and two net-worth routes use service-backed definitions; the framework derives CLI commands, the `reports` catalog entry, parameter flags, and column masking from those definitions. New reports extend the catalog without adding MCP tool slots.
- **The extension contract (in flight)** — A contributor-facing surface for adding your own **reports**, **analysis packages**, and **data providers**, with a Quality Scale (Bronze → Platinum). Designed in [`extension-contracts.md`](specs/extension-contracts.md); v1 ships two reference packages (`assets`, `us_tax`) at Platinum quality as worked examples.

## What's planned

These are visible gaps a migrant or agent author will notice. See [Roadmap](roadmap.md) for the full milestone view.

- **Budgeting** — Monthly budgets, target-vs-actual, rollovers. Planned.
- **Daily valued-holdings series and net-worth integration** — A dated series of what each position was worth on each day, and folding investment positions into net worth. Independent price feeds and your own price marks shipped alongside the ledger, tax lots, four-method cost basis, realized gain/loss (1099-B surface), and broker-carried market value — see [Investments](#investments) above. Planned (core, not a package).
- **Multi-currency** — Original currency is captured from OFX and Plaid instead of being silently assumed USD, and every transaction and balance resolves its currency from its own source or its account's setting. Reports that sum money sub-total each currency separately rather than adding dollars to euros; `moneybin profile set home_currency EUR` records which one the profile treats as home, and is the currency `--display-currency` defaults to; `moneybin system doctor` flags accounts whose currency is unknown. Exchange rates, with your own corrections outranking the provider, ship today — see [Accounts and balances](#accounts-and-balances). Three of the eight reports price their rows into one currency at read time; the five that aggregate per currency stay sub-totalled and say why. FX gain/loss is planned.
- **Web UI dashboard** — Local web UI plus Streamable HTTP MCP transport (so remote clients like ChatGPT web can reach MoneyBin). Planned.
- **Hosted tier** — Same code, hosted. Planned.
- **Drop-any-PDF import** — AI-assisted extraction of bank-statement PDFs: native-text statements extract locally and free, harder layouts escalate to the AI agent you're already driving MoneyBin with, and a learned recipe replays for free next time. Transaction-shaped rows route to `core`; everything else lands as queryable JSON seeds. **Phase 2a shipped (PR #233)** — auto-derived recipes persist to `app.pdf_formats` keyed by layout fingerprint, reconcile to within 1¢ of the statement's reported balance delta, and replay deterministically on subsequent imports. **Phase 2b bridge round-trip shipped** — a layout the deterministic rung can't crack escalates to the agent you're driving MoneyBin with (with a plain transparency notice), and your confirmed recipe is re-run, reconciled against the statement balances, and loaded; every hand-off is audit-logged (MCP surface today). A drifted saved recipe now auto-recovers (re-derived and version-bumped on the next import instead of stranding the broken recipe), and a scanned/image-only PDF with no text layer returns an explicit "needs a vision-capable backend" message rather than failing opaquely. See [`smart-import-pdf.md`](specs/smart-import-pdf.md).
- **Import confirmation & confidence** — One trust step across every import channel (tabular, Sheets, PDF): nothing lands unconfirmed on first contact, a confirmed layout replays silently, and recovery from a wrong guess is one step away (`import_confirm`). In flight.
- **Extension contract** — The contributor-facing surface for reports, analysis packages, and providers (see [Extensibility](#extensibility)). In flight; ships at v1 with two reference packages at Platinum quality.
- **Reference package: `assets`** — Real estate, vehicles, and valuables. First reference package; demonstrates the package contract.
- **Reference package: `us_tax`** — Locale-specific tax reporting helpers (realized gain/loss summaries, cost-basis snapshots). Built on top of investment tracking; not Schedule D generation.
- **First-class split rows** — Splits ship as annotations on the parent row; that's the intended shape. First-class split lines are parked, revisited only if budgeting needs or real-data feedback force them.
- **Subscription-cancellation workflow** — `reports.recurring_subscriptions` surfaces the candidates; a "mark cancelled / paused" tracking surface is planned.
- **Native mobile apps** — MoneyBin is desktop and CLI; the planned web UI runs in a phone browser. [Where MoneyBin fits](comparison.md#where-moneybin-is-not-the-best-fit) names the mobile tools to use instead.
- **Household / shared budgets** — Multi-user accounts within one profile. MoneyBin is single-user; [Where MoneyBin fits](comparison.md#where-moneybin-is-not-the-best-fit) names the shared-budget tools to use instead.

Post-launch candidates (AI-assisted parsing of non-PDF file types, ML-powered categorization, mobile read-only viewer, expanded privacy tiers) live on the same page.
