<!-- Last reviewed: 2026-07-21 -->
# CLI Reference

MoneyBin's CLI covers everything its MCP server does. Read commands return text or JSON with `--output json`; every interactive prompt has a flag equivalent so scripts and agents can drive the same commands. Parity is **functional, not nominal** — the same outcomes are reachable on both surfaces, but tool names don't always map 1:1 (e.g., `moneybin transactions list` reaches the MCP tool `transactions`). See [`mcp-server.md`](mcp-server.md) for the MCP catalog.

This page covers the full user-facing surface. Per-command flag detail lives in `moneybin <cmd> --help`. `--help` is always side-effect free — it does not touch profiles, open the database, or hit the network.

## Standard flags

These flags appear on commands across every group. They are not repeated in the per-group tables below.

| Flag | Scope | Description |
|---|---|---|
| `-p, --profile <name>` | Global (root) | Pick the profile to operate against. Overrides `MONEYBIN_PROFILE` and the saved default for one invocation. |
| `-v, --verbose` | Global (root) | Enable debug logging on stderr. |
| `-o, --output {text,json}` | All read commands | Output format. `text` is human-readable; `json` returns the standard response envelope (same shape as the MCP equivalent). |
| `-q, --quiet` | All read commands | Suppress informational chatter (status lines, the trailing `✅`). Result rows are never suppressed — they are the data. |
| `--json-fields a,b,c` | Read commands that opt in | Comma-separated field projection. Silently ignored unless `--output json` is active. Available fields are enumerated in the command's `--help`. |
| `-y, --yes` | Mutating commands with prompts | Skip the confirmation prompt. Required for non-interactive use. |

**Leaf vs sub-group.** Leaf commands like `stats` and `logs` take action directly; sub-groups like `db`, `import`, and `transactions` require a subcommand (`moneybin db info`, not `moneybin db`).

### Date and duration formats

- **Date arguments** (`--from`, `--to`, `--as-of`, `--date`) are ISO 8601 `YYYY-MM-DD`. Month-grain commands like `reports cashflow` use `--from-month`/`--to-month` and document `YYYY-MM-01` in their `--help`.
- **Duration shortcuts** (`7d`, `24h`, `5m`) are accepted on `logs` (`--since`, `--until`) and `stats` (`--since`). They are **not** accepted on report or sync date filters — use absolute dates there.
- Timestamps in JSON output are ISO 8601; dates are `YYYY-MM-DD` strings (not epoch seconds).

## Exit codes

| Code | Meaning |
|---|---|
| `0` | Success. |
| `1` | Runtime error: the operation ran and failed (file not found, database locked, upstream API 5xx, validation error on data already accepted, partial-batch error). Mutating commands also exit `1` when any item in a batch fails or is skipped (e.g., `transactions categorize commit` with one bad row). |
| `2` | Usage error: missing argument, invalid flag value, unknown subcommand, bad combination of flags. |

Diagnostic output goes to stderr (fd 2). Data output goes to stdout (fd 1). Pipes (`| jq`, `| less`) are safe in both modes.

## Output envelopes

`--output json` returns the response envelope documented in [`docs/architecture.md`](../architecture.md). Top-level keys:

```json
{
  "status": "ok",
  "summary": {
    "total_count": 0,
    "returned_count": 0,
    "has_more": false,
    "sensitivity": "low",
    "display_currency": "USD"
  },
  "data": [],
  "actions": []
}
```

`error` is present when `status` is `"error"`; `next_cursor` is present when more rows remain. Three concrete shapes follow — every other command's `data` payload is inferable by running it with `--output json` once. `Decimal` values serialize as JSON numbers, not strings.

**Read response — `transactions list`** (list payload):

```json
{
  "status": "ok",
  "summary": {"total_count": 2, "returned_count": 2, "has_more": false, "sensitivity": "medium", "display_currency": "USD"},
  "data": [
    {"transaction_id": "csv_a1b2c3d4e5f6a7b8", "account_id": "chk_001", "transaction_date": "2026-04-12", "amount": -42.17, "description": "STARBUCKS #1234", "memo": null, "source_type": "csv", "category": "Food & Drink", "subcategory": "Coffee", "notes": null, "tags": ["work"], "splits": null}
  ],
  "actions": []
}
```

**Snapshot response — `reports networth`** (single-record payload):

```json
{
  "status": "ok",
  "summary": {"total_count": 1, "returned_count": 1, "has_more": false, "sensitivity": "low", "display_currency": "USD"},
  "data": {
    "balance_date": "2026-05-17",
    "net_worth": 124530.42,
    "total_assets": 198200.00,
    "total_liabilities": 73669.58,
    "account_count": 7,
    "per_account": [{"display_name": "Chase Checking", "balance": 4210.18, "observation_source": "assertion"}]
  },
  "actions": []
}
```

**Mutating response — `transactions categorize commit`** (write summary):

```json
{
  "status": "ok",
  "summary": {"total_count": 50, "returned_count": 1, "has_more": false, "sensitivity": "medium", "display_currency": "USD"},
  "data": {"applied": 47, "skipped": 2, "errors": 1, "merchants_created": 3, "error_details": [{"transaction_id": "csv_xyz", "reason": "unknown category"}]},
  "actions": ["Use transactions_categorize_rules to review auto-created rules"]
}
```

`db query` is the exception: it extends `--output` to `text|json|csv|markdown|box` (DuckDB's native formats); its `json` is raw row data, **not** the envelope shape. Use other read commands when you need envelope parity.

## Long-running commands

`sync pull`, `refresh`, and `transform apply` can run for several seconds to minutes. Progress and status lines stream to **stderr** by default (visible interactively, hidden when redirected); `--output json` returns a single envelope at completion. There is no incremental JSON progress stream today — agents that need progress should poll `sync status` / `transform status` from a separate invocation.

Concurrent **writes** against the same profile serialize on the database lock; a cron-driven `sync pull` overlapping with an interactive write retries briefly (up to 5 s) and then exits `1` rather than blocking indefinitely. Reads rarely contend with writes — write windows are per-operation rather than per-session — but a read overlapping a long write retries on the same backoff before failing. Use `db ps` to see who's holding the file and `db kill` if needed.

## Which command for which task?

The CLI has a few task-shaped overlaps; this section disambiguates the common ones.

**"Review my transactions" — three candidates, pick by intent:**

- **`transactions list`** — filtered scanning ("show me April groceries"). Supports `--account-id`, `--from`/`--to`, `--category`, `--uncategorized`, `--limit`. Returns raw rows; no workflow.
- **`transactions categorize pending`** — specifically hunting uncategorized rows for a categorization pass. Supports `--sort {date,impact}`, `--min-amount`, and `--account`.
- **`transactions review`** — the interactive curator queue: pending dedup/transfer matches plus uncategorized rows in one stream. Use `--type {matches,categorize,all}` and `--confirm`/`--reject` to drive it from a script.

**"Refresh / transform / categorize run — which?"**

- **`moneybin refresh`** — the right answer 99% of the time. Runs matching → SQLMesh apply → categorization in order; idempotent.
- **`transform <verb>`** — drop here only for SQLMesh-only operator work (debugging a model, restating a date range, validating SQL).
- **`transactions categorize run`** — drop here only when you want to re-run categorization engines without touching transforms (e.g., after editing rules).

**Four `status` commands — which?**

- **`system status`** — "Am I set up correctly? What does my data look like?" Run this first when in doubt.
- **`import status`** — "What did my last imports load, and from where?"
- **`sync status`** — "Where is each connected institution? When did it last pull?"
- **`transform status`** — "Are my SQLMesh models current with their inputs?"

## Setup and orientation

### `profile`

Per-user profile lifecycle. Each profile has an isolated encrypted database, config, and log directory.

| Command | Purpose | Key flags |
|---|---|---|
| `profile create <name>` | Create a profile with directory layout, config, and encrypted database. | — |
| `profile list` | List all profiles, marking the active one. | — |
| `profile switch <name>` | Set a different profile as the active default. | — |
| `profile show [<name>]` | Show resolved settings for a profile (defaults to active). | — |
| `profile set <key> <value>` | Set a config value on a profile (e.g., `logging.level info`). | `--profile <name>` |
| `profile delete <name>` | Delete a profile and ALL its data (database, logs, config). | `-y, --yes` |

### `system`

Top-level orientation: where the data lives, whether it's healthy, what the audit log says.

| Command | Purpose |
|---|---|
| `system status` | Data inventory (account count, transaction count, date range, last import) plus pending review-queue counts. |
| `system doctor` | Run pipeline integrity checks across all invariants. Exit non-zero on any check failure. |
| `system audit list` | List audit-log events with filters (`--actor`, `--action`, `--target-table`, `--target-id`, `--from`, `--to`, `--limit`). |
| `system audit show <audit-id>` | Show one audit event plus any chained children. |

**Related guides:** [`profiles.md`](profiles.md).

## Ingestion

### `import`

File imports and inbox drain. `import files` auto-detects type (CSV / OFX / QFX / PDF) and runs the refresh pipeline after.

| Command | Purpose | Key flags |
|---|---|---|
| `import files <paths>...` | Import one or more financial files. Per-file overrides available. | `--account-name`, `--institution`, `--format-name`, `--refresh/--no-refresh` |
| `import preview <path>` | Inspect file structure without importing (dry run, no DB writes). | — |
| `import history` | List recent import batches with counts and timestamps. | `--limit` |
| `import revert <batch-id>` | Undo an import batch (deletes rows from raw + downstream). | `-y, --yes` |
| `import status` | Summary of all imported data by source. | — |
| `import formats list` | List built-in and user-saved format definitions. | — |
| `import formats show <name>` | Show a saved format's column mapping. | — |
| `import formats delete <name>` | Delete a user-saved format. | `-y, --yes` |
| `import inbox` | Drain the watched inbox: import everything in `inbox/`, move successes to `processed/`, failures to `failed/` with sidecars. Default action when invoked bare. | — |
| `import inbox list` | Show what a drain would do without moving anything. | — |
| `import inbox path` | Print the active profile's inbox parent directory (use with `$(...)` substitution). | — |
| `import labels add <batch-id> <labels>...` | Apply labels to an import batch. | — |
| `import labels remove <batch-id> <labels>...` | Remove labels from an import batch. | — |
| `import labels list [<batch-id>]` | List labels on a batch (or all batches). | — |

### `sync`

Pull transactions from external services through the moneybin-sync proxy. **`sync login` is required first** — most subcommands fail without a valid JWT.

| Command | Purpose | Key flags |
|---|---|---|
| `sync login` | Authenticate with moneybin-sync via Device Authorization Flow. | `--no-browser` |
| `sync logout` | Clear the stored JWT. | — |
| `sync link [<institution>]` | Link a new institution via Plaid Hosted Link. Prints URL to stderr and (optionally) opens the browser. | `--no-browser` |
| `sync link-status` | Show pending link state (after `sync link` started). | — |
| `sync disconnect <item-id>` | Disconnect a linked institution. | `-y, --yes` |
| `sync pull [<item-id>]` | Pull new transactions (and, for brokerage/retirement accounts, securities, investment transactions, and holdings) and run the refresh pipeline. Use without an item-id to pull every connected institution. | `--refresh/--no-refresh`, `--since`, `--full` |
| `sync status` | Show last-sync timestamps and pending-cursor state per linked institution. | — |
| `sync key rotate` | Rotate the sync server's encryption key. | — |
| `sync schedule set <cron>` | Configure a scheduled sync job. | — |
| `sync schedule show` | Show the active sync schedule. | — |
| `sync schedule remove` | Disable scheduled sync. | — |

**Related guides:** [`data-import.md`](data-import.md), [`data-pipeline.md`](data-pipeline.md).

## Refresh pipeline

`refresh` is the always-visible umbrella entry point for the post-load pipeline: matching → SQLMesh apply → categorization. CLI peer of the `refresh_run` MCP tool.

| Command | Purpose | Key flags |
|---|---|---|
| `refresh` | Run the full cascade. Idempotent — safe to retry. Matching and categorization are best-effort; only SQLMesh apply errors fail the command. | `--step {match,transform,categorize}` (repeatable; default = full cascade) |

The `transform` group below is the lower-level operator path. Reach for `refresh` first.

### `transform`

Direct access to the SQLMesh pipeline. Use these when debugging models or restating a date range; for normal post-load refresh, use `moneybin refresh`.

| Command | Purpose |
|---|---|
| `transform plan` | Preview SQLMesh changes without applying them. |
| `transform apply` | Apply pending SQLMesh changes. |
| `transform seed` | Refresh seed-only models. |
| `transform status` | Current model state. |
| `transform validate` | Check that model SQL parses correctly. |
| `transform audit` | Run data-quality audits. |
| `transform restate <model> <start> <end>` | Force-recompute a model for a date range. |

## Curation: transactions

### `transactions`

Browsing transactions and per-transaction state (notes, tags, splits, manual entries, audit).

| Command | Purpose | Key flags |
|---|---|---|
| `transactions list` | List transactions with filters. | `--account-id`, `--from`, `--to`, `--limit`, `--category`, `--uncategorized` |
| `transactions create` | Create a manual transaction (no upstream source). | `--account-id`, `--date`, `--amount`, `--description`, `--category` |
| `transactions audit <transaction-id>` | Show the audit chain for one transaction. | — |
| `transactions review` | Unified review queue: pending matches and uncategorized rows. | `--status`, `--type {matches,categorize,all}`, `--confirm <id>`, `--reject <id>`, `--confirm-all`, `--limit` |

### `transactions notes`

Multi-note threads attached to a transaction.

| Command | Purpose |
|---|---|
| `transactions notes add <transaction-id> <text>` | Append a note. |
| `transactions notes list <transaction-id>` | List all notes on a transaction. |
| `transactions notes edit <note-id> <text>` | Edit an existing note. |
| `transactions notes delete <note-id>` | Delete a note. |

### `transactions tags`

Slug-flavored labels applied to a transaction.

| Command | Purpose |
|---|---|
| `transactions tags add <transaction-id> <tags>...` | Apply one or more tags. |
| `transactions tags remove <transaction-id> <tags>...` | Remove one or more tags. |
| `transactions tags list [<transaction-id>]` | List tags on a transaction, or all tags in use. |
| `transactions tags rename <old> <new>` | Rename a tag everywhere it appears. |

### `transactions splits`

Allocate one transaction across multiple categories. Non-zero residual is a warning, not an error.

| Command | Purpose |
|---|---|
| `transactions splits add <transaction-id> <amount> <category>` | Add one split row. |
| `transactions splits list <transaction-id>` | List splits on a transaction with residual. |
| `transactions splits remove <split-id>` | Remove one split row. |
| `transactions splits clear <transaction-id>` | Remove all splits on a transaction. |

### `transactions matches`

Dedup and transfer matching state.

| Command | Purpose | Key flags |
|---|---|---|
| `transactions matches run` | Run the matcher against existing transactions. | `--skip-transform`, `--auto-accept-transfers` |
| `transactions matches history` | List previously-confirmed matches. | `--limit`, `--type` |
| `transactions matches undo <match-id>` | Revert one confirmed match. | — |
| `transactions matches backfill` | Re-match historical rows after rule changes. | — |

### `transactions categorize`

Categorization workflow. Engines: deterministic rules + merchant mappings (local, no LLM). LLM-assist is exposed as `assist` (read) → `commit` (write). `commit` reads a JSON array of `{transaction_id, category, subcategory?}` objects.

| Command | Purpose | Key flags |
|---|---|---|
| `transactions categorize run` | Run the engine cascade over uncategorized rows. Engines run in order; a rule write blocks a merchant write at the same priority. | `--methods rules,merchants` |
| `transactions categorize assist` | Return uncategorized rows as redacted records for LLM categorization (description/memo redacted; no amount, date, or account). Same shape as the `transactions_categorize_assist` MCP tool. | `--limit`, `--account-filter`, `--date-range` |
| `transactions categorize commit` | Commit externally-decided categorizations from a JSON array. | `--input <path>` or `-` (stdin) |
| `transactions categorize commit-from-file <path>` | Convenience wrapper around `commit --input <path>`. | — |
| `transactions categorize export-uncategorized` | Export uncategorized rows for offline review. | `--limit`, `--output` |
| `transactions categorize stats` | Categorization coverage summary (total / categorized / pct / by-source breakdown). | — |
| `transactions categorize rules list` | List active categorization rules. | — |
| `transactions categorize rules create <name>` | Create a rule (single or `--from-file <path>` for batch). | `--pattern`, `--match-type {exact,contains,regex}`, `--category`, `--subcategory`, `--priority`, `--reapply` |
| `transactions categorize rules apply` | Apply only active rules to uncategorized transactions. | — |
| `transactions categorize rules delete <rule-id>` | Delete a rule. | `--reapply` |
| `transactions categorize auto review` | List pending auto-rule proposals with sample transactions. | `--limit` |
| `transactions categorize auto accept <proposal-id>` | Accept one auto-rule proposal. | `--all` |
| `transactions categorize auto rules` | List rules created from auto-proposals. | — |
| `transactions categorize auto stats` | Auto-rule activity summary. | — |
| `transactions categorize ml status` / `train` / `apply` | ML-assisted categorization (stub). | — |

**Related guides:** [`categorization.md`](categorization.md).

## Curation: reference data

### `categories`

Category taxonomy. Default (seeded) categories cannot be deleted — disable them with `set --inactive`.

| Command | Purpose | Key flags |
|---|---|---|
| `categories list` 🚧 | List all categories (stub). | — |
| `categories create <name>` 🚧 | Create a category (stub). | `--parent <name>` |
| `categories set <category-id>` 🚧 | Update settings (today: `--active/--inactive` only) (stub). | `--active/--inactive` |
| `categories delete <category-id>` | Hard-delete a user-created category. Refuses if referenced unless `--force`. | `--force` |

### `merchants`

Merchant name mappings.

| Command | Purpose | Key flags |
|---|---|---|
| `merchants list` 🚧 | List merchant mappings (stub). | — |
| `merchants create <pattern> <canonical>` 🚧 | Create a mapping (stub). | `--default-category` |

## Accounts and balances

### `accounts`

Account entities (dim records) plus per-account workflows.

| Command | Purpose | Key flags |
|---|---|---|
| `accounts list` | List accounts. Hides archived by default. | `--include-archived`, `--type <subtype>` |
| `accounts get <account-id>` | Show one account's full dim record + settings. | — |
| `accounts set <account-id>` | Update structural and behavioral fields. At least one field flag required. | `--official-name`, `--last-four`, `--subtype`, `--holder-category`, `--currency`, `--credit-limit`, `--default-cost-basis-method`, `--display-name`, `--include/--exclude`, `--archive/--unarchive`, `--clear-FIELD`, `-y, --yes` |
| `accounts resolve <query>` | Fuzzy-match a free-text reference (e.g., `"my Chase account"`) to ranked account-ID candidates. Use this before commands that need an account-id. | `-n, --limit` |
| `accounts balance show <account-id>` | Current balance for one account. | `--as-of <date>` |
| `accounts balance list` | Latest balance across all accounts. | — |
| `accounts balance history <account-id>` | Balance history with daily carry-forward interpolation. | `--from`, `--to` |
| `accounts balance assert <account-id> <amount>` | Record a point-in-time balance assertion (reconciles via delta row). | `--as-of <date>` |
| `accounts balance assertion-delete <assertion-id>` | Delete one balance assertion. | `-y, --yes` |
| `accounts balance reconcile <account-id>` | Recompute reconciliation deltas for an account. | — |

`accounts set` cascades atomically: `--archive` also sets `--exclude` for net-worth in the same write; `--unarchive` does NOT auto-restore `--include`.

**Related guides:** [`profiles.md`](profiles.md), [`data-pipeline.md`](data-pipeline.md).

### `assets`

Physical assets (real estate, vehicles, valuables). Group is reserved; commands ship with the asset-tracking spec.

### `investments`

Investment ledger, positions, tax lots, realized gains, and the manually-maintained securities catalog. Promotes the former `accounts investments` placeholder to a top-level group. All commands support `--output json`.

| Command | Purpose | Key flags |
|---|---|---|
| `investments add` | Record one ledger event. `--type reinvest` writes the acquisition + paired income row atomically. | `--account`, `--type`, `--date`, `--security`, `--quantity`, `--price`, `--amount`, `--fees`, `--subtype`, `--acquired`, `--basis`, `--event-group`, `--currency`, `--description` |
| `investments list` | List ledger events from `core.fct_investment_transactions`. | `--account`, `--security`, `--type`, `--from`, `--to` |
| `investments holdings` | Current positions: quantity, cost basis, average cost. (Market value awaits price feeds — Pillar C.) | `--account` |
| `investments gains` | Realized gain/loss (the 1099-B surface) from `core.fct_realized_gains`. | `--account`, `--security`, `--from`, `--to`, `--term {short,long}` |
| `investments lots list` | Tax lots with remaining quantity and basis. Open lots only by default. | `--account`, `--security`, `--open/--all` |
| `investments lots select <disposal-txn-id>` | Set the full specific-identification lot selection for a disposal (declarative replace). `--clear` reverts to FIFO. | `--lot LOT_ID:QTY` (repeatable), `--clear` |
| `investments securities list` | List the securities catalog. | `--type` |
| `investments securities add` | Add one security to the catalog. | `--name`, `--type`, `--ticker`, `--exchange`, `--cusip`, `--isin`, `--figi`, `--coingecko-id`, `--cash-equivalent`, `--method`, `--currency` |
| `investments securities set <security-id>` | Partial update of one security. At least one field flag required. | `--name`, `--ticker`, `--exchange`, `--cusip`, `--isin`, `--figi`, `--coingecko-id`, `--method`, `--currency` |
| `investments securities links pending` | List pending security merge decisions the Plaid sync resolver couldn't auto-bind (identifier tie, stripped ticker, fuzzy name), grouped by provider ref. | — |
| `investments securities links set <decision-id>` | Accept (merge) or reject one pending decision. `--into` is a confirming check — it must equal the decision's own candidate security id. | `--accept --into <candidate-security-id>`, `--reject` |
| `investments securities links history` | Show recent security-link decisions of any status. | `--limit` |

The per-account cost-basis default is a field on `accounts set --default-cost-basis-method`; the per-security override is `investments securities set --method`.

**Related guides:** [`investments-data-model.md`](../specs/investments-data-model.md), [`sync-plaid-investments.md`](../specs/sync-plaid-investments.md).

## Reports

Cross-domain analytical views. All commands support `--output json` and return the standard envelope.

| Command | Purpose | Key flags |
|---|---|---|
| `reports networth` | Current net worth snapshot. | `--as-of`, `--account` |
| `reports networth-history` | Net worth over time with period-over-period change. | `--from`, `--to`, `--interval {daily,weekly,monthly}` |
| `reports cashflow` | Income vs spending by period. | `--from-month`, `--to-month` (both `YYYY-MM`), `--by {account,category,account-and-category}` |
| `reports spending` | Spending trend by category. | `--from-month`, `--to-month`, `--category`, `--compare {yoy,mom,trailing}` |
| `reports recurring` | Detected recurring subscriptions with confidence and annualized cost. | `--min-confidence`, `--status {active,inactive,all}`, `--cadence {weekly,biweekly,monthly,quarterly,yearly,irregular}` |
| `reports merchants` | Merchant activity rollup. | `--top`, `--sort {spend,count,recent}` |
| `reports large-transactions` | Large transactions, optionally anomaly-filtered. | `--top`, `--anomaly {none,account,category}` |
| `reports balance-drift` | Where computed balance diverges from asserted balance. | `--account`, `--status {drift,warning,clean,no-data,all}`, `--since` |

**Related guides:** [`../features.md`](../features.md#reports).

## Budget

The `budget` group reserves the CLI namespace; full implementation lands with the owning spec.

| Command | Purpose |
|---|---|
| `budget set <category> <amount>` 🚧 | Set or update a budget target (stub). |
| `budget delete <category>` 🚧 | Delete a budget target (stub). |

## Privacy

| Command | Purpose |
|---|---|
| `privacy redact <text>` | Run the redaction pipeline against an input string. Used for debugging the redactor; same contract the MCP tools apply to PII-bearing fields. |

## Database

### `db`

Lifecycle, exploration, and key management on the encrypted database.

| Command | Purpose | Key flags |
|---|---|---|
| `db init` | Create a new encrypted database for the active profile. | `--force` |
| `db info` | Database metadata: size, table list, encryption status, SQLMesh and migration versions. | — |
| `db shell` | Interactive DuckDB SQL shell against the active profile's database. | — |
| `db ui` | Open the DuckDB web UI in a browser. | — |
| `db query <sql>` | Run one SQL query. Output formats: `text`, `json`, `csv`, `markdown`, `box`. JSON here is raw rows, not the envelope. | `-o, --output`, `--params` |
| `db lock` | Lock the database (purge the cached key). | — |
| `db unlock` | Unlock the database (load the key from keychain). | — |
| `db backup` | Create a timestamped encrypted backup. | `--dest <path>` |
| `db restore <backup-path>` | Restore from a backup file. | `-y, --yes` |
| `db ps` | List processes currently holding the database file. | — |
| `db kill` | Kill processes holding the database. | `-y, --yes` |
| `db key show` | Print the encryption key to stderr (use with care). | — |
| `db key rotate` | Re-encrypt with a new key. | `-y, --yes` |
| `db key export <path>` | Export the key to a file (encrypted). | — |
| `db key import <path>` | Import a key from a file. | — |
| `db key verify` | Verify the cached key matches the database. | — |
| `db migrate apply` | Apply pending schema migrations. | `--dry-run` |
| `db migrate status` | Show applied migrations and pending ones. | — |

**Related guides:** [`database-security.md`](database-security.md), [`threat-model.md`](threat-model.md).

## Integrations

### `mcp`

MCP server lifecycle and client install.

| Command | Purpose | Key flags |
|---|---|---|
| `mcp serve` | Start the MCP server (stdio by default). Non-stdio transports are unauthenticated and refuse to start without `--insecure`. | `-t, --transport {stdio,sse,streamable-http}`, `--insecure` |
| `mcp install` | Install MoneyBin into an MCP client's config. Supported clients: claude-desktop, claude-code, codex, chatgpt-desktop, vscode, cursor, windsurf, gemini-cli. (`chatgpt-desktop` shares Codex's `~/.codex/config.toml`; ChatGPT on the web cannot reach a local stdio server.) | `-c, --client`, `-p, --profile`, `--print`, `-y, --yes` |
| `mcp list-tools` | List every registered MCP tool with its sensitivity tier. | `-o, --output` |
| `mcp list-prompts` | List every registered MCP prompt. | `-o, --output` |
| `mcp config` | Show active MCP server configuration (profile, database path, max-rows, max-chars). | — |
| `mcp config path` | Print an MCP client's config-file install path. Used by `make claude-mcp` and similar. | `-c, --client`, `-p, --profile` |

**Related guides:** [`mcp-server.md`](mcp-server.md).

### `export`

Publish a closed 13-table canonical bundle or one registered report. Local
delivery defaults to redacted CSV under
`~/Documents/MoneyBin/<profile>/exports/`; every completed local run is a new
immutable artifact with a manifest, checksums, and data dictionary. Pass
`--unredacted` affirmatively as an explicit per-run choice. Interactive CLI
omission prompts on every run; `--yes` and non-TTY execution select the safe
redacted default. `--unredacted` selects unredacted output affirmatively.
Destination configuration never remembers a redaction choice.

| Command | Purpose | Key flags |
|---|---|---|
| `moneybin export bundle` | Publish the canonical bundle. Defaults to CSV and `local:exports`. | `--format {csv,parquet,xlsx}`, `--to local:<name>\|sheets:<name>`, `--compress zip`, `--unredacted`, `-y, --yes`, `--output {text,json}` |
| `moneybin export report <report-id>` | Execute one catalog report once, retain its parameters and SQL provenance, and publish that result. | Repeat `--param key=value`; delivery flags match `bundle`. |
| `moneybin export destination list` | List the built-in and named destinations with readiness. | `--output {text,json}`, `--quiet` |
| `moneybin export destination add local <name> <path>` | Add or replace a named local root. | — |
| `moneybin export destination add sheets <name> <url>` | Add or replace an output-only workbook and request Sheets write authorization. | — |
| `moneybin export destination remove <name>` | Remove configuration without deleting files, workbooks, or tabs. | — |

CSV and Parquet are directory bundles. `--compress zip` publishes a ZIP beside
the completed bundle; ZIP is the only compression format. XLSX is one workbook
with data, manifest, and dictionary worksheets and rejects `--compress` because
it is already a ZIP container. Sheets uses its native format and rejects both
`--format` and `--compress`.

Inbound and output Sheets are separate contracts. MoneyBin refuses a
destination workbook already configured as a `gsheet` input, replaces only its
own managed tabs after staging and validation, and preserves the latest good
visible state if publication fails. Local artifacts retain history; Sheets is
managed latest state.

MCP reaches the same service outcomes through `export_run`, `exports_set`, and
the existing `system_status(sections=["exports"])` readiness view. Command and
tool names are intentionally not required to map 1:1.

## Diagnostics

These are leaf commands (no subcommands).

| Command | Purpose | Key flags |
|---|---|---|
| `logs <stream>` | View, follow, or filter logs for the active profile. Streams: `cli`, `mcp`, `sqlmesh`. | `-f, --follow`, `-n, --lines`, `--level`, `--since`, `--until`, `--grep`, `--print-path`, `--prune --older-than <duration>` |
| `stats` | Lifetime metric aggregates from `app.metrics`. | `--since <duration>`, `--metric <family>` |

The `stream` argument on `logs` is required unless `--print-path` or `--prune` is used (exit code 2 on misuse — convention of `docker logs`, `kubectl logs`).

## Test data

### `synthetic`

Generate and manage synthetic financial data for testing and demos. Each profile is isolated, so synthetic data never collides with real data.

| Command | Purpose | Key flags |
|---|---|---|
| `synthetic generate` | Generate synthetic data into a fresh profile. | `--persona`, `--months`, `--seed` |
| `synthetic reset` | Wipe and regenerate from scratch. | `--persona`, `-y, --yes` |

Whole-pipeline scenarios live under `tests/scenarios/` and are driven via `make test-scenarios` rather than a CLI command.

## Common workflows

### Monthly close (no JSON pipelines)

```bash
moneybin sync pull                          # latest from connected banks
moneybin import files ~/Downloads/*.ofx     # any OFX files you downloaded
moneybin refresh                            # run the post-load pipeline
moneybin transactions categorize pending    # see what's still uncategorized
# ... categorize via transactions review or transactions categorize rules ...
moneybin reports networth                   # this month's net worth
moneybin reports cashflow                   # this month's income vs spending
```

Each step is idempotent — re-run safely if interrupted. `import files` auto-runs `refresh` after the load, so an OFX-only month can skip the explicit `refresh` call.

### First-time setup

```bash
moneybin profile create personal
moneybin import files ~/Downloads/checking.qfx
moneybin transactions categorize run
moneybin reports networth
```

`categorize run` is a no-op until you have rules or merchant mappings — the auto-rule snowball kicks in after a few LLM-assist cycles.

### Year-end / tax-prep

```bash
moneybin reports cashflow --from-month 2026-01-01 --to-month 2026-12-01
moneybin reports merchants --top 20 --sort spend
moneybin reports spending --from-month 2026-01-01 --to-month 2026-12-01 --compare yoy
```

The `tax` group is reserved for future automated form-data extraction; for now, the reports above plus a `db query` against `core.fct_transactions` cover most tax-prep needs.

### Drain the watched inbox

```bash
cp ~/Downloads/*.qfx "$(moneybin import inbox path)/inbox/"
moneybin import inbox            # drain; auto-refresh; processed/ + failed/ sidecars
moneybin import inbox list       # preview without moving
```

### Categorize with an LLM, agent-driven

```bash
# 1. Pull redacted records out for the LLM.
moneybin transactions categorize assist --limit 50 --output json > to_categorize.json

# 2. Run your LLM workflow against to_categorize.json; produce decisions.json.
# 3. Commit decisions back.
moneybin transactions categorize commit --input decisions.json

# Or stream end-to-end via stdin:
moneybin transactions categorize assist --limit 50 --output json \
  | your-llm-tool \
  | moneybin transactions categorize commit -
```

### Find large uncategorized transactions for review

```bash
moneybin transactions categorize pending --output json \
  | jq '.data[] | select((.amount | tonumber | fabs) > 100)'
```

### Connect an AI assistant

```bash
moneybin mcp install --client claude-desktop --yes
moneybin mcp install --client claude-code --profile personal --yes
```

### Query SQL directly

```bash
moneybin db query "SELECT category, SUM(amount) FROM core.fct_transactions GROUP BY 1" --output csv
moneybin db shell
```

### Database hygiene

```bash
moneybin db info
moneybin db backup
moneybin db migrate status
moneybin system doctor
```

### Status-check a long-running pipeline from a script

```bash
moneybin system status --output json | jq -e '.summary.total_count > 0' \
  && echo "data present" \
  || moneybin transactions categorize run
```

## See also

- [`data-import.md`](data-import.md) — import formats and the import lifecycle
- [`categorization.md`](categorization.md) — rules, merchants, LLM-assist
- [`../features.md`](../features.md#reports) — what each report shows
- [`mcp-server.md`](mcp-server.md) — the MCP peer surface
- [`database-security.md`](database-security.md) — encryption, backups, migrations
