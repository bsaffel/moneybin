<!-- Last reviewed: 2026-09-02 -->
# CLI Reference

MoneyBin's CLI covers everything its MCP server does. Read commands return text or JSON with `--output json`; every interactive prompt has a flag equivalent so scripts and agents can drive the same commands. Parity is **functional, not nominal** — the same outcomes are reachable on both surfaces, but tool names don't always map 1:1 (e.g., `moneybin transactions list` reaches the MCP tool `transactions`). See [`mcp-server.md`](mcp-server.md) for the MCP catalog.

This page covers the full user-facing surface. Per-command flag detail lives in `moneybin <cmd> --help`. `--help` is always side-effect free — it does not touch profiles, open the database, or hit the network.

**🚧 marks a command that is not fully built.** Twelve of them are also hidden from `--help`, so the CLI never advertises what it cannot do: `budget set`/`delete`, `sync key rotate`, `sync schedule set`/`show`/`remove`, `transactions categorize ml train`/`status`/`apply`, and `db key export`/`import`/`verify`. Each stays invocable, so a script that already calls one keeps working; the first nine exit `0` and the three `db key` names exit `1`.

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

**Report response — `reports networth`** (list payload, not a single record — a totals row per currency, then one row per account; captured against a synthetic profile):

```json
{
  "status": "ok",
  "summary": {"total_count": 3, "returned_count": 3, "has_more": false, "sensitivity": "high", "display_currency": "USD"},
  "data": [
    {"account_id": null, "account_name": null, "currency_code": "USD", "observation_source": null, "balance_date": "2025-12-31", "account_count": 2, "account_balance": null, "total_assets": 69788.0, "total_liabilities": 0.0, "net_worth": 69788.0},
    {"account_id": "SYN00010001", "account_name": "Chase Bank checking …0001", "currency_code": "USD", "observation_source": null, "balance_date": "2025-12-31", "account_count": null, "account_balance": 69696.79, "total_assets": null, "total_liabilities": null, "net_worth": null},
    {"account_id": "SYN00010002", "account_name": "Capital One credit card", "currency_code": "USD", "observation_source": "tabular", "balance_date": "2025-12-31", "account_count": null, "account_balance": 91.21, "total_assets": null, "total_liabilities": null, "net_worth": null}
  ],
  "actions": ["Run reports(report_id='core:networth_history', parameters={'from_date': 'YYYY-MM-DD', 'to_date': 'YYYY-MM-DD'}) for the time series"]
}
```

The totals row carries `account_id: null` and fills the four headline columns (`account_count`, `total_assets`, `total_liabilities`, `net_worth`); each account row carries only its own `account_balance` and leaves the totals columns null. One totals row per currency the profile holds.

**Mutating response — `transactions categorize commit`** (write summary):

```json
{
  "status": "ok",
  "summary": {"total_count": 50, "returned_count": 1, "has_more": false, "sensitivity": "medium", "display_currency": "USD"},
  "data": {"applied": 47, "skipped": 2, "errors": 1, "merchants_created": 3, "error_details": [{"transaction_id": "csv_xyz", "reason": "unknown category"}]},
  "actions": ["Use transactions_categorize_rules to review auto-created rules"]
}
```

Six commands are the exceptions, and each is one on purpose:

- `db query` extends `--output` to `text|json|csv|markdown|box` (DuckDB's native formats); its `json` is raw row data, **not** the envelope shape. It is the operator-bypass surface, so no privacy middleware applies either — use `sql query` when you need envelope parity and masking.
- `db info` and `db ps` report the database *file* rather than its contents — path, size, encryption and lock state, per-table row counts, and the PIDs holding it open — and stay on their own JSON shapes. `db info` also has to keep answering while the database is locked, which is the one state a typed payload cannot be read in.
- `stats`, `logs`, and `migrate status` emit operations metadata — metric values, log lines, schema versions — rather than ledger data, and stay on their own JSON shapes. `logs` in particular cannot gain masking from the envelope: its content is already-written free text, and the control that governs it is the no-PII log policy at write time.

Every other command's `--output json` is the envelope.

## Long-running commands

`sync pull`, `refresh`, and `transform apply` can run for several seconds to minutes. Progress and status lines stream to **stderr** by default (visible interactively, hidden when redirected); `--output json` returns a single envelope at completion. There is no incremental JSON progress stream today — agents that need progress should poll `sync status` / `transform status` from a separate invocation.

Concurrent **writes** against the same profile serialize on the database lock; a cron-driven `sync pull` overlapping with an interactive write retries briefly (up to 10 s) and then exits `1` rather than blocking indefinitely. Reads rarely contend with writes — write windows are per-operation rather than per-session — but a read overlapping a long write retries on the same backoff before failing. Use `db ps` to see who's holding the file and `db kill` if needed.

## Which command for which task?

The CLI has a few task-shaped overlaps; this section disambiguates the common ones.

**"Review my transactions" — three candidates, pick by intent:**

- **`transactions list`** — filtered scanning ("show me April groceries"). Supports `--account-id`, `--from`/`--to`, `--category`, `--uncategorized`, `--limit`. Returns raw rows; no workflow.
- **`transactions categorize pending`** — specifically hunting uncategorized rows for a categorization pass. Supports `--sort {date,impact}`, `--min-amount`, and `--account`.
- **`review`** — the curator queue across every pending decision: dedup/transfer matches, uncategorized rows, and account/merchant/security links. A bare `moneybin review` prints the counts; `--type` narrows to one queue and `--confirm`/`--reject` drive matches from a script.

**"Refresh / transform / categorize run — which?"**

- **`moneybin refresh`** — the right answer 99% of the time. Runs gsheet → match → transform → categorize → identity → rates in order; idempotent.
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

### `review`

What needs your attention, across every queue that holds a pending decision.

| Command | Purpose | Key flags |
|---|---|---|
| `review` | Pending counts for all five queues: matches, uncategorized, account-links, merchant-links, security-links. | `--type {all,matches,categorize,account-links,merchant-links,security-links}`, `--status`, `--interactive`, `-o/--output`, `-q` |
| `review --confirm <id>` / `--reject <id>` | Decide one pending match without opening a queue command. Requires `--type matches`. | `--confirm-all` to accept the whole match queue |

Counts are what a bare `moneybin review` prints. To see the rows behind a
count, use that queue's own command — `transactions matches pending`,
`transactions categorize pending`, `accounts links pending`, `merchants links
pending`, `investments securities links pending`. The item-by-item walk
(`--interactive`) is not built yet; the decision flags cover the same ground
non-interactively for matches.

## Ingestion

### `import`

File imports and inbox drain. `import files` auto-detects type (CSV / OFX / QFX / PDF) and runs the refresh pipeline after.

| Command | Purpose | Key flags |
|---|---|---|
| `import files <paths>...` | Import one or more financial files. Per-file overrides available. | `--account-name`, `--institution`, `--format`, `--refresh/--no-refresh` |
| `import preview <path>` | Inspect file structure without importing (dry run, no DB writes). | — |
| `import history` | List recent import batches with counts and timestamps. Default view is the batch, its status, and the rows in and rejected; `--wide` adds the source file's full path, which is what tells two same-named imports apart. | `--limit`, `--wide` |
| `import revert <batch-id>` | Undo an import batch (deletes rows from raw + downstream). | `-y, --yes` |
| `import status` | Summary of all imported data by source. | — |
| `import formats list` | List built-in and user-saved format definitions. For `--type=pdf` the default view is name, institution, routing and last-used; `--wide` adds the front end, recipe version, and use count. | `--type`, `--wide` |
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
| `sync key rotate` 🚧 | Rotate the sync server's encryption key (stub). | — |
| `sync schedule set <cron>` 🚧 | Configure a scheduled sync job (stub). | — |
| `sync schedule show` 🚧 | Show the active sync schedule (stub). | — |
| `sync schedule remove` 🚧 | Disable scheduled sync (stub). | — |

**Related guides:** [`data-import.md`](data-import.md), [`data-pipeline.md`](data-pipeline.md).

## Refresh pipeline

`refresh` is the always-visible umbrella entry point for the post-load pipeline: gsheet → match → transform → categorize → identity → rates. CLI peer of the `refresh_run` MCP tool.

| Command | Purpose | Key flags |
|---|---|---|
| `refresh` | Run the full cascade. Idempotent — safe to retry. Matching, categorization, identity, and rates are best-effort; only SQLMesh apply errors fail the command. | `--step {match,transform,categorize,identity,rates}` (repeatable; default = full cascade; gsheet runs in the unscoped default) |

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
| `transactions list` | List transactions with filters. `--cursor` takes the `next_cursor` from a previous `--output json` response; treat it as opaque and restart from page one if it is rejected. | `--account`, `--from`, `--to`, `--limit`, `--category`, `--uncategorized`, `--cursor` |
| `transactions create` | Create a manual transaction (no upstream source). | `--account-id`, `--date`, `--amount`, `--description`, `--category` |
| `transactions audit <transaction-id>` | Show the audit chain for one transaction. | — |
| `transactions review` | Deprecated alias for the top-level `review`; removed after one minor release. | Same flags as `review` |

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
| `transactions categorize assist` | Return uncategorized rows as PII-scrubbed records for LLM categorization — merchant text (description/memo) is sent in full, with only embedded PII (e.g. account numbers) masked; no amount, date, or account ID. Same shape as the `transactions_categorize_assist` MCP tool. | `--limit`, `--account-filter`, `--date-range` |
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
| `transactions categorize ml status` / `train` / `apply` 🚧 | ML-assisted categorization (stub). | — |

**Related guides:** [`categorization.md`](categorization.md).

## Curation: reference data

### `categories`

Category taxonomy. Default (seeded) categories cannot be deleted — disable them with `set --inactive`.

| Command | Purpose | Key flags |
|---|---|---|
| `categories list` | List all categories. | — |
| `categories create <name>` | Create a category. | `--parent <name>` |
| `categories set <category-id>` | Update settings (today: `--active/--inactive` only). | `--active/--inactive` |
| `categories delete <category-id>` | Hard-delete a user-created category. Refuses if referenced unless `--force`. | `--force` |

### `merchants`

Merchant name mappings.

| Command | Purpose | Key flags |
|---|---|---|
| `merchants list` | List merchant mappings. | — |
| `merchants create <pattern> <canonical>` | Create a mapping. | `--default-category` |

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
| `accounts links pending` | Provisional accounts and the merges proposed for them, with the ledger evidence behind each. | `-o/--output`, `-q` |
| `accounts links set <decision-id>` | Merge the provisional into a candidate, or keep it standalone. | `--into <account-id>`, `--standalone`, `-y, --yes` |
| `accounts links run [<id> <id>]` | With no ids, sweep every account for duplicates. With two, propose exactly that pair — the escape hatch for a duplicate no signal reaches. | `-o/--output` |

`accounts set` cascades atomically: `--archive` also sets `--exclude` for net-worth in the same write; `--unarchive` does NOT auto-restore `--include`.

**Related guides:** [`profiles.md`](profiles.md), [`data-pipeline.md`](data-pipeline.md).

### `assets`

Physical assets (real estate, vehicles, valuables). Group is reserved; commands ship with the asset-tracking spec.

### `investments`

Investment ledger, positions, tax lots, realized gains, and the securities catalog (user-created entries plus those minted during a Plaid sync). Promotes the former `accounts investments` placeholder to a top-level group. All commands support `--output json`.

| Command | Purpose | Key flags |
|---|---|---|
| `investments add` | Record one ledger event. `--type reinvest` writes the acquisition + paired income row atomically. | `--account`, `--type`, `--date`, `--security`, `--quantity`, `--price`, `--amount`, `--fees`, `--subtype`, `--acquired`, `--basis`, `--event-group`, `--currency`, `--description` |
| `investments list` | List ledger events from `core.fct_investment_transactions`. | `--account`, `--security`, `--type`, `--from`, `--to` |
| `investments holdings` | Current positions: what you hold, what it is worth, and whether you are up. A position with no usable price — or a known-wrong share count — shows `-` rather than a zero, and the `status` column beside it says which, because the two have different remedies. `--wide` adds the cost basis, the average cost, and the date the close was observed. | `--account`, `--wide` |
| `investments gains` | Realized gain/loss (the 1099-B surface) from `core.fct_realized_gains`. Default view is when it sold, what it was, what it fetched, what you made, in what currency and how it is taxed; `--wide` adds the quantity and cost basis behind the gain, plus a `note` column marking each row whose basis is incomplete. A run with any such row says so on stderr, and `-q` does not silence that. | `--account`, `--security`, `--from`, `--to`, `--term {short,long}`, `--wide` |
| `investments lots list` | Tax lots with remaining quantity and basis. Open lots only by default; `--all` returns the open-and-closed history and adds a `state` column. `--wide` shows every column, including the currency and the cost-basis method. | `--account`, `--security`, `--open/--all`, `--wide` |
| `investments lots select <disposal-txn-id>` | Set the full specific-identification lot selection for a disposal (declarative replace). Requires the security to resolve to `specific` cost basis; `--clear` reverts to FIFO and needs no election. | `--lot LOT_ID:QTY` (repeatable), `--clear` |
| `investments securities list` | List the securities catalog. | `--type` |
| `investments securities add` | Add one security to the catalog. | `--name`, `--type`, `--ticker`, `--exchange`, `--cusip`, `--isin`, `--figi`, `--coingecko-id`, `--cash-equivalent`, `--method`, `--currency` |
| `investments securities set <security-id>` | Partial update of one security. At least one field flag required. | `--name`, `--ticker`, `--exchange`, `--cusip`, `--isin`, `--figi`, `--coingecko-id`, `--method`, `--currency` |
| `investments securities links pending` | List pending security merge decisions the Plaid sync resolver couldn't auto-bind (identifier tie, stripped ticker, fuzzy name), grouped by provider ref. | — |
| `investments securities links set <decision-id>` | Accept (merge) or reject one pending decision. `--into` is a confirming check — it must equal the decision's own candidate security id. | `--accept --into <candidate-security-id>`, `--reject` |
| `investments securities links history` | Show recent security-link decisions of any status. | `--limit` |

The per-account cost-basis default is a field on `accounts set --default-cost-basis-method`; the per-security override is `investments securities set --method`.

**Related guides:** [`investments-data-model.md`](../specs/investments-data-model.md), [`sync-plaid-investments.md`](../specs/sync-plaid-investments.md).

### `fx`

Exchange rates for one currency pair on one date, and the corrections that outrank them. All commands support `--output json`.

| Command | Purpose | Key flags |
|---|---|---|
| `fx rate <from> <to> [date]` | Resolve one pair on one date and name the source. Precedence: your correction, then a cached rate, then a live fetch. Date defaults to today. | — |
| `fx list <from> <to>` | Stored rate series for one pair, newest first, with the source that won each date. Reads only what is on disk; never fetches. | `--since` |
| `fx set <from> <to> <date> <rate>` | Record your own rate for one pair and date, outranking every provider rate for that date. RATE is units of TO per one FROM. | `--note` |
| `fx delete <from> <to> <date>` | Remove a correction, returning that date to provider pricing. | — |

A weekend or holiday resolves to the last business day the provider published, and `fx rate` names that day rather than reporting the rate as the requested day's own. `fx set` writes `app.exchange_rate_overrides` with a paired audit-log row; `fx delete` is the only way to withdraw one, since `set` can only change the number.

**Related guides:** [`multi-currency.md`](../specs/multi-currency.md).

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

### Any report, any tier

These seven work on built-in, extension, and your own saved reports alike.
`HANDLE` is a report ID or a name, resolved in that order — so a name contested
across tiers still has an ID that resolves.

| Command | Purpose | Key flags |
|---|---|---|
| `reports list` | Every registered report and its tier. `--include-archived` adds the saved reports you have archived, marked `[archived]` in the tier column. | `--include-archived`, `--tier {builtin,extension,user}` |
| `reports run HANDLE` | Run one report by ID or name. | `--param key=value` (repeatable), `--limit` |
| `reports explain HANDLE` | The report's query in both forms, each column's privacy class and where it came from, its lineage, freshness, and whether it can be materialized. Runs nothing. | `--param key=value` |

### Your own reports

`create` / `set` / `delete` / `reclassify` act only on saved reports — a built-in
is a file in the repo. Privacy classes are derived from the SQL and stored; you
never declare them.

| Command | Purpose | Key flags |
|---|---|---|
| `reports create NAME` | Save a read-only SELECT as a durable report. | `--sql` or `--sql-file` (exactly one), `--description`, `--param name[:type][=default]` |
| `reports set HANDLE` | Rename, re-describe, re-query, archive, or restore. Changing SQL or parameters re-derives the privacy contract. `--clear-params` is the only way to drop every declaration. | `--name`, `--description`, `--sql`/`--sql-file`, `--param`, `--clear-params`, `--archive`, `--restore` |
| `reports delete HANDLE` | Delete permanently; `system audit undo` restores it. | `--yes` |
| `reports reclassify HANDLE` | Lower one column's masking floor. Audited, and the only path that does so. | `--column`, `--to`, `--reason` (all required), `--yes` |

```bash
uv run moneybin reports create coffee \
  --sql "SELECT merchant_name, SUM(amount) AS spend
           FROM core.fct_transactions
          WHERE category = \$category
          GROUP BY merchant_name" \
  --param category:str \
  --description "Spend per merchant in one category"

uv run moneybin reports run coffee --param category=Dining
uv run moneybin reports explain coffee --param category=Dining
```

Parameter types are `str` (default), `int`, `float`, `bool`, `date`, and
`decimal`; a parameter is required unless it declares a default. Queries may read
`core.*`, `app.*`, `reports.*`, `raw.*`, and `prep.*`, and must be row-returning
and read-only. A report reading `raw.*` or `prep.*` runs and serves rows, and
`reports explain` reports its graduation to a materialized view as `blocked` —
materialization derives only from `core.*` and `app.*`.

Two behaviours worth knowing before they surprise you:

- **`reports explain` withholds a sensitive parameter's value.** The executed SQL
  form renders a literal only for parameters classed at the lowest tier;
  everything above keeps its `$name`. Rendering is not execution, so it never
  passes through the redaction the report's own rows do — printing the value
  there would publish what every result row masks.
- **`reclassify --yes` states a human decision, and the audit row says it was a
  flag.** Nothing at a terminal can tell a person from an assistant, so the flag
  is taken at its word — but never recorded as a prompt. `system audit list
  --action user_report.set` shows `confirmed_via` as `prompt` or `flag`, so a
  downgrade an assistant confirmed on your behalf is distinguishable after the
  fact from one you approved. An assistant driving this command must not supply
  the flag unasked. With no prompt available and no `--yes`, the command refuses
  rather than assuming either answer.
- **Archiving hides a report; it does not retire it.** An archived report stays
  runnable, exportable, and explainable by ID or name — `--archive` suppresses
  catalog noise, and only the listing honours it. To retire one for good, delete
  it (`system audit undo` still brings it back). `reports list
  --include-archived` shows the hidden ones alongside the active catalog, so
  nothing is reachable-but-invisible.

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
| `db key export <path>` 🚧 | Export the key to a file (encrypted) (stub). | — |
| `db key import <path>` 🚧 | Import a key from a file (stub). | — |
| `db key verify` 🚧 | Verify the cached key matches the database (stub). | — |
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
| `mcp config` | Show active MCP server configuration (profile, database path, max-rows) and identify deprecated inert compatibility settings. | — |
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
| `synthetic generate` | Generate synthetic data into a fresh profile. | `--persona`, `--years`, `--seed` |
| `synthetic reset` | Wipe and regenerate from scratch. | `--persona`, `-y, --yes` |

Whole-pipeline scenarios live under `tests/scenarios/` and are driven via `make test-scenarios` rather than a CLI command.

## Common workflows

### Monthly close (no JSON pipelines)

```bash
moneybin sync pull                          # latest from connected banks
moneybin import files ~/Downloads/*.ofx     # any OFX files you downloaded
moneybin refresh                            # run the post-load pipeline
moneybin transactions categorize pending    # see what's still uncategorized
# ... categorize via review or transactions categorize rules ...
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

There is no dedicated `tax` command group. The reports above, `investments gains` (the 1099-B surface), and a `db query` against `core.fct_transactions` cover most tax-prep needs today.

### Drain the watched inbox

```bash
cp ~/Downloads/*.qfx "$(moneybin import inbox path)/inbox/"
moneybin import inbox            # drain; auto-refresh; processed/ + failed/ sidecars
moneybin import inbox list       # preview without moving
```

### Categorize with an LLM, agent-driven

```bash
# 1. Pull PII-scrubbed records out for the LLM (merchant text sent in full).
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
