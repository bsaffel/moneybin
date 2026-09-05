<!-- Last reviewed: 2026-09-04 -->
# CLI Reference

MoneyBin's CLI covers everything its MCP server does. Read commands return text or JSON with `--output json`; every interactive prompt has a flag equivalent so scripts and agents can drive the same commands. Parity is **functional, not nominal** — the same outcomes are reachable on both surfaces, but tool names don't always map 1:1 (e.g., `moneybin transactions list` reaches the MCP tool `transactions`). See [`mcp-server.md`](mcp-server.md) for the MCP catalog.

This page covers the full user-facing surface. Per-command flag detail lives in `moneybin <cmd> --help`. `--help` is always side-effect free — it does not touch profiles, open the database, or hit the network.

**Twelve commands are stubs hidden from `--help`**, so the CLI never advertises what it cannot do: `budget set`/`delete`, `sync key rotate`, `sync schedule set`/`show`/`remove`, `transactions categorize ml train`/`status`/`apply`, and `db key export`/`import`/`verify`. Each stays invocable, so a script that already calls one keeps working; the first nine exit `0` and the three `db key` names exit `1`.

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

## Command groups

Each group below names what its commands are for and the behaviour you cannot
read off `--help`. The commands themselves — every argument, option, type, and
default — are on the group's page under
[`reference/cli/`](../reference/cli/README.md), generated from the registered
command tree and checked against it in CI.

## Setup and orientation

### `profile`

Per-user profile lifecycle. Each profile has an isolated encrypted database,
config, and log directory. `profile set <key> <value>` writes one config value
(for example `logging.level info`) to the profile; `profile show` prints the
resolved settings, and `profile delete` removes the database, logs, and config
together after a confirmation. Commands:
[`reference/cli/profile.md`](../reference/cli/profile.md).

**Related guides:** [`profiles.md`](profiles.md).

### `system`

Top-level orientation: where the data lives, whether it is healthy, what the
audit log says. `system status` is the data inventory (account and transaction
counts, date range, last import) plus pending review-queue counts. `system
doctor` runs every pipeline invariant and exits non-zero when any check fails.
`system audit list` filters the audit log by actor, action, target, and date;
`system audit show <audit-id>` prints one event with its chained children.
Commands: [`reference/cli/system.md`](../reference/cli/system.md).

### `review`

What needs your attention, across every queue that holds a pending decision.
A bare `moneybin review` prints the counts for all five queues: matches,
uncategorized, account-links, merchant-links, security-links. `--type` narrows
to one queue; `--confirm <id>` and `--reject <id>` decide one pending match
without opening a queue command and require `--type matches`; `--confirm-all`
accepts the whole match queue. To see the rows behind a count, use that queue's
own command — `transactions matches pending`, `transactions categorize
pending`, `accounts links pending`, `merchants links pending`, `investments
securities links pending`. The item-by-item walk (`--interactive`) is not built
yet. Commands: [`reference/cli/review.md`](../reference/cli/review.md).

## Ingestion

### `import`

File imports and inbox drain. `import files` auto-detects the type (CSV, OFX,
QFX, PDF) and runs the refresh pipeline after loading; `import preview` reads
the file's structure without writing anything. `import history` shows each
batch with its status and rows in and rejected; `--wide` adds the source file's
full path, which is what tells two same-named imports apart. `import revert
<batch-id>` deletes the batch's rows from `raw` and everything downstream.
`import formats list` covers built-in and user-saved definitions; for
`--type=pdf` the default view is name, institution, routing, and last use, and
`--wide` adds the front end, recipe version, and use count. A bare `import
inbox` drains the watched folder — everything in `inbox/` is imported,
successes move to `processed/`, failures to `failed/` with a sidecar — while
`import inbox list` shows what a drain would do and `import inbox path` prints
the inbox parent directory for use in `$(...)` substitution. `import labels`
adds, removes, and lists labels on a batch; `import labels list --import-id`
scopes the listing to one batch. Commands:
[`reference/cli/import.md`](../reference/cli/import.md).

### `sync`

Pull transactions from external services through the moneybin-sync proxy.
**`sync login` is required first** — most subcommands fail without a valid JWT.
`sync link` links a new institution through Plaid Hosted Link or
re-authenticates a connected one, printing the URL to stderr and opening the
browser unless told not to; `sync link-status` shows the pending link state.
`sync pull` fetches new transactions — and, for brokerage and retirement
accounts, securities, investment transactions, and holdings — then runs the
refresh pipeline; without `--institution` it pulls every connected institution,
and `--force` resets the cursor and re-fetches the full history. `sync status`
shows last-sync timestamps and cursor state per institution. `sync key rotate`
and the `sync schedule` commands are stubs hidden from `--help`. Commands:
[`reference/cli/sync.md`](../reference/cli/sync.md).

**Related guides:** [`data-import.md`](data-import.md), [`data-pipeline.md`](data-pipeline.md).

## Refresh pipeline

`refresh` is the always-visible umbrella entry point for the post-load
pipeline: gsheet → match → transform → categorize → identity → rates. It is the
CLI peer of the `refresh_run` MCP tool and is idempotent, so a retry is safe.
Matching, categorization, identity, and rates are best-effort; only a SQLMesh
apply error fails the command. `--step` (repeatable, one of `match`,
`transform`, `categorize`, `identity`, `rates`) runs a subset; the gsheet pull
runs only in the unscoped default. Commands:
[`reference/cli/refresh.md`](../reference/cli/refresh.md).

The `transform` group below is the lower-level operator path. Reach for
`refresh` first.

### `transform`

Direct access to the SQLMesh pipeline: plan, apply, seed, status, validate,
audit, and restate. Use these when debugging a model or restating a date range;
for a normal post-load refresh, use `moneybin refresh`. `transform restate
--model <model> --start <date>` forces a recompute for a date range, and
`--end` defaults to today. Commands:
[`reference/cli/transform.md`](../reference/cli/transform.md).

## Curation: transactions

### `transactions`

Browsing transactions and per-transaction state: notes, tags, splits, manual
entries, audit. `transactions list` filters by account, date range, category,
and uncategorized state; its `--cursor` takes the `next_cursor` from a previous
`--output json` response — treat it as opaque and restart from page one if it
is rejected. `transactions create <amount> <description>` records a manual
transaction with no upstream source. `transactions audit <transaction-id>`
shows one transaction's audit chain. `transactions review` is a deprecated
alias for the top-level `review`, removed after one minor release. Commands:
[`reference/cli/transactions.md`](../reference/cli/transactions.md).

### `transactions notes`

Multi-note threads attached to a transaction: add, list, edit, delete.

### `transactions tags`

Slug-flavored labels applied to a transaction: add, remove, list (one
transaction, or every tag in use), and rename everywhere a tag appears.

### `transactions splits`

Allocate one transaction across several categories. A non-zero residual is a
warning, not an error. `add` takes the transaction, the amount, and
`--category`; `list` shows the splits with the residual; `remove` drops one
split row and `clear` drops them all.

### `transactions matches`

Dedup and transfer matching state. `run` matches existing transactions and can
skip the transform step or auto-accept transfers; `history` lists confirmed
matches; `undo <match-id>` reverts one; `backfill` re-matches historical rows
after a rule change.

### `transactions categorize`

Categorization workflow. Engines: deterministic rules + merchant mappings
(local, no LLM). LLM-assist is exposed as `assist` (read) → `commit` (write).
`commit` reads a JSON array of `{transaction_id, category, subcategory?}`
objects from `--input <path>` or `-` for stdin; `commit-from-file <path>` is
the convenience wrapper.

`run` executes the engine cascade over uncategorized rows in order; a rule
write blocks a merchant write at the same priority. `assist` returns
uncategorized rows as PII-scrubbed records for LLM categorization — merchant
text (description/memo) is kept as the categorization signal, scrubbed of
embedded PII (card and account numbers, phone numbers, emails, dates,
city/state); no amount, date, or account ID. It has the same shape as the
`transactions_categorize_assist` MCP tool. `export-uncategorized` writes rows
for offline review and `stats` prints coverage (total, categorized, percent,
by source).

`rules create <name>` takes a single pattern or `--from-file <path>` for a
batch; `rules apply` applies only active rules to uncategorized transactions;
`rules delete` can `--reapply` afterwards. `auto review` lists pending auto-rule
proposals with sample transactions; `auto accept` accepts or rejects proposals
by id or every pending one at once, and refuses a proposal that would match
broadly unless `--allow-broad` is passed; `auto rules` and `auto stats` show
the rules created from proposals and the activity summary. The `ml` commands
are stubs hidden from `--help`.

**Related guides:** [`categorization.md`](categorization.md).

## Curation: reference data

### `categories`

Category taxonomy: list, create (with `--parent`), set, delete. Default
(seeded) categories cannot be deleted — disable them with `set --inactive`.
`delete` refuses a category that is still referenced unless `--force` is
passed. Commands:
[`reference/cli/categories.md`](../reference/cli/categories.md).

### `merchants`

Merchant name mappings: `list`, and `create <pattern> <canonical>` with an
optional default category. Commands:
[`reference/cli/merchants.md`](../reference/cli/merchants.md).

## Accounts and balances

### `accounts`

Account entities (dim records) plus per-account workflows. `accounts list`
hides archived accounts by default. `accounts resolve <query>` fuzzy-matches a
free-text reference (for example `"my Chase account"`) to ranked account-ID
candidates; use it before any command that needs an account id. `accounts
balance show` gives the current or as-of balance per account, `balance
history` a daily series with carry-forward interpolation, `balance assert
<account-id> <date> <amount>` a point-in-time assertion that reconciles through
a delta row, `balance assertion-delete` its removal, and `balance reconcile`
the observed days whose reconciliation delta is non-zero. `accounts links
pending` lists provisional accounts and the merges proposed for them with the
ledger evidence behind each; `links set <decision-id>` merges the provisional
into a candidate or keeps it standalone; `links run` with no ids sweeps every
account for duplicates, and with two ids proposes exactly that pair — the
escape hatch for a duplicate no signal reaches. Commands:
[`reference/cli/accounts.md`](../reference/cli/accounts.md).

`accounts set` cascades atomically: `--archive` also sets `--exclude` for net
worth in the same write; `--unarchive` does NOT auto-restore `--include`. At
least one field flag is required, and each structural field has a
`--clear-<field>` twin.

**Related guides:** [`profiles.md`](profiles.md), [`data-pipeline.md`](data-pipeline.md).

### `assets`

Physical assets (real estate, vehicles, valuables). The group is reserved;
commands ship with the asset-tracking spec.

### `investments`

Investment ledger, positions, tax lots, realized gains, and the securities
catalog (user-created entries plus those minted during a Plaid sync). All
commands support `--output json`. Commands:
[`reference/cli/investments.md`](../reference/cli/investments.md).

Behaviour to know before reading the tables there:

- `investments add --type reinvest` writes the acquisition and its paired
  income row atomically.
- `investments holdings` shows what you hold, what it is worth, and whether you
  are up. A position with no usable price — or a known-wrong share count —
  shows `-` rather than a zero, and the `status` column beside it says which,
  because the two have different remedies. `--wide` adds the cost basis, the
  average cost, and the date the close was observed.
- `investments gains` is the 1099-B surface: when it sold, what it was, what it
  fetched, what you made, in what currency, and how it is taxed. `--wide` adds
  the quantity and cost basis behind the gain plus a `note` column marking each
  row whose basis is incomplete; a run with any such row says so on stderr, and
  `-q` does not silence that.
- `investments lots list` shows open lots by default; `--all` returns the
  open-and-closed history and adds a `state` column. `lots select
  <disposal-txn-id>` sets the full specific-identification lot selection for a
  disposal as a declarative replace, requires the security to resolve to the
  `specific` cost-basis method, and `--clear` reverts to FIFO.
- `investments securities links pending` lists the security merges the Plaid
  sync resolver could not auto-bind (identifier tie, stripped ticker, fuzzy
  name); `links set <decision-id> --accept --into <candidate-security-id>`
  merges, where `--into` must equal the decision's own candidate, and
  `--reject` declines.

The per-account cost-basis default is a field on `accounts set
--default-cost-basis-method`; the per-security override is `investments
securities set --method`.

**Related guides:** [`investments-data-model.md`](../specs/investments-data-model.md), [`sync-plaid-investments.md`](../specs/sync-plaid-investments.md).

### `fx`

Exchange rates for one currency pair on one date, and the corrections that
outrank them. `fx rate <from> <to> [date]` resolves one pair and names the
source, with precedence your correction, then a cached rate, then a live fetch;
the date defaults to today. `fx list <from> <to>` prints the stored series for
a pair, newest first, with the source that won each date, and never fetches.
`fx set <from> <to> <date> <rate>` records your own rate, outranking every
provider rate for that date, where the rate is units of `to` per one `from`;
`fx delete` removes a correction and returns that date to provider pricing.
All commands support `--output json`. Commands:
[`reference/cli/fx.md`](../reference/cli/fx.md).

A weekend or holiday resolves to the last business day the provider published,
and `fx rate` names that day rather than reporting the rate as the requested
day's own. `fx set` writes `app.exchange_rate_overrides` with a paired
audit-log row; `fx delete` is the only way to withdraw one, since `set` can
only change the number.

**Related guides:** [`multi-currency.md`](../specs/multi-currency.md).

## Reports

Cross-domain analytical views. All commands support `--output json` and return
the standard envelope. The eight built-in reports — `networth`,
`networth-history`, `cashflow`, `spending`, `recurring`, `merchants`,
`large-transactions`, `balance-drift` — each have their own command with the
filters that fit their grain (`--from-month`/`--to-month` on `cashflow` and
`spending`, `--from`/`--to` on `networth-history`, `--since` on
`balance-drift`, `--as-of` on snapshots, `--account` and `--category` where
they apply);
[`features.md`](../features.md#reports) says what each one shows, and the
[reports guide](reports.md) shows each one's output. Commands:
[`reference/cli/reports.md`](../reference/cli/reports.md).

### Any report, any tier

`reports list`, `reports run HANDLE`, and `reports explain HANDLE` work on
built-in, extension, and your own saved reports alike. `HANDLE` is a report ID
or a name, resolved in that order — so a name contested across tiers still has
an ID that resolves. `reports list` shows every registered report and its
tier; `--include-archived` adds the saved reports you have archived, marked
`[archived]` in the tier column. `reports run` takes `--param key=value`
(repeatable) and `--limit`. `reports explain` prints the report's query in
both forms, each column's privacy class and where it came from, its lineage,
freshness, and whether it can be materialized — and runs nothing.

### Your own reports

`create` / `set` / `delete` / `reclassify` act only on saved reports — a
built-in is a file in the repo. Privacy classes are derived from the SQL and
stored; you never declare them.

- `reports create NAME` saves a read-only SELECT as a durable report from
  `--sql` or `--sql-file` (exactly one), with `--description` and `--param
  name[:type][=default]` declarations.
- `reports set HANDLE` renames, re-describes, re-queries, archives, or
  restores. Changing the SQL or the parameters re-derives the privacy contract;
  `--clear-params` is the only way to drop every declaration.
- `reports delete HANDLE` deletes permanently; `system audit undo` restores it.
- `reports reclassify HANDLE` lowers one column's masking floor. It is audited,
  takes `--column`, `--to`, and `--reason` (all required), and is the only path
  that does so.

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

The `budget` group reserves the CLI namespace; `budget set` and `budget delete`
are stubs hidden from `--help`, and the full implementation lands with the
owning spec.

## Privacy

`privacy redact <text>` runs the redaction pipeline against an input string —
the same contract the MCP tools apply to PII-bearing fields, exposed for
debugging the redactor. Commands:
[`reference/cli/privacy.md`](../reference/cli/privacy.md).

## Database

### `db`

Lifecycle, exploration, and key management on the encrypted database. `db
init` creates the active profile's database with an auto-generated key or, with
`--passphrase`, one derived from a passphrase; `db info` reports size, tables,
encryption status, and the SQLMesh and migration versions. `db shell` and `db
ui` open DuckDB directly, with no privacy middleware. `db query <sql>` runs one
statement with `text`, `json`, `csv`, `markdown`, or `box` output; its JSON is
raw rows, not the envelope. `db lock` purges the cached key and `db unlock`
loads it back from the keychain. `db backup` writes a timestamped encrypted
backup and `db restore --from <backup-path>` (or `--latest`) restores one. `db
ps` lists the processes holding the database file and `db kill` ends them. `db
key show` prints the key to stderr and `db key rotate` re-encrypts under a new
one; `db key export`, `import`, and `verify` are stubs hidden from `--help`.
`db migrate apply` runs pending schema migrations (`--dry-run` previews) and
`db migrate status` lists applied and pending ones. Commands:
[`reference/cli/db.md`](../reference/cli/db.md).

**Related guides:** [`database-security.md`](database-security.md), [`threat-model.md`](threat-model.md).

## Integrations

### `mcp`

MCP server lifecycle and client install. `mcp serve` starts the server on
stdio by default; the other transports are unauthenticated and refuse to start
without `--insecure`. `mcp install` writes MoneyBin into an MCP client's
config; the supported clients are claude-desktop, claude-code, codex,
chatgpt-desktop, vscode, cursor, windsurf, and gemini-cli, and
`chatgpt-desktop` shares Codex's `~/.codex/config.toml` (ChatGPT on the web
cannot reach a local stdio server). `mcp list-tools` lists every registered
tool with its sensitivity tier and `mcp list-prompts` every prompt. `mcp
config` shows the active server configuration and flags deprecated inert
settings; `mcp config path` prints a client's config-file install path, which
`make claude-mcp` and similar use. Commands:
[`reference/cli/mcp.md`](../reference/cli/mcp.md).

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

- `moneybin export bundle` publishes the canonical bundle, defaulting to CSV
  and `local:exports`; `--format` picks csv, parquet, or xlsx, `--to` names a
  `local:<name>` or `sheets:<name>` destination, and `--compress zip` adds a
  ZIP.
- `moneybin export report <report-id>` executes one catalog report once,
  retains its parameters and SQL provenance, and publishes that result;
  `--param key=value` repeats, and the delivery flags match `bundle`.
- `moneybin export destination list` lists the built-in and named destinations
  with readiness.
- `moneybin export destination add local <name> <path>` adds or replaces a
  named local root.
- `moneybin export destination add sheets <name> <url>` adds or replaces an
  output-only workbook and requests Sheets write authorization.
- `moneybin export destination remove <name>` removes configuration without
  deleting files, workbooks, or tabs.

Commands: [`reference/cli/export.md`](../reference/cli/export.md).

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

`logs` and `stats` are leaf commands. `logs <stream>` views, follows, or
filters the active profile's logs, where the stream is `cli`, `mcp`, or
`sqlmesh`; the `stream` argument is required unless `--print-path` or `--prune`
is used (exit code 2 on misuse — the convention of `docker logs` and `kubectl
logs`). `stats` prints lifetime metric aggregates from `app.metrics`, narrowed
by `--since <duration>` and `--metric <family>`. Commands:
[`reference/cli/logs.md`](../reference/cli/logs.md),
[`reference/cli/stats.md`](../reference/cli/stats.md).

## Test data

### `synthetic`

Generate and manage synthetic financial data for testing and demos. Each
profile is isolated, so synthetic data never collides with real data.
`synthetic generate` writes a persona's data into a fresh profile; `synthetic
reset` wipes and regenerates from scratch. Commands:
[`reference/cli/synthetic.md`](../reference/cli/synthetic.md).

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
# 1. Pull PII-scrubbed records out for the LLM (merchant text preserved, embedded PII stripped).
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
