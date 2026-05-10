# CLI Restructure

## Status
<!-- draft | ready | in-progress | implemented -->
in-progress

> **v2 revision (2026-05-02, in-progress):** v1 of this spec is implemented. v2 supersedes the taxonomy decisions (dissolves the `track` group, introduces entity groups `accounts` and `transactions`, adds top-level `categories`, `merchants`, `assets`, `reports`, `system`, separates `tax`, unifies the rule across CLI / MCP / future HTTP). Profile system, pipeline orchestration, and infrastructure command groups are unchanged. Implementation pass moves status `ready` → `in-progress`. See [Revision History](#revision-history) and [Migration v1 → v2](#migration-v1--v2).

## Goal

Define a single command taxonomy that holds across MoneyBin's three primary interfaces (CLI, MCP, future HTTP). Specifically: profiles as first-class entities, `import` as the magical golden path, **entity groups** (`accounts`, `transactions`) that own their per-instance workflows and aggregations, a **reports** group for cross-cutting analytical views, and infrastructure commands as top-level peers. Every future spec references this document for "where does my surface go?" — and the answer is the same shape regardless of which interface is being designed.

## Background

The current CLI grew organically during early development. Several pain points:

- **`config` is a grab bag.** Profile management, credential validation, and config display are conflated. There's no way to list profiles or manage their lifecycle.
- **`data` subgroup is incoherent.** It houses extractors (being superseded by smart import), SQLMesh transforms, and categorization — three unrelated concerns.
- **`data extract *` causes confusion.** Users see both `import file` and `data extract ofx/csv/w2`. The old extractors should not compete with the smart importer.
- **Profiles are implicit.** Created as a side effect of `db init`, with no way to enumerate, create, or delete them intentionally.
- **`get_base_dir()` defaults to `cwd`.** This breaks for `pip install` users who have no repo checkout.
- **MCP lock management lives under `mcp`.** `mcp show`/`mcp kill` are about database connections, not MCP.

This spec establishes the target CLI surface that all other specs reference for command placement.

Related specs and docs:
- [`privacy-data-protection.md`](privacy-data-protection.md) — `db lock/unlock/rotate-key` commands
- [`smart-import-tabular.md`](smart-import-tabular.md) — `import file` flags and detection engine
- [`sync-overview.md`](sync-overview.md) — `sync` subcommands
- [`matching-same-record-dedup.md`](matching-same-record-dedup.md) / [`matching-transfer-detection.md`](matching-transfer-detection.md) — `matches` commands
- [`categorization-auto-rules.md`](categorization-auto-rules.md) — `categorize auto-*` commands
- [`net-worth.md`](net-worth.md) — `accounts balance` / `accounts networth` commands (v2)
- [`observability.md`](observability.md) — `logs` and `stats` commands
- [`database-migration.md`](database-migration.md) — `db migrate` commands
- [`mcp-architecture.md`](mcp-architecture.md) / [`mcp-tool-surface.md`](mcp-tool-surface.md) — MCP tool/prompt enumeration

## Design Principles

### One profile per session

A profile is an isolation boundary. Every CLI invocation and every MCP server instance is explicitly scoped to exactly one profile. There is no runtime profile switching — this is a security property, not a limitation. It prevents data leakage between profiles and simplifies the mental model: "everything I see belongs to this profile."

The global `--profile` flag provides ephemeral override without changing the default. MCP servers receive `--profile` via client config, baked in at setup time.

### Import is the golden path

`moneybin import file` and `moneybin sync pull` are the two ways data enters the system. Both automatically execute the full pipeline: load to raw, transform (SQLMesh), match (dedup + transfers), categorize. Users should never need to manually run pipeline stages in sequence.

Individual commands (`matches run`, `categorize apply-rules`, `transform apply`) exist for configuration, troubleshooting, and power-user control — not as steps in a manual pipeline.

### Entity groups own their workflows and aggregations

Top-level groups represent **entities** (`accounts`, `transactions`) or **cross-cutting concerns** (`reports`, `import`, `sync`, `export`, infrastructure). Per-instance workflows and aggregations live *under* their entity, not as siblings. This applies uniformly across CLI, MCP, and HTTP.

Concretely:

- `accounts` owns its per-account workflows (`balance`) and its aggregation (`networth`)
- `transactions` owns its per-transaction workflows (`matches`, `categorize`) and entity ops (`list`, `show`, `search`)
- `reports` holds analytical lenses on transaction-level data (spending, cashflow, tax, budget vs actual) — cross-cutting, read-only

The rule replaces v1's "domain commands are top-level" principle, which produced a flat surface (`matches`, `categorize`, `track`) that hid the entity hierarchy and forced unrelated things (balance, budget, recurring) into one bucket.

### Sub-group naming

Sub-group names are the natural English name for the workflow or concept. Usually a noun (`balance`, `networth`, `matches`); a verb-form is fine when it names the workflow more clearly than the equivalent noun (`categorize` rather than `categorization`). Action verbs at the leaf are always imperative single-word: `list`, `show`, `assert`, `accept`, `reject`, `apply`, `delete`.

### Universal flags

All commands support these flags (per `mcp-architecture.md` CLI symmetry):

| Flag | Purpose |
|---|---|
| `--profile NAME` / `-p` | Specify active profile (ephemeral, does not change default) |
| `--verbose` / `-v` | Enable debug logging |
| `--output json\|table` | Output format (default: `table` for humans, `json` for AI/script consumers) |
| `--yes` / `-y` | Non-interactive mode — auto-accept confirmations |

### `--validate` not `--dry-run`

Import validation (`--validate`) parses the file, detects format, and reports what it found — without writing to the database. It cannot preview downstream effects (matching, categorization) because those require the data to exist in DuckDB. The name `--validate` sets honest expectations; `--dry-run` implies a full preview that isn't feasible.

## Command Tree

### Complete target surface

```
moneybin [--profile NAME] [--verbose] [--output json|table] [--yes]
|
+-- profile
|   +-- create <name>              -- Create profile + directory + DB + keychain
|   +-- list                       -- All profiles, active marked, status
|   +-- switch <name>              -- Set active profile
|   +-- delete <name>              -- Remove profile + data (confirm)
|   +-- show [name]                -- Resolved settings (defaults to active)
|   +-- set <key> <value>          -- Set config value
|
+-- import
|   +-- file <path>                -- Smart import with full pipeline
|   |     [--validate]               Parse and validate only
|   |     [--account-name NAME]      Account name for multi-account files
|   |     [--format NAME]            Use saved tabular format
|   |     [--save-format NAME]       Save detection as reusable format
|   |     [--override MAPPING]       Override detected column mapping
|   |     [--sheet NAME]             Excel sheet selection
|   |     [--skip-transform]         Load to raw only
|   |     [--skip-match]             Skip dedup/transfer detection
|   |     [--skip-categorize]        Skip auto-categorization
|   |     [--yes]                    Non-interactive mode
|   +-- status                     -- End-to-end import health and diagnostics
|
+-- sync
|   +-- login                      -- Authenticate with moneybin-server (device flow)
|   +-- logout                     -- Clear stored JWT from keychain
|   +-- connect                    -- Connect a bank account (opens provider UI)
|   +-- disconnect --institution NAME -- Remove an institution
|   +-- pull [--force] [--institution NAME] -- Pull data + full pipeline
|   +-- status                     -- Connected institutions, health, errors
|   +-- schedule
|   |   +-- set --time HH:MM      -- Install daily sync (launchd/cron)
|   |   +-- show                   -- Current schedule details
|   |   +-- remove                 -- Uninstall scheduled job
|   +-- rotate-key                 -- Rotate E2E encryption key pair
|
+-- accounts
|   +-- list                       -- List accounts
|   +-- show <account_id>          -- Show one account
|   +-- rename <account_id> <name> -- Rename an account
|   +-- include <account_id>       -- Toggle include_in_net_worth [--no]
|   +-- archive <account_id>       -- Mark archived; cascades exclude_in_net_worth=FALSE
|   +-- unarchive <account_id>     -- Clear archived flag (does NOT restore include)
|   +-- set <account_id>           -- Bulk metadata update
|   |   [--official-name NAME] [--last-four XXXX] [--subtype TYPE]
|   |   [--holder-category CAT] [--currency CODE] [--credit-limit AMT]
|   |   [--clear-official-name] [--clear-last-four] [--clear-subtype]
|   |   [--clear-holder-category] [--clear-credit-limit]
|   +-- balance                    -- Per-account balance workflow (net-worth.md)
|   |   +-- show [--account ID] [--as-of DATE]
|   |   +-- assert <account_id> <date> <amount> [--notes] [--yes]
|   |   +-- list [--account ID]
|   |   +-- delete <account_id> <date> [--yes]
|   |   +-- reconcile [--account ID] [--threshold AMOUNT]
|   |   +-- history [--from DATE] [--to DATE] [--interval]
|   +-- investments                -- (future spec, gated on investment-tracking.md)
|
+-- assets                         -- (future spec) Physical assets (real estate, vehicles, valuables)
|                                     Workflows defined in asset-tracking.md.
|                                     Contributes to reports networth alongside accounts.
|
+-- transactions
|   +-- list                       -- List transactions [--account ID] [--from] [--to]
|   +-- show <txn_id>              -- Show one transaction
|   +-- search <query>             -- Full-text / structured search
|   +-- review                     -- Unified review queue (matches + categorize)
|   |     [--type matches|categorize|all]   Default all; walks matches first then categorize
|   |     [--status]                        Counts only, no interactive loop
|   |     [--confirm <id>]                  Non-interactive: confirm one (auto-detects type by ID)
|   |     [--reject <id>]                   Non-interactive: reject one
|   |     [--confirm-all]                   Non-interactive: confirm all in scope
|   |     [--limit N]                       Cap items per session
|   +-- matches                    -- Transfer detection + dedup workflow (no review — see transactions review)
|   |   +-- run [--type transfer|dedup]
|   |   +-- log [--type] [--status] [--debug]
|   |   +-- undo <match_id>
|   |   +-- backfill
|   +-- categorize                 -- Categorization workflow + rules (taxonomy/merchants live in top-level groups; review lives at transactions review)
|   |   +-- bulk <category_id> --txn-ids ...
|   |   +-- stats                  -- Coverage metrics
|   |   +-- rules                  -- Rule management (list, create, apply, delete)
|   |   |   +-- list
|   |   |   +-- create <pattern> <category_id>
|   |   |   +-- apply               -- Run rules against uncategorized
|   |   |   +-- delete <rule_id>
|   |   +-- auto                    -- Auto-rule proposals workflow
|   |   |   +-- review
|   |   |   +-- confirm [--approve ID ...] [--reject ID ...] [--approve-all] [--reject-all]
|   |   |   +-- stats
|   |   +-- ml                      -- ML-assisted categorization
|   |       +-- status
|   |       +-- train
|   |       +-- apply
|   +-- recurring                  -- (future spec) Recurring transaction detection
|
+-- categories                     -- Category taxonomy (reference data)
|   +-- list
|   +-- create <name> [--parent] [--icon]
|   +-- toggle <category_id>       -- Enable / disable
|   +-- delete <category_id>
|
+-- merchants                      -- Merchant mappings (reference data)
|   +-- list
|   +-- create <pattern> <canonical_name> [--default-category]
|
+-- reports                        -- Cross-domain analytical and aggregation views (read-only)
|   +-- networth                   -- Cross-domain net worth aggregation (accounts + assets)
|   |   +-- show [--as-of DATE]
|   |   +-- history [--from DATE] [--to DATE] [--interval]
|   +-- spending                   -- (future spec)
|   +-- cashflow                   -- (future spec)
|   +-- budget                     -- (future spec) Budget vs actual report
|   +-- health                     -- Cross-domain financial snapshot (was overview health)
|
+-- tax                            -- Tax domain (forms, deductions, capital gains, estimates)
|   +-- w2 <year>                  -- W-2 form data
|   +-- deductions <year>          -- Categorized deductible expenses
|                                     Future: 1099, capital_gains, estimate, carryforward
|
+-- system                         -- System / data status meta-view
|   +-- status                     -- What data exists, freshness, pending review queues
|
+-- budget                         -- (future spec) Budget target management (mutation)
|
+-- export                         -- (future spec) Export to CSV / Excel / Sheets
|
+-- logs
|   +-- clean --older-than <duration> [--dry-run]
|   +-- path                       -- Print log directory for current profile
|   +-- tail [--stream mcp|sqlmesh] [-f]
|
+-- stats                          -- Lifetime metric aggregates
|       [--since <duration>] [--metric <family>]
|
+-- db
|   +-- init [--passphrase]        -- Create encrypted DB (power user)
|   +-- shell                      -- Interactive DuckDB shell
|   +-- ui                         -- DuckDB web UI
|   +-- query <sql> [--format table|csv|json|markdown|box]
|   +-- ps                         -- Show processes holding DB open
|   +-- kill                       -- Release DB locks (confirm)
|   +-- lock                       -- Clear cached encryption key
|   +-- unlock                     -- Prompt for passphrase, cache key
|   +-- rotate-key                 -- Re-encrypt database with new key
|   +-- migrate
|       +-- apply [--dry-run]      -- Apply pending migrations
|       +-- status                 -- Show migration state
|
+-- mcp
|   +-- serve [--transport stdio|sse|streamable-http]
|   |     (catches DB lock error -> recommends db ps / db kill)
|   +-- list-tools                 -- Show available MCP tools
|   +-- list-prompts               -- Show available MCP prompts
|   +-- config path                -- Print the install-target config path for a client
|   +-- install                    -- Install MoneyBin into a client's MCP config
|         [--client claude-desktop|claude-code|cursor|vscode|chatgpt-desktop|codex|gemini-cli]
|         [--profile NAME]           Profile to configure (default: active)
|         [--print]                  Print the snippet instead of writing
|         [--yes]                    Skip the install confirmation prompt
|
+-- transform
    +-- plan [--apply]             -- Preview pending SQLMesh changes
    +-- apply                      -- Execute SQLMesh changes
    +-- status                     -- Show current model state and environment
    +-- validate                   -- Check model SQL parses and resolves
    +-- audit                      -- Run data quality assertions
    +-- restate --model NAME --start DATE -- Force recompute for a date range (confirm)
```

### Mental model

```
Entity groups:  accounts (+ balance), transactions (+ matches, categorize), assets
Reference data: categories, merchants (taxonomies that transactions reference)
Reports:        reports (networth, spending, cashflow, financial health, budget vs actual — cross-domain read-only)
Tax:            tax (forms, deductions, future capital gains)
System:         system (data status meta-view)
Data in:        import, sync
Data out:       export
Mutation:       budget (target management; vs-actual report lives under reports/budget)
Operational:    logs, stats
Infrastructure: profile, db, mcp, transform
```

### Top-level command count: 18

`profile`, `import`, `sync`, `accounts`, `transactions`, `assets`, `categories`, `merchants`, `reports`, `tax`, `system`, `budget`, `export`, `logs`, `stats`, `db`, `mcp`, `transform`

## Cross-Interface Taxonomy

The same hierarchy expresses across CLI, MCP, and (future) HTTP. Each protocol encodes the hierarchy in its native idiom; the noun ordering is identical, only the verb position and separators differ.

### The unified rule

> Hierarchy is the entity path. Verb is the leaf action. Aggregations live with their entity. Workflows live with the entity they operate on. Reports are cross-cutting analytical views.

### Encoding per protocol

| Protocol | Hierarchy | Verb position | Example |
|---|---|---|---|
| CLI | space-separated path | trailing word | `accounts balance assert <id> <date> <amt>` |
| MCP | underscore prefix | trailing token | `accounts_balance_assert` |
| HTTP | URL path | HTTP method + sub-path for non-CRUD | `POST /accounts/{id}/balances` |

### Naming examples

| Concept | CLI | MCP | HTTP |
|---|---|---|---|
| List accounts | `accounts list` | `accounts_list` | `GET /accounts` |
| Show one account | `accounts show <id>` | `accounts_get` | `GET /accounts/{id}` |
| Show current balances | `accounts balance show` | `accounts_balance_list` | `GET /accounts/balances` |
| Assert a balance | `accounts balance assert ...` | `accounts_balance_assert` | `POST /accounts/{id}/balances` |
| Balance history | `accounts balance history` | `accounts_balance_history` | `GET /accounts/{id}/balances/history` |
| Net worth now | `accounts networth show` | `accounts_networth_get` | `GET /accounts/networth` |
| List matches | `transactions matches list` | `transactions_matches_list` | `GET /transactions/matches` |
| Confirm a match | `transactions matches confirm <id>` | `transactions_matches_confirm` | `POST /transactions/matches/{id}/confirm` |
| Spending report | `reports spending` | `reports_spending_get` | `GET /reports/spending` |

### Pluralization

Pluralization is the one place clean symmetry breaks down — each protocol uses its own idiom:

| Protocol | Convention |
|---|---|
| CLI | Top-level groups plural (`accounts`, `transactions`, `reports`); sub-resource nouns named for the *concept* (singular for types: `balance`, `networth`; plural for relationship collections: `matches`); verbs always singular |
| MCP | Mirrors CLI exactly (just swap spaces for underscores) |
| HTTP | Standard REST: plural for collections (`/accounts/{id}/balances`), singular for single instances (`/accounts/{id}/balances/{date}`) |

The structural symmetry (entity → sub-resource → action ordering) holds across all three. The asymmetry — `balance` (CLI/MCP) vs `balances` (REST sub-resource) — is documented and intentional. REST readers expect plural collection paths; CLI/MCP readers don't.

### Why this rule

Three justifications:

1. **Learn-once-use-everywhere.** A user who knows `accounts balance list` already knows `accounts_balance_list` and `GET /accounts/balances`. Web UI navigation maps the same way. One mental model across four surfaces.
2. **Discoverability scales with catalog.** As MoneyBin's MCP catalog grows beyond ~30 tools, prefix-clustered names (`accounts_balance_*`, `transactions_matches_*`) sort related operations together in `mcp list-tools` output, helping both humans and LLMs scan the surface.
3. **Future HTTP is a free win.** When MoneyBin adds an HTTP layer (web UI backend, third-party integrations), the URL paths are already designed.

## Profile System

### Concept

A profile is the top-level organizational unit in MoneyBin. It isolates databases, configuration, logs, and data. Profiles can represent users, companies, analysis scenarios, or anything that needs its own financial data boundary.

### Directory structure

```
~/.moneybin/
+-- config.yaml              -- Global: active profile only
+-- profiles/
    +-- default/
    |   +-- config.yaml      -- Profile-specific settings
    |   +-- moneybin.duckdb  -- Encrypted database
    |   +-- logs/
    |   +-- temp/
    +-- business/
        +-- config.yaml
        +-- moneybin.duckdb
        +-- logs/
        +-- temp/
```

### Global config (`~/.moneybin/config.yaml`)

Minimal — only tracks the active profile:

```yaml
active_profile: default
```

### Profile config (`~/.moneybin/profiles/{name}/config.yaml`)

Generated by `profile create` with all sections and sensible defaults:

```yaml
# Profile: default
# Created: 2026-04-20

database:
  encryption_key_mode: auto    # auto (OS keychain) or passphrase
  # backup_path: null          # uncomment to enable backups

logging:
  level: INFO                  # DEBUG, INFO, WARNING, ERROR, CRITICAL
  log_to_file: true
  max_file_size_mb: 50

sync:
  enabled: false
  # server_url: null           # set when connecting to MoneyBin Sync
```

### `get_base_dir()` resolution

Replaces current logic entirely. Solves the `distribution-roadmap.md` concern.

| Priority | Source | Resolves to | Use case |
|---|---|---|---|
| 1 | `MONEYBIN_HOME` env var | Whatever the user sets | Explicit override (CI, custom installs) |
| 2 | `MONEYBIN_ENVIRONMENT=development` | `<cwd>/.moneybin` | Developer working in repo checkout |
| 3 | Repo checkout detection (`.git` + `pyproject.toml` with `name = "moneybin"`) | `<cwd>/.moneybin` | Developer who didn't set env var |
| 4 | Default | `~/.moneybin/` | Installed package user (the common case) |

This inverts the current default (which is `cwd`) to `~/.moneybin/`. Installed users get the right thing with zero config. Developers get the right thing automatically via repo detection.

### Profile resolution (unchanged priority)

`--profile` flag > `MONEYBIN_PROFILE` env var > saved default in `~/.moneybin/config.yaml` > prompt user

### Migration from current state

On first run after upgrade, if `~/.moneybin/config.yaml` exists with an old-format default profile, auto-migrate to the new directory structure. Existing `.env.*` files are read during migration but not created for new installs — all new profiles use per-profile `config.yaml`.

## Pipeline Orchestration

### `import file` golden path

When a user runs `moneybin import file`, the system executes a full pipeline:

1. **Detect & parse** — Identify format (smart-import heuristic engine), extract records
2. **Load to raw** — Write to `raw.*` tables via `Database.ingest_dataframe()`
3. **Transform** — Run SQLMesh (staging views -> core tables)
4. **Match** — Execute dedup + transfer detection against existing data
5. **Categorize** — Apply rules (manual -> user-defined -> auto-generated -> ML -> Plaid)

Default output:

```
$ moneybin import file march-statement.csv
⚙️  Importing march-statement.csv...
  Detected: CSV (Wells Fargo checking format)
  Loaded: 47 transactions (2026-03-01 -> 2026-03-31)
  Matched: 3 duplicates skipped, 2 transfers detected
  Categorized: 38 auto-classified, 4 need review
✅ Imported 44 new transactions
👀 4 uncategorized transactions and 2 transfers need review
💡 Run 'moneybin categorize auto review' or 'moneybin matches review'
```

Error handling: if any pipeline stage fails, prior stages' data is preserved (raw data is already loaded). The error message identifies which stage failed and how to retry just that stage.

### `sync pull` follows the same pattern

After fetching new data from providers, `sync pull` runs the same transform -> match -> categorize pipeline and reports identically.

### `--validate` (parse-only preview)

`--validate` parses and validates without writing:
- What format was detected
- How many records were parsed
- What date range and accounts are present
- Whether the file looks valid

It cannot preview matching or categorization results — those require the data to exist in DuckDB. The name `--validate` (not `--dry-run`) sets honest expectations.

## MCP Enhancements

### `mcp serve` error handling

When the database is locked by another process:

```
$ moneybin mcp serve
❌ Database is locked by another process
💡 Run 'moneybin db ps' to see what's holding it, or 'moneybin db kill' to release
```

### `mcp list-tools` / `mcp list-prompts`

Enumerate registered MCP tools and prompts from the server. Essential for debugging "why can't Claude see this tool?" scenarios.

### `mcp install`

Install MoneyBin into a client's MCP config (default behavior). The verb matches user intent — the previous `mcp config generate --install` invocation surfaced the install action as a flag on a generation command, which obscured the dominant use case.

```
$ moneybin mcp install --client claude-desktop --profile alice
This will add MoneyBin (alice) to ~/.config/claude/claude_desktop_config.json
Proceed? [y/N]: y
✅ MoneyBin (alice) added to Claude Desktop config
💡 Restart Claude Desktop to pick up the change
```

Supported clients: `claude-desktop`, `claude-code`, `cursor`, `vscode`, `chatgpt-desktop`, `codex`, `gemini-cli`. Each profile generates a separate MCP server entry in the client config (e.g., "MoneyBin (alice)" and "MoneyBin (business)"). The server receives `--profile` as an argument, baked into the generated config.

**Flags:**

- `--print` — emit the snippet to stdout without writing to a config file. Use when scripting, when inspecting before installing, or for clients (`chatgpt-desktop`) that have no programmatic install path. The latter case auto-falls back to print + manual setup instructions when no `--print` is passed.
- `--yes` / `-y` — skip the install confirmation prompt.
- Interactive mode (no `--client` flag) prompts the user to select a client and profile.

Scope: install only. Does not edit or remove existing entries — that's the user's responsibility. Re-running `--install` for a different profile **adds** an additional entry rather than replacing the previous one.

### `mcp config path`

Print the install-target config file path for a given client + profile, without modifying anything. Lookup-only; useful for scripting or for hand-editing a config file the user already controls.

```
$ moneybin mcp config path --client claude-desktop --profile alice
/Users/alice/Library/Application Support/Claude/claude_desktop_config.json
```

## Migration v1 → v2

This is a hard cut. No aliases, no deprecation period. v1 paths break in the same release that ships v2. MoneyBin is pre-1.0 single-user; the cost of clean is small and one-time.

### CLI moves

| v1 path | v2 path | Notes |
|---|---|---|
| `track balance show` | `accounts balance show` | Same surface, new parent |
| `track balance assert` | `accounts balance assert` | |
| `track balance list` | `accounts balance list` | |
| `track balance delete` | `accounts balance delete` | |
| `track balance reconcile` | `accounts balance reconcile` | |
| `track balance history` | `accounts balance history` | |
| `track networth show` | `reports networth show` | Cross-domain rollup (accounts + assets) — moved out of `accounts` to honor that it aggregates more than accounts |
| `track networth history` | `reports networth history` | |
| `track budget *` | `budget *` | Top-level (mutation); reports → `reports budget` |
| `track recurring *` | `transactions recurring *` | Pattern detection on transactions |
| `track investments *` | `accounts investments *` | Holdings as account-typed entity |
| `matches *` | `transactions matches *` | Workflow on transactions |
| `categorize *` (workflow + rules + auto + ml) | `transactions categorize *` | Workflow on transactions |
| `categorize categories / create-category / toggle-category` | `categories list / create / toggle / delete` | Reference-data taxonomy → top-level entity group |
| `categorize merchants / create-merchants` | `merchants list / create` | Reference-data taxonomy → top-level entity group |
| (none) | `accounts list / show / rename / include` | New entity ops |
| (none) | `transactions list / show / search` | New entity ops |
| (none) | `reports {spending, cashflow, tax, budget}` | New analytical group (subcommands future) |

The `track` group is dissolved entirely.

### MCP renames

MCP tool names migrate to the path-prefix-verb-suffix convention. As with CLI, hard cut: rename in place, update tool registry, update any client configs that reference old names.

| v1 tool name | v2 tool name |
|---|---|
| `get_net_worth` | `accounts_networth_get` |
| `get_net_worth_history` | `accounts_networth_history` |
| `get_balances` | `accounts_balance_list` |
| `get_balance_assertions` | `accounts_balance_assertions_list` |
| (existing transaction tools) | `transactions_*` prefix |
| (existing match tools) | `transactions_matches_*` prefix |
| (existing categorize tools) | `transactions_categorize_*` prefix |

Specific existing-tool renames are enumerated in `mcp-tool-surface.md` as part of v2 implementation.

## Migration Table (v0 → v1, historical)

### Removed commands

| Command | Reason |
|---|---|
| `config show` | Replaced by `profile show` |
| `config get-default-profile` | Replaced by `profile list` (marks active) |
| `config set-default-profile` | Replaced by `profile switch` |
| `config reset` | Too blunt. `profile delete` + `profile create` is more intentional |
| `config path` | Developer convenience; `profile show` includes paths |
| `config credentials validate` | Moves to relevant subsystems |
| `config credentials list-services` | Provider discovery happens through `sync connect` |
| `data extract ofx` | Superseded by `import file` |
| `data extract csv` | Superseded by `import file` |
| `data extract w2` | Superseded by `import file` |

### Moved/renamed commands

| From | To | Notes |
|---|---|---|
| `data transform plan` | `transform plan` | Promoted from dissolved `data` subgroup |
| `data transform apply` | `transform apply` | Promoted from dissolved `data` subgroup |
| `data categorize apply-rules` | `categorize apply-rules` | Promoted to top-level |
| `data categorize seed` | `categorize seed` | Promoted to top-level |
| `data categorize stats` | `categorize stats` | Promoted to top-level |
| `data categorize list-rules` | `categorize list-rules` | Promoted to top-level |
| `mcp show` | `db ps` | It's about DB connections, not MCP |
| `mcp kill` | `db kill` | Same |
| `sync all` | `sync pull` | Per sync-overview spec |

### Dissolved groups

The `config` and `data` command groups are fully dissolved. `config` is replaced by `profile`. `data`'s children are either removed (extract), promoted (categorize, transform), or no longer needed.

## Implementation Phasing

### v2 implementation pass (current)

Restructure-only. Move and rename existing commands to the new tree; rename MCP tools to the new convention. **No new functionality** — that stays with the owning specs.

| Change | Scope |
|---|---|
| Create `accounts` group with `list`, `show`, `rename`, `include` (thin entity ops) | New CLI module |
| Move `track balance` → `accounts balance` (preserve subcommands as stubs where they were) | Rename + reparent |
| Move `track networth` → `reports networth` (preserve subcommands as stubs) | Rename + reparent into reports group |
| Add top-level `assets` group (placeholder; workflows owned by `asset-tracking.md`) | New CLI module, all stubs |
| Keep `tax` top-level (not nested in `reports`) | Stub initially; tools added by `tax-*.md` specs |
| Create `transactions` group with `list`, `show`, `search` (thin entity ops) | New CLI module |
| Move `matches` → `transactions matches` (existing functionality preserved) | Reparent + update tests |
| Move `categorize` → `transactions categorize` (workflow tools + rules + auto + ml) | Reparent + update tests |
| Pull `categorize categories *` and `categorize create-category` / `categorize toggle-category` → top-level `categories` group | Promote to entity group |
| Pull `categorize merchants *` and `categorize create-merchants` → top-level `merchants` group | Promote to entity group |
| Move `track budget` → `budget` (top-level, still stub) | Rename + flatten |
| Move `track recurring` → `transactions recurring` (still stub) | Reparent |
| Move `track investments` → `accounts investments` (still stub) | Reparent |
| Dissolve `track` group | Delete CLI module |
| Add `reports` group with stubbed subcommands (`spending`, `cashflow`, `tax`, `budget`) | New CLI module, all stubs |
| Rename MCP tools to path-prefix-verb-suffix convention | Update tool registry, regenerate client configs via `mcp install` |
| Collapse `transactions matches review` and `transactions categorize review` into unified `transactions review` (CLI). Add MCP `transactions_review_status` orientation tool | New CLI command + new MCP tool |
| Rename `import_csv_preview` → `import_file_preview` (format-agnostic) | MCP tool rename + service method rename |
| Expose `sync_*` to MCP (all except `sync_rotate_key`) — login, logout, connect, disconnect, pull, status, schedule_set/show/remove | New MCP tools wrapping existing CLI sync surface |
| Expose `transform_*` to MCP (all except `transform_restate`) — status, plan, validate, audit, apply | New MCP tools wrapping existing CLI transform surface |
| Update `mcp-tool-surface.md` with new names + new MCP exposures | Doc edit |
| Update specs that reference v1 CLI paths | Doc edits across specs (see [Specs Requiring CLI Section Updates](#specs-requiring-cli-section-updates)) |

Hard cut. v1 paths break in the same release. Tests, scripts, docs, and `mcp install` output all update together.

### v1 phasing (historical, completed)

The following phases describe the original v1 implementation (completed 2026-04-20). Retained for reference; the v2 implementation pass above supersedes Phase 2/3 entries that touched `track`, `matches`, `categorize`.

#### Phase 1 (v1, historical)

Structural changes, profile system, and thin wrappers around existing tools.

| Change | Scope |
|---|---|
| `profile` command group (full lifecycle) | New: create, list, switch, delete, show, set |
| `get_base_dir()` rewrite | Rewrite config.py resolution logic |
| Per-profile config.yaml generation | Part of `profile create` |
| Migration from old config format | Auto-detect and migrate on first run |
| Remove `config` command group | Delete CLI module |
| Remove `data extract *` | Delete CLI module |
| Dissolve `data` subgroup | Move categorize + transform to top-level |
| `db ps` / `db kill` (moved from `mcp show`/`mcp kill`) | Rename |
| `db lock` / `db unlock` / `db rotate-key` | Per privacy-data-protection spec |
| `mcp serve` DB lock error handling | Add try/catch with helpful message |
| `mcp list-tools` / `mcp list-prompts` | Enumerate from tool registry |
| `mcp install` / `mcp config path` | Template generation + file write (install); install-target lookup (config path) |
| `transform status` | Thin wrapper: `sqlmesh info` |
| `transform validate` | Thin wrapper: `sqlmesh plan --no-prompts` (dry) |
| `transform audit` | Thin wrapper: `sqlmesh audit` |
| `transform restate` | Thin wrapper: `sqlmesh restate` (with confirmation) |
| `logs clean` / `logs path` / `logs tail` | File system operations on log directory |

#### Phase 2 (v1, historical) — Stub now ("not implemented" message)

Reserve the namespace. Users see the command in `--help` but get a clear message directing them to the relevant spec or future release.

| Command group | Owning spec |
|---|---|
| `sync` subcommands | `sync-overview.md` |
| `matches` group | `matching-same-record-dedup.md`, `matching-transfer-detection.md` |
| `track balance` / `track networth` | `net-worth.md` |
| `track budget` | `budget-tracking.md` |
| `track recurring` | Future spec |
| `track investments` | Future spec (gated on `investment-tracking.md`) |
| `export` | Future spec |
| `stats` | `observability.md` (depends on metrics tables) |
| `db migrate` | `database-migration.md` |

#### Phase 3 (v1, historical) — Defer to owning spec

These commands are fully implemented when their owning spec is implemented. The owning spec should reference this document for command placement and flag conventions.

| Command | Owning spec | Notes |
|---|---|---|
| `matches run/review/log/undo/backfill` | `matching-same-record-dedup.md`, `matching-transfer-detection.md` | Implementation lands with matching feature |
| `categorize auto-review/auto-confirm/auto-stats/auto-rules` | `categorization-auto-rules.md` | Implementation lands with auto-rules feature |
| `track balance show/assert/list/delete/reconcile/history` | `net-worth.md` | Updated from original top-level `balance`/`reconciliation` placement |
| `track networth show/history` | `net-worth.md` | Updated from original top-level `networth` placement |
| `track budget *` | `budget-tracking.md` | CLI TBD when spec matures |
| `track recurring *` | Future spec | Recurring transaction detection |
| `track investments *` | Future spec | Gated on `investment-tracking.md` |
| `sync login/logout/connect/disconnect/pull/status/schedule/rotate-key` | `sync-overview.md` | Full sync implementation |
| `export *` | Future spec | Export to CSV, Excel, Google Sheets |
| `stats` | `observability.md` | Depends on metrics tables and instrumentation |
| `db migrate apply/status` | `database-migration.md` | Depends on migration framework |
| `import file` pipeline orchestration (auto match + categorize) | `smart-import-tabular.md` | Detection engine + pipeline wiring |

## Specs Requiring CLI Section Updates

These existing specs define CLI commands that need updates to reflect v2's taxonomy:

| Spec | CLI change needed (v2) | MCP change needed (v2) |
|---|---|---|
| `net-worth.md` | `track balance` → `accounts balance`. `track networth` → `reports networth` (cross-domain rollup, accounts + assets). `reconciliation show` → `accounts balance reconcile`. | `get_balances` → `accounts_balance_list`, etc. `get_net_worth` → `reports_networth_get`. |
| `asset-tracking.md` | CLI namespace: top-level `assets` group (parallel to `accounts`). Net worth contribution flows through `core.agg_net_worth` consumed by `reports networth`. | Asset MCP tools take `assets_*` prefix (path-prefix-verb-suffix per v2). |
| `account-management.md` (planned) | Owns the `accounts` namespace entity ops (`list`, `show`, `rename`, `archive`, `include`). Balance subcommands stay nested per `net-worth.md`. Drafted as a separate spec to give per-account configuration, merging, and archival their own design space. | Owns `accounts_list`, `accounts_get`, `accounts_rename`, `accounts_archive`, `accounts_include`. |
| `matching-same-record-dedup.md` / `matching-transfer-detection.md` | `matches *` → `transactions matches *` | Match-related tools take `transactions_matches_*` prefix |
| `categorization-overview.md` / `categorization-auto-rules.md` / `categorize-bulk.md` | `categorize *` workflow → `transactions categorize *`. Pull category-taxonomy and merchant-mapping commands to top-level `categories *` and `merchants *` groups | Categorize workflow tools take `transactions_categorize_*` prefix; category and merchant CRUD become `categories_*` / `merchants_*` top-level |
| `budget-tracking.md` | `track budget *` → `budget *`; budget-vs-actual report goes under `reports budget` | When MCP tools are added, follow new naming |
| `mcp-tool-surface.md` | n/a | Adopt path-prefix-verb-suffix convention; enumerate all existing tool renames |
| `observability.md` | No structural change. Verify command signatures match. | n/a |
| `privacy-data-protection.md` | `db lock`/`unlock`/`rotate-key` already match. | n/a |
| `database-migration.md` | Verify `db migrate apply/status` matches. | n/a |

## Future Specs to Add

These were identified during design and should be added to the spec index:

| Spec | Type | Summary |
|---|---|---|
| `export.md` | Feature | Export analysis results to CSV, Excel, Google Sheets. First-class citizen for getting data out. |
| `cli-ux-standards.md` | Architecture | CLI interaction patterns: progressive disclosure, review queues, status command conventions, output formatting. Revisit after implementing `import` and `profile` to learn from real usage. |
| `mcp-ux-standards.md` | Architecture | MCP interaction patterns: tool naming, error surfaces, prompt design, resource conventions. Revisit after MCP tools are in production use. |

## Testing Strategy

### Profile system
- Create/list/switch/delete lifecycle
- `get_base_dir()` resolution: all four priority levels
- Migration from old config format
- Profile isolation (switching profiles changes database, logs, config)
- Profile name normalization (spaces, caps, special characters)
- Concurrent profile operations (create while another is active)

### Command routing
- All removed commands return helpful "use X instead" message (not silent failure)
- All moved commands work at their new location
- All stubbed commands show "not implemented" with owning spec reference
- `--help` output for every command group

### Pipeline orchestration
- `import file` executes full pipeline (mock each stage)
- `--skip-*` flags suppress their respective stages
- `--validate` parses without writing
- Pipeline failure at each stage preserves prior work
- Error messages identify failed stage and recovery command

### MCP enhancements
- `mcp serve` with locked DB shows helpful error
- `mcp list-tools/list-prompts` enumerate correctly
- `mcp install --print` produces valid config for each supported client
- `mcp install` writes to the correct location for the named client
- `mcp install --profile` bakes profile into server args

### Transform wrappers
- Each wrapper invokes the correct `sqlmesh` primitive
- `transform restate` requires confirmation (or `--yes`)
- Error output from `sqlmesh` is surfaced cleanly

## Dependencies

- `privacy-data-protection.md` — `db lock/unlock/rotate-key` commands, `Database` class
- `smart-import-tabular.md` — `import file` detection flags
- `sync-overview.md` — `sync` subcommand surface
- SQLMesh — `transform` wrapper commands
- `typer` — CLI framework (existing dependency)

## Out of Scope

- **Runtime profile switching in MCP sessions.** One profile per session by design. Each MCP server instance is scoped to one profile via `--profile` arg in client config.
- **Editing other tools' MCP configs.** `mcp install` adds entries only. Removal and modification are the user's responsibility.
- **CLI UX standards** (progressive disclosure, review queue patterns, output formatting). Deferred to `cli-ux-standards.md` — learn from real usage first.
- **MCP UX standards** (tool naming, error surfaces, prompt design). Deferred to `mcp-ux-standards.md`.
- **`stats` implementation.** Depends on `observability.md` metrics tables. Stubbed only.
- **`db migrate` implementation.** Depends on `database-migration.md` migration framework. Stubbed only.
- **MCP and HTTP UX standards.** Naming and hierarchy are settled here. Tool-level error surfaces, prompt design, response envelopes, content negotiation, auth, and rate limiting belong in `mcp-tool-surface.md`, `mcp-ux-standards.md`, and a future `http-api.md`. v2 only mandates the naming convention.
- **Future HTTP layer.** No HTTP server is built or designed in v2. The cross-interface taxonomy reserves the URL paths and naming so future HTTP work inherits a coherent surface.

## Revision History

| Date | Version | Summary |
|---|---|---|
| 2026-05-02 | v2 | Dissolved `track`; introduced entity groups (`accounts`, `transactions`); added `reports` group; unified taxonomy across CLI / MCP / future HTTP; renamed MCP tools to path-prefix-verb-suffix convention. Hard cut, no aliases. |
| 2026-04-20 (orig) | v1 | Initial restructure: profile system, dissolved `config`/`data`, top-level `matches`/`categorize`/`track`. Implemented. |
