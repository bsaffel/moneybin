# CLI Restructure

## Status
<!-- draft | ready | in-progress | implemented -->
implemented

## Goal

Redesign MoneyBin's CLI command tree to reflect the product's matured architecture: profiles as first-class entities, `import` as the magical golden path, domain commands (`matches`, `categorize`, `track`) as top-level citizens, and a structure that every future spec can reference for "where does my CLI surface go?"

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
- [`net-worth.md`](net-worth.md) — `track balance/networth` commands (updated from original `balance`/`reconciliation`/`networth` top-level placement)
- [`observability.md`](observability.md) — `logs` and `stats` commands
- [`database-migration.md`](database-migration.md) — `db migrate` commands
- [`mcp-architecture.md`](mcp-architecture.md) / [`mcp-tool-surface.md`](mcp-tool-surface.md) — MCP tool/prompt enumeration
- `private/specs/distribution-roadmap.md` — `get_base_dir()` fix, first-run experience
- `private/specs/mvp-roadmap.md` — implementation leverage ordering

## Design Principles

### One profile per session

A profile is an isolation boundary. Every CLI invocation and every MCP server instance is explicitly scoped to exactly one profile. There is no runtime profile switching — this is a security property, not a limitation. It prevents data leakage between profiles and simplifies the mental model: "everything I see belongs to this profile."

The global `--profile` flag provides ephemeral override without changing the default. MCP servers receive `--profile` via client config, baked in at setup time.

### Import is the golden path

`moneybin import file` and `moneybin sync pull` are the two ways data enters the system. Both automatically execute the full pipeline: load to raw, transform (SQLMesh), match (dedup + transfers), categorize. Users should never need to manually run pipeline stages in sequence.

Individual commands (`matches run`, `categorize apply-rules`, `transform apply`) exist for configuration, troubleshooting, and power-user control — not as steps in a manual pipeline.

### Domain commands are top-level

Major feature domains that have user-facing review workflows and will grow across multiple specs deserve top-level placement: `matches`, `categorize`, `track`. Infrastructure commands (`db`, `transform`, `mcp`) are also top-level but are used less frequently.

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
+-- matches
|   +-- run [--type transfer|dedup] -- Run matcher + SQLMesh
|   +-- review                     -- Interactive: accept/reject/skip/quit
|   |     [--type transfer|dedup]    Filter by match type
|   |     [--accept <match_id>]      Non-interactive: accept specific
|   |     [--reject <match_id>]      Non-interactive: reject specific
|   |     [--accept-all]             Non-interactive: accept all pending
|   +-- log                        -- Show recent match decisions
|   |     [--type transfer|dedup]    Filter by type
|   |     [--status rejected]        Filter by status
|   |     [--debug]                  Show below-threshold pairs
|   +-- undo <match_id>            -- Reverse a match decision
|   +-- backfill                   -- One-time scan of all existing transactions
|
+-- categorize
|   +-- apply-rules                -- Run all rules against uncategorized txns
|   +-- seed                       -- Initialize default categories
|   +-- stats                      -- Coverage statistics
|   +-- list-rules                 -- List all active rules (manual + auto)
|   +-- auto-review                -- Table of pending proposals
|   +-- auto-confirm               -- Act on proposals
|   |     [--approve <id> ...]       Approve specific
|   |     [--reject <id> ...]        Reject specific
|   |     [--approve-all]            Approve all pending
|   |     [--reject-all]             Reject all pending
|   +-- auto-stats                 -- Auto-rule health metrics
|   +-- auto-rules                 -- List active auto-generated rules
|
+-- track
|   +-- balance
|   |   +-- show [--account ID] [--as-of DATE]
|   |   +-- assert <account_id> <date> <amount> [--notes] [--yes]
|   |   +-- list [--account ID]
|   |   +-- delete <account_id> <date> [--yes]
|   |   +-- reconcile [--account ID] [--threshold AMOUNT]
|   |   +-- history [--from DATE] [--to DATE] [--interval]
|   +-- networth
|   |   +-- show [--as-of DATE]
|   |   +-- history [--from DATE] [--to DATE] [--interval]
|   +-- budget                     -- (CLI TBD when budget-tracking spec matures)
|   +-- recurring                  -- (future spec)
|   +-- investments                -- (future spec)
|
+-- export                         -- (future spec)
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
|   +-- config                     -- Show current MCP server config
|   +-- config generate            -- Generate client config
|         [--client claude-desktop|claude-code|cursor|vscode]
|         [--profile NAME]           Profile to configure (default: active)
|         [--install]                Write config to client's config file
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
Data in:        import, sync
Data quality:   matches, categorize
Tracking:       track (balance, networth, budget, recurring, investments)
Data out:       export
Operational:    logs, stats
Infrastructure: profile, db, mcp, transform
```

### Top-level command count: 12

`profile`, `import`, `sync`, `matches`, `categorize`, `track`, `export`, `logs`, `stats`, `db`, `mcp`, `transform`

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

### `mcp config generate`

Generate and optionally install MCP client configuration:

```
$ moneybin mcp config generate --client claude-desktop --profile alice --install
This will add MoneyBin (alice) to ~/.config/claude/claude_desktop_config.json
Proceed? [y/N]: y
✅ MoneyBin (alice) added to Claude Desktop config
💡 Restart Claude Desktop to pick up the change
```

Supported clients: `claude-desktop`, `claude-code`, `cursor`, `vscode`. Each profile generates a separate MCP server entry in the client config (e.g., "MoneyBin (alice)" and "MoneyBin (business)"). The server receives `--profile` as an argument, baked into the generated config.

Interactive mode (no `--client` flag) prompts the user to select a client and profile.

Scope: generate and install only. Does not edit or remove existing entries — that's the user's responsibility.

## Migration Table

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

### Phase 1: Implement now (this spec)

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
| `mcp config` / `mcp config generate --install` | Template generation + file write |
| `transform status` | Thin wrapper: `sqlmesh info` |
| `transform validate` | Thin wrapper: `sqlmesh plan --no-prompts` (dry) |
| `transform audit` | Thin wrapper: `sqlmesh audit` |
| `transform restate` | Thin wrapper: `sqlmesh restate` (with confirmation) |
| `logs clean` / `logs path` / `logs tail` | File system operations on log directory |

### Phase 2: Stub now ("not implemented" message)

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

### Phase 3: Defer to owning spec

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

These existing specs define CLI commands that should be updated to reflect the new command tree structure established by this spec:

| Spec | CLI change needed |
|---|---|
| `net-worth.md` | Move `balance`, `reconciliation`, `networth` from top-level to `track balance` and `track networth`. `reconciliation show` becomes `track balance reconcile`. |
| `observability.md` | No structural change needed — `logs` and `stats` are already top-level. Verify command signatures match. |
| `privacy-data-protection.md` | `db lock`/`unlock`/`rotate-key` already match. Add `db ps`/`db kill` references. |
| `database-migration.md` | Verify `db migrate apply/status` matches (currently specced as `data migrate apply/status`). |
| `budget-tracking.md` | Update CLI to `track budget *` when spec is rewritten. |

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
- `mcp config generate` produces valid config for each supported client
- `mcp config generate --install` writes to correct location
- `mcp config generate --profile` bakes profile into server args

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
- **Editing other tools' MCP configs.** `mcp config generate --install` adds entries only. Removal and modification are the user's responsibility.
- **CLI UX standards** (progressive disclosure, review queue patterns, output formatting). Deferred to `cli-ux-standards.md` — learn from real usage first.
- **MCP UX standards** (tool naming, error surfaces, prompt design). Deferred to `mcp-ux-standards.md`.
- **`stats` implementation.** Depends on `observability.md` metrics tables. Stubbed only.
- **`db migrate` implementation.** Depends on `database-migration.md` migration framework. Stubbed only.
