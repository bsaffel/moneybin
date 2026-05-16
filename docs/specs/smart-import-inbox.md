# Feature: Smart Import Inbox

> Companions: [`smart-import-overview.md`](smart-import-overview.md) (umbrella), [`smart-import-tabular.md`](smart-import-tabular.md) (importer used under the hood), [`moneybin-cli.md`](moneybin-cli.md) (CLI tree), [`mcp-architecture.md`](mcp-architecture.md) (MCP namespacing, response envelope), [`privacy-data-protection.md`](privacy-data-protection.md) (file permissions)

## Status

implemented

## Goal

Give users a "drop a file here, then ask MoneyBin to import it" workflow that mirrors the watch-folder pattern they already know from Hazel, paperless-ngx, scanner workflows, and email rules — without exposing file contents to the LLM via chat-attachment uploads. A single sync action drains the inbox; success and failure are both visible on the filesystem so the same model works for CLI, MCP/chat, and a future local web UI without inventing new protocols.

## Background

Today's import surface is path-based: `moneybin import file <path>` (CLI) and `import.file` (MCP). Both are correct and private — the LLM only sees the path string, never the file's contents. But two ergonomic gaps remain:

- **Discoverability.** A new user has no obvious place to put a downloaded statement. They reach for chat-attachment upload, which exposes the entire file to the LLM (and the host's logs/cache) before any tool ever runs. The current docs don't push them toward a private alternative because there isn't a particularly natural one.
- **Batch flow.** Importing five files means typing five paths or five chat turns. There's no "drain everything new" gesture that matches how users already think about sync folders.

This spec adds a filesystem-state-as-API convention: `~/Documents/MoneyBin/<profile>/inbox/` for incoming files, `~/Documents/MoneyBin/<profile>/processed/YYYY-MM/` for successful imports, `~/Documents/MoneyBin/<profile>/failed/YYYY-MM/` for failures with structured error sidecars. A pull-style sync command drains it. The convention is forward-compatible with a local web UI (the browser visualizes folders directly) and with future interactive resolution (a question/answer protocol can be layered on top without changing the v1 surface).

### Two homes, two jobs

MoneyBin already has an app-data home at `~/.moneybin/` (database, keys, logs, profiles per `config.py`). This spec deliberately introduces a *second*, visible home for user-facing files. The split is intentional and matches the modern professional convention used by Obsidian, paperless-ngx, Logseq, Hazel, Plex, and others:

| Concern | Location | Visibility | Job |
|---|---|---|---|
| App-internal state | `~/.moneybin/` (today) | Hidden | "Don't touch this." Database, encryption keys, logs, profile metadata. |
| User-facing workspace | `~/Documents/MoneyBin/<profile>/` (this spec) | Visible | "This is yours." Inbox, processed history, failed-import sidecars, future exports/backups/reports. |

Hiding the inbox would defeat its purpose — users need to find it in Finder/Explorer to drop files into it. Putting app state in a visible folder would invite accidental modification of the database. The two homes serve genuinely different audiences (app vs. user) and should stay separate.

The existing `~/.moneybin/` location is the older Unix dotdir form of platform conventions. A future migration to `platformdirs.user_data_dir("MoneyBin")` (which gives `~/Library/Application Support/MoneyBin/` on macOS, `$XDG_DATA_HOME/moneybin/` on Linux, `%LOCALAPPDATA%\MoneyBin\` on Windows) is out of scope here but worth noting as the eventual destination for app state. This spec does not touch `~/.moneybin/`.

## Requirements

### Functional

1. **Inbox layout.** A configured root directory (default `~/Documents/MoneyBin/`) contains one subdirectory per profile. Each profile subdirectory contains three siblings: `inbox/`, `processed/`, `failed/`. The trio are conventions inside each profile dir, not separately configurable. Default full path for the active profile is `<inbox_root>/<profile>/{inbox,processed,failed}/`.
   - **Per-profile isolation.** All inbox operations act on the active profile's subdirectory only. Cross-profile imports are not silently possible — switching profiles (via `--profile` flag or `MoneyBinSettings.profile`) switches which inbox is drained. This mirrors the existing `~/.moneybin/profiles/<profile>/` isolation for app state.
2. **Auto-create.** All three directories are created on first call to any inbox CLI command or MCP tool. Missing directories are not an error condition; they are reified.
3. **File permissions.** Inbox parent and all three subdirectories are created with mode `0700` (owner read/write/execute only), matching the database-file posture defined by `privacy-data-protection.md`.
4. **Account-by-subfolder.** A file located at `inbox/<account-slug>/<filename>` is imported with `account_name=<account-slug>` (slug fed through the existing fuzzy resolver in `ImportService`). A file in the `inbox/` root is imported with no account hint and relies on auto-detection (OFX, multi-account CSVs, etc.).
5. **Drain semantics.** A successful import moves the file to `processed/YYYY-MM/<original-filename>`. A failed import moves the file to `failed/YYYY-MM/<original-filename>` and writes a sidecar `<original-filename>.error.yml` with structured error details. Filename collisions in destination directories are resolved by appending a numeric suffix (`-1`, `-2`, …) before the extension.
6. **Error sidecar contract.** Failure sidecars are YAML with at least these fields: `error_code` (machine-readable identifier), `stage` (which import stage failed), `message` (human-readable summary), `suggestion` (when applicable, what the user can do next), and structured hints relevant to the error (e.g., `available_accounts` for `needs_account_name`).
7. **Idempotent re-runs.** Re-running sync over an empty inbox is a no-op success. Re-importing an already-processed file (user copies it back into inbox) is handled by the existing content-hash dedup; the file moves to `processed/` again with a numeric suffix and the import is recorded as a duplicate (zero new transactions).
8. **Concurrency safety.** Sync acquires an exclusive lockfile at `<inbox_root>/<profile>/.inbox.lock` (per-profile lock; concurrent syncs across different profiles are allowed). A second concurrent sync of the same profile returns `inbox_busy` immediately rather than queuing.
9. **Atomic file movement.** Each file is processed in three filesystem steps: source → staging path inside the destination directory → final destination. Movement uses `os.rename` (atomic on the same filesystem). A crash mid-import leaves the file either in `inbox/` (not yet moved), in a discoverable `staging-*` path inside `processed/` or `failed/`, or at its final destination — never partially written or duplicated. A startup recovery pass (run at the start of every sync) cleans up stale `staging-*` entries by reverting them to `inbox/`.
10. **Out-of-scope on inbox/processed/failed boundaries.** Sync only acts on regular files directly inside `inbox/` (root) or directly inside `inbox/<single-subfolder>/`. Nested subfolders deeper than one level, symlinks, and hidden files (starting with `.`) are skipped with an `ignored` entry in the response. This rules out sync recursing into `processed/`, `failed/`, or accidentally following a symlink out of the home directory.
11. **CLI surface.** `moneybin import inbox` drains the active profile's inbox. `moneybin import inbox list` previews without moving. `moneybin import inbox path` prints the active profile's inbox parent (`<inbox_root>/<profile>/`). All three live under the existing `import` group per `moneybin-cli.md` and respect the global `--profile` flag.
12. **MCP surface.** `import_inbox_sync` drains. `import_inbox_list` previews. Both are `low` sensitivity (return aggregate counts, filenames, and error codes — never file contents).

### Non-Functional

13. **No background processes.** v1 is pull-only. No filesystem watcher, no daemon, no launchd/systemd integration.
14. **No interactive resolution protocol.** v1 reports failures structurally; the LLM, the user, or a future web UI resolves by moving the file (typically into the right `<account-slug>/` subfolder) and re-running sync. v2 may layer a question/answer protocol on top.
15. **Response envelopes.** MCP tools return the standard envelope per `mcp-architecture.md`. Sync results carry `summary` (counts), `data.processed`, `data.failed`, `data.skipped`, `data.ignored`, and `actions` hints (e.g., "Resolve files in failed/2026-05/").
16. **Logging.** Filenames may appear in logs. File contents, account numbers, and amounts must not. The existing `SanitizedLogFormatter` covers the latter as a safety net.
17. **Observability.** Add metrics per `observability.md`: `inbox_sync_total{outcome=processed|failed|skipped|ignored}` counter, `inbox_sync_duration_seconds` histogram.

## Data Model

No new database tables. The inbox is filesystem state; the database side is unchanged because each file's import path goes through the existing `ImportService.import_file()`.

Existing tables touched indirectly via `ImportService`:

- `raw.import_batches` — receives one row per processed file (existing behavior).
- `core.fct_transactions`, `core.dim_accounts`, etc. — populated by the existing pipeline.

## Implementation Plan

### Files to Create

- `src/moneybin/services/inbox_service.py` — `InboxService` class encapsulating directory layout, locking, the recovery pass, file movement, and the sync/list operations. Calls `ImportService.import_file()` for each file. Returns a dataclass result (`InboxSyncResult`) with `processed`, `failed`, `skipped`, `ignored` lists.
- `src/moneybin/cli/import_inbox.py` — Typer subcommands `inbox`, `inbox list`, `inbox path` registered under the existing `import` group.
- `src/moneybin/mcp/tools/import_inbox.py` — `import_inbox_sync` and `import_inbox_list` MCP tools (or extend `import_tools.py` if it stays small).
- `tests/services/test_inbox_service.py` — service-level unit + integration tests.
- `tests/cli/test_import_inbox.py` — CLI subprocess tests.
- `tests/mcp/test_import_inbox_tools.py` — MCP tool tests.

### Files to Modify

- `src/moneybin/config.py` — add `ImportSettings` submodel with `inbox_root: Path = Path.home() / "Documents" / "MoneyBin"`. Wire into `MoneyBinSettings`. The active-profile inbox path (`<inbox_root>/<profile>/`) is derived at access time, not stored, so a profile switch picks up the new path without restart.
- `src/moneybin/cli/import_.py` (or wherever the `import` group lives) — register the new subcommands.
- `src/moneybin/mcp/_registration.py` — register the two new tools.
- `src/moneybin/metrics/registry.py` — add the two new metrics.
- `docs/specs/INDEX.md` — add an entry under "Smart Import."
- `README.md` — add a roadmap entry (📐) and a brief mention in the import section.

### Key Decisions

- **Why root-dir config, not per-profile dirs.** Configuring `inbox_root` and deriving `<profile>/{inbox,processed,failed}/` keeps the surface tiny. A user who wants to relocate moves one setting, not three-times-N-profiles. The trio always travels together because the lifecycle of a file requires all three; profile dirs always travel together with the root because they share the same "user-facing workspace" purpose.
- **Why per-profile subdirs, not a shared inbox with a sync flag.** A shared inbox with `--profile` selecting the destination is the dangerous variant: drop a business statement into the inbox, forget the active profile, sync runs against `personal`, file lands in the wrong DB. Per-profile subdirs make the destination visible at file-drop time. They also mirror the existing `~/.moneybin/profiles/<profile>/` isolation for app state — same mental model on both sides.
- **Why YAML sidecars (vs. JSON or DB rows).** Sidecars need to be human-readable, hand-editable in a pinch, and discoverable from the filesystem alone. YAML matches the rest of the project's user-facing structured data (test fixtures, format definitions). DB rows would require sync to be the only path that learns about failures — bad for forward-compat with a web UI that wants to render `failed/` directly.
- **Why atomic-rename with a staging pass.** `os.rename` is the only widely portable atomic filesystem operation, and a crash is otherwise indistinguishable from "import in progress" when scanning. The recovery pass at sync start ("any `staging-*` entries? move them back to inbox") makes the recovery model explicit.
- **Why `0700` permissions.** Inbox files are about to become database rows that are encrypted at rest. Anything weaker on the staging directory would be the weakest link.
- **Why no interactive resolution in v1.** A user "moves the file into the right folder and re-runs sync" is a gesture they already understand from a hundred other tools. A bespoke pending-question protocol is real surface area (schema, retry semantics, idempotence on partial answers) that we should only build with evidence it's needed. The LLM, with `Bash` and a structured error sidecar, can drive the move-and-retry flow conversationally without us having one.

## CLI Interface

```text
moneybin import inbox            Drain the inbox: import all eligible files,
                                 move successes to processed/, failures to failed/.
moneybin import inbox list       Show what a sync would do, without moving anything.
moneybin import inbox path       Print the configured inbox parent path
                                 (handy for shell composition).
```

Output format follows `cli-ux-standards.md` (planned) and the existing `import file` command. `--output json` returns the same structure as the MCP envelope's `data` field.

Example session:

```text
$ cp ~/Downloads/chase-april-2026.csv $(moneybin import inbox path)/inbox/chase-checking/
# (path resolves to ~/Documents/MoneyBin/<active-profile>/)
$ moneybin import inbox
✓ chase-checking/chase-april-2026.csv  →  imported (47 transactions)
Done: 1 imported, 0 failed.
```

Failure example:

```text
$ moneybin import inbox
✓ chase-checking/march.ofx              →  imported (118 transactions)
✗ unknown-bank.csv                      →  failed (needs_account_name)
                                           See ~/MoneyBin/failed/2026-05/unknown-bank.csv.error.yml
Done: 1 imported, 1 failed.
```

## MCP Interface

Two new tools under the existing `import.*` namespace.

### `import_inbox_sync`

- **Sensitivity:** `low` (returns counts, filenames, error codes; never file contents)
- **Args:** none
- **Behavior:** runs the same operation as `moneybin import inbox`.
- **Response data:**
  ```json
  {
    "processed": [
      {"filename": "chase-checking/march.ofx", "transactions": 118, "import_id": "..."}
    ],
    "failed": [
      {"filename": "unknown-bank.csv", "error_code": "needs_account_name",
       "moved_to": "failed/2026-05/unknown-bank.csv",
       "sidecar": "failed/2026-05/unknown-bank.csv.error.yml",
       "available_accounts": ["chase-checking", "chase-credit", "amex-platinum"]}
    ],
    "skipped": [],
    "ignored": [{"path": ".DS_Store", "reason": "hidden_file"}]
  }
  ```
- **Actions hints:** `"Move failed files into inbox/<account-slug>/ and re-run import_inbox_sync"` when any failures are returned.

### `import_inbox_list`

- **Sensitivity:** `low`
- **Args:** none
- **Behavior:** lists files in the inbox without moving them. Returns the same shape as `inbox_sync`, but every entry is in a fourth top-level array `would_process` (rather than `processed`/`failed`), and items carry a `predicted_outcome` field (`auto_detect`, `account_from_folder`, `would_fail:<error_code>`).

### Tool visibility

Both tools live in the core `import` namespace and are visible at session connect (no `moneybin.discover` step). This matches existing `import.file` and is appropriate because importing files is one of the primary user goals.

## Testing Strategy

Tests at every layer per `testing.md`:

- **Service layer (`test_inbox_service.py`)** — primary correctness coverage:
  - Auto-creation of inbox/processed/failed with `0700` permissions on first call.
  - Account-from-subfolder resolution (matches existing slug, doesn't match → falls through to existing `ImportService` resolution path).
  - File-in-root with auto-detectable format (OFX, multi-account CSV) → success.
  - File-in-root with single-account CSV (no embedded account info) → fails with `needs_account_name`, sidecar written, file in `failed/YYYY-MM/`.
  - Filename collisions in destination → numeric suffix appended.
  - Atomic rename + recovery: kill mid-import (simulate by leaving a `staging-*` directory), assert recovery on next sync moves the file back to inbox.
  - Lock contention: second concurrent sync returns `inbox_busy`.
  - Hidden files, symlinks, deeply-nested subfolders → ignored entries with reasons.
  - Idempotence: empty inbox sync is a no-op success; re-imported file (content hash already known) lands in `processed/` with a numeric suffix and zero new transactions.
- **CLI (`test_import_inbox.py`)** — subprocess-style tests covering `inbox`, `inbox list`, `inbox path` happy paths and the failure-with-sidecar output.
- **MCP (`test_import_inbox_tools.py`)** — envelope shape, sensitivity tier, actions hints in the failure case.
- **E2E** — extend the existing E2E suite with a "drop file in inbox, run sync, query transactions" scenario per `e2e-testing.md`.

Synthetic data: existing `synthetic` fixtures suffice; the inbox service is composed on top of the importer rather than introducing new data shapes.

## Synthetic Data Requirements

None new. Existing tabular and OFX fixtures used by `smart-import-tabular.md` cover the import path; the inbox layer wraps the importer without changing what gets imported.

## Dependencies

- No new Python packages. `os.rename`, `pathlib.Path`, and `fcntl.flock` (or `portalocker` if cross-platform locking becomes a concern — punt until needed) are stdlib.
- Pre-requisite features: `smart-import-tabular.md` (implemented), the existing `ImportService`, `MoneyBinSettings`.

## Out of Scope

- **Background watcher / auto-import on file drop.** Pure pull-only in v1.
- **Interactive resolution protocol.** Failures land in `failed/` with structured sidecars; resolution is "move and retry," driven by user, LLM, or future web UI.
- **Auto-pruning of `processed/` and `failed/`.** Manual cleanup only. Revisit if disk pressure shows up; would add `MoneyBinSettings.import.processed_retention_days` / `failed_retention_days` and a `moneybin import inbox prune` command.
- **Web UI.** This spec defines the filesystem contract a future web UI will consume; the UI itself is a separate spec.
- **Multi-machine sync of the inbox.** If a user has the inbox path inside a Dropbox/iCloud folder, MoneyBin treats it as a normal local directory. Cross-machine race conditions are the user's problem in v1 (the lockfile is local).
- **Per-account aliasing beyond slug match.** Subfolder name maps to `account_name` and goes through the existing fuzzy resolver. Users who need richer mappings can pre-create the account in MoneyBin and use its slug as the folder name.
