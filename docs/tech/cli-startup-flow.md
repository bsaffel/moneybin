# CLI Startup Flow

How a MoneyBin CLI command goes from `moneybin <cmd>` to executing user code, what happens at each stage, and where things can go wrong.

## Overview

```mermaid
flowchart TD
    A["moneybin &lt;cmd&gt; [--profile X] [-v]"] --> B[Module-level init]
    B --> C[main_callback — inert]
    C --> C1[stash flags]
    C1 --> C2["setup_observability(profile=None)"]
    C2 --> C3{explicit --profile<br/>or env var?}
    C3 -->|yes| C4[set_current_profile<br/>name only — no I/O]
    C3 -->|no| C5
    C4 --> C5{subcommand in<br/>profile/synthetic?}
    C5 -->|yes| C6[return — these manage<br/>their own lifecycle]
    C5 -->|no| C7[register lazy<br/>profile resolver]
    C7 --> J[Subcommand executes]
    C6 --> J
    J --> J1{Subcommand calls<br/>get_settings?}
    J1 -->|yes, first time| J2["resolve_profile()<br/>full chain + wizard +<br/>dir-check + log re-init"]
    J1 -->|no| J3[Skip resolution]
    J2 --> J3
    J3 --> K[Process exit]
    K --> L[atexit: flush metrics]
```

## Stage 1: Module-Level Initialization

When `moneybin.config` is first imported, a module-level variable is initialized:

```python
_current_profile: str | None = None
```

No profile is set at import time — it stays `None` until `set_current_profile()` is called, either eagerly by `main_callback` (when `--profile` or `MONEYBIN_PROFILE` is explicit) or lazily by `resolve_profile()` on first settings access. Calling `get_settings()` or `get_current_profile()` before that raises `RuntimeError`.

**Side effects:** None. No YAML reads, no directories, no database.

## Stage 2: main_callback (inert)

The Typer callback is intentionally inert. It stashes flags, runs early observability with no profile, optionally sets the active profile *name* (no I/O), and registers a lazy resolver. Heavy work — wizard, dir-check, profile-scoped logging — fires only when a command first calls `get_settings()` / `get_current_profile()`.

```mermaid
flowchart TD
    A[main_callback entry] --> A1[stash_cli_flags]
    A1 --> A2["setup_observability(stream=cli, profile=None)"]
    A2 --> B{explicit --profile<br/>or MONEYBIN_PROFILE?}
    B -->|yes| B1["set_current_profile(name)<br/>format-validate only — no I/O"]
    B1 --> B2{Bad name format?}
    B2 -->|yes| B3[BadParameter — exit 2]
    B2 -->|no| C
    B -->|no| C
    C{subcommand in<br/>profile / synthetic?}
    C -->|yes| C1[return — these manage<br/>their own profile lifecycle]
    C -->|no| D[register_profile_resolver]
    D --> E[Subcommand runs]
    C1 --> E
```

### Why inert?

`--help`, bare-group invocations (e.g. `moneybin db`), and docker-style usage errors (`moneybin logs` with no stream) MUST be side-effect free — no wizard, no profile-dir creation, no log file rotation. Click short-circuits explicit `--help` before the callback runs; bare-group exits via `NoArgsIsHelpError` before any subcommand body runs. Because the callback never calls `resolve_profile()`, all of those paths are automatically safe — no walker or argv inspection needed.

### Profile Resolution Priority (lazy path)

When a command actually needs settings, `resolve_profile()` (in `cli/utils.py`) walks this chain:

| Priority | Source | When used |
|---|---|---|
| 1 | `--profile` flag (stashed) | Always wins |
| 2 | `MONEYBIN_PROFILE` env var | If no flag |
| 3 | `ensure_default_profile()` | If neither flag nor env var |
| 4 | Interactive first-run wizard | If `ensure_default_profile()` finds no saved default |

After resolution, `resolve_profile()` validates the profile dir exists, re-initializes observability with profile-scoped log files, and emits the `Using profile: X` banner.

### Profile and Synthetic Commands Are Special

When `ctx.invoked_subcommand` is `"profile"` or `"synthetic"`, the callback returns *before* registering the lazy resolver:

- `profile create` legitimately runs against a profile that doesn't yet exist — the dir-check would block recovery.
- `synthetic` commands manage their own profile lifecycle (create, populate, tear down) and call `set_current_profile` directly.

Both rely on `get_current_profile(auto_resolve=False)` so they never trigger the lazy chain. The eager `set_current_profile(name)` in the callback still runs for these — that just sets the active name from the flag/env, which both subgroups expect.

## Stage 3: setup_observability

Called for every command. Sets up logging and registers the atexit metrics handler.

```mermaid
flowchart TD
    A[setup_observability] --> B{profile set?}
    B -->|yes| C["get_settings().logging"]
    C --> D["setup_logging(level, format,<br/>log_to_file, log_file_path)"]
    B -->|no| E["setup_logging() — defaults<br/>(console only, INFO)"]

    D --> H{log_to_file?}
    H -->|yes| I["mkdir log dir<br/>(parents=False)"]
    I -->|parent missing| J[Skip file logging]
    I -->|ok| K[Create FileHandler]
    H -->|no| L[Console only]
    E --> L

    A --> M{First init?}
    M -->|yes| N[Register atexit handler]
    M -->|no| O[Skip]

    A --> P{stream == 'mcp'?}
    P -->|yes| Q[Start periodic metrics flush timer]
    P -->|no| R[Done]
```

`setup_logging()` does not call `get_settings()`. All config values are passed explicitly by `setup_observability()`. When no profile is set (e.g. profile commands), `setup_logging()` runs with defaults: console-only, INFO level, human format.

### Directory Creation Rules

**Profile root** (`<base>/profiles/<name>/`) is only created by `ProfileService.create()`. No other code path creates it. This is the single invariant that prevents deleted profiles from being silently resurrected.

**Subdirectories** (logs, temp) are created by `ProfileService.create()` during profile setup.

**Log directory** uses `mkdir(parents=False)` — it will create `logs/` inside an existing profile dir, but won't create the profile dir itself. If the profile dir is missing, file logging is skipped gracefully.

**Database directory** uses `mkdir(parents=False, exist_ok=True)` — same principle. The profile root must already exist.

### The atexit Handler

`_flush_metrics_on_exit()` runs when the process exits. It only flushes metrics if a `Database` instance was already created during the session. It never calls `get_database()` to create a new one — that would trigger the full database initialization chain (directory creation, schema init, migrations) on shutdown, which is wrong for commands that never touch the database.

## Stage 4: Subcommand Execution

After the callback completes, Typer dispatches to the subcommand function.

### Regular Commands (import, sync, categorize, etc.)

These call `get_database()` on first use, triggering lazy initialization:

```mermaid
flowchart TD
    A["get_database() — singleton"] --> B{Instance<br/>exists?}
    B -->|yes| C[Return cached]
    B -->|no| D[get_settings]
    D --> E[SecretStore.get_key]
    E -->|not found| F["DatabaseKeyError<br/>'run moneybin db init'"]
    E -->|found| G[duckdb.connect — in-memory]
    G --> H["ATTACH encrypted file<br/>+ USE moneybin"]
    H --> I[init_schemas — idempotent DDL]
    I --> J{auto_upgrade<br/>enabled?}
    J -->|yes| K[MigrationRunner.apply_all]
    K --> L[Record version in app.versions]
    L --> M[sqlmesh migrate if version changed]
    J -->|no| N[Skip migrations]
    M --> O[Database ready]
    N --> O
```

**Note:** `Database.__init__` calls `db_path.parent.mkdir(parents=False, exist_ok=True)`. This ensures the immediate parent exists but will not recreate a deleted profile's root directory — the `parents=False` flag means the profile root must already exist.

### Profile Commands

No database access. Use `ProfileService` directly:

```mermaid
flowchart TD
    A[profile create] --> B[normalize name]
    B --> C["mkdir profile root<br/>(exist_ok=False)"]
    C -->|exists| D[ProfileExistsError]
    C -->|created| E[mkdir logs/, temp/]
    E --> F[Write config.yaml]

    G[profile delete] --> H[normalize name]
    H --> I{Is default<br/>profile?}
    I -->|yes| J["ValueError:<br/>'switch first'"]
    I -->|no| K[shutil.rmtree]

    L[profile list] --> M[Iterate profiles/ dir]
    M --> N[Return dirs with config.yaml]
```

### db shell / db ui

These are special — they don't use the `Database` class at all. They spawn a DuckDB CLI subprocess with an init script that ATTACHes the encrypted database:

```mermaid
flowchart TD
    A[db shell / db ui] --> B{DB file exists?}
    B -->|no| C["Exit 1: 'run db init'"]
    B -->|yes| D[Get encryption key from keychain]
    D -->|not found| E["Exit 1: 'run db unlock'"]
    D -->|found| F[Write temp init script<br/>with ATTACH statement]
    F --> G["subprocess.run duckdb -init script"]
    G --> H[User interactive session]
    H --> I[Cleanup temp script]
```

This bypasses all Python-level database initialization (schema init, migrations). The user gets a raw DuckDB shell.

### MCP Server

Similar to regular commands but long-running:

```mermaid
flowchart TD
    A[mcp start] --> B[get_database — full init]
    B --> C[Instantiate MCPServer]
    C --> D["fastmcp.run(transport='stdio')"]
    D --> E[Serve tools indefinitely]
    E --> F[Process exit — atexit flushes metrics]
```

Additionally, MCP mode starts a periodic metrics flush timer (every 5 minutes) since the process may run for hours.

## Stage 5: First-Run Wizard

Triggered when `ensure_default_profile()` finds no saved default and no `--profile` flag was provided.

```mermaid
flowchart TD
    A[ensure_default_profile] --> B{Default profile<br/>in config.yaml?}
    B -->|yes| C[Return it]
    B -->|no| D["Prompt: 'First name?'"]
    D --> E[Normalize name]
    E --> F["Confirm: 'Is this okay?'"]
    F -->|no| D
    F -->|Ctrl+C| G[Abort — no profile created]
    F -->|yes| H[set_default_profile<br/>writes config.yaml]
    H --> I[ProfileService.create]
    I --> J[set_current_profile]
    J --> K["Auto-init DB (best-effort)"]
    K -->|success| L[Generate key, store in keychain,<br/>create Database]
    K -->|failure| M["Suggest 'moneybin db init'"]
    L --> N[Return profile name]
    M --> N
```

**Side effects (all-or-nothing on success):**
1. `~/.moneybin/config.yaml` created/updated with `active_profile`
2. Profile directory tree created
3. Encryption key generated and stored in OS keychain
4. Encrypted database created with schema

**On Ctrl+C:** Nothing is created. The wizard re-triggers on next invocation.

## Base Directory Resolution

Where MoneyBin looks for profile data:

```mermaid
flowchart TD
    A[get_base_dir] --> B{MONEYBIN_HOME<br/>env var set?}
    B -->|yes| C["Use $MONEYBIN_HOME"]
    B -->|no| D{MONEYBIN_ENVIRONMENT<br/>== 'development'?}
    D -->|yes| E["Use cwd/.moneybin"]
    D -->|no| F{cwd is moneybin<br/>repo checkout?}
    F -->|yes| G["Use cwd/.moneybin"]
    F -->|no| H["Use ~/.moneybin/"]
```

In development (running from the repo), the base dir is `<repo>/.moneybin/`, so profiles live at `<repo>/.moneybin/profiles/<name>/`. In production, they live at `~/.moneybin/profiles/<name>/`.

## Invariants

These are the rules that prevent the bugs we've encountered:

1. **Only `ProfileService.create()` creates profile root directories.** No other code path — not `setup_logging()`, not `Database.__init__()` — creates `<base>/profiles/<name>/`. Both `setup_logging()` and `Database.__init__()` use `mkdir(parents=False)`, so they can create immediate subdirectories but will fail (gracefully) if the profile root doesn't exist.

2. **The atexit handler never creates database connections.** It only flushes metrics if a `Database` singleton was already initialized during the session. Otherwise it's a no-op.

3. **`main_callback` is inert.** It stashes flags, registers a lazy profile resolver, and (when explicit) sets the active profile *name* — but it never runs the wizard, validates dirs, or opens files. Heavy work happens only when a command calls `get_settings()` / `get_current_profile()`. This is what keeps `--help` and bare-group invocations side-effect free.

4. **Profile and synthetic commands skip the lazy resolver.** The callback returns before `register_profile_resolver()` when `invoked_subcommand` is `"profile"` or `"synthetic"`. Both subgroups call `get_current_profile(auto_resolve=False)` and manage their own profile lifecycle.

5. **`get_settings()` is a pure read.** It loads configuration from files and environment variables but creates no directories and has no filesystem side effects. Safe to call from any context. Raises `RuntimeError` if called before `set_current_profile()`.

6. **`setup_logging()` is decoupled from `get_settings()`.** All log config values (level, format, file path) are passed explicitly by `setup_observability()`. The early `setup_observability(profile=None)` in the callback gives console-only logging until the lazy resolver re-initializes with profile-scoped files.

7. **No module-level profile initialization.** `_current_profile` starts as `None`. It is set either eagerly in the callback (when an explicit name is provided) or lazily via `resolve_profile()` on first settings access. Either way, by the time any command body reads the profile, it has been set.

## Known Remaining Issues

### `ensure_default_profile` Does Too Much

The first-run wizard in `ensure_default_profile()` handles profile creation, config writing, key generation, and database initialization — all in one function in `user_config.py`. This couples user config management to database and keychain operations. A cleaner design would have the wizard only create the profile and config, with `db init` handling key/database setup separately (or as a clearly separate step).

## Simplifications Applied

The bugs we fixed were symptoms of a deeper issue: **too many things knew how to create directories**, and **startup side effects were scattered across modules that shouldn't own them**. All four simplifications have been applied:

1. **Removed `create_directories()` from `get_settings()`** — The `create_dirs` config flag and `create_directories()` method were deleted from `MoneyBinSettings`. `get_settings()` is now a pure read with no filesystem side effects.
2. **Fixed `Database.__init__` directory creation** — Changed from `mkdir(parents=True)` to `mkdir(parents=False)`, closing the last path that could recreate a deleted profile.
3. **Eliminated module-level `_current_profile`** — `_current_profile` starts as `None` instead of eagerly reading `config.yaml` at import time. `get_settings()` and `get_current_profile()` raise `RuntimeError` if called before `set_current_profile()`.
4. **Decoupled `setup_logging()` from `get_settings()`** — `setup_logging()` accepts all config values as explicit parameters. `setup_observability()` resolves them from settings when a profile is available, or uses defaults (console-only, INFO level) when not. Profile commands get proper logging without touching the settings chain.
