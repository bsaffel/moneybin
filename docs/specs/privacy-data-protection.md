# Feature: Data Protection

## Status
<!-- draft | ready | in-progress | implemented -->
in-progress

## Goal
Protect financial data at rest through DuckDB encryption, centralized connection
management, file permission enforcement, and PII log sanitization — so that a stolen
laptop, cloud-synced directory, or shared machine never exposes transaction history,
account numbers, or spending patterns.

## Background
- [ADR-002: Privacy Tiers](../decisions/002-privacy-tiers.md) — data custody model
  (Local Only / Encrypted Sync / Managed). This spec governs **data at rest** in the
  Local Only tier; `privacy-and-ai-trust.md` governs **data in motion** to AI backends.
- [ADR-004: E2E Encryption](../decisions/004-e2e-encryption.md) — age + Argon2 encryption
  for Plaid sync payloads flowing through the server. Different problem from local file
  encryption (this spec).
- [ADR-005: Security Tradeoffs](../decisions/005-security-tradeoffs.md) — threat model
  for the sync tier.
- [ADR-013: Encryption Key Management](../decisions/013-encryption-key-management.md) —
  Argon2id KDF for passphrase mode, key retrieval chain, key rotation design.
- [DuckDB Encryption Extension](https://duckdb.org/2025/11/19/encryption-in-duckdb) —
  AES-256-GCM encryption at rest, introduced in DuckDB v1.4.
- `CLAUDE.md` Security section and `.claude/rules/security.md` — existing PII logging
  and SQL injection rules that this spec formalizes and extends.
- [`database-migration.md`](database-migration.md) — migration system that must work
  against encrypted databases. This spec defines the connection contract it depends on.
- [`privacy-and-ai-trust.md`](privacy-and-ai-trust.md) — data sensitivity taxonomy
  referenced by the log sanitization rules.

### Competitive context
No open-source personal finance tool encrypts the local database by default. See
`private/specs/strategic-analysis.md` §1 for the full competitor comparison. This is a
genuine differentiator: MoneyBin is the only tool in this space where a copied database
file is a useless encrypted blob without the key.

## Threat Model

Every design decision in this spec is grounded in a concrete threat. The two highest-
impact threats — device theft and cloud sync exposure — are the primary motivation for
encryption at rest.

| Threat | Likelihood | Impact | Primary defense |
|---|---|---|---|
| **Device theft/loss** | Moderate | Full exposure — single portable `.duckdb` file | **Encryption at rest** |
| **Cloud sync exposure** (iCloud, Dropbox, Time Machine) | High | Invisible replication to unintended cloud providers | **Encryption at rest** |
| **Shared machine access** | Low-moderate | Full read access — DuckDB has no built-in auth | Encryption + **file permissions** |
| **Temp file leakage** (crash leaves spill files) | Low | Partial exposure of in-flight query data | Encryption (DuckDB encrypts temps) + **file permissions** |
| **Log/error PII leakage** | Moderate | Partial exposure via log files, stack traces, bug reports | **Log sanitization** |
| **File permission drift** | Low-moderate | Other local users can read the database | **File permissions** + encryption as defense in depth |
| **Forensic recovery** (deleted files, SSD wear-leveling) | Low | Historical data recoverable from disk sectors | **Encryption at rest** |
| **Malware / process-level exfiltration** | Low | Full exposure — malware has user's OS privileges | **Not protected** (honest limitation) |

### Honest limitations
Encryption at rest does **not** protect against malware running as the user. If an
attacker has the user's OS privileges, they can read whatever the user can read —
including keychain entries and decrypted database contents in memory. Passphrase mode
raises the bar slightly (attacker must keylog the passphrase or extract from keychain),
but auto-key mode offers no additional protection here. This is documented, not
engineered around.

## Requirements

### Encryption at Rest
1. All new databases are encrypted with AES-256-GCM via DuckDB's encryption extension.
   Unencrypted databases are never the default state.
2. The `httpfs` extension is loaded for OpenSSL-backed encryption writes (hardware AES
   acceleration, negligible overhead).
3. DuckDB temp files are automatically encrypted when the database is encrypted.
4. Two key modes are supported, chosen at `db init` time:
   - **Auto-key (default):** Random 256-bit key generated and stored in OS keychain.
     Zero daily friction. Protects against device theft and cloud sync exposure.
   - **Passphrase:** User provides a passphrase, key derived via Argon2id
     ([ADR-013](../decisions/013-encryption-key-management.md)). Derived key cached in
     OS keychain until explicitly locked. Stronger posture for shared machines.

### Key Management
5. Key retrieval chain: OS keychain (`keyring` library) → `MONEYBIN_DATABASE__ENCRYPTION_KEY`
   env var → `DatabaseKeyError` with actionable instructions.
6. OS keychain is the primary storage backend: macOS Keychain, Linux Secret Service,
   Windows Credential Manager — all abstracted by `keyring`.
7. Env var fallback (`MONEYBIN_DATABASE__ENCRYPTION_KEY`) is the CI/headless path. The
   user/CI system is responsible for securing the env var.
8. The encryption key is never stored in config files, `.env` files, settings, or any
   file on disk.
9. Passphrase mode supports `lock` (clear cached key from keychain) and `unlock` (prompt
   for passphrase, derive key, cache in keychain).
10. Key rotation (`rotate-key`) re-encrypts the database with a new key via DuckDB's
    `COPY FROM DATABASE` mechanism — attach old with current key, create new with new
    key, copy, swap files, update keychain.

### Secret Retrieval (`SecretStore`)
11. A `SecretStore` class centralizes all local secret management. It is the only
    module that imports `keyring`. Three operations:
    - **`get_key(name)`**: OS keychain → `MONEYBIN_{NAME}` env var →
      `SecretNotFoundError`. Used for encryption keys (database, e2e).
    - **`set_key(name, value)` / `delete_key(name)`**: Write to / clear from OS
      keychain. Used by CLI commands (`db init`, `db lock`, `db unlock`,
      `db rotate-key`) to manage key lifecycle. SecretStore does not own the
      lifecycle logic — it provides the keychain interface.
    - **`get_env(name)`**: `MONEYBIN_{NAME}` env var → `SecretNotFoundError`. Used
      for API keys, server credentials — secrets that don't need keychain storage.
12. `SecretStore` does not cache, derive, rotate, or orchestrate secret lifecycle.
    Passphrase derivation (Argon2id) and rotation sequencing live in the CLI commands
    that call `set_key()` / `delete_key()`. `SecretStore` is the keychain and env var
    interface — nothing more.
13. `SecretStore` replaces the orphaned `secrets_manager.py` module entirely.
14. All consumers access secrets through `SecretStore` — the `Database` class for
    encryption keys, `MoneyBinSettings` for sensitive config values, future sync
    clients for server API keys. No other module imports `keyring` or reads secret
    env vars directly.

### Connection Management (`Database` Class)
15. A single `Database` class in `src/moneybin/database.py` is the sole entry point for
    all database access — CLI commands, MCP server, loaders, and services.
16. One long-lived read-write connection per process. No read-only / read-write
    coordination. No connection pooling.
17. The `Database` class owns the full initialization sequence:
    a. Retrieve encryption key via `SecretStore().get_key("DATABASE__ENCRYPTION_KEY")`
    b. Open in-memory DuckDB connection
    c. Load required extensions (`httpfs`)
    d. Attach encrypted database file via `ATTACH ... (ENCRYPTION_KEY ?)`
    e. `USE <attached_db>`
    f. Run `init_schemas()` (idempotent baseline DDL)
    g. Run `MigrationRunner.apply_all()` (pending schema migrations)
    h. Check SQLMesh version, run `sqlmesh migrate` if needed
    i. Record version state in `app.versions`
    j. Connection is ready
18. The `Database` class exposes:
    - `conn` property — the underlying `duckdb.DuckDBPyConnection`
    - `execute(query, params)` — parameterized SQL execution
    - `sql(query)` — convenience for parameter-free queries
    - `close()` — close connection and release resources
19. A module-level `get_database()` function provides singleton access, following the
    `get_settings()` pattern.
20. The `Database` class does NOT own query logic, transaction boundaries, domain rules,
    or data access patterns. It is infrastructure — the schema is the API.
21. When `MONEYBIN_NO_AUTO_UPGRADE=1` is set, the initialization sequence skips steps
    (g), (h), and (i) but still performs encryption, attachment, and schema init.

### Service Layer Contract
22. Services are stateless functions that receive a `Database` instance as their first
    parameter. They never receive `db_path: Path` or raw `duckdb.DuckDBPyConnection`.
23. Services use `db.execute()` for parameterized queries and `db.conn` when bulk
    operations require the raw connection (e.g., `Database.ingest_dataframe()`).
24. Services return typed results — dataclasses or Pydantic models. Never raw dicts,
    tuples, or JSON strings.
25. Services define their own domain exceptions for expected failures (e.g.,
    `CategoryNotFoundError`, `ImportValidationError`). Services catch infrastructure
    exceptions (DuckDB errors, file I/O errors) and re-raise as domain exceptions with
    operational context (IDs, counts, status codes — never financial data or PII).
26. Callers (CLI commands, MCP tools) are responsible for translating domain exceptions
    to exit codes or error envelopes. Services never format output for a specific
    consumer.
27. No shared base exception class is required. The convention is: domain exceptions
    carry clean, PII-free messages. The `SanitizedLogFormatter` (logging) and privacy
    middleware (MCP) act as independent safety nets at output boundaries.

### File Permissions
28. Database file created with `0600` (owner read-write only) on macOS/Linux.
29. Data directories (`data/<profile>/`, `backups/`, `temp/`, `raw/`) created with `0700`.
30. Log files created with `0600`.
31. On every database open, validate file permissions — warn (not fail) if the database
    file is group- or world-readable. Warning includes the fix command.
32. Windows: file permission enforcement deferred to distribution phase. Encryption is
    the primary control. See §Windows Support.

### Temp File Hygiene
33. DuckDB temp directory explicitly configured via `SET temp_directory` to
    `data/<profile>/temp/` — not the system `/tmp`.
34. Temp directory created with `0700` permissions.
35. DuckDB encrypts temp files automatically when the database is encrypted — no
    additional work needed beyond ensuring the database is encrypted.

### PII Sanitization in Logs and Errors
36. A custom `SanitizedLogFormatter` scans formatted log output for PII patterns before
    they reach the log file:
    - Account number patterns (sequences of 8+ digits)
    - SSN patterns (NNN-NN-NNNN)
    - Dollar amounts ($N,NNN.NN or similar)
    - Known high-sensitivity field names in structured output
37. When a pattern matches: mask it in the output and emit a separate `WARNING`-level
    entry identifying the leak source (module, line number).
38. The formatter masks and emits — it never suppresses log entries.
39. The formatter is a runtime safety net, not a substitute for writing clean log
    statements. Developers are still responsible for following the PII rules in
    `CLAUDE.md` and `.claude/rules/security.md`.
40. Error messages returned to users (CLI output, MCP responses) follow the same rules:
    generic descriptions, never raw financial data. CLI error handlers catch specific
    exceptions and return clean messages.

### What must NEVER appear in logs or error output
- Account numbers, routing numbers, SSNs
- Transaction amounts or balances
- Full transaction descriptions or merchant names
- Full names, addresses, phone numbers
- Encryption keys or passphrases

### What CAN appear
- Record counts ("Loaded 142 transactions")
- Entity IDs (transaction_id, account_id)
- Masked identifiers ("account ...1234")
- Category labels, institution names (low sensitivity)
- Status codes, error types, operation names
- File paths (not file contents)

### Database Backup and Restore
41. `db backup` creates a timestamped copy of the encrypted database file in the backup
    directory. Backups are encrypted with the same key — safe to store anywhere.
42. `db restore` lists available backups, lets the user pick one (or `--from <path>`),
    auto-backs-up the current database first, then swaps files.
43. Restore attempts the current encryption key first. If it fails (backup from before a
    key rotation), prompts for the original key. On success with an old key, re-encrypts
    the restored database with the current key.
44. No automatic backup rotation or cleanup in v1. Users manage old backups manually.

## Data Model

No new tables. This spec modifies how the database is opened and accessed, not what's
stored in it.

**Configuration changes to `DatabaseConfig`:**

```python
class DatabaseConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    path: Path = Field(...)
    backup_path: Path | None = Field(
        default=None,
        description="Backup directory. Defaults to data/<profile>/backups/",
    )
    create_dirs: bool = Field(...)

    # New fields
    encryption_key_mode: Literal["auto", "passphrase"] = Field(
        default="auto",
        description="How the encryption key is managed: auto-generated or user passphrase",
    )
    temp_directory: Path | None = Field(
        default=None,
        description="DuckDB temp spill directory. Defaults to data/<profile>/temp/",
    )
```

`backup_path` and `temp_directory` resolve to profile-relative defaults in
`MoneyBinSettings.__init__`, following the existing pattern for `raw_data_path`.

**Data directory structure:**

```
~/.moneybin/                              # or CWD in dev mode (MONEYBIN_HOME)
  data/<profile>/
    moneybin.duckdb                       # 0600, encrypted (AES-256-GCM)
    backups/                              # 0700
      moneybin_2026-04-18_143201.duckdb   # timestamped encrypted copies
    temp/                                 # 0700, DuckDB temp spill (encrypted)
    raw/                                  # 0700, imported source files
  logs/<profile>/
    moneybin.log                          # 0600
```

## Implementation Plan

### Files to Create
- `src/moneybin/secrets.py` — `SecretStore` class, `SecretNotFoundError` exception.
  Sole `keyring` consumer: `get_key()`, `set_key()`, `delete_key()` for keychain
  secrets; `get_env()` for sensitive env vars.
- `src/moneybin/database.py` — `Database` class, `get_database()` singleton,
  `DatabaseKeyError` exception. Uses `SecretStore` for key retrieval.
- `src/moneybin/log_sanitizer.py` — `SanitizedLogFormatter` with PII pattern detection
- `src/moneybin/cli/commands/db_encrypt.py` — `lock`, `unlock`, `key`, `rotate-key`
  subcommands (or integrated into existing `db.py`). Uses `SecretStore.set_key()` /
  `delete_key()` for keychain operations.
- `tests/moneybin/test_secrets.py` — `SecretStore` unit tests (keychain hit, env
  fallback, missing secret, set/delete)
- `tests/moneybin/test_database.py` — `Database` class unit tests
- `tests/moneybin/test_log_sanitizer.py` — formatter tests with PII patterns

### Files to Modify
- `src/moneybin/config.py` — add `encryption_key_mode`, `temp_directory` to
  `DatabaseConfig`; add `backup_path` default resolution to `MoneyBinSettings.__init__`;
  use `SecretStore.get_env()` for sensitive config values instead of raw `os.getenv()`
- `src/moneybin/cli/commands/db.py` — rewrite `init`, `shell`, `ui`, `query` to use
  `Database` class and `-init` temp script approach for DuckDB CLI/UI launch; add
  `info`, `backup`, `restore` commands
- `src/moneybin/schema.py` — `init_schemas` receives a connection from `Database`,
  no longer opens its own
- `src/moneybin/loaders/*.py` — replace all `duckdb.connect()` calls with
  `get_database().conn` or accept a `Database` instance
- `src/moneybin/services/import_service.py` — change from `db_path: Path` to
  `db: Database`; remove internal `duckdb.connect()` calls
- `src/moneybin/services/categorization_service.py` — change from
  `conn: DuckDBPyConnection` to `db: Database`
- `src/moneybin/services/*.py` — all other services follow the same contract
- `src/moneybin/mcp/server.py` — replace connection management
  (`refresh_read_connection`, `get_write_connection`) with `get_database()`
- `src/moneybin/cli/commands/categorize.py` — replace `duckdb.connect()` calls
- `src/moneybin/cli/commands/import_cmd.py` — replace `duckdb.connect()` calls
- `src/moneybin/logging_config.py` (or wherever logging is configured) — wire in
  `SanitizedLogFormatter`
- `src/moneybin/utils/secrets_manager.py` — delete (replaced by `secrets.py`)

### Key Decisions
- **Encryption algorithm:** AES-256-GCM (authenticated, tamper-detecting). DuckDB
  default. The alternative (AES-CTR) is faster but lacks authentication.
- **Auto-key as default:** Covers the two biggest threats (device theft, cloud sync)
  with zero friction. Passphrase mode is opt-in for users who want shared-machine
  protection.
- **Single r/w connection:** Eliminates read/write coordination complexity. Lock
  contention between multiple MoneyBin processes is handled by existing CLI tooling.
  Users who need concurrent read access use separate test environments.
- **`Database` class, not module-level function:** The multi-step initialization
  sequence (key → connect → attach → migrate → init) needs a single owner. A bare
  `get_connection()` function would scatter the recipe across entry points.
- **`Database` does not own domain logic:** The schema is the API. Well-commented
  tables and DuckDB's catalog comments make the data self-explanatory to any consumer
  (human, AI, service code). No repository pattern, no data access layer.
- **`SecretStore` as keychain/env abstraction:** Centralizes all secret retrieval
  behind a single class rather than scattering `keyring` imports and `os.getenv()`
  calls across modules. The `Database` class, CLI commands, and settings all use
  the same interface. This also makes testing straightforward — mock `SecretStore`
  instead of patching `keyring` and `os.environ` separately in every test.
- **Service layer contract as convention, not base class:** Services follow a typed
  pattern (`db: Database` → typed result → domain exceptions) without inheriting from
  a shared base. A base class would add coupling without value at this stage. The
  convention can be extracted into a base class later if the pattern proves stable.
- **`keyring` library for OS keychain:** Abstracts macOS Keychain, Linux Secret
  Service, Windows Credential Manager. Well-maintained, widely used. Accessed
  exclusively through `SecretStore`.
- **`-init` temp script for CLI/UI launch:** DuckDB CLI supports `-init <file>` to run
  SQL on startup. Combined with `-ui`, this enables seamless encrypted database access
  in the DuckDB web UI without manual paste. Temp script created with `0600`, deleted
  after launch. Falls back to printing `ATTACH` statement if `-init` + `-ui`
  combination doesn't work (validated during implementation).
- **PII sanitization as a safety net:** The `SanitizedLogFormatter` catches accidental
  PII logging at runtime. It does not replace the developer's responsibility to log
  correctly — it's a backstop for the cases that slip through code review.
- **Backup = file copy:** An encrypted DuckDB file is its own backup artifact. No
  export/import step, no format conversion. Safe to store anywhere.

## CLI Interface

### Encryption Lifecycle

```
moneybin db init [--database PATH]
```
Creates an encrypted database. Prompts for key mode (auto-key default, passphrase
opt-in). On passphrase: prompts twice for confirmation, derives key via PBKDF2, stores
derived key in keychain. Runs schema init and migrations.

```
moneybin db lock
```
Passphrase mode only. Clears the cached derived key from OS keychain. Subsequent
commands fail with "Database is locked. Run `moneybin db unlock` to continue."

```
moneybin db unlock
```
Passphrase mode only. Prompts for passphrase, derives key via PBKDF2, caches in OS
keychain. Validates by attempting to attach the database — wrong passphrase errors
immediately.

```
moneybin db key
```
Prints the encryption key. Auto-key mode: prints directly from keychain. Passphrase
mode: if unlocked, prints cached key; if locked, prompts for passphrase first but does
NOT cache (does not implicitly unlock). Emits security warning.

```
moneybin db rotate-key
```
Changes the encryption key or passphrase. Attaches old database with current key,
creates new encrypted database with new key, copies via `COPY FROM DATABASE`, swaps
files, updates keychain. Warns that existing backups remain encrypted with the old key.

### Database Access

```
moneybin db shell [--database PATH]
```
Opens interactive DuckDB CLI with auto-attach via `-init` temp script.

```
moneybin db ui [--database PATH]
```
Opens DuckDB web UI with auto-attach via `-init` temp script. If DuckDB CLI is not
installed, prints installation instructions.

```
moneybin db query <SQL> [--database PATH] [--format table|csv|json|markdown|box]
```
One-shot SQL execution with auto-attach via `-init` temp script.

### Database Management

```
moneybin db info [--database PATH]
```
Prints: database file size, table counts, row counts per table, encryption status
(encrypted/unencrypted), key mode (auto/passphrase), lock state, DuckDB version,
MoneyBin version.

```
moneybin db backup [--output PATH]
```
Creates timestamped copy in backup directory. Default path:
`data/<profile>/backups/moneybin_YYYY-MM-DD_HHMMSS.duckdb`.

```
moneybin db restore [--from PATH]
```
Lists available backups. User selects one or provides `--from <path>`. Auto-backs-up
current database first. Swaps files. Tries current key; if it fails (post-rotation
backup), prompts for original key and re-encrypts with current key on success.

## MCP Interface

`db info` data (encryption status, key mode, database size) could be exposed as an MCP
resource in the future for AI clients to report on database health. Not in scope for
this spec — the CLI is the primary interface for infrastructure concerns.

## Testing Strategy

### Unit: `SecretStore`
- **`get_key` keychain hit:** keychain contains secret → returns it.
- **`get_key` env fallback:** keychain miss + `MONEYBIN_{NAME}` set → returns env var.
- **`get_key` missing:** both miss → raises `SecretNotFoundError` with actionable
  instructions.
- **`get_env`:** env var set → returns value. Missing → raises `SecretNotFoundError`.
- **`set_key` / `delete_key`:** writes to / clears from keychain (mock keyring in
  tests).
- **Isolation:** no other module imports `keyring` — verified by test or lint rule.

### Unit: `Database` class
- **Key retrieval:** delegates to `SecretStore.get_key("DATABASE__ENCRYPTION_KEY")`.
  Mock `SecretStore` in tests.
- **Initialization:** creates in-memory connection, loads `httpfs`, attaches encrypted
  file, sets temp directory, runs init_schemas, runs migrations.
- **Singleton:** `get_database()` returns same instance on repeated calls. Cache
  invalidation on profile change.
- **Auto-key mode:** `db init` generates 256-bit key, stores via
  `SecretStore.set_key()`, creates encrypted database.
- **Passphrase mode:** `db init` derives key via Argon2id, stores derived key via
  `SecretStore.set_key()`.
- **Lock/unlock:** `lock` calls `SecretStore.delete_key()`. Subsequent
  `get_database()` fails. `unlock` derives and stores via `SecretStore.set_key()`,
  `get_database()` succeeds.
- **Close:** connection is closed, resources released, subsequent `conn` access raises.

### Unit: Service layer contract
- **`Database` parameter:** services that accept `db_path: Path` or raw
  `DuckDBPyConnection` fail code review. Verified by testing that service functions
  accept `Database` instances.
- **Typed returns:** service functions return dataclass/Pydantic instances, not dicts.
- **Domain exceptions:** infrastructure errors (e.g., `duckdb.Error`) are caught and
  re-raised as domain exceptions with PII-free messages.

### Unit: `SanitizedLogFormatter`
- SSN patterns masked: `123-45-6789` → `***-**-****`
- Account number patterns masked: `12345678901234` → `****...1234`
- Dollar amounts masked: `$1,234.56` → `$***`
- Clean log lines pass through unchanged.
- Warning emitted with source location when masking occurs.
- Known field names in structured output detected and masked.

### Unit: File permissions
- Database file created with `0600`.
- Directories created with `0700`.
- Permission check warns on `0644` or `0666`.
- Permission check passes on `0600`.

### CLI: `db` commands
- `db init`: creates encrypted database, schema initialized, migrations applied.
- `db init --passphrase`: prompts for passphrase, derives key, encrypted database
  created.
- `db lock` / `db unlock`: keychain entry cleared / restored.
- `db key`: prints key (mock keychain in tests).
- `db info`: displays database metadata.
- `db backup`: creates timestamped copy in backup directory.
- `db restore`: restores from backup, auto-backs-up current first.
- `db shell` / `db ui` / `db query`: temp init script created with correct `ATTACH`,
  subprocess launched with `-init` flag.

### Integration
- Full lifecycle: `db init` → import data → `db backup` → `db restore` → verify data
  intact.
- Key rotation: `db init` → import data → `db rotate-key` → verify data accessible
  with new key.
- Migration against encrypted DB: `db init` → add migration file → restart → migration
  applied automatically.

## Dependencies
- `keyring` — OS keychain abstraction (macOS Keychain, Linux Secret Service, Windows
  Credential Manager)
- `duckdb` — encryption extension (built-in since v1.4), `httpfs` extension (for
  OpenSSL-backed writes)
- `argon2-cffi` — Argon2id passphrase key derivation (ADR-013)
- `secrets` (stdlib) — random key generation
- `re` (stdlib) — PII pattern matching in log formatter

## Out of Scope
- **Unencrypted database support.** All databases are encrypted. There is no
  `--no-encrypt` flag. Users who need an unencrypted database for debugging use
  `db shell` or `db query` to export data.
- **Column-level encryption.** DuckDB encrypts the entire file. This is sufficient for
  the local-file threat model.
- **Automatic backup scheduling.** Users run `db backup` explicitly. Scheduled backups
  are a future enhancement.
- **Backup rotation / retention policies.** Users manage old backups manually in v1.
- **Key escrow or recovery.** A forgotten passphrase means lost data. This is the
  tradeoff, communicated clearly at setup.
- **Windows file permission enforcement.** Deferred to distribution phase. The `keyring`
  library and env var paths work on Windows; ACL-based permission checks require
  platform-specific implementation.
- **Malware protection.** Encryption at rest does not protect against malware running as
  the user. This is an honest, documented limitation.

## Windows Support

The `keyring` library supports Windows Credential Manager out of the box. Auto-key and
passphrase modes work identically. The env var fallback works on all platforms. The
DuckDB CLI/UI `-init` approach is platform-independent.

File permission enforcement (`chmod 0600`, permission validation on open) is
macOS/Linux-specific. Windows uses ACLs via a completely different API (`icacls` or
Win32). This is deferred to the distribution phase — encryption is the primary control
on Windows. When Windows support is implemented, the permission layer should use
platform-specific abstractions (potentially via a library like `pywin32`) behind a
common interface.

## Success Criteria
- `moneybin db init` creates an encrypted database by default with zero extra flags.
- A copied `.duckdb` file is unreadable without the encryption key.
- All `duckdb.connect()` calls in the codebase are replaced with `Database` /
  `get_database()`.
- All `keyring` imports and secret env var reads go through `SecretStore` — no direct
  `keyring` or `os.getenv()` for secrets in any other module.
- `secrets_manager.py` is deleted.
- All services accept `db: Database` as their first parameter, return typed results,
  and raise domain exceptions — no `db_path: Path` or raw `DuckDBPyConnection`.
- `moneybin db shell`, `db ui`, and `db query` open encrypted databases seamlessly.
- The `SanitizedLogFormatter` catches and masks PII patterns in log output.
- Migration system works transparently against encrypted databases.
- `db backup` / `db restore` round-trips successfully, including cross-key-rotation
  restore with original key provided.
- `db rotate-key` re-encrypts the database and updates the keychain.
- `db info` reports encryption status, key mode, and database health.
