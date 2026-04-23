# Database & Security

Every MoneyBin database is AES-256-GCM encrypted from the moment it's created. There is no unencrypted mode.

## Encryption at Rest

```bash
# Create a new encrypted database
moneybin db init

# Check encryption status
moneybin db info
```

### Key Modes

| Mode | How it works | Best for |
|------|-------------|----------|
| **Auto-key** (default) | A random 256-bit key is generated and stored in the OS keychain | Single-user machines, simplicity |
| **Passphrase** | You provide a passphrase; Argon2id derives the encryption key | Shared machines, extra security |

Auto-key mode requires zero setup — the key is generated on first use and cached in the OS keychain (macOS Keychain, Linux Secret Service, Windows Credential Locker). You never see or manage the key unless you choose to.

Passphrase mode derives a 256-bit key from your passphrase using Argon2id (memory-hard KDF). The derived key is cached in the OS keychain for the session so you don't re-enter the passphrase on every command.

## Key Management

Full lifecycle management for encryption keys:

```bash
# Lock the database (clear cached key from OS keychain)
moneybin db lock

# Unlock with passphrase (cache derived key)
moneybin db unlock

# Show the current encryption key (for backup/recovery)
moneybin db key

# Rotate to a new encryption key
moneybin db rotate-key

# Create a timestamped backup
moneybin db backup

# Restore from a backup
moneybin db restore path/to/backup.duckdb
```

### Key Rotation

`rotate-key` re-encrypts the entire database with a new key. Use this if you suspect key compromise or as a periodic security measure. The old key is invalidated — make sure you have a backup first.

### Backup & Restore

Backups are timestamped copies of the encrypted database file:

```bash
# Creates ~/.moneybin/profiles/default/backups/moneybin_20260423_143022.duckdb
moneybin db backup

# Restore from any backup
moneybin db restore ~/.moneybin/profiles/default/backups/moneybin_20260423_143022.duckdb
```

## Database Tools

```bash
# Open an interactive DuckDB SQL shell
moneybin db shell

# Open the DuckDB web UI (browser-based explorer)
moneybin db ui

# Run a one-off SQL query
moneybin db query "SELECT COUNT(*) FROM core.fct_transactions"

# Show database metadata (file size, tables, encryption, versions)
moneybin db info

# Show processes holding the database file open
moneybin db ps

# Kill stuck processes holding the database
moneybin db kill
```

The SQL shell and web UI connect to the encrypted database transparently — you don't need to manage keys manually to query your data.

## Schema Migrations

MoneyBin includes an automatic database migration system. When you update MoneyBin, schema changes are applied transparently on first run after a package update.

```bash
# Check migration state (applied, pending, drift warnings)
moneybin db migrate status

# Apply pending migrations manually
moneybin db migrate apply
```

### Features

- **Automatic upgrade** on first invocation after a package update
- **Versioned migrations** — SQL and Python migrations, applied in order
- **Drift detection** — warns if the schema doesn't match expectations
- **Stuck migration recovery** — handles interrupted migrations gracefully
- **Encrypted database support** — migrations work against encrypted databases

## Defense in Depth

| Layer | Protection |
|-------|-----------|
| **Encryption** | AES-256-GCM on every database file |
| **Key storage** | OS keychain integration (never on disk in plaintext) |
| **Log sanitization** | PII patterns (SSNs, account numbers, dollar amounts) automatically masked |
| **SQL injection** | Parameterized queries with `?` placeholders throughout |
| **Path validation** | File operations validated against expected directories |
| **File permissions** | Database files created with restrictive permissions |
