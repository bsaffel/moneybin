# Database & Security

Every MoneyBin database is AES-256-GCM encrypted from the moment it's created. There is no unencrypted mode.

> **Threat model summary:** MoneyBin's encryption protects against device theft, cloud-sync replication (iCloud, Dropbox, Time Machine), shared-machine access, and forensic disk recovery. It does **not** protect against malware running as your user, or against an attacker who has both your database file *and* your live keychain session. **Forgotten passphrase + lost auto-key = unrecoverable data** — see the [data-loss mitigation pattern](#data-loss-mitigation) below. AI vendors see the data you ask them about; that's how the integration works. The full honest treatment is in the [Threat Model](threat-model.md).

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

Passphrase mode derives a 256-bit key from your passphrase using Argon2id (memory-hard KDF). The derived key is cached in the OS keychain for the session so you don't re-enter the passphrase on every command. KDF parameters and the full keying lifecycle are documented in [ADR-009: Encryption Key Management](../decisions/009-encryption-key-management.md).

### Data Loss Mitigation

Encryption with no back door is a feature — and it has the obvious consequence: **lose your passphrase + lose your auto-key entry, and your data is unrecoverable.** Argon2id is memory-hard by design; there is no recovery code, password reset, or admin override.

The mitigation pattern, in order of importance:

1. **After `db init`, run `moneybin db key show` once** to get the actual encryption key. Save it somewhere durable: a password manager, an encrypted file, paper in a safe — your call. If you ever lose your passphrase or migrate to a new machine without the keychain entry, you can pass this key explicitly to recover access.
2. **Run `moneybin db backup` regularly.** Backups are encrypted with the same key as the live database. A backup is only useful if you can still decrypt it — which is why item #1 comes first.
3. **Test restore at least once.** Before you trust a backup, restore it (`moneybin db restore <path>`) into a scratch profile and verify it opens.

The hosted tier (Wave 3) handles this with mandatory recovery codes at signup, but local install puts you in charge.

## Key Management

Full lifecycle management for encryption keys:

```bash
# Lock the database (clear cached key from OS keychain)
moneybin db lock

# Unlock with passphrase (cache derived key)
moneybin db unlock

# Show the current encryption key (for backup/recovery)
moneybin db key show

# Rotate to a new encryption key
moneybin db key rotate

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
