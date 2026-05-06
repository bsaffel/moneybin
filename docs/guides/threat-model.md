# Threat Model

This page is the honest one-pager: what MoneyBin's encryption and architecture protect against, and what they don't. The full design lives in [`privacy-data-protection.md`](../specs/privacy-data-protection.md); the keying decisions are in [ADR-009](../decisions/009-encryption-key-management.md). This page is the summary you'd want before trusting MoneyBin with real financial data.

## What MoneyBin's encryption protects against

Every database is AES-256-GCM encrypted from the moment it's created. DuckDB handles encryption natively; temp files are automatically encrypted when the database is. Two key modes:

- **Auto-key (default)** — a random 256-bit key generated at `db init` and stored in the OS keychain (macOS Keychain, Linux Secret Service, Windows Credential Locker). You never see the key.
- **Passphrase mode** — your passphrase is run through Argon2id (a memory-hard KDF) to derive the encryption key. The derived key is cached in the OS keychain for the session so you don't re-enter the passphrase on every command. See [ADR-009](../decisions/009-encryption-key-management.md) for KDF parameters and rotation.

Both modes defeat the same threats:

| Threat | What happens with encryption | Without encryption |
|---|---|---|
| **Stolen / lost laptop** | Disk image is unreadable without the key. The keychain is locked when the system is locked. | Full data exposure. |
| **Cloud-sync replication** (iCloud, Dropbox, Time Machine, OneDrive) | The replicated `.duckdb` file is a useless encrypted blob to the cloud provider. | Your transactions become a row in a cloud provider's storage. |
| **Shared machine access** | Other users on the same machine can't open the file without the key in their own keychain. | Any user with read access reads everything. |
| **Forensic disk recovery** | Recovered file sectors are encrypted. | Historical data recoverable from deleted-file remnants and SSD wear-leveling. |
| **Database file in a backup dump** | Encrypted blob; useless without key. | Plaintext exposure on every backup target. |

This is unusual for the personal finance space. No other open-source PFM tool encrypts the local database by default — Beancount (plain text), Actual (plain SQLite), Firefly III (plain MySQL/Postgres), Wealthfolio (plain SQLite) all rely on OS-level disk encryption. MoneyBin's encryption is database-level, on by default, and protects you even when the OS encryption doesn't (synced folders being the obvious case).

## What MoneyBin's encryption does *not* protect against

Encryption is a layer, not a magic wand. The following threats are **not** addressed by encryption-at-rest, and we'd rather you know than discover later:

### Forgotten passphrase + lost auto-key = data loss

If you used **passphrase mode** and you forget the passphrase, your data is unrecoverable. Argon2id is memory-hard by design — there is no back door, recovery code, or password reset.

If you used **auto-key mode** and you destroy the OS keychain entry (or migrate to a new machine without the keychain export), same outcome: unrecoverable.

**Mitigation pattern:**
- After `db init`, run `moneybin db key show` once to get the actual key. Save it somewhere durable (password manager, encrypted file, paper in a safe — your call). If you lose your passphrase or keychain, you can pass this key explicitly to recover access.
- Run `moneybin db backup` regularly. Backups are encrypted with the same key.
- The hosted tier (Wave 3) handles this with mandatory recovery codes at signup, but the local install puts you in charge.

### Malware running as your user

If an attacker has your OS privileges — through malware, a backdoored dependency, or social engineering — they can read whatever you can read. That includes:

- Your keychain entries (and therefore the encryption key).
- Decrypted database contents in memory while MoneyBin is running.
- Any file MoneyBin has open.

Encryption at rest raises the bar for *device theft* and *cloud sync exposure*. It does not raise the bar for *active compromise of your user account*. We document this rather than engineer around it; the right defense for active compromise is endpoint security, not application-level encryption.

### An attacker with both your DB file *and* your live keychain session

If someone walks up to your unlocked, logged-in laptop with MoneyBin open or your keychain unlocked, they can read your data. The keychain is a session-bound secret store, not a magic shield. Lock your laptop.

### Anything you ask an AI to read

This one matters specifically because MoneyBin is AI-native. **When you ask Claude, ChatGPT, Cursor, or any MCP client a question about your finances, that AI's vendor sees the data needed to answer.**

Concretely:

- Local stdio MCP (`moneybin mcp config generate --client claude-desktop`) sends data to Anthropic when you ask Claude.
- ChatGPT Desktop's MCP equivalent sends data to OpenAI.
- The hosted Streamable HTTP MCP (Wave 3) authenticates via Bearer token, but the AI vendor still sees the data needed to answer your prompt.

This isn't a bug — it's the entire point of the AI integration. We're saying it explicitly because some products imply "your data never leaves your machine" while quietly streaming it to a model API. MoneyBin doesn't.

**Practical implications:**

- Choose your AI client deliberately. Anthropic, OpenAI, Google, and others have different data-use policies; review them.
- The hosted MoneyBin tier (Wave 3) uses **zero-knowledge encryption** — *we* can't read your data, but the AI vendor you connect to still sees what it needs to answer.
- A future redaction layer is on the post-launch roadmap (`privacy-and-ai-trust.md`) — opt-in scrubbing of merchant names and amounts before MCP responses leave your machine. Not shipped yet.

## Defense in depth (layers beyond encryption)

| Layer | What it does |
|---|---|
| **AES-256-GCM at rest** | Encrypts every database file (including DuckDB temp/spill files). |
| **OS keychain** | Stores the master key; never on disk in plaintext. macOS Keychain / Linux Secret Service / Windows Credential Locker. |
| **Argon2id KDF** | Memory-hard derivation for passphrase mode; raises the cost of offline brute force. |
| **PII log sanitization** | The `SanitizedLogFormatter` masks SSNs, account numbers, dollar amounts, and other PII patterns before any log handler writes them. Prevents accidental leakage via log files, stack traces, and bug reports. |
| **Parameterized SQL** | Every SQL query uses `?` placeholders. SQL injection is not a concern in MoneyBin's threat model. |
| **Path validation** | File operations validate paths against expected directories. Prevents path traversal in import. |
| **File permissions** | Database files are created with restrictive permissions (`0600`) on POSIX systems. |
| **Single-writer lock** | Only one MCP session per profile can run at a time; concurrent writers are explicitly prevented. |

See [`docs/guides/database-security.md`](database-security.md) for the operational commands (key rotation, backup/restore, lock management).

## Hosted tier (Wave 3) — additional protections

Wave 3 brings hosted SaaS at `app.moneybin.io`. The hosted threat model extends this one:

- **Zero-knowledge passphrase model.** Server stores ciphertext only. Decryption happens in the request-handling process using a key derived from your passphrase.
- **Mandatory recovery codes.** Generated at signup, can't be skipped — fixes the "forgotten passphrase = data loss" trap.
- **Same AGPL code, self-hostable.** The hosted runtime (`moneybin-server`) is open source. You can self-host the same hosted experience.
- **Plaid OAuth handled server-side.** You never hold Plaid credentials directly.

Full hosted threat model lands with `hosted-strategy.md`'s implementation in Wave 3.

## Reporting a vulnerability

[`SECURITY.md`](../../SECURITY.md) is the disclosure path. Private vulnerability reporting via GitHub or email; acknowledgment within 48 hours; severity tiers and patch SLAs documented.
