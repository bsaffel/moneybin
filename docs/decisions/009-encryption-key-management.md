# ADR-009: Encryption Key Management

## Status
accepted

Scope clarification, 2026-10-18: this record concerns local database keys.
[ADR-017](017-client-owned-encryption-and-recovery.md) governs the accepted
principles for device authorization, encrypted history, and recovery. It does
not change local key parameters or settle the passphrase persistence policy.
The design below persists the derived key in the keychain: access to that key
can permit decryption without the passphrase, regardless of the KDF. The
shared-machine protection claim therefore remains a policy question, not a
guarantee established by this ADR.

## Context

MoneyBin encrypts all local DuckDB databases at rest using AES-256-GCM
([`privacy-data-protection.md`](../specs/privacy-data-protection.md)). The encryption extension takes a
key string via the `ATTACH` statement's `ENCRYPTION_KEY` parameter and derives the
actual AES key internally. This ADR decides how MoneyBin generates, stores, and
retrieves the key string that feeds into DuckDB's encryption.

### Requirements

1. **Two key modes:** auto-key (zero friction default) and passphrase (opt-in for shared
   machine protection).
2. **Passphrase mode needs a KDF:** the user's passphrase must be transformed into a
   high-entropy key string before storage.
3. **Key storage:** OS keychain for interactive use, environment variable for CI/headless.
4. **No plaintext key on disk:** the key must never be written to config files, `.env`
   files, or any persistent file.

### KDF options considered

| KDF | Brute-force resistance | Dependencies | Standards |
|---|---|---|---|
| **Argon2id** | Memory-hard — resists GPU/ASIC parallelism | `argon2-cffi` (already planned for ADR-004) | RFC 9106, Password Hashing Competition winner (2015) |
| **PBKDF2-HMAC-SHA256** | CPU-only — vulnerable to GPU parallelism | `hashlib` (stdlib) | NIST SP 800-132 |
| **scrypt** | Memory-hard | `hashlib` (stdlib, Python 3.6+) | RFC 7914 |
| **bcrypt** | Moderate (fixed memory) | `bcrypt` | De facto standard for password hashing |

## Decision

### KDF: Argon2id

Use **Argon2id** for passphrase-to-key derivation. Argon2id combines data-dependent and
data-independent memory access patterns, providing the best resistance against both
side-channel attacks and GPU brute-force attacks.

**Parameters:**
```
passphrase + profile_name (salt)
  → Argon2id (memory: 64MB, iterations: 3, parallelism: 4)
  → 256-bit key (base64-encoded for storage/transport)
```

These parameters were originally aligned with the now-superseded
[ADR-004](004-e2e-encryption.md). That historical alignment does not prescribe
parameters for sync delivery or multi-device recovery.

**Why not PBKDF2:** PBKDF2 is CPU-bound and can be parallelized efficiently on GPUs. A
password-guessing attack is more expensive with a memory-hard KDF. This does
not protect a usable derived key already obtained from the keychain.

**Relationship to DuckDB:** DuckDB's encryption extension runs its own internal KDF on
whatever key string it receives. Our Argon2id derivation is upstream of DuckDB — it
transforms the user's passphrase into the key string that DuckDB consumes. The two KDFs
are independent and compose safely: Argon2id protects the passphrase, DuckDB's internal
KDF derives the AES key material.

### Key modes

**Auto-key (default):**
- Generate 256-bit random key via `secrets.token_bytes(32)`
- Base64-encode and store in OS keychain via `keyring`
- No KDF involved — the key is already high-entropy

**Passphrase:**
- User provides passphrase at `db init` time
- Derive key via Argon2id (parameters above)
- Base64-encode derived key and store in OS keychain
- Keychain entry cleared on `db lock`, restored on `db unlock`

### Key retrieval chain

1. **OS keychain** — `keyring.get_password("moneybin", profile_name)`
2. **Environment variable** — `MONEYBIN_PROFILE__<PROFILE>__DATABASE__ENCRYPTION_KEY` (uppercase the normalized profile name and replace hyphens with underscores; named profiles refuse the former global `MONEYBIN_DATABASE__ENCRYPTION_KEY`)
3. **Error** — `DatabaseKeyError` with instructions for the user's key mode

### Key storage

- **Keychain entry:** `service="moneybin"`, `username=<profile_name>`,
  `password=<base64-encoded-key>`
- **Environment variable:** base64-encoded key string
- **Never on disk:** not in config files, `.env` files, settings, or key files

### Key rotation

`moneybin db key rotate`:
1. Attach current database with current key
2. Prompt for new passphrase (or generate new auto-key)
3. Create new encrypted database with new key via `COPY FROM DATABASE`
4. Swap files (old → `.bak`, new → primary)
5. Update keychain with new key
6. Warn that existing backups remain encrypted with the old key

## Consequences

- Passphrase-derived keys are resistant to GPU brute-force attacks (Argon2id).
- Auto-key mode has zero daily friction — no passphrase to remember.
- `argon2-cffi` is already a planned dependency (ADR-004), so no new dependency.
- Sync and recovery key derivation must be designed independently under ADR-017.
- Local database recovery depends on the remaining usable key material; loss of
  the passphrase alone is not equivalent to loss of a persisted derived key.
- Key rotation requires a full database copy (acceptable for personal finance volumes).

## References

- [`privacy-data-protection.md`](../specs/privacy-data-protection.md) — encryption at rest spec
- [ADR-017: Client-owned encryption and recovery](017-client-owned-encryption-and-recovery.md) — accepted custody and recovery boundaries
- [Argon2 RFC 9106](https://datatracker.ietf.org/doc/html/rfc9106)
- [DuckDB Encryption Extension](https://duckdb.org/2025/11/19/encryption-in-duckdb)
