# End-to-End Encryption Architecture for MoneyBin Sync

## Overview

MoneyBin Sync will implement end-to-end encryption (E2E) similar to password managers (1Password, Bitwarden) where:

- Financial data is encrypted on the server before transmission
- Only the client possesses the decryption key
- Server never has access to plaintext financial data
- Even if server is compromised, data remains encrypted

## Security Model

### Threat Model

**Protected Against:**

- ✅ Server compromise (data encrypted at rest on server)
- ✅ Network interception (TLS + E2E encryption)
- ✅ Database breach (only encrypted data stored)
- ✅ Insider threats (server operators can't read data)
- ✅ Subpoenas (server has no plaintext data to provide)

**Not Protected Against:**

- ❌ Client device compromise (malware on user's machine)
- ❌ Lost/forgotten master password (no recovery possible)
- ❌ Rubber-hose cryptanalysis (physical coercion)

### Trust Boundaries

```
User Device (Trusted)
├── Master Password (never leaves device)
├── Encryption Keys (derived locally, never transmitted)
├── Plaintext Financial Data (decrypted locally)
└── MoneyBin Client

Network (Untrusted)
└── TLS Encrypted Transport

MoneyBin Sync Server (Semi-Trusted - Transient Plaintext Access)
├── Sees plaintext data transiently when received from Plaid
├── Converts to Parquet format in memory
├── Encrypts immediately with user's session public key
├── NEVER stores plaintext data (no database, no disk, no logs)
├── Cannot decrypt user's stored/transmitted data (lacks private key)
├── Never sees master password or encryption keys
└── Encrypted data at rest is unreadable even to server operators

Plaid API (Third-Party)
├── Server authenticates on behalf of user
├── Returns plaintext transaction data to server
├── Server processes and encrypts immediately
└── No long-term plaintext storage on server
```

## Encryption Architecture

### Key Management

**Master Password**

- User creates master password during account setup
- Never transmitted to server
- Used to derive encryption keys via PBKDF2 or Argon2

**Key Derivation**

```python
# Client-side only (never on server)
master_password = user_input()
salt = user_account_id  # Unique per user, stored on server

# Derive 256-bit encryption key
encryption_key = argon2.hash(
    password=master_password,
    salt=salt,
    time_cost=3,      # Iterations
    memory_cost=65536, # 64 MB
    parallelism=4,
    hash_len=32       # 256 bits
)
```

**Key Storage**

- Encryption key stored in client's secure keychain (macOS Keychain, Windows Credential Manager, Linux Secret Service)
- Optional: Encrypt key with device-specific key for added security
- Session keys: Ephemeral keys for sync sessions, derived from master key

### Encryption Flow

#### Account Linking (Plaid Link)

```
1. User initiates bank link via client
2. Client → Server: Request link token (authenticated via OAuth)
3. Server → Plaid: Generate link token
4. Server → Client: Plaid Link URL/token
5. Client: Opens Plaid Link UI
6. User authenticates with bank (happens in Plaid's UI)
7. Plaid → Server: Access token (server-side webhook)
8. Server: Stores access token encrypted with system key (not user key)
   - Reason: Server needs to read tokens to call Plaid API
9. Server → Client: Link confirmation (no token transmitted)
```

#### Data Sync Flow (E2E Encrypted)

```
1. Client → Server: Sync request (OAuth token + session key for response encryption)
   POST /api/v1/sync
   Headers:
     Authorization: Bearer {oauth_token}
     X-Session-Public-Key: {ephemeral_public_key}

2. Server: Authenticates request
3. Server → Plaid: Fetch transactions (using stored access token)
4. Server: Receives plaintext transaction data from Plaid
5. Server: IMMEDIATELY encrypt data with user's public key
   - Uses X-Session-Public-Key for this sync session
   - Never stores plaintext data

6. Server → Client: Encrypted Parquet data
   Response:
     {
       "institutions": {
         "chase": {
           "accounts": "encrypted_base64_parquet_data",
           "transactions": "encrypted_base64_parquet_data"
         }
       },
       "encryption": {
         "algorithm": "age",
         "format": "parquet_then_encrypt"
       }
     }

7. Client: Decrypts data with private key (derived from master password)
8. Client: Saves plaintext Parquet files locally (device encrypted)
```

### Implementation Details

#### Recommended Encryption Library: `age` (rage in Python)

**Why age?**

- Modern, simple, secure
- File encryption focused (perfect for Parquet files)
- Resistant to common mistakes
- Good Python bindings (`pyrage`)
- Widely audited and reviewed

**Alternative: NaCl (libsodium)**

- Also excellent choice
- More complex API
- Python: `PyNaCl`

#### Encryption Format

**Option 1: Encrypt Parquet Files Directly**

```python
# Server-side
parquet_bytes = accounts_df.to_parquet()
encrypted_data = age.encrypt(parquet_bytes, recipient_public_key)

# Client-side
parquet_bytes = age.decrypt(encrypted_data, private_key)
accounts_df = pl.read_parquet(BytesIO(parquet_bytes))
```

**Option 2: Encrypt Individual Fields (Column-Level)**

```python
# More complex, allows selective decryption
# Probably overkill for MoneyBin
```

**Recommendation: Option 1 (File-Level Encryption)**

- Simpler implementation
- Better performance (single decrypt operation)
- Entire transaction history encrypted as single unit

### Key Rotation

**User Key Rotation**

```
1. User provides old master password
2. Client derives old encryption key
3. Client requests all encrypted data from server
4. Client decrypts with old key
5. User provides new master password
6. Client derives new encryption key
7. Client re-encrypts all data with new key
8. Client uploads re-encrypted data to server
9. Server replaces old encrypted data with new
```

**System Key Rotation (Server-Side)**

```
- Plaid access tokens encrypted with system key
- Regular rotation (e.g., every 90 days)
- Server-side operation, no client involvement
- Different from user encryption keys
```

## Implementation Plan

### Phase 1: TLS Only (Current)

- ✅ HTTPS/TLS for transport security
- ❌ No E2E encryption
- ⚠️ Server can read plaintext data

### Phase 2: E2E Encryption Infrastructure

1. Add encryption dependencies

   ```toml
   [project.dependencies]
   pyrage = ">=1.0.0"  # age encryption
   argon2-cffi = ">=21.0.0"  # Key derivation
   ```

2. Create encryption module

   ```python
   # src/moneybin_server/encryption.py
   class DataEncryption:
       @staticmethod
       def encrypt_parquet(df, recipient_public_key) -> bytes:
           pass

   # src/moneybin/encryption.py
   class DataDecryption:
       @staticmethod
       def decrypt_parquet(encrypted_data, private_key) -> pl.DataFrame:
           pass
   ```

3. Update server to encrypt before transmission

   ```python
   # In PlaidConnector on server
   accounts_df = self.get_accounts(access_token)
   encrypted_accounts = DataEncryption.encrypt_parquet(
       accounts_df,
       user_session_public_key
   )
   return encrypted_accounts
   ```

4. Update client to decrypt after receipt

   ```python
   # In PlaidSyncConnector on client
   encrypted_data = self._sync_remote(...)
   accounts_df = DataDecryption.decrypt_parquet(
       encrypted_data["accounts"],
       user_private_key
   )
   ```

### Phase 3: Key Management

1. Master password setup during account creation
2. Key derivation on client
3. Secure key storage in system keychain
4. Session key generation for each sync

### Phase 4: Key Rotation

1. Support for password changes
2. Re-encryption of existing data
3. Emergency key recovery (with security tradeoffs)

## Configuration

### Client Configuration

```bash
# Enable E2E encryption
MONEYBIN_SYNC__E2E_ENCRYPTION_ENABLED=true

# Key derivation settings
MONEYBIN_SYNC__KEY_DERIVATION_ALGORITHM=argon2  # or pbkdf2
MONEYBIN_SYNC__KEY_DERIVATION_ITERATIONS=3
MONEYBIN_SYNC__KEY_DERIVATION_MEMORY=65536

# Encryption algorithm
MONEYBIN_SYNC__ENCRYPTION_ALGORITHM=age  # or nacl
```

### Server Configuration

```bash
# E2E encryption support
MONEYBIN_SERVER__E2E_ENCRYPTION_ENABLED=true

# System key for access token encryption (different from user keys)
MONEYBIN_SERVER__SYSTEM_ENCRYPTION_KEY=base64_encoded_key

# Key rotation
MONEYBIN_SERVER__SYSTEM_KEY_ROTATION_DAYS=90
```

## Security Considerations

### Performance Impact

- Encryption/decryption adds ~10-50ms per operation
- Acceptable for batch sync operations
- Consider streaming encryption for very large datasets

### Key Storage Security

- Client: Use OS keychain (macOS Keychain, Windows Credential Manager)
- Server: Use HSM or KMS for system keys (AWS KMS, HashiCorp Vault)
- Never log or print keys
- Secure key deletion when no longer needed

### Audit Trail

- Log all encryption/decryption operations (metadata only, not keys)
- Track key rotation events
- Monitor for suspicious patterns

### Compliance

- GDPR: Right to be forgotten (delete all encrypted data)
- CCPA: Encrypted data considered "deidentified"
- PCI DSS: E2E encryption satisfies many requirements
- SOC 2: Demonstrates strong security controls

## Future Enhancements

### Multi-Device Support

- Key synchronization across devices (encrypted with device-specific keys)
- QR code key transfer
- Secure key backup (encrypted with recovery key)

### Shared Household Accounts

- Shared encryption keys for household profiles
- Each family member has own key, can decrypt shared data
- Key sharing protocol (similar to password sharing in 1Password)

### Zero-Knowledge Architecture

- Server never sees master password
- Server never sees encryption keys
- Server never sees plaintext data
- All encryption/decryption on client
- Server only stores and forwards encrypted data

## References

- [age Specification](https://age-encryption.org/v1)
- [Argon2 RFC](https://datatracker.ietf.org/doc/html/rfc9106)
- [1Password Security Design](https://1password.com/security/)
- [Bitwarden Security Whitepaper](https://bitwarden.com/help/bitwarden-security-white-paper/)
- [Signal Protocol](https://signal.org/docs/)

## Decision Log

| Date | Decision | Rationale |
|------|----------|-----------|
| 2025-01 | Use age for encryption | Modern, simple, file-focused |
| 2025-01 | Use Argon2 for KDF | Memory-hard, resistant to GPU attacks |
| 2025-01 | File-level encryption | Simpler than column-level, sufficient for use case |
| 2025-01 | Client-side key derivation | Server never sees master password |
| 2025-01 | Zero-knowledge architecture | Maximum security, similar to password managers |
