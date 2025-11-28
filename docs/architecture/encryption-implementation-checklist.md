# E2E Encryption Implementation Checklist

This checklist tracks the implementation of end-to-end encryption for MoneyBin Sync.

## Phase 1: Dependencies and Infrastructure

- [ ] Add encryption dependencies to `pyproject.toml`

  ```toml
  pyrage = ">=1.0.0"        # age encryption (Rust-based, fast)
  argon2-cffi = ">=23.0.0"  # Key derivation (memory-hard)
  cryptography = ">=42.0.0" # Additional crypto primitives
  ```

- [ ] Create encryption modules
  - [ ] `src/moneybin/crypto/encryption.py` - Client-side decryption
  - [ ] `src/moneybin/crypto/key_derivation.py` - Master password → encryption key
  - [ ] `src/moneybin/crypto/key_storage.py` - Secure keychain integration
  - [ ] `src/moneybin_server/crypto/encryption.py` - Server-side encryption

- [ ] Add encryption configuration
  - [ ] Update `SyncConfig` with encryption settings
  - [ ] Add master password prompt to CLI
  - [ ] Add key derivation parameters

## Phase 2: Key Management

- [ ] Master password setup
  - [ ] CLI command for setting master password
  - [ ] Password strength validation
  - [ ] Confirmation prompt
  - [ ] Store salt on server (derived from user ID)

- [ ] Key derivation
  - [ ] Implement Argon2 key derivation on client
  - [ ] Use secure parameters (memory: 64MB, iterations: 3)
  - [ ] Derive 256-bit encryption key
  - [ ] Generate user-specific salt

- [ ] Key storage
  - [ ] macOS: Integrate with Keychain
  - [ ] Windows: Integrate with Credential Manager
  - [ ] Linux: Integrate with Secret Service
  - [ ] Fallback: Encrypted file with device-specific key

- [ ] Session keys
  - [ ] Generate ephemeral key pair per sync session
  - [ ] Use X25519 elliptic curve
  - [ ] Automatic key cleanup after session

## Phase 3: Server-Side Encryption

- [ ] Plaid data encryption
  - [ ] Encrypt accounts DataFrame before transmission
  - [ ] Encrypt transactions DataFrame before transmission
  - [ ] Use age encryption with user's session public key
  - [ ] Base64 encode encrypted data for JSON transport

- [ ] API updates
  - [ ] Add `X-Session-Public-Key` header to sync endpoint
  - [ ] Validate public key format
  - [ ] Return encrypted data in response
  - [ ] Add encryption metadata to response

- [ ] Access token security
  - [ ] Encrypt Plaid access tokens at rest (system key)
  - [ ] Use AWS KMS or HashiCorp Vault for system keys
  - [ ] Implement key rotation for system keys
  - [ ] Audit logging for access token usage

## Phase 4: Client-Side Decryption

- [ ] Receive encrypted data
  - [ ] Parse encrypted response from server
  - [ ] Validate encryption metadata
  - [ ] Extract Base64 encoded encrypted Parquet

- [ ] Decryption
  - [ ] Decrypt with session private key
  - [ ] Handle decryption errors gracefully
  - [ ] Verify decrypted data integrity
  - [ ] Convert bytes back to Parquet DataFrame

- [ ] Local storage
  - [ ] Save plaintext Parquet to profile directory
  - [ ] Rely on OS-level disk encryption (FileVault, BitLocker)
  - [ ] Optional: Client-side encryption at rest

## Phase 5: User Experience

- [ ] Master password management
  - [ ] Initial setup wizard
  - [ ] Password change flow
  - [ ] Password strength meter
  - [ ] Biometric unlock (Touch ID, Windows Hello)

- [ ] Key caching
  - [ ] Cache derived key in memory during session
  - [ ] Configurable cache timeout
  - [ ] Secure memory zeroing on exit
  - [ ] Re-prompt after timeout

- [ ] Error handling
  - [ ] Clear error messages for decryption failures
  - [ ] Master password retry logic
  - [ ] Recovery options for lost password
  - [ ] Key rotation guidance

## Phase 6: Key Rotation

- [ ] Password change
  - [ ] Re-derive encryption key from new password
  - [ ] Fetch all encrypted data from server
  - [ ] Decrypt with old key
  - [ ] Re-encrypt with new key
  - [ ] Upload re-encrypted data
  - [ ] Update server-side salt if needed

- [ ] System key rotation (server)
  - [ ] Scheduled rotation (e.g., every 90 days)
  - [ ] Decrypt Plaid tokens with old system key
  - [ ] Re-encrypt with new system key
  - [ ] Zero-downtime rotation
  - [ ] Audit trail

## Phase 7: Testing and Validation

- [ ] Unit tests
  - [ ] Key derivation tests
  - [ ] Encryption/decryption round-trip tests
  - [ ] Error handling tests
  - [ ] Performance benchmarks

- [ ] Integration tests
  - [ ] Full sync flow with encryption
  - [ ] Key rotation tests
  - [ ] Multi-device sync tests
  - [ ] Concurrent access tests

- [ ] Security audit
  - [ ] External security review
  - [ ] Penetration testing
  - [ ] Key management audit
  - [ ] Compliance validation (GDPR, CCPA)

## Phase 8: Documentation

- [ ] User documentation
  - [ ] Master password setup guide
  - [ ] Security explanation (non-technical)
  - [ ] Key recovery options
  - [ ] Multi-device setup

- [ ] Developer documentation
  - [ ] Encryption architecture overview
  - [ ] API documentation with encryption examples
  - [ ] Key management guide
  - [ ] Security best practices

- [ ] Compliance documentation
  - [ ] Security whitepaper
  - [ ] Data handling policy
  - [ ] Encryption specification
  - [ ] Audit reports

## Implementation Order

### Milestone 1: Basic Infrastructure (Week 1-2)

- Add dependencies
- Create crypto modules
- Implement key derivation
- Basic keychain integration

### Milestone 2: Server Encryption (Week 3-4)

- Server-side encryption implementation
- API updates for encrypted transport
- Access token encryption at rest
- System key management

### Milestone 3: Client Decryption (Week 5-6)

- Client-side decryption implementation
- Session key generation
- Encrypted sync flow end-to-end
- Error handling

### Milestone 4: Polish and Security (Week 7-8)

- Master password UX
- Key caching and management
- Key rotation implementation
- Security testing and audit

## Success Criteria

✅ Server never stores plaintext financial data
✅ Server never has access to user's encryption keys
✅ Even with server compromise, data remains encrypted
✅ Client can decrypt and use data seamlessly
✅ Key rotation works without data loss
✅ Multi-device support (future)
✅ Passes external security audit
✅ Zero-knowledge architecture verified

## References

- Main design: [`docs/architecture/e2e-encryption.md`](./e2e-encryption.md)
- age encryption: <https://age-encryption.org/>
- Argon2 RFC: <https://datatracker.ietf.org/doc/html/rfc9106>
- 1Password security: <https://1password.com/security/>
- Bitwarden security: <https://bitwarden.com/help/bitwarden-security-white-paper/>
