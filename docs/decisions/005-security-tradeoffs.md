# ADR-005: Security Tradeoffs and Threat Model

## Status
proposed

## Context

MoneyBin Sync's Encrypted Sync tier faces a fundamental tradeoff: to provide automatic bank sync via Plaid, the server must briefly see plaintext data. This document honestly analyzes what we can and cannot protect against.

### The problem

To provide automatic bank sync:
1. Store Plaid access tokens securely (can't ship to clients)
2. Call Plaid API with these tokens (must happen server-side)
3. Receive plaintext data from Plaid (API returns unencrypted JSON)
4. Convert to Parquet format in memory
5. Encrypt for user (only user can decrypt)

**The server must see plaintext data briefly during steps 3-5.**

## Decision

Adopt **server-side encryption with transient plaintext** as the security model, with clear, honest disclosure to users.

### Alternatives considered

| Approach | Verdict | Why |
|----------|---------|-----|
| Client-side Plaid | Good for free tier | No automatic sync when client offline; each device needs separate token |
| Server with transient plaintext | **Chosen** | Best practical compromise for automatic sync |
| Trusted Execution Environment (TEE) | Future option | Too complex and expensive for v1 |
| Homomorphic encryption | Not practical | Orders of magnitude too slow; can't convert formats |
| Manual export only | Free tier already | Defeats purpose of paid sync feature |

### What we protect against

| Threat | Protected? | How |
|--------|-----------|-----|
| Database breach | Yes | Only encrypted data stored |
| Disk compromise | Yes | No plaintext persisted |
| Network sniffing | Yes | TLS + E2E encryption |
| Subpoena for stored data | Yes | Server can't decrypt |
| Passive server compromise | Yes | Encrypted at rest |
| Active server compromise | Partial | Could intercept during processing |
| Malicious server operator | No | Has access during processing |

### What requires trust

The user must trust that the server:
1. Encrypts immediately (provable via open source code)
2. Doesn't store plaintext (provable via code + audits)
3. Doesn't log plaintext (auditable, but requires trust)
4. Doesn't send plaintext elsewhere (auditable, but requires trust)
5. Runs the audited code (verifiable via reproducible builds)

### Mitigation strategies

1. **Minimize plaintext window** -- encrypt immediately after Parquet conversion, zero original memory
2. **No persistence** -- no database writes, disk caching, debug logs, or error messages with plaintext
3. **Memory security** -- zero memory after encryption, no core dumps with sensitive data
4. **Code transparency** -- open source encryption logic, regular security audits, reproducible builds
5. **Access controls** -- minimal server operator access, audit logging, anomaly detection

### Honest communication

**Good (honest):**
- "Your financial data is encrypted end-to-end"
- "We can't decrypt your stored data -- only you can"
- "We process data securely and encrypt immediately"

**Bad (misleading):**
- "We never see your financial data" (false)
- "True zero-knowledge encryption" (not technically accurate)
- "Complete privacy guaranteed" (requires trust during processing)

## Consequences

- Users can make informed decisions about which tier to use.
- The Local Only tier remains available for users who want zero server involvement.
- The security model is honestly communicated, building long-term trust.
- The Encrypted Sync tier is significantly better than most financial SaaS (which stores plaintext indefinitely).
- Future TEE integration could further reduce the trust requirement.

### Comparison

- **Better than** traditional financial aggregators (Mint, YNAB) -- they store plaintext indefinitely
- **Similar to** email with PGP -- server handles the message, then encrypts
- **Not as good as** true zero-knowledge services (Signal) -- but they don't handle third-party API data

## References

- [ADR-002: Privacy Tiers](002-privacy-tiers.md) -- Custody model
- [ADR-004: E2E Encryption](004-e2e-encryption.md) -- Encryption architecture
- [Plaid Integration Spec](../specs/plaid-integration.md) -- Implementation plan
