# ADR-005: Security Tradeoffs and Threat Model

## Status
Trust boundary retained and clarified by
[ADR-017: Client-owned encryption, device authorization, and recovery](017-client-owned-encryption-and-recovery.md)
on 2026-10-18. The broker sees provider plaintext during processing and encrypts
it for the client before durable storage. The requirements below describe the
target protections, not evidence of deployed enforcement; detailed mechanisms
and operational verification remain required.

## Context

MoneyBin Sync's Encrypted Sync tier faces a fundamental tradeoff: to provide automatic bank sync via Plaid, the server must briefly see plaintext data. This document honestly analyzes what we can and cannot protect against.

### The problem

To provide automatic bank sync:
1. Store Plaid access tokens securely (can't ship to clients)
2. Call Plaid API with these tokens (must happen server-side)
3. Receive plaintext data from Plaid (API returns unencrypted JSON)
4. Prepare the delivery payload in memory
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

### Target protections and limits

| Threat | Target protection | Limitation |
|--------|-----------|-----|
| Disclosure of stored financial payloads | Client-owned keys protect ciphertext | Metadata and provider credentials require separate controls |
| Broker disk compromise | No persisted plaintext financial payloads | Requires evidence covering caches, logs, crash dumps, and backups |
| Network observation | Authenticated TLS protects transport | The broker still receives readable provider data |
| Disclosure of service-held material | The service cannot decrypt stored payloads using its own keys | This makes no blanket claim about legal requests or other retained information |
| Active server compromise | Stored ciphertext remains subject to the client-key boundary | An attacker can intercept provider plaintext or alter active processing |
| Malicious server operator | No protection for plaintext during processing | Operator access and processing behavior require independent controls |

### What requires trust

The user must trust that the server:
1. Encrypts before durable storage (requires implementation and deployment evidence)
2. Doesn't store plaintext (requires storage, failure-path, and operational verification)
3. Doesn't log plaintext (auditable, but requires trust)
4. Doesn't send plaintext elsewhere (auditable, but requires trust)
5. Runs the reviewed code and configuration (source and reproducible builds alone do not prove deployed behavior)

### Mitigation strategies

1. **Minimize plaintext window** -- encrypt after required payload processing; bound plaintext lifetime
2. **No persistence** -- no database writes, disk caching, debug logs, or error messages with plaintext
3. **Memory security** -- control dumps, swap, and snapshots; verify memory-handling limits rather than promising complete erasure
4. **Code transparency** -- open source encryption logic, regular security audits, reproducible builds
5. **Access controls** -- minimal server operator access, audit logging, anomaly detection

### Honest communication

Describe the intended boundary during design. Claim it as delivered only after
verification:

- "The broker reads provider data during processing and encrypts it before storage."
- "Stored financial payloads require user-controlled decryption material."

**Bad (misleading):**
- "We never see your financial data" (false)
- "True zero-knowledge encryption" (not technically accurate)
- "Bank-to-client end-to-end encryption" (the broker receives plaintext)
- "Complete privacy guaranteed" (requires trust during processing)

## Consequences

- Users can make informed decisions about which tier to use.
- The Local Only tier remains available for users who want zero server involvement.
- The security model is honestly communicated, building long-term trust.
- Client custody reduces exposure of stored payloads but retains trust in active broker processing.
- Future TEE integration could further reduce the trust requirement.

### Where the trust boundary sits

- Plaintext-at-rest aggregation retains raw transaction data server-side indefinitely; MoneyBin's sync tier handles plaintext only in transit through the broker.
- The broker fetches and encrypts provider data; it is not merely relaying ciphertext created by the bank.
- It is not zero-knowledge: third-party API data necessarily transits the broker readable before encryption.

## References

- [ADR-002: Privacy Tiers](002-privacy-tiers.md) -- Custody model
- [ADR-017: Client-owned encryption and recovery](017-client-owned-encryption-and-recovery.md) -- Accepted custody, enrollment, and recovery principles
- [ADR-004: E2E Encryption](004-e2e-encryption.md) -- Historical superseded proposal
- [Plaid Integration Spec](../specs/sync-plaid.md) -- Implementation plan
