# ADR-017: Client-owned encryption, device authorization, and recovery

## Status

Accepted principles, 2026-10-18. Multi-device protocol design and implementation
remain separate work. The supersession scope is defined below; this decision
does not replace every encryption contract.

### Relationship to earlier decisions

| Record or approach | What changes | What remains authoritative |
|---|---|---|
| [ADR-004](004-e2e-encryption.md) | Supersede its master-password-derived session-key scheme, password-coupled whole-data rotation, and no-recovery policy. | Client custody and encryption before durable broker storage remain principles; a library's appearance in the old proposal does not disqualify its selection by a later contract. |
| [ADR-005](005-security-tradeoffs.md) | Correct end-to-end wording and blanket protection or deployment-verification claims. | Retain its broker-processing trust boundary: transient provider plaintext followed by encryption for the client. |
| [ADR-009](009-encryption-key-management.md) | Clarify scope and the limits of a persisted derived key. | Local database key management; its passphrase persistence policy remains an unresolved decision. |
| Client-owned sync encryption (`sync-client-encryption.md`; separately maintained, in-progress specification) | Do not reopen its selected cryptography or delivery lifecycle. | Its age/X25519 delivery format, encrypted recovery wrapper, and rotation/recovery rules govern bank delivery within that design's scope. |
| Historical sync-overview device enrollment | Replace login-only enrollment as authority to decrypt existing financial history. | The opaque broker API boundary remains; authenticated login is still required but is insufficient to recover decryption authority. |

That specification remains separate work and does not establish shipped
protections. Its sync-delivery contract supports recovering the same recipient
key on another device. That creates another copy of the secret, not an
independently revocable cryptographic recipient. Lost-device response requires session
revocation and recipient rotation on a clean device. This remains a valid
delivery boundary; it does not establish the future device-authorization or
multi-device history-synchronization protocol described here.

## Context

Encrypted financial history must remain usable when a person adds a mobile
device, replaces a desktop, or loses a device. Binding recovery solely to one
computer's local database key would make that computer the permanent authority.
Giving the service a usable recovery key would instead make the service a
decryption authority. We choose user-controlled recovery and accept the loss
boundary that follows.

Here, **service identity** means the person's authenticated MoneyBin identity,
not a financial Account or a local Profile as defined in
[`CONTEXT.md`](../../CONTEXT.md). Authentication establishes who may request
access; it does not itself grant decryption of existing financial history.

## Decision

### Separate the encryption responsibilities

| Responsibility | Boundary |
|---|---|
| Stored financial history | Belongs to the user across authorized devices; independent of any one device's local database key. |
| Device access | Each device is separately authorized and protects its local key material using platform facilities. |
| Local database encryption | Protects a Profile's database on that device; devices need not share this key. |
| Sync delivery encryption | Protects broker-to-client deliveries and has its own key generations and rotation lifecycle. |

These are responsibility boundaries, not a selected key hierarchy or a mandate
for one shared key. Use established protocols and maintained libraries when
designing the concrete mechanism.

### Enrollment and recovery

A new desktop or mobile device gains decryption authority through approval by
an existing authorized device or possession of the user's recovery secret.
Logging in or resetting the login password alone cannot decrypt existing
financial history. The service may retain ciphertext and encrypted key
material, but must not hold the usable recovery secret or the means to unwrap
that material on its own.

If every authorized device and the recovery secret are lost, the existing
encrypted history is unrecoverable. Support cannot override this boundary.
Revoking a device must prevent future authorized access; it cannot erase keys
or plaintext already copied. The protocol must define how future data becomes
inaccessible to a revoked device even if it retained old keys.

### Sync delivery rotation

Routine rotation verifies the new key and its recovery backup before
activation. New jobs use the new generation; old local keys remain available
for in-flight deliveries until they are acknowledged or expire. Interrupted
rotation must resume without silently losing access to pending deliveries.

Compromise recovery is a distinct operation. It requires explicit consent for
discarding affected pending deliveries, revokes affected authorization
sessions, and permits requesting provider data again where available. Discard
must not advance a delivery cursor as if the data had been consumed. Rotation
cannot retract previously copied data or guarantee that a provider will return
every discarded record. Reuse the sync delivery contract rather than creating
a second rotation mechanism for local database keys.

### Scheduled access preserves authentication

The first scheduled-sync release runs in the logged-in user's session through
the operating system scheduler and existing sync command, with a fixed
Profile. Login is not equivalent to database unlock. Missing credentials or a
locked database stop the run with actionable status; scheduling adds no hidden
unlock, extra stored password, or authentication bypass. Unattended service
execution requires a separate authorization design.

### Stored-data protection has a processing boundary

The sync broker receives readable provider data before encrypting it for the
client. It must avoid persisting or logging that plaintext. Client-owned keys
protect stored encrypted financial content; this is not bank-to-client
end-to-end encryption or a zero-knowledge broker. Active broker compromise,
malicious processing, compromised clients, metadata, and provider credentials
require their own controls. Published source and reproducible builds alone do
not establish how a deployed service handles plaintext.

## Consequences and remaining design

Recovery has a real user burden. Setup, backup verification, device replacement,
loss of the final device, and support interactions require a future recovery UX
decision. That review may propose another custody model, but changing this
boundary requires an explicit decision covering security, migration, and user
consent. It does not authorize a service-held recovery key today.

The future multi-device history service still requires a reviewed key
hierarchy, authenticated device enrollment and revocation protocol, recovery
format, synchronization and conflict rules, secure delivery and replay
handling, and platform choices. Existing bank-delivery mechanisms remain
governed by their own contract; their reuse for shared history needs explicit
design. Independent security review and operational evidence are release
gates. This ADR selects no new algorithms, parameters, wire format, or database
schema, and makes no claim that these protections have shipped.

The local passphrase question remains separate: persisting a derived database
key can permit decryption through keychain access without entering the
passphrase. Recovery and scheduling must not assume that policy has been
resolved or silently weaken it.
