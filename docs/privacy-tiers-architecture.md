# Privacy Architecture & Data Custody Model

This document summarizes the architectural decisions and tradeoffs discussed for designing a personal finance application that defaults to **hard privacy**, while supporting optional convenience features through clearly defined privacy tiers.

The core outcome is a **three-tier data custody model** that makes trust boundaries explicit, defensible, and user-controlled.

---

## Design Principle

> **The app must be fully usable without the developer ever seeing a single transaction.**

All convenience features must be **opt-in**, reversible, and clearly scoped. Privacy guarantees should be enforced by architecture, not policy or intent.

---

## Privacy Tiers (Custody Models)

### 1. Managed

**Description**
A traditional SaaS-style experience similar to Monarch or Tiller.

**Characteristics**
- Server-readable transaction data
- Fastest onboarding
- Rich server-side analytics and insights
- Easier support and debugging
- Aggregator-based bank sync (Plaid/Yodlee)
- Cloud-first experience

**Tradeoffs**
- Requires trust in the provider
- Data is accessible (by design) to backend systems
- Strong privacy depends on policy and access controls, not cryptography

**User framing**
> “We manage the data so everything just works.”

---

### 2. Encrypted Sync

**Description**
End-to-end encrypted, user-custodied data with cloud backup and multi-device sync.

**Characteristics**
- Client-side encryption
- User holds the encryption keys
- Server stores only opaque ciphertext
- Bank sync is opt-in
- Cloud provides transport, durability, and coordination only
- No server-side analytics on raw transactions

**What this enables**
- Cross-device usage
- Encrypted backups
- Reasonable convenience without server visibility

**What this costs**
- No server-side intelligence on transaction content
- More complex sync and conflict resolution
- Harder support/debugging
- Recovery depends on user-held keys

**User framing**
> “We store it, but we can’t read it.”

---

### 3. Local Only

**Description**
Maximum privacy, offline-first, fully self-custodied.

**Characteristics**
- All data stored locally (DuckDB / Parquet)
- Encrypted at rest with user-owned keys
- No cloud storage
- No background sync
- Manual imports only (CSV, OFX, PDF)
- Fully usable offline

**Tradeoffs**
- No multi-device sync
- No cloud backup unless user provides their own
- User assumes full responsibility for data durability

**User framing**
> “Nothing leaves this machine.”

---

## Bank Aggregation & Encryption Model

### Reality of Aggregators (Plaid / Yodlee)

- Aggregators are typically server-to-server
- Plaintext transaction data must exist **somewhere** during fetch
- Absolute “server never sees plaintext” requires advanced techniques

### Viable Implementation Options

**Baseline (most realistic)**
- Backend fetches plaintext from aggregator
- Immediately encrypts payload to a device-held public key
- Stores only ciphertext
- No plaintext logs or persistence

**Stronger (future option)**
- Fetch + encryption occurs inside a Trusted Execution Environment (TEE)
- Plaintext never accessible to host OS or operators

**Hardest / Purest**
- Client-only fetching (rarely feasible with US aggregators)
- No backend involvement at all

---

## Feature Tradeoffs of Encryption-First Design

### Features You Lose or Must Redesign
- Server-side analytics and population-wide insights
- Global merchant normalization
- Easy support inspection
- Instant web dashboards without key entry
- Simple conflict resolution
- Guaranteed account recovery

### Features You Keep
- Sync
- Automation
- Bank feeds
- Performance
- Scalability

The loss is **server visibility**, not capability.

---

## Comparison to 1Password

**Why 1Password’s model works more easily**
- Small, discrete, user-authored records
- Low mutation rate
- No need for population-level analytics

**Why finance data is harder**
- High-volume time-series data
- Continuous ingestion from third parties
- Frequent corrections and normalization
- Heavy aggregation and querying needs

The core difference is **where computation is allowed to happen**:
- Client (maximum privacy, least power)
- Enclave (high privacy, moderate power)
- Server (maximum power, least privacy)

---

## Recommended Naming (Final)

These names are explicit, durable, and legally defensible:

- **Managed**
- **Encrypted Sync**
- **Local Only**

They should be presented as **different custody models**, not “good/better/best.”

Suggested framing:
> “Choose how your data is handled.”

---

## Strategic Implications

**Costs**
- Slower onboarding for some users
- Higher engineering complexity
- Harder support workflows
- Fewer growth shortcuts

**Benefits**
- Strong, defensible privacy guarantees
- Clear trust boundaries
- Long-term user trust
- No dependency on surveillance-based incentives
- Clear differentiation from mainstream finance apps

---

## Key Takeaway

This architecture prioritizes **data ownership and user custody first**, then layers convenience on top without collapsing trust boundaries.

Privacy is not a setting — it is a structural property of the system.
