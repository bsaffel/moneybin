# Privacy & Security Roadmap

## Overview

MoneyBin follows a three-tier data custody model that makes trust boundaries explicit. The **Local Only** tier is the current default. The Encrypted Sync and Managed tiers are future capabilities.

For the architectural rationale, see:

- [ADR-002: Privacy Tiers](../architecture/002-privacy-tiers.md)
- [ADR-004: E2E Encryption](../architecture/004-e2e-encryption.md)
- [ADR-005: Security Tradeoffs](../architecture/005-security-tradeoffs.md)

## Current: Local Only

> "Nothing leaves this machine."

- All data stored locally in DuckDB
- Manual imports only (OFX, CSV, PDF)
- Fully usable offline
- Maximum privacy -- no cloud, no sync, no third-party access

## Future: Encrypted Sync

> "We store it, but we can't read it."

- E2E encrypted cloud backup and multi-device sync
- Bank sync via Plaid with immediate encryption
- Server stores only opaque ciphertext
- You hold the encryption keys

See [`docs/architecture/004-e2e-encryption.md`](../architecture/004-e2e-encryption.md) for the encryption design.

## Future: Managed

> "We manage the data so everything just works."

- Traditional SaaS-style experience
- Server-readable data for rich analytics
- Fastest onboarding

## Security Controls

These controls are implemented today in the Local Only tier:

| Control | Description |
|---------|-------------|
| **Read-only MCP** | DuckDB opened in read-only mode; write operations rejected |
| **Result limits** | Configurable row and character limits on query results |
| **Table allowlist** | Optional restriction on which tables the MCP server can access |
| **Profile isolation** | Each user profile has its own database and credentials |
| **No credential exposure** | Credentials never passed on command line |
