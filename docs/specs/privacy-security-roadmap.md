<!-- Last reviewed: 2026-07-12 -->
# Privacy & Security Posture

MoneyBin is local-first today. This page states the current trust boundary; it
does not promise a hosted or remote product.

## Current: Local-First

- Each profile uses an encrypted DuckDB database on the user's machine.
- File imports stay local. Plaid sync is optional and introduces Plaid and the
  configured `moneybin-sync` broker into the data flow.
- The CLI and local MCP server are the primary interfaces. MCP write operations
  use dedicated, audited application paths; direct SQL access remains an
  operator capability, not a privacy boundary.
- Using an AI client is an explicit data-sharing decision. MoneyBin does not
  provide a general egress or consent gate for MCP tool results today.

Read the [Threat Model](../guides/threat-model.md) before using real data. It
explains the protections that exist, the data flows that do not stay local, and
the threats MoneyBin does not address. The [Database & Security
guide](../guides/database-security.md) covers encryption, key lifecycle,
backups, and recovery.

## Future Work

Remote access, optional hosted storage, and self-hosted server operation remain
future directions. Before any such capability is offered, MoneyBin will publish
its data-custody, authentication, consent, key-management, and operational
contracts. No release date or hosted-security claim is made by this document.

## Related Decisions

- [ADR-002: Privacy Tiers](../decisions/002-privacy-tiers.md)
- [ADR-004: E2E Encryption](../decisions/004-e2e-encryption.md)
- [ADR-005: Security Tradeoffs](../decisions/005-security-tradeoffs.md)
