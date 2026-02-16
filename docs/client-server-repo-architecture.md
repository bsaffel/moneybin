# Client-Server Architecture & Privacy Tiers

## Overview

MoneyBin is an open-source, local-first platform. The architecture separates local components from optional server components along clear trust boundaries defined by the [privacy tiers](privacy-tiers-architecture.md).

The **MCP server runs locally** -- it is not a remote service. The "server" in this document refers to the optional **Encrypted Sync service** that handles Plaid bank aggregation with E2E encryption.

## Architecture by Privacy Tier

### Local Only (Default)

Everything runs on your machine. No network, no cloud, no server.

```text
┌─────────────────────────────────────────────────────────┐
│                    Your Machine                          │
│                                                          │
│  Source Files ──→ Extractors ──→ DuckDB ──→ MCP Server  │
│  (OFX/CSV/PDF)                              (stdio)     │
│                                    │                     │
│                              Data Toolkit                │
│                         (DuckDB/dbt/Jupyter)             │
└─────────────────────────────────────────────────────────┘
```

**Components**:
- `src/moneybin/` -- Extractors, loaders, CLI, MCP server
- `dbt/` -- Transformation models
- `data/{profile}/` -- Profile-isolated DuckDB database
- No network access required

**Trust boundary**: You trust only your own machine.

### Encrypted Sync (Future)

A hosted service fetches bank data via Plaid, encrypts it immediately, and syncs the ciphertext to your machine. The service never stores plaintext.

```text
┌──────────────────────────┐         ┌─────────────────────────────────┐
│   Encrypted Sync Server  │         │         Your Machine             │
│                          │         │                                   │
│  Plaid API ──→ Encrypt ──┼────────→│  Decrypt ──→ DuckDB ──→ MCP     │
│         (brief plaintext) │  E2E   │  (your key)           (stdio)   │
│                          │ cipher  │                  │               │
│  Zero stored plaintext   │  text   │            Data Toolkit          │
└──────────────────────────┘         └─────────────────────────────────┘
```

**Components**:
- `src/moneybin_server/` -- Encrypted Sync service (future)
- Uses Plaid/Yodlee for bank aggregation
- Encrypts to device-held public key immediately
- Stores only opaque ciphertext
- Client decrypts with master password-derived key

**Trust boundary**: Server sees plaintext briefly during Plaid fetch and encryption. See [`architecture/security-tradeoffs.md`](architecture/security-tradeoffs.md) for honest analysis.

### Managed (Future, Low Priority)

Traditional SaaS-style where the server stores readable data for rich server-side analytics.

**Trust boundary**: You trust the service provider with your data (like Monarch Money, Empower, etc.).

---

## Repo Structure

The repository contains both the open-source local platform and the scaffolding for the future Encrypted Sync service:

```text
moneybin/
├── src/moneybin/               # Open-source local platform
│   ├── mcp/                    # MCP server (runs locally)
│   ├── cli/                    # CLI interface
│   ├── extractors/             # File parsers (OFX, PDF, CSV)
│   ├── loaders/                # DuckDB data loaders
│   └── connectors/             # API integrations
├── src/moneybin_server/        # Encrypted Sync service (future)
│   ├── connectors/             # Plaid/Yodlee integration
│   ├── api/                    # FastAPI server
│   └── config.py               # Server configuration
├── dbt/                        # dbt transformation models
├── data/{profile}/             # Profile-isolated data
└── tests/                      # Test suites for both
```

---

## Design Principles

### 1. MCP Server is Local

The MCP server is **not a remote service**. It runs as a local process via stdio, connecting directly to the user's DuckDB file in read-only mode. It is part of the open-source `moneybin` package.

### 2. Server = Sync Service Only

The "server" component (`moneybin_server`) exists solely for the Encrypted Sync tier:
- Connect to Plaid/Yodlee
- Fetch bank data
- Encrypt immediately
- Transmit ciphertext to client
- **No business logic**: No categorization, deduplication, or enrichment

All "smart" data processing happens locally via dbt models on the client side.

### 3. Contracts First

The Encrypted Sync service will expose a small, well-defined API:

- `POST /sync/link-token` -- Initiate Plaid Link
- `POST /sync/exchange-token` -- Exchange public token
- `POST /sync/trigger` -- Trigger a sync job
- `GET /sync/status` -- Check sync job status
- `GET /sync/data` -- Download encrypted payload

The client depends on this API contract, not on server internals.

### 4. Raw Data Preservation

The sync service stores data exactly as received from Plaid (encrypted). All transformation and normalization happens locally:
- Raw Plaid data decrypted and stored in `raw.plaid_*` tables
- dbt staging models normalize to standard schema
- Core tables unify all sources

### 5. Open Source First

The open-source repo is the product. The Encrypted Sync service is a convenience layer for users who want automatic bank feeds without manual OFX/CSV export. The platform must be fully functional without it.

---

## Security Model

See [`architecture/e2e-encryption.md`](architecture/e2e-encryption.md) for the complete encryption design.

### Key Points

- **Local Only tier**: No encryption needed -- data never leaves your machine
- **Encrypted Sync tier**: E2E encryption with user-held keys
  - Server encrypts to device-held public key
  - Only the client can decrypt with the master password
  - Server stores only opaque ciphertext
  - Brief plaintext exposure during Plaid fetch (honest disclosure)
- **Managed tier**: Standard server-side security (access controls, encryption at rest)

### What This Protects

- Database breach: Only encrypted data compromised
- Disk compromise: Encrypted at rest
- Network sniffing: TLS + E2E encryption

### What Requires Trust

- Server operator during active Plaid data processing (brief plaintext exposure)
- This is comparable to email with PGP -- the server handles the message, then encrypts

See [`architecture/security-tradeoffs.md`](architecture/security-tradeoffs.md) for the full threat model.

---

## Future: Managed Tier

If a managed tier is ever built, it would be a separate service that:
- Stores readable transaction data (not E2E encrypted)
- Provides server-side analytics, insights, and dashboards
- Offers the fastest onboarding experience
- Requires the most trust from the user

This is **not a near-term priority**. The focus is on the Local Only and Encrypted Sync tiers.
