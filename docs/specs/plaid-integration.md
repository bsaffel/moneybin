# Feature: Plaid Integration (Encrypted Sync Tier)

## Status
draft

## Goal
Enable automatic bank transaction sync via Plaid with E2E encryption, so users can get ongoing transaction data without manual file exports.

## Background
- [ADR-002: Privacy Tiers](../architecture/002-privacy-tiers.md) -- Encrypted Sync tier
- [ADR-004: E2E Encryption](../architecture/004-e2e-encryption.md) -- Encryption design
- [ADR-005: Security Tradeoffs](../architecture/005-security-tradeoffs.md) -- Threat model
- [Data Sources](../reference/data-sources.md) -- Priority 5 source
- Existing connector: `src/moneybin/connectors/plaid_sync.py`

## Requirements

1. User connects bank accounts via Plaid Link UI.
2. Encrypted Sync server fetches data from Plaid API.
3. Data encrypted immediately to user's session public key (never stored as plaintext).
4. Encrypted payload transmitted to client.
5. Client decrypts with master password-derived key and loads into `raw.plaid_*` tables.
6. dbt transforms Plaid data into core tables alongside OFX/CSV data.
7. Incremental sync: only fetch new transactions since last sync.
8. Support accounts, transactions, balances, and (future) investments/liabilities.

## Data Model

### Raw tables

```sql
CREATE TABLE IF NOT EXISTS raw.plaid_accounts (
    account_id VARCHAR NOT NULL,
    account_type VARCHAR,
    account_subtype VARCHAR,
    institution_name VARCHAR,
    official_name VARCHAR,
    mask VARCHAR,          -- last 4 digits
    source_file VARCHAR NOT NULL,
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (account_id, source_file)
);

CREATE TABLE IF NOT EXISTS raw.plaid_transactions (
    transaction_id VARCHAR NOT NULL,
    account_id VARCHAR NOT NULL,
    transaction_date DATE NOT NULL,
    amount DECIMAL(18, 2) NOT NULL,   -- Plaid convention: positive = expense
    description VARCHAR,
    merchant_name VARCHAR,
    category VARCHAR,
    pending BOOLEAN DEFAULT false,
    source_file VARCHAR NOT NULL,
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (transaction_id, source_file)
);

CREATE TABLE IF NOT EXISTS raw.plaid_balances (
    account_id VARCHAR NOT NULL,
    balance_date DATE NOT NULL,
    current_balance DECIMAL(18, 2),
    available_balance DECIMAL(18, 2),
    source_file VARCHAR NOT NULL,
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (account_id, balance_date, source_file)
);
```

### Staging views (dbt)

- `prep.stg_plaid__accounts` -- Standardize to match OFX staging schema
- `prep.stg_plaid__transactions` -- **Flip amount sign** (Plaid: positive = expense; MoneyBin: negative = expense)
- `prep.stg_plaid__balances` -- Standardize column names

### Core integration

Add CTE + `UNION ALL` in `dim_accounts.sql` and `fct_transactions.sql` with `source_system = 'plaid'`.

## Implementation Plan

### Phase 1: Encryption infrastructure

**Files to create**:
- `src/moneybin/crypto/encryption.py` -- Client-side decryption
- `src/moneybin/crypto/key_derivation.py` -- Master password -> encryption key (Argon2)
- `src/moneybin/crypto/key_storage.py` -- OS keychain integration
- `src/moneybin_server/crypto/encryption.py` -- Server-side encryption

**Dependencies**: `pyrage >= 1.0.0`, `argon2-cffi >= 23.0.0`, `cryptography >= 42.0.0`

### Phase 2: Server-side encryption

**Files to create/modify**:
- `src/moneybin_server/connectors/plaid_connector.py` -- Plaid API integration
- `src/moneybin_server/api/sync.py` -- Sync endpoints

**API surface**:
- `POST /sync/link-token` -- Initiate Plaid Link
- `POST /sync/exchange-token` -- Exchange public token
- `POST /sync/trigger` -- Trigger a sync job
- `GET /sync/status` -- Check sync job status
- `GET /sync/data` -- Download encrypted payload

### Phase 3: Client-side decryption and loading

**Files to create**:
- `src/moneybin/connectors/plaid_sync.py` -- Sync client (modify existing)
- `src/moneybin/loaders/plaid_loader.py` -- DuckDB loading
- `src/moneybin/sql/schema/raw_plaid_*.sql` -- DDL
- `dbt/models/plaid/stg_plaid__*.sql` -- Staging views
- `dbt/models/plaid/schema.yml` -- dbt tests

**Files to modify**:
- `dbt/models/core/dim_accounts.sql` -- Add Plaid CTE + UNION ALL
- `dbt/models/core/fct_transactions.sql` -- Add Plaid CTE + UNION ALL
- `src/moneybin/cli/commands/sync.py` -- Sync CLI commands

### Phase 4: Key management and UX

- Master password setup CLI
- Key caching with configurable timeout
- Password change / key rotation flow

### Key decisions

- **Amount sign flip**: Plaid uses positive = expense; MoneyBin uses negative = expense. Flip happens in staging view.
- **Incremental sync**: Use Plaid's `transactions/sync` endpoint with cursor-based pagination.
- **No server-side business logic**: Server only fetches, encrypts, and transmits. All transformation happens locally.
- **Parquet as encryption unit**: Encrypt entire Parquet files, not individual fields or rows.

## CLI Interface

```bash
# Initial setup
moneybin sync setup                     # Master password + key derivation
moneybin sync link                      # Plaid Link flow

# Ongoing sync
moneybin sync run                       # Fetch + decrypt + load
moneybin sync run --force               # Full re-sync (ignore cursor)
moneybin sync status                    # Check sync status

# Key management
moneybin sync change-password           # Re-derive key, re-encrypt data
```

## MCP Interface

No new tools initially. Plaid data flows through existing core tables and is accessible via all existing MCP tools (transactions, accounts, balances).

Future: `investments.holdings`, `investments.performance`, `liabilities.summary` tools become functional once Plaid investment/liability data is available.

## Testing Strategy

- Unit tests for encryption/decryption round-trip
- Unit tests for key derivation
- Integration tests for full sync flow (mock Plaid API)
- Test amount sign flip in staging views
- Test incremental sync with cursor
- Test key rotation
- Security tests: verify no plaintext in logs, errors, or disk

## Dependencies

- `pyrage >= 1.0.0` -- age encryption
- `argon2-cffi >= 23.0.0` -- Key derivation
- `cryptography >= 42.0.0` -- Crypto primitives
- `plaid-python` -- Plaid API client
- System: Plaid API credentials (dev/sandbox for testing)

## Out of Scope

- Managed tier (server-readable data)
- Investment holdings and performance tracking (separate spec)
- Liability tracking (separate spec)
- Multi-device key sync
- TEE-based encryption
