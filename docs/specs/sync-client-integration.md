# Sync Client Integration

## Status
draft

## Goal
Enable the moneybin Python client to authenticate with moneybin-server, sync bank data via the REST API, load JSON payloads into DuckDB `raw.plaid_*` tables, and transform Plaid data through SQLMesh staging views into core tables alongside existing OFX and CSV sources.

## Background
- [moneybin plaid-integration spec](https://github.com/bsaffel/moneybin/blob/main/docs/specs/plaid-integration.md) -- Raw table schemas, staging views, core integration plan
- [moneybin CLAUDE.md](https://github.com/bsaffel/moneybin/blob/main/CLAUDE.md) -- Python code standards, architecture layers, sign convention
- [server-api-contract.md](../reference/server-api-contract.md) -- Full API surface; build against this
- [ADR-002: Privacy Tiers](https://github.com/bsaffel/moneybin/blob/main/docs/architecture/002-privacy-tiers.md) -- Encrypted Sync tier
- [ADR-007: JSON over Parquet](https://github.com/bsaffel/moneybin/blob/main/docs/architecture/007-json-over-parquet-for-sync.md) -- Why JSON instead of Parquet
- Server API endpoints: `POST /sync/link-token`, `POST /sync/exchange-token`, `POST /sync/trigger`, `GET /sync/status`, `GET /sync/data`
- All changes in this phase are made in the **moneybin** Python project at `/Users/bsaffel/Workspace/moneybin/`

## Requirements

1. Client authenticates with moneybin-server via Device Authorization Flow (server-proxied to Auth0).
2. Client can initiate Plaid Link flow to connect bank accounts.
3. Client triggers sync jobs and polls for completion.
4. Client downloads JSON payload and loads into `raw.plaid_*` DuckDB tables.
5. Deduplication on primary keys prevents duplicate records on re-sync.
6. SQLMesh staging views standardize Plaid data and flip amount sign (Plaid positive = expense becomes MoneyBin negative = expense).
7. Core models include Plaid data via `UNION ALL` with `source_system = 'plaid'`.
8. CLI commands provide full sync workflow (login, link, run, status).
9. MCP tools expose sync operations to AI assistants.

## Sync Flow

```
1. POST /sync/trigger
   -> { job_id: "uuid", status: "completed", transaction_count: 142 }

2. GET /sync/status?job_id=<uuid>
   -> poll until status == "completed"
   (sync is currently synchronous, so status is already completed after trigger)

3. GET /sync/data?job_id=<uuid>
   -> JSON: { accounts: [...], transactions: [...], balances: [...],
              removed_transactions: [...], metadata: { job_id, synced_at, institutions } }

4. Parse JSON and load into DuckDB raw.plaid_* tables via read_json()

5. Run sqlmesh run to propagate through staging and core layers
```

## Data Model

### Raw tables (DuckDB)

Three tables in the `raw` schema, matching the JSON payload from moneybin-server.

```sql
-- raw.plaid_accounts (PK: account_id, source_file)
-- raw.plaid_transactions (PK: transaction_id, source_file)
-- raw.plaid_balances (PK: account_id, balance_date, source_file)
```

Column schemas match `docs/specs/plaid-integration.md` in the moneybin project. Note: `source_file`, `extracted_at`, and `loaded_at` are client-side fields not present in the JSON response -- the client generates them:
- `source_file`: logical identifier, e.g. `sync_{job_id}`
- `extracted_at`: from `metadata.synced_at` in the JSON response
- `loaded_at`: current timestamp at DuckDB insertion time

### Staging views (SQLMesh)

| View | Schema | Key transformation |
|------|--------|--------------------|
| `prep.stg_plaid__accounts` | `prep` | Standardize column names to match OFX staging output |
| `prep.stg_plaid__transactions` | `prep` | Flip amount sign: `-1 * amount`; trim description fields |
| `prep.stg_plaid__balances` | `prep` | Standardize column names |

### Core model changes

| Model | Change |
|-------|--------|
| `core.dim_accounts` | Add `plaid_accounts` CTE selecting from `prep.stg_plaid__accounts` with `source_system = 'plaid'`, UNION ALL into `all_accounts` |
| `core.fct_transactions` | Add `plaid_transactions` CTE selecting from `prep.stg_plaid__transactions` with `source_system = 'plaid'`, UNION ALL into `all_transactions` |

## Implementation Plan

### Files to Create

#### 1. HTTP sync client -- `src/moneybin/connectors/sync_client.py`

`SyncClient` class using `httpx` for all server communication.

**Methods:**
- `login() -> AuthToken` -- Device Authorization Flow: POST to `/auth/device/code`, display verification URL and user code, poll `/auth/device/token` for token.
- `create_link_token() -> LinkTokenResponse` -- POST `/sync/link-token`. Returns link token and expiration for Plaid Link.
- `exchange_token(public_token: str, institution: InstitutionInfo) -> ExchangeResponse` -- POST `/sync/exchange-token`. Sends public token from Plaid Link callback.
- `trigger_sync(item_id: str | None = None, force: bool = False) -> SyncJobResponse` -- POST `/sync/trigger`. Starts a sync job on the server.
- `get_status(job_id: str) -> SyncStatusResponse` -- GET `/sync/status?job_id={job_id}`. Polls sync job status.
- `download_data(job_id: str) -> SyncDataResponse` -- GET `/sync/data?job_id={job_id}`. Downloads JSON sync payload.

**Auth handling:**
- Store JWT in OS keychain via `keyring` library (macOS Keychain, Linux Secret Service, Windows Credential Locker).
- Fall back to `~/.moneybin/.token` file with `0600` permissions if keyring unavailable.
- Attach `Authorization: Bearer {token}` header to all authenticated requests.
- Refresh token automatically when expired (Auth0 refresh token flow).

**Configuration:**
- Server URL from `SyncConfig.server_url` (already in `src/moneybin/config.py`).

**Response models** (Pydantic):
```python
class AuthToken(BaseModel):
    access_token: str
    refresh_token: str | None
    expires_at: datetime


class LinkTokenResponse(BaseModel):
    link_token: str
    expiration: datetime


class InstitutionInfo(BaseModel):
    institution_id: str
    name: str


class ExchangeResponse(BaseModel):
    item_id: str
    institution_name: str


class SyncJobResponse(BaseModel):
    job_id: str
    status: str
    transaction_count: int | None


class SyncStatusResponse(BaseModel):
    job_id: str
    status: str  # pending, running, completed, failed
    transaction_count: int | None
    error: str | None
    started_at: datetime | None
    completed_at: datetime | None
    results: list[dict] | None


class SyncDataResponse(BaseModel):
    accounts: list[dict]
    transactions: list[dict]
    balances: list[dict]
    removed_transactions: list[str]
    metadata: dict


class ConnectedInstitution(BaseModel):
    item_id: str
    institution_id: str | None
    institution_name: str | None
    status: str
    last_sync: datetime | None
```

#### 2. Plaid data loader -- `src/moneybin/loaders/plaid_loader.py`

`PlaidLoader` class following the `OFXLoader` pattern.

**Methods:**
- `__init__(database_path: Path | str)` -- Store path, resolve SQL schema directory.
- `create_raw_tables() -> None` -- Execute DDL files for `raw.plaid_accounts`, `raw.plaid_transactions`, `raw.plaid_balances`.
- `load_json(sync_data: SyncDataResponse, job_id: str) -> dict[str, int]` -- Load JSON arrays into raw tables using DuckDB's `read_json()`, return row counts per table.
- `handle_removed_transactions(removed_ids: list[str]) -> int` -- Delete removed transactions from `raw.plaid_transactions`.

**Loading pattern using read_json:**
```python
# Write JSON arrays to temp files, then use DuckDB read_json()
import tempfile
import json

with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
    json.dump(sync_data.transactions, f)
    temp_path = f.name

conn.execute(f"""
    INSERT OR REPLACE INTO raw.plaid_transactions
    SELECT
        transaction_id, account_id, transaction_date::DATE,
        amount, description, merchant_name, category, pending,
        '{source_file}' AS source_file,
        '{extracted_at}'::TIMESTAMP AS extracted_at,
        CURRENT_TIMESTAMP AS loaded_at
    FROM read_json('{temp_path}',
        columns = {{
            transaction_id: 'VARCHAR',
            account_id: 'VARCHAR',
            transaction_date: 'VARCHAR',
            amount: 'DECIMAL(18,2)',
            description: 'VARCHAR',
            merchant_name: 'VARCHAR',
            category: 'VARCHAR',
            pending: 'BOOLEAN'
        }}
    )
""")
```

#### 3. Raw table DDL -- `src/moneybin/sql/schema/`

One file per table, following the existing DDL pattern.

**`raw_plaid_accounts.sql`**
```sql
/* Bank accounts connected via Plaid Link; one record per account per sync payload */
CREATE TABLE IF NOT EXISTS raw.plaid_accounts (
    account_id VARCHAR, -- Plaid account_id; stable identifier across syncs; part of primary key
    account_type VARCHAR, -- Plaid account type: depository, credit, loan, investment, other
    account_subtype VARCHAR, -- Plaid account subtype: checking, savings, credit card, mortgage, etc.
    institution_name VARCHAR, -- Human-readable institution name from Plaid
    official_name VARCHAR, -- Official account name from the institution, e.g. "Platinum Checking"
    mask VARCHAR, -- Last 4 digits of the account number
    source_file VARCHAR, -- Logical identifier generated by client: sync_{job_id}; part of primary key
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- When the server fetched this data from Plaid (from metadata.synced_at)
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- When this record was inserted into the database
    PRIMARY KEY (account_id, source_file)
);
```

**`raw_plaid_transactions.sql`**
```sql
/* Transaction records fetched from Plaid transactions/sync endpoint; one record per transaction per sync payload */
CREATE TABLE IF NOT EXISTS raw.plaid_transactions (
    transaction_id VARCHAR, -- Plaid transaction_id; stable unique identifier; part of primary key
    account_id VARCHAR, -- Plaid account_id this transaction belongs to; foreign key to raw.plaid_accounts
    transaction_date DATE, -- Date the transaction posted; from Plaid date field
    amount DECIMAL(18, 2), -- Plaid amount; CAUTION: Plaid convention is positive = expense; sign flip happens in staging
    description VARCHAR, -- Plaid name field; merchant or payee description
    merchant_name VARCHAR, -- Plaid merchant_name field; normalized merchant name; NULL when Plaid cannot identify
    category VARCHAR, -- Plaid personal_finance_category.primary; broad spending category
    pending BOOLEAN DEFAULT false, -- True if transaction has not yet settled
    source_file VARCHAR, -- Logical identifier generated by client: sync_{job_id}; part of primary key
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- When the server fetched this data from Plaid (from metadata.synced_at)
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- When this record was inserted into the database
    PRIMARY KEY (transaction_id, source_file)
);
```

**`raw_plaid_balances.sql`**
```sql
/* Account balance snapshots from Plaid; one record per account per balance date per sync payload */
CREATE TABLE IF NOT EXISTS raw.plaid_balances (
    account_id VARCHAR, -- Plaid account_id; foreign key to raw.plaid_accounts; part of primary key
    balance_date DATE, -- Date the balance was reported; part of primary key
    current_balance DECIMAL(18, 2), -- Current balance including pending transactions
    available_balance DECIMAL(18, 2), -- Available balance (current minus holds); NULL for credit accounts
    source_file VARCHAR, -- Logical identifier generated by client: sync_{job_id}; part of primary key
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- When the server fetched this data from Plaid (from metadata.synced_at)
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- When this record was inserted into the database
    PRIMARY KEY (account_id, balance_date, source_file)
);
```

#### 4. SQLMesh staging models -- `sqlmesh/models/prep/`

**`stg_plaid__accounts.sql`**
```sql
MODEL (
  name prep.stg_plaid__accounts,
  kind VIEW
);

SELECT
  account_id,
  NULL::VARCHAR AS routing_number,
  account_type,
  institution_name,
  NULL::VARCHAR AS institution_fid,
  official_name,
  mask,
  account_subtype,
  source_file,
  extracted_at,
  loaded_at
FROM raw.plaid_accounts
```

**`stg_plaid__transactions.sql`**
```sql
MODEL (
  name prep.stg_plaid__transactions,
  kind VIEW
);

SELECT
  transaction_id,
  account_id,
  transaction_date AS posted_date,
  -1 * amount AS amount,
  TRIM(description) AS description,
  TRIM(merchant_name) AS merchant_name,
  category,
  pending AS is_pending,
  source_file,
  extracted_at,
  loaded_at
FROM raw.plaid_transactions
```

The `-1 * amount` in the SELECT flips Plaid's sign convention (positive = expense) to MoneyBin's convention (negative = expense, positive = income).

**`stg_plaid__balances.sql`**
```sql
MODEL (
  name prep.stg_plaid__balances,
  kind VIEW
);

SELECT
  account_id,
  balance_date,
  current_balance,
  available_balance,
  source_file,
  extracted_at,
  loaded_at
FROM raw.plaid_balances
```

#### 5. CLI commands -- `src/moneybin/cli/commands/sync.py`

Replace the current stub with working commands. All commands are thin wrappers around `SyncClient`.

```
moneybin sync login          Device Authorization Flow (via server proxy)
moneybin sync link           Create link token, open Plaid Link
moneybin sync run [--force]  Trigger sync, poll, download JSON, load, transform
moneybin sync status         Show connected institutions and last sync
```

#### 6. MCP tools -- additions to `src/moneybin/mcp/write_tools.py`

```
sync.trigger    Trigger a data sync (calls SyncClient.trigger_sync)
sync.status     Show sync status and connected accounts
sync.connect    Initiate a bank connection (returns link token and URL)
```

### Files to Modify

| File | Change |
|------|--------|
| `sqlmesh/models/core/dim_accounts.sql` | Add `plaid_accounts` CTE + UNION ALL into `all_accounts` |
| `sqlmesh/models/core/fct_transactions.sql` | Add `plaid_transactions` CTE + UNION ALL into `all_transactions` |
| `src/moneybin/cli/commands/sync.py` | Replace stub with working login, link, run, status commands |
| `src/moneybin/mcp/write_tools.py` | Add sync.trigger, sync.status, sync.connect tools |
| `src/moneybin/config.py` | No changes needed; `SyncConfig` already has required fields |
| `pyproject.toml` | Add `httpx` and `keyring` dependencies |

### Key Decisions

- **httpx over requests**: Async-capable, better type stubs, modern API. Aligns with the project's preference for modern libraries.
- **Keyring for token storage**: Uses OS-native credential stores. Falls back to file-based storage for environments without a keyring service (CI, Docker).
- **JSON as transfer format**: DuckDB reads JSON via `read_json()`. No intermediate Parquet conversion needed. See ADR-007.
- **INSERT OR REPLACE for dedup**: Same pattern as OFXLoader. Primary keys prevent duplicate records without requiring separate dedup logic.
- **Amount sign flip in staging only**: Raw tables preserve Plaid's original convention. The flip happens in `stg_plaid__transactions` so raw data is always faithful to the source.
- **Polling for sync status**: The client polls `GET /sync/status` at intervals. Currently sync is synchronous so the job is completed immediately after trigger. The polling loop will activate automatically if the server moves to async execution in Phase 4.
- **source_file as logical key**: Without Parquet files, `source_file` is a logical identifier generated by the client: `sync_{job_id}`. This preserves the deduplication semantics and the ability to re-load a specific sync without creating duplicates.

## CLI Interface

```bash
# Authentication
moneybin sync login
# Calls POST /auth/device/code, displays device code and verification URL
# User visits URL and authorizes in browser
# Client polls POST /auth/device/token until authorized
# Stores JWT in OS keychain

# Connect a bank
moneybin sync link
# Creates link token via server, opens Plaid Link sandbox URL in browser
# User completes Plaid Link flow, client exchanges public token

# Sync data
moneybin sync run
# Triggers sync job, polls for completion, downloads JSON,
# loads into raw.plaid_* tables, runs sqlmesh to refresh staging + core

moneybin sync run --force
# Full re-sync: ignores cursor, re-fetches all available transaction history

# Check status
moneybin sync status
# Displays connected institutions, account counts, last sync times
```

### CLI output examples

```
$ moneybin sync login
To sign in, visit: https://your-tenant.auth0.com/activate
Enter code: ABCD-EFGH
Waiting for authorization...
Logged in as user@example.com

$ moneybin sync link
Opening Plaid Link...
Connected Chase (****1234, ****5678)

$ moneybin sync run
Starting sync for all connected institutions...
Waiting for sync job abc-123... completed (142 transactions)
Loading accounts... 2 accounts
Loading transactions... 142 transactions
Loading balances... 2 balance snapshots
Running SQLMesh plan...
Synced 142 transactions from 1 institution

$ moneybin sync status
Chase (connected 2026-03-15)
  Accounts: ****1234 (checking), ****5678 (savings)
  Last sync: 2026-04-07 14:30 UTC (142 transactions)
```

## MCP Interface

### Tools

| Tool | Description | Parameters |
|------|-------------|------------|
| `sync.trigger` | Trigger a data sync from connected banks | `institution_name: str \| None` (optional filter) |
| `sync.status` | Show connected institutions and sync status | None |
| `sync.connect` | Start bank connection flow | None; returns link token URL for user to visit |

MCP tools require an active auth session (same JWT from `moneybin sync login`). If not authenticated, tools return an error message directing the user to run `moneybin sync login` first.

Plaid data flows through existing core tables after sync, so all existing MCP read tools (transactions, accounts, balances, spending summaries) automatically include Plaid-sourced data.

## Testing Strategy

### Unit tests

- `SyncClient` methods with mocked httpx responses (success, auth error, server error, timeout)
- `PlaidLoader.load_json()` with test JSON data and in-memory DuckDB
- Token storage and retrieval (keyring mock + file fallback)
- Device Authorization Flow polling logic (success, timeout, denied)

### SQL tests

- `stg_plaid__transactions`: Verify amount sign flip (-1 * positive becomes negative)
- `stg_plaid__accounts`: Verify column mapping to core-compatible schema
- `dim_accounts`: Verify Plaid accounts appear with `source_system = 'plaid'`
- `fct_transactions`: Verify Plaid transactions appear with correct sign convention and `source_system = 'plaid'`
- Dedup: Load same JSON twice, verify no duplicate rows in raw tables

### CLI tests

- `sync login`: Mock Device Authorization Flow, verify token stored
- `sync run`: Mock SyncClient, verify download + load + SQLMesh sequence
- `sync status`: Mock SyncClient, verify output formatting
- Error cases: server unreachable, auth expired, sync job failed

### Integration tests (marked `@pytest.mark.integration`)

- Full flow with running moneybin-server (Plaid Sandbox): login, link, sync, verify data in DuckDB
- Incremental sync: first sync loads N transactions, second sync loads only new ones

## Dependencies

### New Python packages (add via `uv add`)
- `httpx >= 0.27.0` -- HTTP client for server API calls
- `keyring >= 25.0.0` -- OS-native credential storage for JWT tokens

### Existing packages (already in moneybin)
- `duckdb` -- Database engine
- `polars` -- DataFrame operations
- `typer` -- CLI framework
- `pydantic` -- Response models and config
- `fastmcp` -- MCP server

### External requirements
- Running moneybin-server instance (Phases 1-2 complete)
- Auth0 tenant configured with Device Authorization Flow enabled
- Plaid Sandbox credentials (for testing)

## Verification

```bash
cd /Users/bsaffel/Workspace/moneybin

# Authenticate
uv run moneybin sync login

# Connect a bank (Plaid Sandbox)
uv run moneybin sync link

# Sync transactions
uv run moneybin sync run

# Verify data loaded
uv run moneybin db shell
# SELECT COUNT(*) FROM raw.plaid_transactions;
# SELECT * FROM core.fct_transactions WHERE source_system = 'plaid' LIMIT 5;
# Verify amounts are negative for expenses:
# SELECT amount, description FROM core.fct_transactions
#   WHERE source_system = 'plaid' AND amount > 0 LIMIT 5;
# (should be income transactions only)

# Run pre-commit checks
uv run ruff format . && uv run ruff check . && uv run pyright && uv run pytest tests/
```

## Out of Scope

- E2E encryption of JSON payloads (Phase 5 in moneybin-server)
- MCP App UI widgets for Plaid Link
- Investment and liability data from Plaid
- Multi-device token sync
- Webhook-based real-time sync (polling is sufficient for MVP)
- Offline queue for sync commands when server is unreachable
