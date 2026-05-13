# Plaid Sync Phase 1 — Design Document

**Date:** 2026-05-13
**Status:** Approved — implementation starting
**Branch:** `feat/plaid-sync`
**Specs implemented:** `sync-plaid.md` (Phase 1), `sync-overview.md` (status → ready)

---

## Background

MoneyBin's sync stubs have been placeholders since the architecture was laid down. This document captures the design decisions reached during two rounds of Phase 1 brainstorming (one initial, one after independent review). It supersedes the implementation sketch in `sync-plaid.md` where the two disagree.

Key inputs:

1. Current Plaid documentation (Plaid Hosted Link, `SESSION_FINISHED` webhook)
2. Provider-agnostic API design vetted across Plaid, SimpleFIN, MX, Teller, TrueLayer
3. ADR-010 writer-coordination constraints (per-call short-lived `Database` connections)
4. Project rules: `.claude/rules/security.md`, `cli.md`, `mcp-server.md`, `database.md`, `data-extraction.md`, `identifiers.md`
5. A re-read of the existing CLI/MCP stubs (`src/moneybin/cli/commands/sync.py`, `src/moneybin/mcp/tools/sync.py`)

---

## Section 1: Provider-Agnostic Connect API

### Plaid Hosted Link, not server-hosted page

Plaid Hosted Link hosts the Link UI on Plaid's infrastructure. The server creates a Link token with `hosted_link: {}`, exposes the resulting `hosted_link_url` to the client, and receives a `SESSION_FINISHED` webhook when the user finishes. The server then exchanges the public token for an access token entirely server-side. **The client never holds Plaid credentials, never sees the public token, and never calls an exchange endpoint.**

### `connect_type` discriminator

The connect API is provider-agnostic via a `connect_type` field in the initiate response. Phase 1 implements only `widget_flow`; the discriminator exists so future providers slot in without breaking the client.

| `connect_type` | Server pattern | Phase 1 status |
|---|---|---|
| `widget_flow` | Server returns a hosted URL; webhook signals completion | Implemented (Plaid) |
| `token_paste` | User obtains token externally; CLI submits via separate endpoint | Deferred to Phase 2 (SimpleFIN) |

The client parses `connect_type`. If the value isn't `widget_flow` in Phase 1, the client raises `NotImplementedError("connect_type 'X' is not supported in this version")`.

### Endpoints

**New:**

```
POST /sync/connect/initiate
  Body: {
    "provider": "plaid",
    "provider_item_id": "item_abc",     # optional — update mode for re-auth
    "return_to": "https://app.moneybin.io/sync/callback"   # optional — for web UI; CLI sends null
  }
  Response 200: {
    "session_id": "sess_abc123",
    "link_url": "https://hosted.plaid.com/link/...",
    "connect_type": "widget_flow",
    "expiration": "2026-05-13T13:30:00Z"
  }

GET /sync/connect/status?session_id=sess_abc123
  Response 200: {
    "session_id": "sess_abc123",
    "status": "pending" | "connected" | "failed",
    "provider_item_id": "item_abc123",   # only when status == "connected"
    "institution_name": "Chase",          # only when status == "connected"
    "error": null,                        # error message when status == "failed"
    "expiration": "2026-05-13T13:30:00Z"  # session expiration; agent can decide when to give up
  }

POST /auth/refresh
  Body: { "refresh_token": "..." }
  Response 200: { "access_token": "...", "refresh_token": "...", "expires_in": 3600 }
  Refresh tokens MUST rotate on each call — server returns a new refresh_token; old one invalidated.

POST /webhooks/plaid                      # internal, not in client API surface
  Handles SESSION_FINISHED + Plaid item lifecycle webhooks
  Verified via Plaid-Verification JWT header; replay-window enforced on `iat`
```

**Renamed:** `item_id` → `provider_item_id` everywhere in client-visible responses (`GET /institutions`, `GET /sync/status.results[]`, `GET /sync/data.metadata.institutions[]`).

**Removed from client API:**
- `POST /sync/link-token` (replaced by `POST /sync/connect/initiate`)
- `POST /sync/exchange-token` (now internal webhook handler)

**Retained:**
- `POST /sync/trigger` — synchronous, returns final state, `provider_item_id` body param
- `GET /sync/status` — fallback for crash recovery
- `GET /sync/data` — unchanged shape; one-shot read (server deletes from TTL store on read)
- `DELETE /institutions/:id` — unchanged

### `return_to` (forward-compat for web UI)

The `return_to` field is added now so the M3D Web UI doesn't require a breaking API change. CLI sends `null`; the server tells Plaid to display its default "session complete" page; CLI polls `GET /sync/connect/status` for completion. The future web UI sends its callback URL; the server passes it to Plaid Hosted Link's `redirect_uri` config; Plaid redirects the user's browser back to the web app after they finish. The status endpoint remains the authoritative completion signal even when redirect-based UX is in use, since the webhook may race the browser redirect.

### Update mode (re-authentication)

When an institution returns `ITEM_LOGIN_REQUIRED`, the user needs to re-authenticate without losing transaction history. Plaid supports "update mode": create a Link token tied to an existing `item_id`; the same `item_id` survives.

The client passes `provider_item_id` to `POST /sync/connect/initiate`; the server detects this and creates an update-mode Link token instead of a new-connection token. From the client's perspective the response shape is identical.

---

## Section 2: SyncClient

**File:** `src/moneybin/connectors/sync_client.py`

Pure HTTP transport — no business logic, no database access. Takes a `MoneyBinSettings` (for `server_url` + timeouts) and a `SecretStore` (for token storage). Methods correspond 1:1 to server endpoints.

### Token storage

Primary: `keyring` (service `"moneybin-sync"`, keys `"jwt"` and `"refresh_token"`).
Fallback: `~/.moneybin/.sync_token` (0600 permissions, JSON `{"jwt": ..., "refresh_token": ...}`).

The fallback handles environments without an OS keychain (headless Linux without Secret Service, some Docker setups). On `keyring.errors.KeyringError`, fall through to the file. Both paths read/write through the same internal interface so callers don't branch.

### Authentication (Device Authorization Flow, RFC 8628)

`login()`:
1. `POST /auth/device/code` → `{user_code, verification_uri_complete, device_code, interval}`
2. Display `user_code` and `verification_uri_complete` to user (stderr); call `webbrowser.open()` unless `--no-browser` was set
3. Poll `POST /auth/device/token` with `{device_code}`:
   - `200` → store JWT + refresh token, return
   - `202 {status: "pending"}` → wait `interval` seconds, retry
   - `202 {status: "slow_down"}` → increase interval by 5s (RFC 8628 §3.5), retry
   - `403` → user denied; raise `SyncAuthError`
   - `400` → code expired; raise `SyncAuthError`

### Token refresh (rotating)

On any `401 Unauthorized` from a sync endpoint:
1. If a refresh token is stored, call `POST /auth/refresh` with it
2. On success: update stored JWT **and refresh token** (server rotates on every call), retry the original request once
3. On refresh failure (refresh token expired or revoked): clear keyring/file entries, raise `SyncAuthError("session expired — run moneybin sync login")`

Refresh is transparent to method callers — they see either success or `SyncAuthError`.

### Browser handling

`webbrowser.open()` returns True on success, but on some Linux systems it lies (returns True with no actual browser launched). To handle this robustly:

- Default: call `webbrowser.open()`; whether it returns True or False, always also print the URL to stderr so a headless user can copy it
- `--no-browser` flag: skip the `webbrowser.open()` call entirely; print URL only

### Connect flow

```python
def connect(self, provider_item_id: str | None = None, return_to: str | None = None) -> ConnectResult:
    body = {"provider": "plaid"}
    if provider_item_id:
        body["provider_item_id"] = provider_item_id   # update mode
    if return_to:
        body["return_to"] = return_to                  # web UI redirect
    resp = self._post("/sync/connect/initiate", body)
    initiate = ConnectInitiateResponse.model_validate(resp)
    if initiate.connect_type != "widget_flow":
        raise NotImplementedError(
            f"connect_type '{initiate.connect_type}' is not supported in this version"
        )
    self._open_url(initiate.link_url)        # honors --no-browser
    return self._poll_connect(initiate.session_id)

def _poll_connect(self, session_id: str) -> ConnectResult:
    deadline = time.time() + _LONG_TIMEOUT
    interval = 3.0
    while time.time() < deadline:
        time.sleep(interval)
        r = self._get(f"/sync/connect/status?session_id={session_id}")
        status = ConnectStatusResponse.model_validate(r)
        if status.status == "connected":
            return ConnectResult(provider_item_id=status.provider_item_id, ...)
        if status.status == "failed":
            raise SyncConnectError(status.error or "connect failed")
    raise SyncTimeoutError("connect flow timed out — user may have abandoned the browser")
```

### Timeouts

Two constants, not config knobs:

```python
_DEFAULT_TIMEOUT = httpx.Timeout(15.0, connect=10.0)   # most endpoints
_LONG_TIMEOUT = httpx.Timeout(120.0, connect=10.0)     # POST /sync/trigger, connect-poll deadline
```

Promote to `SyncConfig` only when a real user file says they need different values. Don't add knobs speculatively.

### Exception taxonomy

`SyncClient` raises typed exceptions:
- `SyncAuthError` — auth failure (401 after refresh attempt, user denied device flow)
- `SyncConnectError` — connect session failed (`status: "failed"` from server)
- `SyncTimeoutError` — operation exceeded its timeout
- `SyncAPIError` — generic server error (5xx, unexpected response shape)

The CLI's `handle_cli_errors` maps these to exit codes and user-friendly messages.

---

## Section 3: Pydantic Response Models

**File:** `src/moneybin/connectors/sync_models.py`

All server responses validated at the boundary. Single source of truth for the contract; imported by both `SyncClient` and `PlaidLoader`. Result/view types for the service layer live alongside, not in a separate module.

```python
from pydantic import BaseModel, Field
from datetime import date, datetime
from decimal import Decimal
from typing import Literal

# ---- Server response models ----

class AuthToken(BaseModel):
    access_token: str
    refresh_token: str
    expires_in: int = Field(gt=0)
    token_type: Literal["Bearer"] = "Bearer"

class ConnectInitiateResponse(BaseModel):
    session_id: str = Field(min_length=1, max_length=128)
    link_url: str
    connect_type: Literal["widget_flow", "token_paste"]
    expiration: datetime

class ConnectStatusResponse(BaseModel):
    session_id: str
    status: Literal["pending", "connected", "failed"]
    provider_item_id: str | None = None
    institution_name: str | None = None
    error: str | None = None
    expiration: datetime

class SyncTriggerResponse(BaseModel):
    job_id: str
    status: Literal["pending", "running", "completed", "failed"]
    transaction_count: int | None = None

class SyncAccount(BaseModel):
    account_id: str
    account_type: str | None = None
    account_subtype: str | None = None
    institution_name: str | None = None
    official_name: str | None = None
    mask: str | None = Field(default=None, max_length=8)

class SyncTransaction(BaseModel):
    transaction_id: str
    account_id: str
    transaction_date: date
    amount: Decimal              # Plaid convention preserved: positive = expense
    description: str | None = None
    merchant_name: str | None = None
    category: str | None = None
    pending: bool = False

class SyncBalance(BaseModel):
    account_id: str
    balance_date: date
    current_balance: Decimal | None = None
    available_balance: Decimal | None = None

class InstitutionResult(BaseModel):
    provider_item_id: str
    institution_name: str | None = None
    status: Literal["completed", "failed"]
    transaction_count: int | None = None
    error: str | None = None
    error_code: str | None = None

class SyncMetadata(BaseModel):
    job_id: str
    synced_at: datetime
    institutions: list[InstitutionResult]

class SyncDataResponse(BaseModel):
    accounts: list[SyncAccount]
    transactions: list[SyncTransaction]
    balances: list[SyncBalance]
    removed_transactions: list[str]
    metadata: SyncMetadata

class ConnectedInstitution(BaseModel):
    id: str                      # internal UUID
    provider_item_id: str
    provider: str
    institution_name: str | None = None
    status: Literal["active", "error", "revoked"]
    last_sync: datetime | None = None
    created_at: datetime

# ---- Service-layer result types ----

class PullResult(BaseModel):
    job_id: str
    transactions_loaded: int
    accounts_loaded: int
    balances_loaded: int
    transactions_removed: int
    institutions: list[InstitutionResult]   # passthrough from sync metadata

class ConnectResult(BaseModel):
    provider_item_id: str
    institution_name: str
    pull_result: PullResult | None = None   # populated when auto_pull=True
```

`SyncClient` methods return these models (not raw dicts). `PlaidLoader.load(sync_data: SyncDataResponse, job_id: str)` consumes the typed model.

---

## Section 4: SyncService

**File:** `src/moneybin/services/sync_service.py`

Business logic and orchestration. The `mcp-server.md` architecture rule applies: CLI and MCP are thin wrappers; the service does the work.

### State source: server, not local

The service does **not** maintain a local `app.sync_connections` cache. Connection state lives on the server (`GET /institutions` returns the current set; `GET /sync/status` per-job returns recent per-institution results with error codes). Reasons:

- Server is already the system of record for connections — it owns the Plaid access tokens
- A local mirror would drift the moment a connection is created via another surface (future web UI)
- Three columns we'd have added (`last_sync_txn_count`, `last_error`, `last_error_code`) are already available from `GET /sync/status` for the latest job
- `sync status` is a low-frequency command; one HTTP call per invocation is fine

### Interface

```python
class SyncService:
    def __init__(self, client: SyncClient, db: Database, loader: PlaidLoader) -> None: ...

    def pull(self, *, institution: str | None = None, force: bool = False) -> PullResult: ...
    def connect(self, *, institution: str | None = None, auto_pull: bool = True, return_to: str | None = None) -> ConnectResult: ...
    def disconnect(self, *, institution: str) -> None: ...
    def list_connections(self) -> list[SyncConnectionView]: ...

    # Internal helpers
    def _resolve_institution(self, name: str) -> str: ...        # name → provider_item_id (via GET /institutions)
    def _map_error_guidance(self, results: list[InstitutionResult]) -> list[str]: ...
```

### `pull()` flow

```python
def pull(self, *, institution=None, force=False) -> PullResult:
    provider_item_id = self._resolve_institution(institution) if institution else None
    trigger_response = self.client.trigger_sync(
        provider_item_id=provider_item_id, reset_cursor=force,
    )
    sync_data = self.client.get_data(trigger_response.job_id)
    self.loader.handle_removed_transactions(sync_data.removed_transactions)
    load_result = self.loader.load(sync_data, trigger_response.job_id)
    return PullResult(
        job_id=trigger_response.job_id,
        transactions_loaded=load_result.transactions,
        accounts_loaded=load_result.accounts,
        balances_loaded=load_result.balances,
        transactions_removed=len(sync_data.removed_transactions),
        institutions=sync_data.metadata.institutions,
    )
```

**No crash-recovery file.** If the client crashes between `trigger_sync` and `get_data`, the user re-runs `sync pull`. The server's TTL store is one-shot (data is consumed on read of `GET /sync/data`), and the Plaid cursor has already advanced, so the lost batch is genuinely lost. Acceptable for Phase 1; the server-side fix (cursor-ack) is tracked as a followup and must land before M3 launch. The CLI logs warnings explicitly in error paths so silent data loss is impossible: every error message names the `job_id` and tells the user to re-run.

### `list_connections()` flow

```python
def list_connections(self) -> list[SyncConnectionView]:
    institutions = self.client.list_institutions()   # GET /institutions
    # Optional: enrich with the latest sync_jobs results for error codes / txn counts
    # For Phase 1, just return what /institutions gives us
    return [
        SyncConnectionView(
            id=i.id,
            provider_item_id=i.provider_item_id,
            institution_name=i.institution_name,
            provider=i.provider,
            status=i.status,
            last_sync=i.last_sync,
            guidance=self._guidance_for(i.status),
        )
        for i in institutions
    ]
```

### Error code → guidance mapping

`SyncService` owns the Plaid-error-code-to-user-message table from `sync-plaid.md`:

```python
ERROR_GUIDANCE = {
    "ITEM_LOGIN_REQUIRED": "{institution} needs re-authentication — run `moneybin sync connect --institution {institution}`",
    "ITEM_NOT_FOUND": "{institution} connection was revoked. Run `moneybin sync connect` to reconnect.",
    "INSTITUTION_NOT_RESPONDING": "{institution} is temporarily unavailable. Try again later.",
    "INSTITUTION_DOWN": "{institution} is down for maintenance. Try again later.",
    "RATE_LIMIT_EXCEEDED": "Rate limit reached. Sync will resume on the next scheduled run.",
    "PRODUCTS_NOT_READY": "{institution} is still processing initial data. Try again in a few minutes.",
}
```

Consumed by `pull()` (post-sync summary) and `list_connections()` (`sync status` output).

---

## Section 5: PlaidLoader

**File:** `src/moneybin/loaders/plaid_loader.py`

### Interface

```python
class PlaidLoader:
    def __init__(self, db: Database) -> None: ...
    def load(self, sync_data: SyncDataResponse, job_id: str) -> LoadResult: ...
    def handle_removed_transactions(self, removed_ids: list[str]) -> int: ...
```

Caller (`SyncService.pull()`) manages connection lifetime per ADR-010:

```python
with get_database(read_only=False) as db:
    service = SyncService(SyncClient.from_settings(), db, PlaidLoader(db))
    result = service.pull(institution=institution, force=force)
```

Connection released as soon as the `with` block exits. The HTTP fetches (`trigger_sync`, `get_data`) happen inside the `with` block but don't hold any DB rows — they're just network calls.

### Loading pattern — `Database.ingest_dataframe()` with Polars

Project rule per `database.md`: `ingest_dataframe()` is the preferred path. No plaintext JSON on disk (no temp files); zero-copy Arrow handoff to DuckDB.

```python
import polars as pl

TRANSACTIONS_SCHEMA = {
    "transaction_id": pl.Utf8,
    "account_id": pl.Utf8,
    "transaction_date": pl.Date,
    "amount": pl.Decimal(18, 2),         # Plaid convention: positive = expense (DO NOT NEGATE HERE)
    "description": pl.Utf8,
    "merchant_name": pl.Utf8,
    "category": pl.Utf8,
    "pending": pl.Boolean,
    "source_file": pl.Utf8,
    "source_type": pl.Utf8,
    "source_origin": pl.Utf8,
    "extracted_at": pl.Datetime,
    "loaded_at": pl.Datetime,
}

def load(self, sync_data: SyncDataResponse, job_id: str) -> LoadResult:
    source_file = f"sync_{job_id}"
    extracted_at = sync_data.metadata.synced_at
    now = datetime.now(timezone.utc)

    # Build per-institution maps so each row carries its source_origin
    item_by_account = self._build_account_to_item_map(sync_data)

    txn_df = pl.DataFrame(
        [
            {
                **txn.model_dump(),
                "source_file": source_file,
                "source_type": "plaid",
                "source_origin": item_by_account[txn.account_id],
                "extracted_at": extracted_at,
                "loaded_at": now,
            }
            for txn in sync_data.transactions
        ],
        schema=TRANSACTIONS_SCHEMA,
    )
    self.db.ingest_dataframe(
        "raw.plaid_transactions", txn_df, on_conflict="upsert"
    )
    # ... same for accounts and balances
```

### Dedup model

PRIMARY KEY: **`(transaction_id, source_origin)`** where `source_origin = provider_item_id`.

This deviates from the OFX pattern (`source_file`-scoped) deliberately. Plaid's `/transactions/sync` is upsert-shaped: each sync returns the current state of recently-added/modified transactions plus a removed-IDs list. With `source_origin` in the PK:

- Re-syncing the same institution UPSERTs the same transactions in place
- `pending = true` → `pending = false` transition is a real REPLACE, not a new row
- `raw.plaid_transactions` stays compact (one row per transaction per institution)
- Cross-institution collisions impossible (Plaid IDs are globally unique, but `source_origin` is defensive in case future provider ports include item-scoped IDs)

`source_file` becomes mutable metadata indicating "last sync that touched this row." Useful for audit/debugging; not part of the PK.

### Sign convention (load-bearing)

The loader stores amounts **exactly as received** — Plaid convention, positive = expense. The `-1 * amount` flip happens **only** in `prep.stg_plaid__transactions`. A prominent comment lives in the loader:

```python
# Plaid convention: positive = expense, negative = income.
# DO NOT NEGATE HERE — the sign flip lives in stg_plaid__transactions.
# Inverting here would silently corrupt cross-source aggregations because the
# matcher and core consumers assume MoneyBin sign convention (negative = expense).
```

### Removed transactions

```python
def handle_removed_transactions(self, removed_ids: list[str]) -> int:
    if not removed_ids:
        return 0
    placeholders = ", ".join("?" for _ in removed_ids)
    self.db.execute(
        f"DELETE FROM raw.plaid_transactions WHERE transaction_id IN ({placeholders})",  # noqa: S608  # placeholders are ?, values parameterized
        removed_ids,
    )
    return len(removed_ids)
```

---

## Section 6: Raw DDL

Three files in `src/moneybin/sql/schema/`. Column comments follow `.claude/rules/database.md` conventions (block `/* */` table comment, inline `--` column comments on the final SELECT).

No `app.sync_connections` table — connection state lives on the server. The only schema files this PR adds are the three raw Plaid tables.

**`raw_plaid_accounts.sql`**

```sql
/* Bank accounts connected via Plaid; one record per account per institution */
CREATE TABLE IF NOT EXISTS raw.plaid_accounts (
    account_id VARCHAR NOT NULL,        -- Plaid account_id; globally unique per Plaid
    account_type VARCHAR,               -- depository, credit, loan, investment, other
    account_subtype VARCHAR,            -- checking, savings, credit card, etc.
    institution_name VARCHAR,           -- Human-readable name from Plaid
    official_name VARCHAR,              -- Official account name from the institution
    mask VARCHAR,                       -- Last 4 digits of the account number
    source_file VARCHAR NOT NULL,       -- Logical identifier: sync_{job_id} (last sync to touch this row)
    source_type VARCHAR NOT NULL DEFAULT 'plaid',
    source_origin VARCHAR NOT NULL,     -- provider_item_id; scopes dedup to the institution connection
    extracted_at TIMESTAMP,             -- From metadata.synced_at
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (account_id, source_origin)
);
```

**`raw_plaid_transactions.sql`**

```sql
/* Transactions fetched from Plaid /transactions/sync; one record per transaction per institution */
CREATE TABLE IF NOT EXISTS raw.plaid_transactions (
    transaction_id VARCHAR NOT NULL,    -- Plaid transaction_id; globally unique per Plaid
    account_id VARCHAR NOT NULL,        -- Plaid account_id; foreign key to raw.plaid_accounts
    transaction_date DATE NOT NULL,     -- Date the transaction posted; from Plaid date field
    amount DECIMAL(18, 2) NOT NULL,     -- CAUTION: Plaid convention is positive = expense; sign flip is in stg_plaid__transactions
    description VARCHAR,                -- Plaid name field
    merchant_name VARCHAR,              -- Plaid merchant_name; NULL when Plaid cannot identify
    category VARCHAR,                   -- Plaid personal_finance_category.primary
    pending BOOLEAN DEFAULT FALSE,
    source_file VARCHAR NOT NULL,       -- Logical identifier: sync_{job_id} (last sync to touch this row)
    source_type VARCHAR NOT NULL DEFAULT 'plaid',
    source_origin VARCHAR NOT NULL,     -- provider_item_id; scopes dedup to the institution connection
    extracted_at TIMESTAMP,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (transaction_id, source_origin)
);
```

**`raw_plaid_balances.sql`**

```sql
/* Account balance snapshots from Plaid; one record per account per balance date per institution */
CREATE TABLE IF NOT EXISTS raw.plaid_balances (
    account_id VARCHAR NOT NULL,        -- Plaid account_id
    balance_date DATE NOT NULL,         -- Date the balance was reported
    current_balance DECIMAL(18, 2),     -- Current balance including pending transactions
    available_balance DECIMAL(18, 2),   -- Available balance; NULL for credit accounts
    source_file VARCHAR NOT NULL,
    source_type VARCHAR NOT NULL DEFAULT 'plaid',
    source_origin VARCHAR NOT NULL,
    extracted_at TIMESTAMP,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (account_id, balance_date, source_origin)
);
```

---

## Section 7: SQLMesh Staging and Core

### Staging views (`sqlmesh/models/prep/`)

`stg_plaid__transactions.sql` — sign flip here and ONLY here:

```sql
MODEL (
  name prep.stg_plaid__transactions,
  kind VIEW
);

SELECT
  transaction_id,
  account_id,
  transaction_date AS posted_date,
  -1 * amount AS amount,                 -- Flip Plaid (positive = expense) → MoneyBin (negative = expense)
  TRIM(description) AS description,
  TRIM(merchant_name) AS merchant_name,
  category AS plaid_category,
  pending AS is_pending,
  source_file,
  source_type,
  source_origin,
  extracted_at,
  loaded_at
FROM raw.plaid_transactions
```

`stg_plaid__accounts.sql` and `stg_plaid__balances.sql` — light type normalization, no sign flip. Per `database.md`, `stg_*` views get no column comments (internal layer).

### Core model additions

`dim_accounts.sql` — add `plaid_accounts` CTE selecting from `prep.stg_plaid__accounts`, `UNION ALL` into the existing `all_accounts` CTE. Set `source_type = 'plaid'`.

`fct_transactions.sql` — add `plaid_transactions` CTE selecting from `prep.stg_plaid__transactions`, `UNION ALL` into existing `all_transactions`. Sign is already correct at this layer.

Cross-source dedup (a transaction appearing in both OFX and Plaid) is handled by the existing matcher pipeline per `matching-overview.md`. No changes to core dedup logic.

---

## Section 8: CLI Commands

**File:** `src/moneybin/cli/commands/sync.py` (replacing `_not_implemented` stubs)

| Command | Description |
|---|---|
| `moneybin sync login [--no-browser] [-o text\|json] [-q]` | Device Auth Flow |
| `moneybin sync logout` | Clear stored JWT |
| `moneybin sync connect [--institution NAME] [--no-pull] [--no-browser] [-y/--yes] [-o text\|json]` | Connect new institution OR re-auth existing; auto-pulls by default |
| `moneybin sync connect-status --session-id SESSION_ID [-o text\|json]` | Verify a pending connect session completed; CLI mirror of MCP `sync_connect_status` |
| `moneybin sync disconnect --institution NAME [-y/--yes] [-o text\|json]` | Remove a connection |
| `moneybin sync pull [--institution NAME] [--force] [-o text\|json] [-q] [--json-fields ...]` | Sync data |
| `moneybin sync status [-o text\|json] [-q] [--json-fields ...]` | Show connections + health |

### `--output json` shapes

Per `.claude/rules/cli.md`, every mutating command also accepts `--output json` so agents can drive end-to-end without scraping human-formatted text.

**`sync pull --output json`:**

```json
{
  "job_id": "550e8400-...",
  "transactions_loaded": 142,
  "accounts_loaded": 2,
  "balances_loaded": 2,
  "transactions_removed": 1,
  "institutions": [
    {"provider_item_id": "item_abc", "institution_name": "Chase", "status": "completed", "transaction_count": 80, "error_code": null},
    {"provider_item_id": "item_def", "institution_name": "Schwab", "status": "failed", "error_code": "ITEM_LOGIN_REQUIRED"}
  ]
}
```

**`sync connect --output json --no-browser`:** returns the link URL and exits without polling, mirroring MCP `sync_connect`:

```json
{
  "session_id": "sess_abc123",
  "link_url": "https://hosted.plaid.com/link/...",
  "expiration": "2026-05-13T13:30:00Z"
}
```

The agent then calls `sync connect-status --session-id sess_abc123 --output json` to verify.

**`sync connect --output json`** (without `--no-browser`): blocks until connected, returns the full result including the auto-pull summary if applicable.

### `sync connect` decision tree

Without `--institution` flag:

- **Zero institutions in `status='error'`:** new-connection flow
- **Exactly one institution in `status='error'`:**
  - Interactive (TTY, no `--yes`): prompt `Re-authenticate {institution}? [Y/n]`
  - Non-interactive (`--yes` OR no TTY): exit code 2 with a clear error: "Found 1 institution needing re-auth. Pass `--institution {name}` to confirm intent, or pass `--institution NEW` to create a new connection." Agents must be explicit.
- **Multiple institutions in `status='error'`:** list them; exit code 2 directing user to pass `--institution NAME`. Even interactive — too easy to pick the wrong one.

With `--institution NAME`: resolve NAME → `provider_item_id` via `GET /institutions`; pass to `POST /sync/connect/initiate` (server detects update mode). If NAME doesn't match any existing connection, treat as new-connection request and let the server's connect flow handle naming.

`--yes` flag's semantics: skip the single-institution re-auth confirmation prompt only. It never selects an institution on its own — selection is always explicit via `--institution`. This matches `.claude/rules/cli.md` non-interactive parity without papering over ambiguity.

### Auto-pull behavior

`sync connect` auto-pulls after a successful connection by default. `--no-pull` opts out (useful for: connecting multiple institutions before pulling once, scripting/testing, agents that want to manage the pull separately).

If the connection succeeds but the auto-pull fails, the connection is kept and the error is reported with a clear next step:

```
✅ Connected Chase
❌ Auto-pull failed: server returned 503
💡 Run `moneybin sync pull --institution Chase` to retry
```

### `key` subgroup

The existing `moneybin sync key rotate` subcommand stays as `_not_implemented` stub. Phase 3 work (E2E encryption) — see Section 12.

---

## Section 9: MCP Tools and Prompt

**File:** `src/moneybin/mcp/tools/sync.py` (replacing `not_implemented_envelope` stubs)

### Tools

| Tool | Sensitivity | Description |
|---|---|---|
| `sync_pull` | `medium` | Trigger sync, fetch, load. Amounts in loaded data follow MoneyBin convention (negative = expense, positive = income). Returns summary envelope with per-institution results. |
| `sync_status` | `low` | List connections with health, last sync, mapped error guidance |
| `sync_connect` | `medium` | Initiate connect flow; returns `{session_id, link_url, expiration}` immediately (does NOT block). The `link_url` is a one-time bearer credential — treat as sensitive. |
| `sync_connect_status` | `low` | Verify a connect session completed; includes `expiration` so agent can decide when to give up |
| `sync_disconnect` | `medium` | Remove a connection |

`sync_login`/`sync_logout` remain CLI-only (browser interaction; credential handling). `sync_pull` and friends return a clear error if not authenticated, directing the user to run `moneybin sync login`.

### `sync_connect` is event-driven, not loop-driven

Tool description (visible to agent at selection time):

> Initiate a bank-connection flow. Returns a URL the user opens in their browser to complete the Plaid Hosted Link UI. Does not wait for completion. After the user confirms they've finished, call `sync_connect_status` with the returned `session_id` to verify. The `link_url` is a sensitive one-time credential — present it to the user but do not include it in logs or summaries.

Response envelope on call:

```json
{
  "summary": {"sensitivity": "medium", "display_currency": "USD"},
  "data": {
    "session_id": "sess_abc",
    "link_url": "https://hosted.plaid.com/link/...",
    "expiration": "2026-05-13T13:30:00Z"
  },
  "actions": [
    "Present the link_url to the user and ask them to complete the connection in their browser.",
    "After they confirm completion, call sync_connect_status with the session_id to verify.",
    "Once verified, call sync_pull to fetch transactions.",
    "Session expires at the expiration timestamp — beyond that, start a new connect flow."
  ]
}
```

`sync_connect_status` when status is `pending`:

```json
{
  "summary": {"sensitivity": "low"},
  "data": {"status": "pending", "session_id": "sess_abc", "expiration": "2026-05-13T13:30:00Z"},
  "actions": [
    "Connection has not completed yet. Ask the user to finish the flow in their browser, or wait and check again.",
    "If the session expiration has passed, start a new connect flow with sync_connect."
  ]
}
```

This shapes agent behavior toward "ask user, then verify once" rather than a tight polling loop. Each tool call is a discrete conversation turn. The included `expiration` lets the agent reason about when to give up.

### `sync_pull` description

> Pull transactions, accounts, and balances from connected institutions through moneybin-server. Amounts in loaded data follow MoneyBin convention (negative = expense, positive = income); the Plaid sign flip happens during ingestion. Returns per-institution results including `error_code` for any failed institutions. Mutates `raw.plaid_*` tables and propagates through SQLMesh to core; not directly revertable but idempotent on re-run (transactions upsert by `(transaction_id, provider_item_id)`).

### `sync_review` MCP prompt

```text
Review my MoneyBin sync state and flag anything that needs attention.

Use these tools (in order):
1. sync_status — list connected institutions with last sync time, status, and any error guidance.
2. spending_summary detail=summary — optional, for context on recent transaction volume per institution.

Report concisely (bulleted, single paragraph if everything is healthy):

- **Errors:** any institutions with status='error' and the specific re-auth or reconnect action — quote the exact command from the actions hint.
- **Stale data:** any institution whose last_sync is more than 7 days ago, even if status='active'. Suggest running `moneybin sync pull`.
- **Anomalies:** institutions whose recent sync transaction counts are dramatically lower than typical volume (use spending_summary as a rough yardstick — a checking account that's been returning ~30/week suddenly returning 0 is worth flagging).
- **Recommended next action:** one specific command, or "no action needed."

Do not include account numbers, balances, individual transaction descriptions, or merchant names. Stick to counts, dates, status codes, and institution names.
```

The prompt is registered alongside the tools via FastMCP's prompts API.

---

## Section 10: Testing

### Unit tests (no server dependency)

`tests/test_plaid_loader.py`:
- Load valid `SyncDataResponse` → assert row counts in `raw.plaid_*`
- Load with duplicate `(transaction_id, source_origin)` → assert UPSERT replaces row
- Load pending transaction, then load non-pending → assert single row with `pending=false`
- `handle_removed_transactions([...])` → assert deletion
- Sign convention: assert amount in raw equals amount in `SyncDataResponse` (no negation in loader)

`tests/test_sync_client.py` (via `respx` for httpx mocking):
- `login()` happy path → assert keyring write
- `login()` `slow_down` response → interval increases by 5s
- `login()` user denied → raises `SyncAuthError`
- 401 → refresh (with rotation) → retry → success; assert new refresh token stored
- 401 → refresh fails → raise `SyncAuthError`; assert stored tokens cleared
- Connect flow polls until `connected`; respects `_LONG_TIMEOUT`
- `trigger_sync()` returns synchronous result; honors `_LONG_TIMEOUT`

`tests/test_sync_service.py`:
- `pull()` happy path → SyncClient + PlaidLoader called in order; returns `PullResult` with correct counts
- Partial failure (one institution `failed`, one `completed`) → both reflected in `PullResult.institutions`
- Re-auth path: `connect(institution="Schwab")` resolves name via `GET /institutions` → passes `provider_item_id` to `connect/initiate`
- `connect(auto_pull=True)` → returns `ConnectResult` with `pull_result` populated
- `connect(auto_pull=True)` with pull failure → returns `ConnectResult` with `pull_result=None` and a clear error

### SQL tests

`tests/fixtures/plaid_sync_response.yaml` — YAML fixture per project convention.

SQLMesh tests:
- `stg_plaid__transactions`: amount sign flip (raw `42.50` → staging `-42.50`; raw `-1500.00` → staging `1500.00`)
- `dim_accounts`: Plaid accounts appear with `source_type='plaid'` after UNION ALL
- `fct_transactions`: Plaid transactions appear with MoneyBin sign convention and `source_type='plaid'`

### Integration tests (deferred)

Plaid Sandbox tests run only when `MONEYBIN_SYNC__TEST_SERVER_URL` is set; marked `@pytest.mark.integration`; skipped by default. Setup details deferred per `sync-overview.md` "Tests requiring a running server."

---

## Section 11: Server-Side Dependencies

The Phase 1 client doesn't work end-to-end until the moneybin-server implements the corresponding endpoints and integrations. Listed inline (no cross-repo path links per project convention):

### Endpoints (new or changed)

- **`POST /sync/connect/initiate`** — body `{provider, provider_item_id?, return_to?}`; response `{session_id, link_url, connect_type, expiration}`. Body shape MUST be supported even when all fields are absent (defaults to new-connection flow for Plaid). With `provider_item_id` → update mode against the existing item. With `return_to` → server configures Plaid Hosted Link's `redirect_uri`; without `return_to` → Plaid displays default completion page.
- **`GET /sync/connect/status?session_id=...`** — response includes `expiration` so the client can decide when to give up. Filter by `auth.user_id` — session table MUST have a `user_id` column and the endpoint MUST 404 on cross-user lookups.
- **`POST /auth/refresh`** — body `{refresh_token}`; response `{access_token, refresh_token, expires_in}`. Refresh tokens MUST rotate (single-use): each call returns a new refresh token and invalidates the old one. Proxies refresh to Auth0 with refresh-token rotation enabled.
- **`POST /webhooks/plaid`** — internal; handles `SESSION_FINISHED` (and `ITEM_LOGIN_REQUIRED`, `ERROR` for re-auth). Implementation requirements below.
- **Rename in responses:** `item_id` → `provider_item_id` throughout client-visible responses (`GET /institutions`, `GET /sync/status.results[]`, `GET /sync/data.metadata.institutions[]`, `POST /sync/trigger` request body).
- **Remove:** `POST /sync/link-token` and `POST /sync/exchange-token` (replaced by the initiate/webhook flow).
- **`GET /sync/data`** — one-shot read; server deletes from TTL store on successful read. The client design (no crash-recovery file) depends on this; if the server keeps data readable for the full TTL window, the design still works but data loss on client crash becomes guaranteed instead of probabilistic.

### Server-side new infrastructure

- **Session storage** — keyed by `session_id`. Required columns: `user_id` (for ownership filtering), `link_token` (Plaid's token, for webhook correlation), `provider_item_id`, `status`, `error`, `created_at`, `expires_at`, `return_to`. PostgreSQL table or Redis (server team's call). Must support 30-min TTL aligned with Plaid Hosted Link session expiry.
- **Plaid Hosted Link integration** — `linkTokenCreate` with `hosted_link: {}` config; include `redirect_uri` config when the client passed `return_to`. Capture the `hosted_link_url` from response. Configure `SESSION_FINISHED` webhook URL pointing to `POST /webhooks/plaid`.
- **Webhook handler responsibilities:**
  - **Signature verification:** verify `Plaid-Verification` JWT against Plaid's webhook public key (rotated periodically — fetch from JWKS, cache with TTL).
  - **Replay-window enforcement:** reject JWTs with `iat` older than 5 minutes to prevent replay attacks.
  - **Request-size limit:** cap webhook body at e.g. 1 MB; reject larger.
  - **Rate limiting:** apply rate limits at the IP and at the JWT-issuer level to mitigate webhook flooding attacks (publicly reachable, unauthenticated by JWT).
  - **Idempotency:** dedupe per webhook type. For `SESSION_FINISHED`: dedupe on `link_session_id` (Plaid's stable session identifier; the `webhook_id` field is not reliably present). For item-lifecycle webhooks: dedupe on `(item_id, webhook_code, environment)`. First arrival wins; duplicates are 200-acked without reprocessing.
  - **On `SESSION_FINISHED`:** lookup session by `link_token`, call `plaid.itemPublicTokenExchange()`. On success: encrypt `access_token` with the existing AES-256-GCM `ENCRYPTION_KEY`, store in `plaid_items`, update session row to `status='connected'` with `provider_item_id` and `institution_name` populated.
  - **On exchange failure (Plaid 5xx, network blip, retries exhausted):** update session row to `status='failed'` with a clear `error` message. **Sessions must not stay `pending` indefinitely** — the client's `GET /sync/connect/status` poll must eventually see a terminal state.
  - **On `ITEM_LOGIN_REQUIRED` / `ERROR`:** update `plaid_items.status` so the next `POST /sync/trigger` for that item reports the error code in `results[]`.

### Server-side cursor ack (required before launch, not Phase 1)

The Phase 1 client design has no crash-recovery file. If the client crashes between `POST /sync/trigger` and `GET /sync/data`, the Plaid cursor has already advanced and the data batch is lost. This is acceptable for early Phase 1 testing but **must be fixed server-side before M3 launch.**

The fix: don't advance the persisted Plaid cursor until the client `acks` receipt. Server holds the data in the TTL store and keeps the prior cursor until acked. The client adds a `POST /sync/data/ack` (or `DELETE /sync/data?job_id=…`) call after successful load. On ack: drop TTL entry, commit the new cursor. On TTL expiry without ack: drop TTL entry, revert the cursor so the next sync re-fetches the lost batch. Tracked as a coordinating-server-PR followup.

### Coordination

Client PR (this branch) and server PR ship paired. Until the server endpoints land, the client's integration tests are skipped (gated by `MONEYBIN_SYNC__TEST_SERVER_URL`). Unit tests using `respx` mocks exercise the full client surface independently.

---

## Section 12: Out of Scope

- `sync schedule` commands (Phase 2 — automation track in `sync-overview.md` build order)
- E2E encryption (Phase 3 — gated on server implementing client-side key exchange). `sync key rotate` stub stays in CLI. Open design question for Phase 3: whether key rotation can be MCP-exposed safely (no passphrase material in the LLM context — the agent triggers rotation; new keys generated locally; public key goes server-via-HTTPS — but operational consistency with `db_unlock`/`db_rotate_key` (which DO require passphrases) argues for CLI-only uniformity). Decision deferred to the Phase 3 spec.
- Post-quantum cryptography (Phase 4 — designed in `sync-overview.md`)
- Plaid Investments product (separate spec, gated on `investment-tracking.md`)
- Plaid Liabilities product
- SimpleFIN, MX, TrueLayer integration (Phase 2/3 — `connect_type` discriminator is in place for forward compat; `POST /sync/connect/submit` endpoint and the `token_paste` client path are NOT in this PR)
- Local web server callback for OAuth on the CLI (Phase 1 polish — no server API changes needed; deferred)
- Plaid Production OAuth approval — submit the application during this PR's review cycle given the 4–8 week approval timeline; can run in parallel with remaining implementation work
- Provider sequencing rationale (SimpleFIN/MX/TrueLayer detailed plan) — tracked separately in product strategy docs
- Server-side cursor ack — required before M3 launch but not blocking Phase 1 client work; see Section 11
- Web UI surface (M3D) — the `return_to` parameter on `connect/initiate` is forward-compatible, but the actual web UI is out of scope here

---

## Open Design Questions (resolved during brainstorming, recorded for context)

| Question | Decision |
|---|---|
| Dedup PK: `(transaction_id, source_file)` or `(transaction_id, source_origin)`? | `(transaction_id, source_origin)`. Plaid's sync API is upsert-shaped; OFX-style append-only doesn't fit. |
| Token refresh: extend `/auth/device/token` or separate `/auth/refresh`? | Separate `/auth/refresh` with rotating refresh tokens (single-use). |
| MCP connect-flow polling: blocking, looped, or event-driven? | Event-driven via separate `sync_connect_status` tool, with `expiration` in responses so agents know when to give up. |
| `--no-browser`: explicit flag, auto-detect, or both? | Both. Auto-print URL alongside browser open; `--no-browser` skips the open attempt. |
| New table: schema file + migration, or schema file only? | Neither — we don't introduce a new local table (see next row). Convention shift for future cases still applies. |
| Local mirror of server connection state in `app.sync_connections`? | Dropped. Server is the system of record; `sync status` reads `GET /institutions` per invocation. Avoids drift when web UI lands. |
| Crash-recovery file for in-flight sync? | Dropped. Server-side cursor-ack is the real fix and tracked as required-before-launch. Phase 1 logs loud warnings on crash; user re-runs `sync pull`. |
| Forward-compat for web UI's connect-redirect needs? | Added `return_to: string \| null` to `POST /sync/connect/initiate` body. CLI sends null; future web UI sends its callback URL. |
| Response models: Pydantic everywhere, partial, or dicts? | Pydantic at every API boundary. Single `sync_models.py` module imported by client + loader; service-layer result types live alongside. |
| Service layer scope: `SyncConnectionService` or broader `SyncService`? | `SyncService` — owns orchestration; no local connection-table operations (state lives on server). |
| Provider ladder section in design doc: full detail, trimmed, or none? | Trimmed. Public design doc mentions the discriminator; sequencing/rationale stays in strategy docs. |
| Auto-pull after connect? | Yes, by default. `--no-pull` to opt out. |
| Per-endpoint timeout configuration? | Two constants in `sync_client.py` (15s default, 120s long-op). No config knobs without evidence of need. |
| CLI `--output json` on mutating commands? | Yes for `pull`, `connect`, `connect-status`, `disconnect` per agent-surface rule. |
| Missing `sync connect-status` CLI command? | Added (CLI symmetry with `sync_connect_status` MCP tool). |
| `sync_connect` MCP sensitivity? | Bumped from `low` to `medium` — `link_url` is a one-time bearer credential. |
