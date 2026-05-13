# Plaid Sync Phase 1 — Design Document

**Date:** 2026-05-13
**Status:** Approved — implementation starting
**Branch:** `feat/plaid-sync`
**Specs implemented:** `sync-plaid.md` (Phase 1), `sync-overview.md` (status → ready)

---

## Background

MoneyBin's sync stubs have been placeholders since the architecture was laid down. This document captures the design decisions reached during the Phase 1 brainstorming session before cutting code. It supersedes the implementation sketch in `sync-plaid.md` where the two disagree.

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
  Body: { "provider": "plaid", "provider_item_id": "item_abc" }   # provider_item_id optional (update mode)
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
    "error": null                         # error message when status == "failed"
  }

POST /auth/refresh
  Body: { "refresh_token": "..." }
  Response 200: { "access_token": "...", "refresh_token": "...", "expires_in": 3600 }

POST /webhooks/plaid                      # internal, not in client API surface
  Handles SESSION_FINISHED + Plaid item lifecycle webhooks
  Verified via Plaid-Verification JWT header
```

**Renamed:** `item_id` → `provider_item_id` everywhere in client-visible responses (`GET /institutions`, `GET /sync/status.results[]`, `GET /sync/data.metadata.institutions[]`).

**Removed from client API:**
- `POST /sync/link-token` (replaced by `POST /sync/connect/initiate`)
- `POST /sync/exchange-token` (now internal webhook handler)

**Retained:**
- `POST /sync/trigger` — synchronous, returns final state, `provider_item_id` body param
- `GET /sync/status` — fallback for crash recovery
- `GET /sync/data` — unchanged shape
- `DELETE /institutions/:id` — unchanged

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

### Token refresh

On any `401 Unauthorized` from a sync endpoint:
1. If a refresh token is stored, call `POST /auth/refresh` with it
2. On success: update stored JWT + refresh token, retry the original request once
3. On refresh failure (refresh token expired or revoked): clear keyring/file entries, raise `SyncAuthError("session expired — run moneybin sync login")`

Refresh is transparent to method callers — they see either success or `SyncAuthError`.

### Browser handling

`webbrowser.open()` returns True on success, but on some Linux systems it lies (returns True with no actual browser launched). To handle this robustly:

- Default: call `webbrowser.open()`; whether it returns True or False, always also print the URL to stderr so a headless user can copy it
- `--no-browser` flag: skip the `webbrowser.open()` call entirely; print URL only
- Auto-detection of headless env (no `DISPLAY` on Linux, no Aqua session on macOS, etc.) is **not** implemented in Phase 1 — explicit flag is enough and more predictable

### Connect flow

```python
def connect(self, provider_item_id: str | None = None) -> ConnectResult:
    body = {"provider": "plaid"}
    if provider_item_id:
        body["provider_item_id"] = provider_item_id   # update mode
    resp = self._post("/sync/connect/initiate", body)
    initiate = ConnectInitiateResponse.model_validate(resp)
    if initiate.connect_type != "widget_flow":
        raise NotImplementedError(
            f"connect_type '{initiate.connect_type}' is not supported in this version"
        )
    self._open_url(initiate.link_url)        # honors --no-browser
    return self._poll_connect(initiate.session_id)

def _poll_connect(self, session_id: str) -> ConnectResult:
    deadline = time.time() + self.config.connect_timeout_seconds   # default 300
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

### Per-endpoint timeouts

httpx default (5s) is far too short for `POST /sync/trigger`. Timeouts configured per-endpoint via `SyncConfig`:

```python
# src/moneybin/config.py — SyncConfig additions
timeout_auth_seconds: float = 10.0       # /auth/* endpoints
timeout_default_seconds: float = 15.0    # connect/status, institutions, etc.
timeout_data_seconds: float = 30.0       # GET /sync/data (JSON download)
timeout_trigger_seconds: float = 120.0   # POST /sync/trigger (long-running)
connect_timeout_seconds: float = 300.0   # _poll_connect deadline (5 min)
```

Users can override via env vars (e.g., `MONEYBIN_SYNC__TIMEOUT_TRIGGER_SECONDS=180`).

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

All server responses validated at the boundary. Single source of truth for the contract; imported by both `SyncClient` and `PlaidLoader`.

```python
from pydantic import BaseModel, Field
from datetime import date, datetime
from decimal import Decimal
from typing import Literal

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
```

`SyncClient` methods return these models (not raw dicts). `PlaidLoader.load(sync_data: SyncDataResponse, job_id: str)` consumes the typed model.

---

## Section 4: SyncService

**File:** `src/moneybin/services/sync_service.py`

Business logic and orchestration. The `mcp-server.md` architecture rule applies: CLI and MCP are thin wrappers; the service does the work.

### Interface

```python
class SyncService:
    def __init__(self, client: SyncClient, db: Database, loader: PlaidLoader) -> None: ...

    def pull(self, *, institution: str | None = None, force: bool = False) -> PullResult:
        """Full sync: resume any in-flight job, trigger, fetch, load, update connections."""
        ...

    def connect(self, *, institution: str | None = None, auto_pull: bool = True) -> ConnectResult:
        """Connect a new institution OR re-auth an existing one. If auto_pull, follow with pull()."""
        ...

    def disconnect(self, *, institution: str) -> None:
        """Resolve name → id; call DELETE /institutions/:id; remove from app.sync_connections."""
        ...

    def list_connections(self) -> list[SyncConnectionView]:
        """Read app.sync_connections; map error codes to user-facing guidance."""
        ...

    # Internal helpers
    def _resolve_institution(self, name: str) -> str: ...        # name → provider_item_id
    def _resume_or_trigger(self, ...) -> SyncTriggerResponse: ...   # crash recovery
    def _update_connections(self, results: list[InstitutionResult]) -> None: ...
```

### Crash recovery (see Section 8: `.in_flight_sync.json`)

Inside `pull()`:

```python
def pull(self, *, institution=None, force=False) -> PullResult:
    resumed = self._try_resume_in_flight()
    if resumed:
        logger.info(f"Found in-flight sync from {resumed.age_minutes:.0f} min ago — resuming")
        sync_data = self.client.get_data(resumed.job_id)
        self._load_and_record(sync_data, resumed.job_id)
        self._clear_in_flight()
        # fall through to trigger a fresh sync (the resumed one is now loaded)

    provider_item_id = self._resolve_institution(institution) if institution else None
    trigger_response = self.client.trigger_sync(provider_item_id=provider_item_id, reset_cursor=force)
    self._write_in_flight(trigger_response.job_id)
    try:
        sync_data = self.client.get_data(trigger_response.job_id)
        result = self._load_and_record(sync_data, trigger_response.job_id)
    finally:
        self._clear_in_flight()
    return result
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

The mapping is consumed by both `pull()` (post-sync summary) and `list_connections()` (`sync status` output).

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

## Section 7: `app.sync_connections` (schema file only)

**File:** `src/moneybin/sql/schema/app_sync_connections.sql`

No V009 migration. The schema file's `CREATE TABLE IF NOT EXISTS` runs on every app startup via `init_schemas`; that handles fresh installs *and* upgrades from existing DBs that don't have the table yet. A migration would just duplicate the same DDL.

> **Convention shift:** This is the first table that ships without a paired V00X migration. The pattern going forward is: schema files for new tables, migrations for ALTERs/backfills/drops/renames. Retroactive cleanup of V004/V005-style redundant migrations and an update to `migrations/README.md` are tracked as followup work.

```sql
/* Connected institutions and their sync health; one row per provider connection */
CREATE TABLE IF NOT EXISTS app.sync_connections (
    item_id VARCHAR NOT NULL,           -- provider_item_id (named item_id locally for brevity); maps to server's provider_item_id
    provider VARCHAR NOT NULL DEFAULT 'plaid',     -- Aggregator: plaid, simplefin, mx
    institution_name VARCHAR,           -- Human-readable institution name
    status VARCHAR NOT NULL DEFAULT 'active',      -- active, error, revoked
    last_sync_at TIMESTAMP,             -- Last successful sync completion
    last_sync_txn_count INTEGER,        -- Transactions returned in the last sync
    last_error VARCHAR,                 -- Most recent error message (NULL when healthy)
    last_error_code VARCHAR,            -- Provider error code for programmatic handling
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (item_id)
);
```

---

## Section 8: Crash Recovery (`in_flight_sync.json`)

The risk: between `POST /sync/trigger` (server advances Plaid cursor) and `GET /sync/data` (client downloads), if the client crashes, the `job_id` is lost from memory. Server holds the data in a 30-min TTL store. If TTL expires before the client recovers, that batch is silently lost because the cursor already advanced.

### Mechanism

`SyncService.pull()` persists `{job_id, started_at}` to `~/.moneybin/.in_flight_sync.json` (0600) immediately after `trigger_sync` returns. The file is deleted after `get_data` + `load` complete successfully.

On every `sync pull` invocation, before triggering a new sync:

```python
def _try_resume_in_flight(self) -> ResumeRecord | None:
    if not in_flight_path.exists():
        return None
    record = json.loads(in_flight_path.read_text())
    age = datetime.now(timezone.utc) - datetime.fromisoformat(record["started_at"])
    if age > timedelta(minutes=25):    # 5-min buffer before TTL expiry
        logger.warning(
            f"In-flight sync from {age} ago is past safe-resume window; abandoning. "
            f"Job ID: {record['job_id']}. Data may have been lost."
        )
        in_flight_path.unlink()
        return None
    return ResumeRecord(**record)
```

If resume succeeds, the loaded data is committed and the flow continues with a fresh trigger (which fetches anything new since the resumed batch).

If resume fails (404 from server — TTL expired despite our window estimate, or job was a different user's), log the data loss and proceed with a fresh sync. The user knows because the warning is loud.

### Server-side followup

The cleanest fix lives server-side: don't advance the Plaid cursor until the client `acks` receipt. The client mitigation above shrinks the window but doesn't close the hole (laptop sleeping > 30 min during sync still loses data). Tracked as a server-side followup; coordinate with moneybin-server before launch so this PR doesn't ship a known silent-data-loss path.

---

## Section 9: SQLMesh Staging and Core

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

## Section 10: CLI Commands

**File:** `src/moneybin/cli/commands/sync.py` (replacing `_not_implemented` stubs)

| Command | Description |
|---|---|
| `moneybin sync login [--no-browser]` | Device Auth Flow |
| `moneybin sync logout` | Clear stored JWT |
| `moneybin sync connect [--institution NAME] [--no-pull] [--no-browser] [-y/--yes]` | Connect new institution OR re-auth existing one; auto-pulls by default |
| `moneybin sync disconnect --institution NAME [-y/--yes]` | Remove a connection |
| `moneybin sync pull [--institution NAME] [--force]` | Sync data |
| `moneybin sync status [-o/--output text|json] [-q/--quiet] [--json-fields ...]` | Show connections + health |

### `sync connect` interactive UX

Without `--institution`, the command auto-detects re-auth need:

```
$ moneybin sync connect
Scanning your connections...
⚠️  Schwab is in error state: ITEM_LOGIN_REQUIRED.
Re-authenticate Schwab now? [Y/n]: y
⚙️  Opening browser to re-authenticate Schwab...
   If your browser didn't open, visit: https://hosted.plaid.com/link/...
Waiting for connection... (Ctrl-C to cancel)
✅ Re-authenticated Schwab
⚙️  Pulling latest transactions...
✅ Synced 12 transactions from Schwab
```

Decision tree:
- Exactly one institution in `status='error'` AND no `--institution` flag → prompt to re-auth that one
- Zero error-state institutions AND no `--institution` flag → new connection flow
- Multiple error-state institutions → list them, ask user to pick or pass `--institution NAME`
- `--institution NAME` provided → resolve to `provider_item_id`; pass to `POST /sync/connect/initiate` (update mode)
- `--yes` skips the re-auth confirmation prompt (parity for agents/scripts per `.claude/rules/cli.md` Non-Interactive Parity)

### Auto-pull behavior

`sync connect` auto-pulls after a successful connection by default. `--no-pull` opts out (useful for: connecting multiple institutions before pulling once, scripting/testing, agents that want to manage the pull separately).

If the connection succeeds but the auto-pull fails, the connection is kept and the error is reported with a clear next step:

```
✅ Connected Chase
❌ Auto-pull failed: server returned 503
💡 Run `moneybin sync pull --institution Chase` to retry
```

### `key` subgroup

The existing `moneybin sync key rotate` subcommand stays as `_not_implemented` stub. Phase 3 work (E2E encryption) — see Section 14.

---

## Section 11: MCP Tools and Prompt

**File:** `src/moneybin/mcp/tools/sync.py` (replacing `not_implemented_envelope` stubs)

### Tools

| Tool | Description | Parameters |
|---|---|---|
| `sync_pull` | Trigger sync, fetch, load. Returns summary envelope. | `institution: str \| None`, `force: bool` |
| `sync_status` | List connections with health, last sync, mapped error guidance | none |
| `sync_connect` | Initiate connect flow; returns `{session_id, link_url}` immediately (does NOT block) | `institution: str \| None` (for re-auth update mode) |
| `sync_connect_status` | Verify a connect session completed | `session_id: str` |
| `sync_disconnect` | Remove a connection | `institution: str` |

`sync_login`/`sync_logout` remain CLI-only (browser interaction; credential handling). `sync_pull` and friends return a clear error if not authenticated, directing the user to run `moneybin sync login`.

### `sync_connect` is event-driven, not loop-driven

Tool description (visible to agent at selection time):

> Initiate a bank-connection flow. Returns a URL the user opens in their browser to complete the Plaid Hosted Link UI. Does not wait for completion. After the user confirms they've finished, call `sync_connect_status` with the returned `session_id` to verify.

Response envelope when called:

```json
{
  "summary": {"sensitivity": "low", "display_currency": "USD"},
  "data": {"session_id": "sess_abc", "link_url": "https://hosted.plaid.com/link/..."},
  "actions": [
    "Present the link_url to the user and ask them to complete the connection in their browser.",
    "After they confirm completion, call sync_connect_status with the session_id to verify.",
    "Once verified, call sync_pull to fetch transactions."
  ]
}
```

`sync_connect_status` when status is `pending`:

```json
{
  "summary": {"sensitivity": "low"},
  "data": {"status": "pending", "session_id": "sess_abc"},
  "actions": [
    "Connection has not completed yet. Ask the user to finish the flow in their browser, or wait and check again."
  ]
}
```

This shapes agent behavior toward "ask user, then verify once" rather than a tight polling loop. Each tool call is a discrete conversation turn.

### `sync_review` MCP prompt

```text
Review my MoneyBin sync state and flag anything that needs attention.

Use these tools (in order):
1. sync_status — list connected institutions with last sync time, status, and any error guidance.
2. spending_summary detail=summary — optional, for context on recent transaction volume per institution.

Report concisely (bulleted, single paragraph if everything is healthy):

- **Errors:** any institutions with status='error' and the specific re-auth or reconnect action — quote the exact command from the actions hint.
- **Stale data:** any institution whose last_sync_at is more than 7 days ago, even if status='active'. Suggest running `moneybin sync pull`.
- **Anomalies:** institutions whose last_sync_txn_count is dramatically lower than their typical recent volume (use spending_summary as a rough yardstick — a checking account that's been returning ~30/week suddenly returning 0 is worth flagging).
- **Recommended next action:** one specific command, or "no action needed."

Do not include account numbers, balances, individual transaction descriptions, or merchant names. Stick to counts, dates, status codes, and institution names.
```

The prompt is registered alongside the tools via FastMCP's prompts API.

---

## Section 12: Testing

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
- 401 → refresh → retry → success
- 401 → refresh fails → raise `SyncAuthError`
- Connect flow polls until `connected`
- `trigger_sync()` returns synchronous result; `timeout_trigger_seconds` respected

`tests/test_sync_service.py`:
- `pull()` happy path → SyncClient + PlaidLoader called in order; `app.sync_connections` updated
- Partial failure (one institution `failed`, one `completed`) → both recorded in `app.sync_connections`; pull returns success with per-institution summary
- Crash recovery: simulate `.in_flight_sync.json` present → `pull()` calls `get_data` first
- Crash recovery: `.in_flight_sync.json` older than 25 min → warning logged, file removed, normal flow continues
- Re-auth path: `connect(institution="Schwab")` resolves name → `provider_item_id`, passes to `connect/initiate`

### SQL tests

`tests/fixtures/plaid_sync_response.yaml` — YAML fixture per project convention (`feedback_fixture_format_yaml`).

SQLMesh tests:
- `stg_plaid__transactions`: amount sign flip (raw `42.50` → staging `-42.50`; raw `-1500.00` → staging `1500.00`)
- `dim_accounts`: Plaid accounts appear with `source_type='plaid'` after UNION ALL
- `fct_transactions`: Plaid transactions appear with MoneyBin sign convention and `source_type='plaid'`

### Integration tests (deferred)

Plaid Sandbox tests run only when `MONEYBIN_SYNC__TEST_SERVER_URL` is set; marked `@pytest.mark.integration`; skipped by default. Setup details deferred per `sync-overview.md` "Tests requiring a running server."

---

## Section 13: Server-Side Dependencies

The Phase 1 client doesn't work end-to-end until the moneybin-server implements the corresponding endpoints and integrations. Listed inline (no cross-repo path links per project convention):

### Endpoints (new or changed)

- **`POST /sync/connect/initiate`** — body `{provider, provider_item_id?}`; response `{session_id, link_url, connect_type, expiration}`. For `provider_item_id` present → create a Plaid Link token in update mode against the existing item; otherwise create a new-connection token.
- **`GET /sync/connect/status?session_id=...`** — response `{session_id, status, provider_item_id?, institution_name?, error?}`. Reads server-side session table.
- **`POST /auth/refresh`** — body `{refresh_token}`; response `{access_token, refresh_token, expires_in}`. Proxies refresh to Auth0.
- **`POST /webhooks/plaid`** — internal; handles `SESSION_FINISHED` (and `ITEM_LOGIN_REQUIRED`, `ERROR` for re-auth). Verified via the `Plaid-Verification` JWT header against Plaid's webhook public key.
- **Rename in responses:** `item_id` → `provider_item_id` throughout client-visible responses (`GET /institutions`, `GET /sync/status.results[]`, `GET /sync/data.metadata.institutions[]`, `POST /sync/trigger` request body).
- **Remove:** `POST /sync/link-token` and `POST /sync/exchange-token` (replaced by the initiate/webhook flow).

### Server-side new infrastructure

- **Session storage** — keyed by `session_id`, mapping `→ {user_id, link_token, provider_item_id?, status, error, created_at, expires_at}`. PostgreSQL table or Redis (server team's call). Must support 30-min TTL aligned with Plaid Hosted Link session expiry.
- **Plaid Hosted Link integration** — `linkTokenCreate` with `hosted_link: {}` config. Capture the `hosted_link_url` field in the response and surface it to the client. Configure `SESSION_FINISHED` webhook URL pointing to `POST /webhooks/plaid`.
- **Webhook handler responsibilities:**
  - Verify `Plaid-Verification` JWT signature against Plaid's webhook public key (rotated periodically — fetch from JWKS).
  - Idempotency: Plaid retries on 5xx and occasionally on 2xx. Dedupe on `(webhook_id, item_id)` — first arrival wins, duplicates are 200-acked but skip processing.
  - On `SESSION_FINISHED`: lookup session by `link_token`, call `plaid.itemPublicTokenExchange()`, encrypt `access_token` with the existing AES-256-GCM `ENCRYPTION_KEY`, store in `plaid_items`, update session row to `status='connected'` with `provider_item_id` and `institution_name` populated.
  - On `ITEM_LOGIN_REQUIRED`/`ERROR`: update `plaid_items.status` so the next `POST /sync/trigger` for that item reports the error code in `results[]`.

### Coordination

Client PR (this branch) and server PR ship paired. Until the server endpoints land, the client's integration tests are skipped (gated by `MONEYBIN_SYNC__TEST_SERVER_URL`). Unit tests using `respx` mocks exercise the full client surface independently.

---

## Section 14: Out of Scope

- `sync schedule` commands (Phase 2 — automation track in `sync-overview.md` build order)
- E2E encryption (Phase 3 — gated on server implementing client-side key exchange). `sync key rotate` stub stays in CLI. Open design question for Phase 3: whether key rotation can be MCP-exposed safely. Argument for: no passphrase material passes through the LLM context (the agent triggers rotation; new keys generated locally; public key goes server-via-HTTPS). Argument against: operational consistency with other key tools (`db_unlock`, `db_rotate_key`) which genuinely require passphrase entry and are correctly CLI-only. Decision deferred to the Phase 3 spec.
- Post-quantum cryptography (Phase 4 — designed in `sync-overview.md`)
- Plaid Investments product (separate spec, gated on `investment-tracking.md`)
- Plaid Liabilities product
- SimpleFIN, MX, TrueLayer integration (Phase 2/3 — `connect_type` discriminator is in place for forward compat; `POST /sync/connect/submit` endpoint and the `token_paste` client path are NOT in this PR)
- Local web server callback for OAuth (Phase 1 polish — no server API changes needed; deferred)
- Plaid Production OAuth approval — submit the application during this PR's review cycle given the 4–8 week approval timeline; can run in parallel with remaining implementation work
- Provider sequencing rationale (SimpleFIN/MX/TrueLayer detailed plan) — tracked separately in product strategy docs

---

## Open Design Questions (resolved during brainstorming, recorded for context)

| Question | Decision |
|---|---|
| Dedup PK: `(transaction_id, source_file)` or `(transaction_id, source_origin)`? | `(transaction_id, source_origin)`. Plaid's sync API is upsert-shaped; OFX-style append-only doesn't fit. |
| Token refresh: extend `/auth/device/token` or separate `/auth/refresh`? | Separate `/auth/refresh`. Clean endpoint per concern. |
| MCP connect-flow polling: blocking, looped, or event-driven? | Event-driven via separate `sync_connect_status` tool. Avoids MCP tool-timeout limits and progress-notification client variance. |
| `--no-browser`: explicit flag, auto-detect, or both? | Both. Auto-print URL alongside browser open; `--no-browser` skips the open attempt. |
| New table: schema file + migration, or schema file only? | Schema file only. Convention shift; followup to clean up redundant migrations. |
| Response models: Pydantic everywhere, partial, or dicts? | Pydantic at every API boundary. Single `sync_models.py` module imported by client + loader. |
| Service layer scope: `SyncConnectionService` or broader `SyncService`? | `SyncService` — owns orchestration plus connection-table operations. |
| Provider ladder section in design doc: full detail, trimmed, or none? | Trimmed. Public design doc mentions the discriminator; sequencing/rationale stays in strategy docs. |
| Auto-pull after connect? | Yes, by default. `--no-pull` to opt out. |
