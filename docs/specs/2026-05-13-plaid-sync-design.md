# Plaid Sync Phase 1 — Design Document

**Date:** 2026-05-13
**Status:** Approved — implementation starting
**Branch:** `feat/plaid-sync`
**Specs implemented:** `sync-plaid.md` (Phase 1), `sync-overview.md` (status → ready)

---

## Background

MoneyBin's sync stubs have been placeholders since the architecture was laid down. This document captures the design decisions made during the Phase 1 brainstorming session before cutting code. It extends and corrects the existing specs based on:

1. Current Plaid documentation (Plaid Hosted Link, SESSION_FINISHED webhook)
2. Provider-agnostic API design across 5 evaluated aggregators
3. ADR-010 writer-coordination constraints
4. A re-read of the current `api-contract.md` and `system-overview.md`

---

## Section 1: Provider-Agnostic Connect API

### Design decision: Plaid Hosted Link (not server-hosted page)

Plaid now supports **Plaid Hosted Link** — Plaid hosts the OAuth/Link UI entirely; the server only needs to create a link token and receive a `SESSION_FINISHED` webhook when the user completes the flow. The server does **not** need to host an iframe, embed the Plaid Link JS SDK, or serve any UI for the connect flow.

This changes the connect flow significantly from what `sync-overview.md` and `system-overview.md` described:

**Old flow (incorrect):**
1. CLI calls `POST /sync/link-token` → gets `link_token`
2. User opens Plaid Link somehow (unclear how CLI was meant to do this)
3. CLI calls `POST /sync/exchange-token` with `public_token`

**Correct flow (Plaid Hosted Link):**
1. CLI calls `POST /sync/connect/initiate` → gets `{session_id, link_url, connect_type: "widget_flow", expiration}`
2. CLI opens `link_url` in the user's browser (via `webbrowser.open()`)
3. User completes Plaid's hosted UI; Plaid fires `SESSION_FINISHED` webhook to the server
4. Server exchanges public token internally (webhook handler) — no CLI involvement
5. CLI polls `GET /sync/connect/status?session_id=` until `status: "connected"` or `"failed"`

There is no `POST /sync/exchange-token` in the public API.

### Provider taxonomy

Research across 5 aggregators identified 4 server-side patterns but only **2 client-facing patterns**:

| `connect_type` | Server-side mechanism | Providers |
|---|---|---|
| `widget_flow` | Server generates hosted URL; webhook signals completion | Plaid (Hosted Link + SESSION_FINISHED), MX (embedded widget), Teller (OAuth redirect) |
| `token_paste` | User obtains token independently; CLI submits it | SimpleFIN (Bridge token from simplefin.org) |

The `connect_type` discriminator in the `POST /sync/connect/initiate` response tells the CLI which client flow to execute:
- `widget_flow`: open `link_url` in browser, poll `GET /sync/connect/status`
- `token_paste`: prompt user to paste a token, `POST /sync/connect/submit`

### API contract changes required

The following changes to `api-contract.md` are part of this PR:

**New endpoints:**

```
POST /sync/connect/initiate
  Request: { "provider": "plaid" }  (optional; defaults to user's plan's available providers)
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
    "provider_item_id": "item_abc123",   // only when status == "connected"
    "institution_name": "Chase",          // only when status == "connected"
    "error": null                         // error message when status == "failed"
  }

POST /sync/connect/submit        (token_paste flow only — SimpleFIN Phase 2)
  Request: { "session_id": "sess_abc123", "token": "<bridge_token>" }
  Response 200: { "provider_item_id": "item_abc123", "institution_name": "SimpleFIN" }

POST /webhooks/plaid             (internal — not in public API surface)
  Handles SESSION_FINISHED webhook from Plaid; calls plaid.itemPublicTokenExchange()
  No auth required; verified via Plaid webhook signature
```

**Renamed field in `GET /institutions`:**
- `item_id` → `provider_item_id` (makes field name provider-agnostic)

**Removed from public API:**
- `POST /sync/link-token` (replaced by `POST /sync/connect/initiate`)
- `POST /sync/exchange-token` (now internal webhook handler)

**Retained:**
- `POST /sync/trigger` — synchronous, returns final state, `item_id` param renamed to `provider_item_id`
- `GET /sync/status` — kept as fallback/timeout recovery
- `GET /sync/data` — unchanged
- `DELETE /institutions/:id` — unchanged

### Why not a local web server for the OAuth callback?

A local web server (`http://localhost:PORT/callback`) was considered for the CLI OAuth flow. The tradeoff:

**Local web server** would give a seamless "browser opens, user logs in, browser closes" UX — the same pattern used by `gh auth login` and `gcloud auth login`. Better user experience; no manual step.

**Current approach (polling)** is simpler to implement, works identically across CLI and MCP (which can't spawn a web server), and has no platform-specific firewall/port issues. The UX is slightly worse (user sees a browser tab that stays open until they poll to completion).

Decision: start with polling for Phase 1. A local web server callback is a polish item — it doesn't change the server API at all, only the client-side connect flow.

---

## Section 2: SyncClient

**File:** `src/moneybin/connectors/sync_client.py`

Replaces the empty `src/moneybin/connectors/` directory (which has only `__init__.py` today).

### Authentication

Device Authorization Flow (RFC 8628):
1. `POST /auth/device/code` → `{user_code, verification_uri_complete, device_code, expires_in, interval}`
2. Display `user_code` and `verification_uri_complete`; open browser via `webbrowser.open()`
3. Poll `POST /auth/device/token` with `{device_code}`:
   - `200` → store JWT + refresh token
   - `202 {status: "pending"}` → wait `interval` seconds, retry
   - `202 {status: "slow_down"}` → increase interval by 5s, retry
   - `403` → user denied; raise `SyncAuthError`
   - `400` (expired) → raise `SyncAuthError`

### Token storage

Primary: `keyring` (service `"moneybin-sync"`, keys `"jwt"` and `"refresh_token"`).
Fallback: `~/.moneybin/.sync_token` (0600 permissions) as a JSON file with `{"jwt": ..., "refresh_token": ...}`.

The fallback applies when keyring is unavailable (headless CI, Docker without a secrets backend). The client checks keyring first; if `keyring.get_password()` raises `KeyringError`, it falls back to the file.

### Connect flow (Phase 1 — widget_flow only)

```python
def connect(self, institution_name: str | None = None) -> ConnectResult:
    resp = self._post("/sync/connect/initiate", {"provider": "plaid"})
    session_id = resp["session_id"]
    webbrowser.open(resp["link_url"])
    # Poll until connected
    return self._poll_connect(session_id)

def _poll_connect(self, session_id: str) -> ConnectResult:
    deadline = time.time() + 300  # 5-minute timeout
    interval = 3.0
    while time.time() < deadline:
        time.sleep(interval)
        r = self._get(f"/sync/connect/status?session_id={session_id}")
        if r["status"] == "connected":
            return ConnectResult(provider_item_id=r["provider_item_id"], ...)
        if r["status"] == "failed":
            raise SyncConnectError(r.get("error", "connect failed"))
    raise SyncTimeoutError("connect flow timed out")
```

### Sync trigger

`POST /sync/trigger` is synchronous — it blocks until the sync completes and returns the final `{job_id, status, transaction_count}`. No polling loop needed for the happy path. `GET /sync/status` is kept as a fallback for crash recovery (e.g., client crashes after `POST /sync/trigger` returns but before `GET /sync/data` is called — client can resume with the `job_id` from the response).

### Error handling

`SyncClient` raises typed exceptions: `SyncAuthError`, `SyncConnectError`, `SyncTimeoutError`, `SyncAPIError`. The CLI layer maps these to user-friendly messages and appropriate exit codes.

---

## Section 3: PlaidLoader

**File:** `src/moneybin/loaders/plaid_loader.py`

### Interface

```python
class PlaidLoader:
    def __init__(self, database: Database) -> None: ...
    def load(self, sync_data: dict, job_id: str) -> LoadResult: ...
    def handle_removed_transactions(self, removed_ids: list[str]) -> int: ...
```

The caller (CLI `sync pull` command) manages the Database connection lifetime per ADR-010:

```python
with get_database(read_only=False) as db:
    loader = PlaidLoader(db)
    result = loader.load(sync_data, job_id)
```

The connection is released as soon as the `with` block exits — not held across multiple operations or across the network call to `GET /sync/data`.

### Loading pattern

For each data array (accounts, transactions, balances), the loader:
1. Serializes the array to a `NamedTemporaryFile` as JSON
2. Calls `db.execute()` with an `INSERT OR REPLACE INTO ... SELECT ... FROM read_json(?)` statement
3. Closes and deletes the temp file

`source_file = f"sync_{job_id}"` — a logical identifier (no physical file on disk).

### Sign convention (critical)

The loader stores amounts **exactly as received from the server** — Plaid convention, positive = expense. The `-1 * amount` flip happens **only** in `prep.stg_plaid__transactions`. It never happens in the loader or in the core models.

This is documented with a comment in the loader to prevent future "correction":

```python
# Plaid convention: positive = expense, negative = income.
# DO NOT negate here — the sign flip happens in stg_plaid__transactions.
```

### Dedup

`INSERT OR REPLACE` on `(transaction_id, source_file)` for transactions. When Plaid sends a transaction with the same `transaction_id` but `pending = false` that was previously loaded as `pending = true`, the `INSERT OR REPLACE` replaces the pending row. Removed transactions are deleted via `handle_removed_transactions()`.

---

## Section 4: Raw DDL

Three files in `src/moneybin/sql/schema/`:

**`raw_plaid_accounts.sql`**

```sql
CREATE TABLE IF NOT EXISTS raw.plaid_accounts (
    account_id VARCHAR NOT NULL,
    account_type VARCHAR,
    account_subtype VARCHAR,
    institution_name VARCHAR,
    official_name VARCHAR,
    mask VARCHAR,
    source_file VARCHAR NOT NULL,
    source_type VARCHAR DEFAULT 'plaid',
    source_origin VARCHAR,
    extracted_at TIMESTAMP,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (account_id, source_file)
);
```

**`raw_plaid_transactions.sql`**

```sql
CREATE TABLE IF NOT EXISTS raw.plaid_transactions (
    transaction_id VARCHAR NOT NULL,
    account_id VARCHAR,
    transaction_date DATE,
    amount DECIMAL(18,2),      -- Plaid convention: positive = expense
    description VARCHAR,
    merchant_name VARCHAR,
    category VARCHAR,
    pending BOOLEAN DEFAULT FALSE,
    source_file VARCHAR NOT NULL,
    source_type VARCHAR DEFAULT 'plaid',
    source_origin VARCHAR,
    extracted_at TIMESTAMP,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (transaction_id, source_file)
);
```

**`raw_plaid_balances.sql`**

```sql
CREATE TABLE IF NOT EXISTS raw.plaid_balances (
    account_id VARCHAR NOT NULL,
    balance_date DATE NOT NULL,
    current_balance DECIMAL(18,2),
    available_balance DECIMAL(18,2),
    source_file VARCHAR NOT NULL,
    source_type VARCHAR DEFAULT 'plaid',
    source_origin VARCHAR,
    extracted_at TIMESTAMP,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (account_id, balance_date, source_file)
);
```

---

## Section 5: Migration V009

**File:** `src/moneybin/sql/migrations/V009__create_sync_connections.sql`

```sql
CREATE TABLE IF NOT EXISTS app.sync_connections (
    item_id VARCHAR NOT NULL,
    provider VARCHAR NOT NULL DEFAULT 'plaid',
    institution_name VARCHAR,
    status VARCHAR NOT NULL DEFAULT 'active',
    last_sync_at TIMESTAMP,
    last_sync_txn_count INTEGER,
    last_error VARCHAR,
    last_error_code VARCHAR,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (item_id)
);
```

`item_id` here stores the `provider_item_id` from the server. The field is named `item_id` in the local schema for brevity; it maps directly to `provider_item_id` in the server response.

---

## Section 6: SQLMesh Staging and Core

### Staging views (3 files in `sqlmesh/models/prep/`)

**`stg_plaid__transactions.sql`** — sign flip is here and only here:

```sql
SELECT
    transaction_id,
    account_id,
    transaction_date AS posted_date,
    -1 * amount AS amount,  -- flip Plaid positive-expense to MoneyBin negative-expense
    description,
    merchant_name,
    category AS plaid_category,
    pending,
    source_file,
    source_type,
    source_origin,
    extracted_at
FROM raw.plaid_transactions
```

**`stg_plaid__accounts.sql`** and **`stg_plaid__balances.sql`** — light type normalization only; no sign flip needed.

### Core model additions

**`dim_accounts.sql`** — add `plaid_accounts` CTE and include in the final `UNION ALL`:

```sql
plaid_accounts AS (
    SELECT
        account_id AS account_id_source,
        account_type,
        account_subtype,
        institution_name,
        official_name AS account_name,
        mask,
        'plaid' AS source_type,
        source_origin
    FROM prep.stg_plaid__accounts
),
...
SELECT ... FROM ofx_accounts
UNION ALL
SELECT ... FROM plaid_accounts
```

**`fct_transactions.sql`** — add `plaid_transactions` CTE and include in final `UNION ALL`. The sign convention is already correct at this layer (staging handled the flip).

---

## Section 7: CLI Commands

**File:** `src/moneybin/cli/commands/sync.py` (replacing `_not_implemented()` stubs)

Six commands under `sync_app`:

| Command | Description |
|---|---|
| `moneybin sync login` | Device Auth Flow; stores JWT in keyring |
| `moneybin sync logout` | Clears JWT from keyring (and fallback file) |
| `moneybin sync connect` | Opens Hosted Link in browser; polls until connected |
| `moneybin sync disconnect` | Calls `DELETE /institutions/:id`; removes from `app.sync_connections` |
| `moneybin sync pull [--all] [--institution NAME] [--force]` | Triggers sync, downloads data, loads into DuckDB |
| `moneybin sync status` | Lists connections with last sync time and status |

`sync pull` flow:
1. `client.trigger_sync()` → `{job_id, status, transaction_count}`
2. `client.get_data(job_id)` → sync data dict
3. `with get_database(read_only=False) as db: PlaidLoader(db).load(sync_data, job_id)`
4. Print summary: accounts, transactions loaded, removed

Output format follows the project-wide JSON/plain-text dual mode per CLI rules.

---

## Section 8: MCP Tools and Prompt

**File:** `src/moneybin/mcp/tools/sync.py` (replacing `not_implemented_envelope` stubs)

Four tools (underscores, not dots):

| Tool | Maps to |
|---|---|
| `sync_pull` | Full pull flow (same as `moneybin sync pull`) |
| `sync_status` | Lists connections; returns JSON envelope |
| `sync_connect` | Initiates connect flow; returns `link_url` for agent to present |
| `sync_disconnect` | Disconnects by institution name or item_id |

**`sync_review` prompt** — guides an agent through reviewing recent sync results, flagging anomalies (large transaction count changes, institutions stuck in error state), and suggesting next steps.

---

## Section 9: Testing

### Unit tests

**`tests/test_plaid_loader.py`**:
- Load valid sync data → assert row counts in `raw.plaid_*`
- Load with duplicate `transaction_id` → assert INSERT OR REPLACE replaces row
- Load pending transaction, then load non-pending version → assert single row with `pending=false`
- Handle removed transactions → assert deletion from `raw.plaid_transactions`
- Assert sign is NOT flipped in raw table (amount stored as Plaid delivers it)

**`tests/test_sync_client.py`**:
- `login()` happy path (mock HTTP; assert keyring.set_password called)
- `login()` slow_down response → interval increases by 5s
- `login()` user denied → raises `SyncAuthError`
- `connect()` widget_flow polls to completion
- `trigger_sync()` returns synchronous result

### SQL tests

**`tests/fixtures/plaid_sync_response.yaml`** — synthetic sync response fixture (YAML per project convention).

SQLMesh `stg_plaid__transactions` test: assert that `amount` in staging is negated vs raw.

### No server dependency

All tests mock HTTP via `respx` (httpx-compatible). No running server required. Plaid Sandbox integration tests are a separate concern for manual QA and are not part of the unit-test suite.

---

## Section 10: Provider Ladder (Phase 2+)

The Phase 1 API design is explicitly provider-agnostic. The `connect_type` discriminator enables adding new providers without breaking the client:

| Phase | Provider | `connect_type` | Notes |
|---|---|---|---|
| 1 (this PR) | **Plaid** | `widget_flow` | Hosted Link + SESSION_FINISHED webhook |
| 2 | **SimpleFIN** | `token_paste` | Bridge token; no widget; flat/no per-transaction fees; no embedded JS |
| 3 | **MX** | `widget_flow` | Embedded widget + postMessage; server must host a page; broader coverage |
| Future EU | **TrueLayer** | `widget_flow` (OAuth redirect) | Preferred over GoCardless; Nordigen/GoCardless availability uncertain for new customers |
| Deferred | Finicity, Teller | — | Finicity = enterprise-focused; Teller = same pattern as MX, limited added value |

**Why SimpleFIN for Phase 2:**
- Flat pricing with no per-transaction or per-user fees at indie scale
- Active user community (Actual Budget, Lunch Money users already use it)
- Simple `token_paste` flow requires zero server changes — just implement `POST /sync/connect/submit` on the server
- Lower US bank coverage than Plaid, but covers the long tail of credit unions

**Why MX for Phase 3 (not Phase 2):**
- Requires server to host a web page for the embedded widget (adds surface area to `moneybin-server`)
- Enterprise-grade reliability and coverage makes it worth adding after initial launch
- Pricing better than Plaid at scale

**Why TrueLayer over GoCardless:**
- GoCardless acquired Nordigen (the free-tier EU aggregator) and wound down new customer onboarding for the free tier
- TrueLayer is the established EU/UK PSD2 aggregator with clearer indie-developer pricing

---

## Out of Scope for This PR

- `sync schedule` commands (Phase 2)
- E2E encryption for sync data (Phase 3 — gated on server implementing client-side key exchange)
- Plaid Investments product (separate spec, gated on investment-tracking.md)
- Plaid Liabilities product
- SimpleFIN, MX, TrueLayer integration (Phase 2/3)
- Local web server callback for OAuth (Phase 1 polish item — no server API changes needed)
- Plaid Production OAuth approval — submit the application during this PR's review cycle given the 4–8 week approval timeline

---

## Server Docs to Update

The following `moneybin-server` docs need updating as part of this PR:

1. **`docs/architecture/api-contract.md`**: Replace `POST /sync/link-token` + `POST /sync/exchange-token` with the new `POST /sync/connect/initiate` + `GET /sync/connect/status` + `POST /sync/connect/submit` endpoints. Add internal `POST /webhooks/plaid`. Rename `item_id` → `provider_item_id` in `GET /institutions` response.

2. **`docs/architecture/system-overview.md`**: Update the Data Flow section to reflect Plaid Hosted Link (step 2 currently says "CLI calls POST /sync/exchange-token" — this is wrong). Update the Mermaid architecture diagram if it references the old exchange-token flow.

3. **`docs/specs/phase-2-plaid-sync.md`** (if it references the exchange-token flow or server-hosted link page): update to reflect SESSION_FINISHED webhook approach.
