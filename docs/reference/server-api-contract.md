<!-- Last reviewed: 2026-05-17 -->
# Server API Contract

`moneybin-server` is a thin HTTP service that brokers connections to upstream banking providers (Plaid today). The MoneyBin client treats it as opaque — this page documents the contract the client *expects*, so a self-hoster running their own `moneybin-server` (or anyone evaluating the architecture) can understand the surface from the client's perspective.

This is **not** a server implementation guide. It is the client-side contract: every endpoint listed here corresponds to a method on `SyncClient` (`src/moneybin/connectors/sync_client.py`); every response shape listed here corresponds to a Pydantic model in `src/moneybin/connectors/sync_models.py` that the client validates at the boundary. Anything not documented here is, by definition, not part of the contract — even if your `moneybin-server` happens to implement it.

> **Pre-v1.** The contract may break before the first tagged release. Post-launch, the conventions in `docs/architecture.md` apply: additive changes preferred, deprecate-then-remove across two releases for anything else.

## Design philosophy

Three properties drive the shape of this contract:

- **The client is offline-first.** The encrypted DuckDB profile holds your data. The server is invoked only when you connect a new bank or pull a sync. It is a broker, not a system of record.
- **Raw credentials never touch the server.** The Plaid Link flow runs in your browser against Plaid's hosted UI. The server sees the short-lived `public_token` that Plaid returns *after* the user authorizes — not the bank password. The client never sees the long-lived `access_token` either; the server holds it encrypted on the user's behalf.
- **The server is replaceable.** Because the contract is narrow (a handful of endpoints, JSON payloads only) and the client validates every response, a self-hoster can run their own `moneybin-server` against their own Plaid credentials. `moneybin-server` is a separate project; setup instructions live in its repository.

## Identity and auth

The client authenticates to `moneybin-server` with a per-user OAuth2 access token. The token is acquired through the **Device Authorization Grant** (RFC 8628) so that headless CLI environments work without a callback URL.

| Concern | Behavior |
|---|---|
| Auth flow | Device Authorization Grant (RFC 8628). `moneybin sync login` prints a user code + verification URL; the user opens it in any browser and approves. |
| Identity provider | The server is presumed to delegate to an external IdP (Auth0 in the reference deployment). The client never talks to the IdP directly. |
| Token type | `Bearer` JWT — passed via `Authorization: Bearer <token>` on every authed request. |
| Refresh | Rotating refresh tokens. On a 401, the client calls `POST /auth/refresh` once and retries. A second 401 clears both tokens and prompts re-login. |
| Token storage (client) | OS keychain via the `keyring` library, with a `0o600` fallback file at `~/.moneybin/.sync_token` for environments without a keychain (headless Linux without Secret Service, some Docker setups). |
| Token storage (server) | Out of scope of this contract. The client treats stored Plaid `access_token`s as the server's problem. |

Identity boundary, restated: the server sees your bank-connection authorization (Plaid `access_token`) and the JSON Plaid returns from sync calls. It does **not** see your bank password, your DuckDB encryption key, or any data that flows through OFX/CSV/PDF import paths.

## Endpoint catalog

Every endpoint below is exercised by `SyncClient`. Paths and methods are taken directly from the client's call sites — if the server names something differently, the client won't reach it.

| Method | Path | Auth | Purpose |
|---|---|---|---|
| `POST` | `/auth/device/code` | none | Begin device authorization. Returns `device_code`, `user_code`, verification URL, polling interval. |
| `POST` | `/auth/device/token` | none | Poll for the device-flow result. Returns tokens once the user approves; `202 pending` / `202 slow_down` while waiting. |
| `POST` | `/auth/refresh` | refresh token | Exchange a refresh token for a new access token (with a rotated refresh token). |
| `GET` | `/institutions` | bearer | List the user's connected institutions across providers. |
| `DELETE` | `/institutions/{id}` | bearer | Disconnect an institution by its internal connection ID. |
| `POST` | `/sync/connect/initiate` | bearer | Start a connect session (Plaid Link today). Returns a hosted `link_url` for the user to open. |
| `GET` | `/sync/connect/status` | bearer | Read the current state of a connect session (`pending` / `connected` / `failed`). |
| `POST` | `/sync/trigger` | bearer | Run a sync. Synchronous from the client's perspective — blocks until the server completes the pull. |
| `GET` | `/sync/data` | bearer | One-shot read of the most recent sync payload by `job_id`. Server deletes from its TTL store after read. |

Three properties hold across the catalog:

- **Bearer-token auth everywhere except the device-flow bootstrap.** No cookies, no sessions. The CLI/MCP context has no browser to hold a cookie.
- **JSON request and response bodies.** Errors included.
- **Synchronous semantics from the client's side.** `POST /sync/trigger` blocks until the server-side pull completes, then `GET /sync/data` returns the payload. No webhooks, no polling for the trigger itself (only for the connect flow, where the user takes the wheel).

## Request and response shapes

The Pydantic models the client validates against — names match `src/moneybin/connectors/sync_models.py`.

### `POST /auth/device/code`

**Request:** empty body.

**Response (200):**

```json
{
  "device_code": "Ag_EE...",
  "user_code": "ABCD-EFGH",
  "verification_uri_complete": "https://idp.example.com/activate?user_code=ABCD-EFGH",
  "interval": 5
}
```

The client reads `user_code` (display), `verification_uri_complete` (open in browser), `device_code` (pass to the poll endpoint), and `interval` (polling cadence in seconds, default 5). Additional fields the server may return are ignored.

### `POST /auth/device/token`

**Request:**

```json
{ "device_code": "Ag_EE..." }
```

**Response (200, approved):** `AuthToken`

```json
{
  "access_token": "eyJ...",
  "refresh_token": "v1.M...",
  "expires_in": 86400,
  "token_type": "Bearer"
}
```

**Response (202, pending):**

```json
{ "status": "pending" }
```

**Response (202, slow_down):**

```json
{ "status": "slow_down" }
```

Per RFC 8628 §3.5, on `slow_down` the client increases its polling interval by 5 seconds and continues polling. On `200` the client persists both tokens and returns. On `403` the client surfaces "user denied device authorization"; on `400` it surfaces "device code expired or invalid; restart login."

### `POST /auth/refresh`

**Request:**

```json
{ "refresh_token": "v1.M..." }
```

**Response (200):** Same `AuthToken` shape as the device-token endpoint. Refresh tokens rotate — the response always includes a new `refresh_token` that supersedes the one used.

Any non-200 response clears both tokens on the client and requires re-login.

### `GET /institutions`

**Response (200):** JSON array of `ConnectedInstitution`:

```json
[
  {
    "id": "550e8400-e29b-41d4-a716-446655440000",
    "provider_item_id": "item_abc123...",
    "provider": "plaid",
    "institution_name": "Chase",
    "status": "active",
    "last_sync": "2026-05-15T14:22:10Z",
    "created_at": "2026-03-15T08:30:00Z",
    "error_code": null
  }
]
```

| Field | Type | Notes |
|---|---|---|
| `id` | string | Server-internal connection ID. The value to pass to `DELETE /institutions/{id}`. |
| `provider_item_id` | string | The provider's own item identifier (Plaid `item_id`). Stable across the connection's lifetime. |
| `provider` | string | Provider name. Today: `"plaid"`. |
| `institution_name` | string \| null | Human-readable institution name. |
| `status` | enum | `"active"` \| `"error"` \| `"revoked"`. |
| `last_sync` | ISO 8601 \| null | Timestamp of last successful sync, server-side. |
| `created_at` | ISO 8601 | When the connection was established. |
| `error_code` | string \| null | Provider error code (e.g., `ITEM_LOGIN_REQUIRED`). Advisory only; treat absence as `null`. |

**Why two IDs?** `id` is the server's UUID for the connection (the disconnect handle). `provider_item_id` is the upstream provider's identifier (what shows up in sync payloads and per-institution results). They are not interchangeable. The client uses `provider_item_id` when targeting a sync at a specific connection and `id` when disconnecting.

### `DELETE /institutions/{id}`

**Path parameter:** `id` — the server-internal UUID from `GET /institutions`.

**Response (204):** No content. The server revokes the upstream access token and removes the connection from its records.

**Errors:** `404` if the connection does not exist or belongs to another user.

### `POST /sync/connect/initiate`

Begin a connect session. The response includes a hosted URL the user opens in a browser; the user completes provider-side flows there (Plaid Link, in today's deployment).

**Request:**

```json
{
  "provider": "plaid",
  "provider_item_id": null,
  "return_to": null
}
```

| Field | Type | Required | Purpose |
|---|---|---|---|
| `provider` | string | yes | Defaults to `"plaid"` client-side. Reserved for multi-provider future. |
| `provider_item_id` | string \| null | no | If set, opens an **update-mode** Link session for an existing connection (e.g., to resolve `ITEM_LOGIN_REQUIRED`). |
| `return_to` | string \| null | no | Where the hosted UI should send the user after completion. Server-defined semantics. |

**Response (200):** `ConnectInitiateResponse`

```json
{
  "session_id": "sess_abc123",
  "link_url": "https://link.example.com/sessions/sess_abc123",
  "connect_type": "widget_flow",
  "expiration": "2026-05-17T15:00:00Z"
}
```

| Field | Notes |
|---|---|
| `session_id` | Opaque token; the client passes it to `GET /sync/connect/status`. |
| `link_url` | Hosted URL the client opens in the user's browser. |
| `connect_type` | `"widget_flow"` (Plaid Link in a hosted browser) or `"token_paste"` (manual token entry, reserved for headless flows). |
| `expiration` | After this timestamp, the session can no longer transition to `connected`. |

### `GET /sync/connect/status`

**Query parameter:** `session_id` from `/sync/connect/initiate`.

**Response (200):** `ConnectStatusResponse`

```json
{
  "session_id": "sess_abc123",
  "status": "connected",
  "provider_item_id": "item_abc123...",
  "institution_name": "Chase",
  "error": null,
  "expiration": "2026-05-17T15:00:00Z"
}
```

| Field | Type | Notes |
|---|---|---|
| `status` | enum | `"pending"` \| `"connected"` \| `"failed"`. |
| `provider_item_id` | string \| null | Populated once `status == "connected"`. |
| `institution_name` | string \| null | Populated once `status == "connected"`. |
| `error` | string \| null | Populated when `status == "failed"`. |

The client polls this endpoint at a 3-second cadence from `SyncClient.poll_connect_status`. `pending` → continue polling. `connected` → success. `failed` → raise `SyncConnectError(error)`. Hitting the client-side deadline (~120 s) raises `SyncTimeoutError`; the user likely abandoned the browser tab.

### `POST /sync/trigger`

**Request:**

```json
{
  "provider_item_id": null,
  "reset_cursor": false
}
```

| Field | Type | Required | Purpose |
|---|---|---|---|
| `provider_item_id` | string \| null | no | If set, syncs only that connection. If omitted, syncs all of the user's active connections. |
| `reset_cursor` | bool | no | If `true`, the server discards its cursor and re-pulls full available history. Default `false` — incremental from the last cursor. |

**Response (200):** `SyncTriggerResponse`

```json
{
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "completed",
  "transaction_count": 142
}
```

`status` reflects the terminal job state because the server completes the pull synchronously before responding. `transaction_count` is the total across all institutions in the job.

The client passes a 120-second timeout on this call because multi-institution syncs can take 30–90 seconds. A timeout from the client side does not necessarily mean the server-side job failed — it means the response did not arrive in time. The cursor-based design means a subsequent `POST /sync/trigger` is safe to retry.

### `GET /sync/data`

**Query parameter:** `job_id` from the trigger response.

**Response (200):** `SyncDataResponse`

```json
{
  "accounts": [
    {
      "account_id": "acc_001",
      "account_type": "depository",
      "account_subtype": "checking",
      "institution_name": "Chase",
      "official_name": "Total Checking",
      "mask": "1234"
    }
  ],
  "transactions": [
    {
      "transaction_id": "txn_001",
      "account_id": "acc_001",
      "transaction_date": "2026-05-12",
      "amount": "42.50",
      "description": "COFFEE SHOP",
      "merchant_name": "Best Coffee",
      "category": "FOOD_AND_DRINK",
      "pending": false
    }
  ],
  "balances": [
    {
      "account_id": "acc_001",
      "balance_date": "2026-05-13",
      "current_balance": "1234.56",
      "available_balance": "1200.00"
    }
  ],
  "removed_transactions": ["txn_old_001"],
  "metadata": {
    "job_id": "550e8400-e29b-41d4-a716-446655440000",
    "synced_at": "2026-05-13T12:00:00Z",
    "institutions": [
      {
        "provider_item_id": "item_abc",
        "institution_name": "Chase",
        "status": "completed",
        "transaction_count": 142,
        "error": null,
        "error_code": null
      }
    ]
  }
}
```

`accounts[]`, `transactions[]`, `balances[]`, and `removed_transactions[]` are guaranteed to be present (possibly empty). `metadata` is always present.

**One-shot read.** The server holds the payload in a short-lived TTL store and removes it after the client fetches it. If the client crashes between `POST /sync/trigger` and `GET /sync/data`, the recovery is to call `POST /sync/trigger` again — sync is idempotent against the server's cursor state.

#### `accounts[]`

| Field | Type | Notes |
|---|---|---|
| `account_id` | string | Provider account ID. Stable across syncs. |
| `account_type` | string \| null | Provider account type (e.g., `depository`, `credit`, `loan`, `investment`, `other`). |
| `account_subtype` | string \| null | Provider subtype (e.g., `checking`, `savings`, `credit card`). |
| `institution_name` | string \| null | Human-readable institution name. |
| `official_name` | string \| null | Account name as the institution reports it. |
| `mask` | string \| null | Last few digits of the account number (≤ 8 chars). |

#### `transactions[]`

| Field | Type | Notes |
|---|---|---|
| `transaction_id` | string | Provider transaction ID. Unique and stable across syncs. |
| `account_id` | string | Provider account ID. |
| `transaction_date` | `YYYY-MM-DD` | Date the transaction posted (or the pending date). |
| `amount` | decimal | **Provider sign convention** — Plaid emits positive for expense, negative for income. The MoneyBin staging layer flips this before data reaches `core`. See [Sign convention](#sign-convention) below. |
| `description` | string \| null | Raw description from the institution. |
| `merchant_name` | string \| null | Provider's normalized merchant name (`null` when unknown). |
| `category` | string \| null | Provider's primary category. For Plaid this is `personal_finance_category.primary`. |
| `pending` | bool | `true` if not yet posted; may transition to `false` (with the same `transaction_id`) in a later sync. |

#### `balances[]`

| Field | Type | Notes |
|---|---|---|
| `account_id` | string | Provider account ID. |
| `balance_date` | `YYYY-MM-DD` | Date the snapshot was captured. |
| `current_balance` | decimal \| null | Current balance including pending. |
| `available_balance` | decimal \| null | Available balance after holds; often `null` for credit accounts. |

#### `removed_transactions[]`

Array of `transaction_id` strings the provider has retracted (reversal, dedup, error correction). The client deletes these rows from `raw.plaid_transactions`.

#### `metadata`

| Field | Type | Notes |
|---|---|---|
| `job_id` | string (UUID) | Echoes the trigger response. |
| `synced_at` | ISO 8601 | When the server pulled this batch from the provider. Used as `extracted_at` in `raw.plaid_*`. |
| `institutions[]` | array | Per-institution outcomes for this job. |

Each entry of `metadata.institutions[]`:

| Field | Type | Notes |
|---|---|---|
| `provider_item_id` | string | Which connection this entry is for. |
| `institution_name` | string \| null | Human-readable name. |
| `status` | enum | `"completed"` \| `"failed"`. |
| `transaction_count` | int \| null | Transactions in this batch from this institution. |
| `error` | string \| null | Error message when `status == "failed"`. |
| `error_code` | string \| null | Provider error code (e.g., `ITEM_LOGIN_REQUIRED`). |

The client uses `institutions[]` to update `app.sync_connections` with per-institution status and to surface targeted re-auth guidance.

## Sign convention

Plaid and MoneyBin use opposite sign conventions. This contract preserves the **provider** convention. The flip to MoneyBin convention happens once, in SQLMesh staging — never in the client transport, never in the server, never in the raw loader.

| Layer | Expense | Income |
|---|---|---|
| `/sync/data` response (this contract) | Positive | Negative |
| `raw.plaid_transactions` | Positive (faithful to source) | Negative |
| `prep.stg_plaid__transactions` and downstream `core.*` | Negative | Positive |

If a future provider arrives with a different native convention, the same rule holds: raw preserves the source; staging flips to MoneyBin convention.

## Error model

The client distinguishes four error classes, mapped from HTTP status and response shape:

| Client exception | When raised | What the user sees |
|---|---|---|
| `SyncAuthError` | Missing/invalid token, refresh failed, two consecutive 401s | "not authenticated — run `moneybin sync login`" / "session expired — run `moneybin sync login`" |
| `SyncConnectError` | Connect session reached `status == "failed"` server-side | The `error` string from `ConnectStatusResponse`, surfaced verbatim |
| `SyncTimeoutError` | Client-side deadline elapsed (connect-poll deadline, sync-trigger long timeout) | "connect flow timed out — user may have abandoned the browser" or similar |
| `SyncAPIError` | Server unreachable, unexpected 4xx/5xx with no clearer match, malformed response | "sync server unreachable at <url>" or "<METHOD> <path> returned <status>: <truncated body>" |

The contract does **not** mandate a uniform error envelope from the server — the client treats any `>=400` status with an unstructured message as `SyncAPIError`. A future revision may tighten this to a structured `{ "error": { "code": "...", "message": "..." } }` shape; until then, the client relies on Pydantic validation to catch malformed success responses and on HTTP status to gate refresh-and-retry behavior.

### Provider error codes inside `metadata.institutions[]`

Per-institution failures inside an otherwise-successful sync do **not** raise. They appear in `metadata.institutions[].error_code` and the client maps them to user guidance:

| Provider code | Client guidance |
|---|---|
| `ITEM_LOGIN_REQUIRED` | "{institution} needs re-authentication — run `moneybin sync connect` to update your credentials." |
| `ITEM_NOT_FOUND` | "{institution} connection was revoked. Run `moneybin sync connect` to reconnect." |
| `INSTITUTION_NOT_RESPONDING` / `INSTITUTION_DOWN` | "{institution} is temporarily unavailable. Try again later." |
| `TRANSACTIONS_SYNC_MUTATION_DURING_PAGINATION` | "Data changed during sync for {institution}. Re-running automatically..." (client retries) |
| `RATE_LIMIT_EXCEEDED` | "Rate limit reached. Sync will resume automatically." (back off + retry) |
| `PRODUCTS_NOT_READY` | "{institution} is still processing initial data. Try again in a few minutes." |
| Unknown | Logged with the raw code; surfaced with generic "Unexpected error from {institution}" guidance. |

## Webhooks

**None today.** The contract is strictly request/response. The server does not push events to the client; the client never opens an inbound port. Real-time sync via provider webhooks is a server-side concern — if a future revision wants the client to react to push events, it will land as a server-sent stream over the same authed channel, not as a client-side listener.

## Privacy boundary

What stays on the client, what crosses to the server, and what never appears on either:

| Data | Client (your machine) | `moneybin-server` | Notes |
|---|---|---|---|
| Bank password / online banking credentials | Never | Never | Entered into Plaid's hosted UI in your browser; goes directly from your browser to Plaid. |
| Plaid `public_token` | Briefly (in URL fragment during redirect) | Yes — exchanged for `access_token` | Short-lived, single-use. |
| Plaid `access_token` | Never | Yes — encrypted at rest | Long-lived bearer that lets the server pull data from Plaid on your behalf. |
| Bank transaction data (amounts, descriptions, merchants) | Yes — in your encrypted DuckDB profile | Yes — transiently during the sync, then in a short-TTL store until you fetch it | After `GET /sync/data` the server drops the payload from its TTL store. |
| DuckDB encryption key | Yes — OS keychain or passphrase | Never | The server has no way to read your local database. |
| OFX / CSV / PDF imports | Yes | Never | The bank-direct sync server is irrelevant to file-based imports. |
| MCP tool invocations and LLM prompts | Yes (and your chosen AI client) | Never | The MCP server is local; the sync server is not in that path. |

The narrowest possible exposure on the server side is the design intent: the server holds the keys that let Plaid hand over your data, runs the pull, hands the payload to you, then forgets. The DuckDB file on your machine is where data persists.

## Self-hosting `moneybin-server`

`moneybin-server` is a separate project. To run your own instance, see its repository for setup, configuration, and Plaid-account requirements. The client cares only about:

- The base URL it talks to (set via `MoneyBinSettings.sync.server_url`).
- That the deployment honors the contract on this page.

A self-hoster bringing their own Plaid credentials gets full control of the provider relationship at the cost of operating a small HTTPS service with an OAuth2 issuer in front of it. A self-hoster who doesn't need bank-direct sync at all should ignore this entire surface — OFX, CSV, and PDF import paths never touch it.

## Versioning

Pre-v1. No `/v1/` URL prefix is implied. Breaking changes are possible before the first tagged release; post-launch the conventions in `docs/architecture.md` apply (additive preferred, deprecate-then-remove across two releases for breaking changes). The client validates every response with Pydantic at the boundary, so a contract drift that adds optional fields is silently ignored; a drift that removes or renames required fields surfaces as a `SyncAPIError` on the next call.
