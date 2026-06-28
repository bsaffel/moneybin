<!-- Last reviewed: 2026-05-17 -->
# Server API Contract

`moneybin-sync` is a thin HTTP service that brokers connections to upstream banking providers (Plaid today). The MoneyBin client treats it as opaque — this page documents the contract the client *expects*, so a self-hoster running their own `moneybin-sync` (or anyone evaluating the architecture) can reason about the surface from the client's perspective.

This is **not** a server implementation guide. Every endpoint here corresponds to a method on `SyncClient` (`src/moneybin/connectors/sync_client.py`); every response shape corresponds to a Pydantic model in `src/moneybin/connectors/sync_models.py` that the client validates at the boundary. Anything not documented here is not part of the contract — even if your `moneybin-sync` happens to implement it. See [Versioning](#versioning) for the pre-launch posture.

## Design philosophy

Three properties drive the shape of this contract:

- **The client is offline-first.** The encrypted DuckDB profile holds your data. The server is invoked only when you connect a new bank or pull a sync. It is a broker, not a system of record.
- **Raw credentials never touch the server.** The Plaid Link flow runs in your browser against Plaid's hosted UI. The server sees the short-lived `public_token` that Plaid returns *after* the user authorizes — not the bank password. The client never sees the long-lived `access_token` either; the server holds it encrypted on the user's behalf.
- **The server is replaceable.** Because the contract is narrow (a handful of endpoints, JSON payloads only) and the client validates every response, a self-hoster can run their own `moneybin-sync` against their own Plaid credentials. `moneybin-sync` is a separate project; setup instructions live in its repository.

## Local-only mode

This entire surface is optional. MoneyBin runs end-to-end without ever contacting `moneybin-sync` when you use file-based imports — OFX, QFX, QBO, CSV, and PDF paths never touch the network and never read `sync.server_url`.

When `sync.server_url` is unset (the default), every `moneybin sync ...` command fails fast with a configuration error pointing at `MONEYBIN_SYNC__SERVER_URL`. Nothing else in the client probes the sync surface — startup, transforms, imports, and reports all proceed normally.

Bank-direct sync via `moneybin-sync` is an additive capability for users who want automated pulls. Skip it entirely and you skip every concern on this page.

## Identity and auth

The client authenticates to `moneybin-sync` with a per-user OAuth2 access token. The token is acquired through the **Device Authorization Grant** (RFC 8628) so that headless CLI environments work without a callback URL.

| Concern | Behavior |
|---|---|
| Auth flow | Device Authorization Grant (RFC 8628). `moneybin sync login` prints a user code + verification URL; the user opens it in any browser and approves. |
| Identity provider | The server is presumed to delegate to an external IdP (Auth0 in the reference deployment). The client never talks to the IdP directly. |
| Token type | `Bearer` JWT — passed via `Authorization: Bearer <token>` on every authed request. |
| Refresh | Rotating refresh tokens. On a 401, the client calls `POST /auth/refresh` once and retries. A second 401 clears both tokens and prompts re-login. |
| Token storage (client) | Client implementation detail; see `src/moneybin/connectors/sync_client.py`. Not part of the contract. |
| Token storage (server) | Out of scope of this contract. The client treats stored Plaid `access_token`s as the server's problem. |

## Endpoint catalog

Every endpoint below is exercised by `SyncClient`. Paths and methods are taken directly from the client's call sites — if the server names something differently, the client won't reach it.

| Method | Path | Auth | Purpose |
|---|---|---|---|
| `POST` | `/auth/device/code` | none | Begin device authorization. Returns `device_code`, `user_code`, verification URL, polling interval. |
| `POST` | `/auth/device/token` | none | Poll for the device-flow result. Returns tokens once the user approves; `202 pending` / `202 slow_down` while waiting. |
| `POST` | `/auth/refresh` | refresh token | Exchange a refresh token for a new access token (with a rotated refresh token). |
| `GET` | `/institutions` | bearer | List the user's connected institutions across providers. |
| `DELETE` | `/institutions/{id}` | bearer | Disconnect an institution by its internal connection ID. |
| `POST` | `/sync/link/initiate` | bearer | Start a link session (Plaid Link today). Returns a hosted `link_url` for the user to open. |
| `GET` | `/sync/link/status` | bearer | Read the current state of a link session (`pending` / `linked` / `failed`). |
| `POST` | `/sync/trigger` | bearer | Run a sync. Synchronous from the client's perspective — blocks until the server completes the pull. |
| `GET` | `/sync/data` | bearer | One-shot read of the most recent sync payload by `job_id`. Server deletes from its TTL store after read. |

Properties across the catalog: bearer-token auth everywhere except the device-flow bootstrap (no cookies, no sessions); JSON request and response bodies (errors included); synchronous semantics — `POST /sync/trigger` blocks until the server-side pull completes, then `GET /sync/data` returns the payload. No webhooks; polling exists only for the link flow, where the user takes the wheel.

### Sync sequence (canonical)

1. Client calls `POST /sync/trigger`.
2. Server runs the upstream provider pull synchronously (may take up to ~120 seconds for multi-institution syncs).
3. Server stores the result in a short-lived TTL cache keyed by `job_id`.
4. Server returns `200` with a sync summary (`job_id`, `status`, `transaction_count`).
5. Client calls `GET /sync/data?job_id=<id>`.
6. Server returns the payload and drops it from the TTL cache.

The trigger blocks because the contract avoids a job-queue/webhook protocol — the client has no inbound port, and the CLI/MCP host is already waiting on the call.

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
{ "device_code": "Ag_EE...", "profile_id": "ab12cd34ef56" }
```

`profile_id` is optional — an opaque, stable per-profile identifier the client
sends so each local profile maps to a distinct broker identity. Omit it for the
legacy single-identity behavior. When present it must match
`^[A-Za-z0-9_-]{1,64}$`; the broker namespaces the minted token's subject by it.

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

### `POST /sync/link/initiate`

Begin a link session. The response includes a hosted URL the user opens in a browser; the user completes provider-side flows there (Plaid Link, in today's deployment).

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

**Response (200):** `LinkInitiateResponse`

```json
{
  "session_id": "sess_abc123",
  "link_url": "https://link.example.com/sessions/sess_abc123",
  "link_type": "widget_flow",
  "expiration": "2026-05-17T15:00:00Z"
}
```

| Field | Notes |
|---|---|
| `session_id` | Opaque token; the client passes it to `GET /sync/link/status`. |
| `link_url` | Hosted URL the client opens in the user's browser. |
| `link_type` | `"widget_flow"` (Plaid Link in a hosted browser) or `"token_paste"` (manual token entry, reserved for headless flows). |
| `expiration` | After this timestamp, the session can no longer transition to `linked`. |

### `GET /sync/link/status`

**Query parameter:** `session_id` from `/sync/link/initiate`.

**Response (200):** `LinkStatusResponse`

```json
{
  "session_id": "sess_abc123",
  "status": "linked",
  "provider_item_id": "item_abc123...",
  "institution_name": "Chase",
  "error": null,
  "expiration": "2026-05-17T15:00:00Z"
}
```

| Field | Type | Notes |
|---|---|---|
| `status` | enum | `"pending"` \| `"linked"` \| `"failed"`. |
| `provider_item_id` | string \| null | Populated once `status == "linked"`. |
| `institution_name` | string \| null | Populated once `status == "linked"`. |
| `error` | string \| null | Populated when `status == "failed"`. |

`pending` → keep polling. `linked` → success. `failed` → surface `error` to the user. Polling cadence and overall deadline are client-side policy, not part of the contract.

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

`status` is always terminal — the server completes the pull synchronously before responding. `transaction_count` is the total across all institutions in the job. A client-side timeout does not mean the server-side job failed; cursor-based sync makes retry safe.

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

`accounts[]`, `transactions[]`, `balances[]`, and `removed_transactions[]` are guaranteed to be present (possibly empty). `metadata` is always present. This endpoint is a one-shot read — see [Failure semantics](#failure-semantics) for retry behavior.

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

What the server returns and how the client classifies it:

| Server signal | Client behavior |
|---|---|
| `401` on an authed call | Refresh once, retry. A second `401` clears tokens and prompts re-login. |
| `403` on `/auth/device/token` | Treated as "user denied authorization." Re-login required. |
| `400` on `/auth/device/token` | Treated as "device code expired." Re-login required. |
| Link session `status == "failed"` | Surfaces the server's `error` string verbatim to the user. |
| Any other `>= 400` | Surfaces `<METHOD> <path> returned <status>: <truncated body>` to the user. |
| Connection refused / DNS failure / TLS error | Surfaces "sync server unreachable at &lt;url&gt;". |
| Client deadline elapsed (connect poll, trigger long timeout) | Surfaces a timeout message; the server-side job may still complete. |

The client wraps each of these into one of `SyncAuthError`, `SyncLinkError`, `SyncTimeoutError`, or `SyncAPIError` — the precise subclass hierarchy is a client implementation detail, defined in `src/moneybin/connectors/sync_errors.py`.

The contract does **not** mandate a uniform error envelope. The client treats any `>= 400` status with an unstructured body as a generic API error. A future revision may tighten this to a structured `{ "error": { "code": "...", "message": "..." } }` shape; until then, the client relies on Pydantic validation to catch malformed success responses and on HTTP status to gate refresh-and-retry behavior.

### Provider error codes inside `metadata.institutions[]`

Per-institution failures inside an otherwise-successful sync do **not** raise — they appear in `metadata.institutions[].error_code` (Plaid error codes, passed through verbatim) and the client maps them to user-facing guidance. The mapping (which codes prompt re-auth, which trigger automatic retry, which suggest "try again later") is a client-side policy detail; see `src/moneybin/connectors/sync_errors.py` for the current set.

## Webhooks

**None.** The contract is strictly request/response. The server does not push events to the client; the client never opens an inbound port. Provider webhooks are a server-side concern; if a future revision adds push semantics, it will be a server-sent stream over the same authed channel, not an inbound listener on the client.

## Privacy boundary

What stays on the client, what crosses to the server, and what never appears on either:

| Data | Client (your machine) | `moneybin-sync` | Notes |
|---|---|---|---|
| Bank password / online banking credentials | Never | Never | Entered into Plaid's hosted UI in your browser; goes directly from your browser to Plaid. |
| Plaid `public_token` | Briefly (in URL fragment during redirect) | Yes — exchanged for `access_token` | Short-lived, single-use. |
| Plaid `access_token` | Never | Yes — encrypted at rest | Long-lived bearer that lets the server pull data from Plaid on your behalf. |
| Bank transaction data (amounts, descriptions, merchants) | Yes — in your encrypted DuckDB profile | Yes — transiently during the sync, then in a short-TTL store until you fetch it | After `GET /sync/data` the server drops the payload from its TTL store. |
| DuckDB encryption key | Yes — OS keychain or passphrase | Never | The server has no way to read your local database. |
| OFX / CSV / PDF imports | Yes | Never | The bank-direct sync server is irrelevant to file-based imports. |

The narrowest possible exposure on the server side is the design intent: the server holds the keys that let Plaid hand over your data, runs the pull, hands the payload to you, then forgets. The DuckDB file on your machine is where data persists.

## Self-host overview

`moneybin-sync` is a separate project in the same GitHub organization as MoneyBin. Setup, configuration, conformance checklist, and the server-side threat model live in its own repository — consult its README directly.

A self-hosted deployment needs, at a high level:

- A reachable HTTPS endpoint.
- Plaid (or future-provider) credentials, scoped to your own account.
- An OAuth2 IdP that issues the bearer JWTs the client expects (Auth0 is the reference IdP; any IdP speaking the Device Authorization Grant + JWT bearer pattern will do).
- A database for the server's own state — connected institutions, encrypted upstream access tokens, sync cursors.

The client points at it via `MoneyBinSettings.sync.server_url` (env var `MONEYBIN_SYNC__SERVER_URL`) and cares only that the deployment honors the contract on this page.

## Operational expectations

What the client assumes about the transport.

| Concern | Behavior |
|---|---|
| TLS | HTTPS required. Client uses `httpx` defaults — system CA bundle via `certifi`; no certificate pinning; `SSL_CERT_FILE` / `SSL_CERT_DIR` env vars honored. |
| Connection reuse | One persistent `httpx.Client` per `SyncClient`. Connect timeout 10 s; read timeout 15 s for most calls, 120 s for `/sync/trigger` and connect polling. |
| Server-side rate limits | No specified `429` / `Retry-After` shape. The client does not handle `429` specially — it surfaces as a generic `SyncAPIError`. A future revision should pin this down. |
| Server retention | `access_token`s persist until `DELETE /institutions/{id}`. Sync cursors live server-side and survive across syncs (incremental sync depends on this). The `/sync/data` TTL cache drops the payload after a successful read. Audit-log and metrics retention are server-policy concerns. |
| Telemetry / egress | The contract requires nothing of the server beyond these endpoints. Any phone-home behavior is documented by the deployment, not by this contract. |
| SLO / latency | None. The 120-second client timeout on `/sync/trigger` is client-side defense, not a server guarantee. |
| Correlation IDs | Not implemented today — no request-ID header sent or parsed. A future revision may add `X-Request-Id` or similar. |

## Failure semantics

What the contract guarantees when things partially fail.

- **TTL atomicity for `/sync/data`.** The contract intent is that the server drops the payload only after handing a complete `200` response to the client; a mid-read drop should leave the payload available for one retry. The exact mechanism is server-implementation detail and not pinned down today. **If the client lost the payload, the safe recovery is to call `POST /sync/trigger` again** — sync is idempotent against the cursor, so re-triggering re-pulls the same incremental window without double-counting.
- **Concurrent `/sync/trigger` calls.** No `Idempotency-Key` header, no specified lock or queue model. The client never issues concurrent triggers; treat concurrent triggers as undefined behavior in any other client until the contract pins this down.
- **Link session reuse.** Opening `link_url` twice is provider-side behavior (Plaid Link manages its own session). The client treats whatever terminal state `/sync/link/status` reports as authoritative.
- **Payload size for `/sync/data`.** Single JSON body, no chunking, no documented maximum. Initial syncs with multi-year history can be large; the client validates the whole payload in memory. Servers emitting payloads above a few hundred MB will cause client-side memory pressure — a known sharp edge.

## Versioning

Pre-launch. No `/v1/` URL prefix, no `MoneyBin-API-Version` header, no formal version negotiation. The contract evolves in lockstep with the client release cycle; the reference `moneybin-sync` deployment is updated alongside the client.

The post-launch versioning mechanism (header? path prefix? content-type?) is genuinely TBD. The intent stated in `docs/architecture.md` is additive changes preferred, deprecate-then-remove across two releases for anything breaking — but the wire-level handle for signalling a break hasn't been picked.

Pydantic validation at the boundary is the only contract-drift signal the client offers today: extra fields pass silently; missing or renamed required fields surface as `SyncAPIError` on the next call.
