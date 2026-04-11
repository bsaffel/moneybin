# API Contract

Base URL: `https://api.moneybin.app` (production), `http://localhost:3000` (development).

## Authentication

All endpoints except `/health` and `/auth/*` require a valid JWT in the `Authorization` header:

```
Authorization: Bearer <jwt-token>
```

The JWT is issued by Auth0 (via server proxy) and validated against the Auth0 JWKS endpoint. Clients never talk to Auth0 directly -- all auth flows go through `api.moneybin.app`.

## Error Response Format

All error responses use a consistent JSON shape:

```json
{
  "error": "Human-readable error message",
  "details": {}
}
```

The `details` field is optional and included only when additional context is available (e.g., validation errors).

## HTTP Status Codes

| Code | Meaning |
|------|---------|
| 200 | Success |
| 201 | Created |
| 202 | Accepted (polling response for device flow) |
| 204 | No Content (successful deletion) |
| 400 | Bad Request (malformed input) |
| 401 | Unauthorized (missing or invalid JWT) |
| 403 | Forbidden (valid JWT but insufficient permissions) |
| 404 | Not Found |
| 422 | Unprocessable Entity (valid JSON but failed validation) |
| 500 | Internal Server Error |
| 501 | Not Implemented (stub endpoints during phased rollout) |

---

## Health

### `GET /health`

Health check endpoint. No authentication required.

**Response** `200`

```json
{
  "status": "ok",
  "timestamp": "2026-04-07T12:00:00.000Z"
}
```

---

## Auth Endpoints (No JWT Required)

These endpoints are part of the authentication flow and do not require an existing JWT.

### `POST /auth/device/code`

Initiates the Device Authorization Flow for CLI clients. The server proxies the request to Auth0.

**Request body**: None.

**Response** `200`

```json
{
  "device_code": "Ag_EE...",
  "user_code": "ABCD-EFGH",
  "verification_uri": "https://your-tenant.auth0.com/activate",
  "verification_uri_complete": "https://your-tenant.auth0.com/activate?user_code=ABCD-EFGH",
  "expires_in": 900,
  "interval": 5
}
```

The CLI displays `user_code` and `verification_uri_complete` to the user, who visits the URL in a browser to authorize. The CLI then polls `POST /auth/device/token`.

### `POST /auth/device/token`

Polls for a device token after the user has authorized the device. The server proxies the request to Auth0.

**Request body**

```json
{
  "device_code": "Ag_EE..."
}
```

**Response** `200` (authorized)

```json
{
  "access_token": "eyJ...",
  "id_token": "eyJ...",
  "refresh_token": "v1.M...",
  "token_type": "Bearer",
  "expires_in": 86400
}
```

**Response** `202` (pending -- user has not yet authorized)

```json
{
  "status": "pending"
}
```

**Response** `202` (slow_down -- polling too fast)

```json
{
  "status": "slow_down"
}
```

**Errors**

- `400` if `device_code` is missing or the device code has expired.
- `403` if the user denied authorization.

### `GET /auth/login`

Redirects the user to Auth0 Universal Login (Authorization Code Flow for web clients).

**Response** `302` redirect to Auth0 login page with CSRF state cookie set.

### `GET /auth/callback`

Auth0 redirects here after successful Authorization Code Flow authentication. The server validates the state parameter (CSRF check), exchanges the authorization code for tokens, and establishes a session.

**Query Parameters**

| Parameter | Type | Description |
|-----------|------|-------------|
| `code` | string | Authorization code from Auth0 |
| `state` | string | CSRF state parameter (must match cookie) |

**Response** `302` redirect to the web UI with session established.

### `GET /auth/logout`

Clears the session and redirects to Auth0's logout endpoint.

**Response** `302` redirect to Auth0 logout, then to post-logout URL.

---

## Auth Endpoints (JWT Required)

### `GET /auth/session`

Returns the currently authenticated user's information. `id` is the server's local UUID (not the Auth0 `sub`).

**Response** `200`

```json
{
  "user": {
    "id": "550e8400-e29b-41d4-a716-446655440000",
    "email": "user@example.com"
  }
}
```

**Error** `401` if no valid JWT.

---

## Sync Endpoints (JWT Required)

### `POST /sync/link-token`

Creates a Plaid Link token. The client uses this token to initialize Plaid Link in the browser.

**Request body**: None.

**Response** `200`

```json
{
  "link_token": "link-sandbox-abc123...",
  "expiration": "2026-04-07T12:30:00Z"
}
```

### `POST /sync/exchange-token`

Exchanges a Plaid public token (received after the user completes Plaid Link) for a persistent access token. The server encrypts the access token with AES-256-GCM and stores it in the `plaid_items` table.

**Request body**

```json
{
  "public_token": "public-sandbox-abc123...",
  "institution_id": "ins_123",
  "institution_name": "Chase"
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `public_token` | string | yes | Public token from Plaid Link completion |
| `institution_id` | string | no | Plaid institution identifier |
| `institution_name` | string | no | Human-readable institution name |

**Response** `201`

```json
{
  "item_id": "item_abc123..."
}
```

**Errors**

- `400` if `public_token` is missing.
- `422` if the token exchange fails with Plaid.

### `POST /sync/trigger`

Triggers a sync job. If `item_id` is provided, syncs only that institution. If omitted, syncs all connected institutions in parallel (`Promise.allSettled`). Blocks until complete.

**Request body**

```json
{
  "item_id": "item_abc123..."
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `item_id` | string | no | Specific Plaid Item to sync. Omit to sync all institutions in parallel. |

**Response** `201`

```json
{
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "completed",
  "transaction_count": 142
}
```

The response always reflects the final job state since sync is synchronous. In a future async implementation, the response would return `status: "pending"` and the client would use the existing polling flow.

### `GET /sync/status`

Returns the current status of a sync job, including per-institution results.

**Query Parameters**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `job_id` | string (UUID) | yes | The sync job identifier |

**Response** `200`

```json
{
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "completed",
  "transaction_count": 142,
  "error": null,
  "started_at": "2026-04-07T12:00:00.000Z",
  "completed_at": "2026-04-07T12:00:05.000Z",
  "results": [
    {
      "item_id": "item_abc",
      "institution_name": "Chase",
      "status": "completed",
      "transaction_count": 80
    },
    {
      "item_id": "item_def",
      "institution_name": "Schwab",
      "status": "failed",
      "error": "ITEM_LOGIN_REQUIRED"
    }
  ]
}
```

| Field | Type | Description |
|-------|------|-------------|
| `job_id` | string (UUID) | Job identifier |
| `status` | string | One of: `pending`, `running`, `completed`, `failed` |
| `transaction_count` | number or null | Total transactions fetched across all institutions |
| `error` | string or null | Top-level error message (set on complete failure) |
| `started_at` | string or null | ISO 8601 timestamp when processing began |
| `completed_at` | string or null | ISO 8601 timestamp when processing finished |
| `results` | array or null | Per-institution outcomes (from JSONB column) |

**Errors**

- `400` if `job_id` query parameter is missing.
- `404` if the job does not exist or belongs to another user.

### `GET /sync/data`

Returns the sync results as JSON. The client calls this after `status` returns `completed`. Data is held in an in-memory TTL store (30-minute default) and expires after that window.

**Query Parameters**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `job_id` | string (UUID) | yes | The completed sync job identifier |

**Response** `200`

```
Content-Type: application/json
```

See [Sync Data Format](#sync-data-format) for the full response shape.

**Errors**

- `400` if `job_id` query parameter is missing.
- `404` if the job does not exist, belongs to another user, or has expired from the TTL store.
- `422` if the job has not completed yet.

---

## Institution Endpoints (JWT Required)

### `GET /institutions`

Lists all connected institutions for the authenticated user.

**Response** `200`

```json
[
  {
    "id": "550e8400-e29b-41d4-a716-446655440000",
    "item_id": "item_abc123...",
    "institution_name": "Chase",
    "status": "active",
    "last_sync": "2026-04-07T12:00:00.000Z",
    "created_at": "2026-03-15T08:30:00.000Z"
  }
]
```

| Field | Type | Description |
|-------|------|-------------|
| `id` | string (UUID) | Internal identifier for this connection |
| `item_id` | string | Plaid Item identifier |
| `institution_name` | string or null | Human-readable institution name |
| `status` | string | Connection status: `active`, `error`, `revoked` |
| `last_sync` | string or null | ISO 8601 timestamp of last successful sync |
| `created_at` | string | ISO 8601 timestamp when institution was connected |

### `DELETE /institutions/:id`

Disconnects an institution. Revokes the Plaid access token and removes the connection from the database.

**Path Parameters**

| Parameter | Type | Description |
|-----------|------|-------------|
| `id` | string (UUID) | Internal identifier for the institution connection |

**Response** `204 No Content`

**Errors**

- `404` if the institution does not exist or belongs to another user.

---

## Sync Data Format

The `GET /sync/data` endpoint returns JSON. Data volumes are small (hundreds to low thousands of transactions per sync), making JSON a natural fit that eliminates heavy server-side Parquet dependencies.

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
      "transaction_date": "2026-04-07",
      "amount": 42.50,
      "description": "COFFEE SHOP",
      "merchant_name": "Best Coffee",
      "category": "FOOD_AND_DRINK",
      "pending": false
    }
  ],
  "balances": [
    {
      "account_id": "acc_001",
      "balance_date": "2026-04-08",
      "current_balance": 1234.56,
      "available_balance": 1200.00
    }
  ],
  "removed_transactions": ["txn_old_001"],
  "metadata": {
    "job_id": "550e8400-e29b-41d4-a716-446655440000",
    "synced_at": "2026-04-08T12:00:00.000Z",
    "institutions": [
      {
        "item_id": "item_abc",
        "institution_name": "Chase",
        "status": "completed",
        "transaction_count": 80
      },
      {
        "item_id": "item_def",
        "institution_name": "Schwab",
        "status": "failed",
        "error": "ITEM_LOGIN_REQUIRED"
      }
    ]
  }
}
```

### Field Reference

#### `accounts[]`

| Field | Type | Description |
|-------|------|-------------|
| `account_id` | string | Plaid account identifier; stable across syncs |
| `account_type` | string | Plaid account type: `depository`, `credit`, `loan`, `investment`, `other` |
| `account_subtype` | string | Plaid account subtype: `checking`, `savings`, `credit card`, etc. |
| `institution_name` | string | Human-readable institution name |
| `official_name` | string | Official account name from the institution |
| `mask` | string | Last 4 digits of the account number |

#### `transactions[]`

| Field | Type | Description |
|-------|------|-------------|
| `transaction_id` | string | Plaid transaction identifier; unique and stable |
| `account_id` | string | Plaid account identifier |
| `transaction_date` | string (YYYY-MM-DD) | Date the transaction posted or is pending |
| `amount` | number | Transaction amount in **Plaid convention: positive = expense, negative = income** |
| `description` | string | Transaction description from the institution |
| `merchant_name` | string or null | Normalized merchant name from Plaid |
| `category` | string or null | Plaid's primary transaction category |
| `pending` | boolean | `true` if the transaction has not yet posted |

#### `balances[]`

| Field | Type | Description |
|-------|------|-------------|
| `account_id` | string | Plaid account identifier |
| `balance_date` | string (YYYY-MM-DD) | Date the balance was captured |
| `current_balance` | number | Total current balance including pending transactions |
| `available_balance` | number or null | Available balance; may be null for credit accounts |

#### `removed_transactions[]`

Array of `transaction_id` strings for transactions that Plaid has removed (e.g., reversed or duplicate). The client should delete these from local storage.

#### `metadata`

| Field | Type | Description |
|-------|------|-------------|
| `job_id` | string (UUID) | Sync job identifier |
| `synced_at` | string (ISO 8601) | When the server fetched this data from Plaid |
| `institutions[]` | array | Per-institution sync outcomes |

### Amount Sign Convention

Plaid and moneybin use opposite sign conventions for transaction amounts:

| System | Expense | Income |
|--------|---------|--------|
| Plaid (server delivers this) | Positive | Negative |
| moneybin core convention | Negative | Positive |

The server delivers Plaid's native convention. The moneybin client's staging model `prep.stg_plaid__transactions` negates the amount (`-1 * amount`) to align with the project-wide convention before data reaches the core layer.

### Client-Side Metadata

The JSON response does not include `source_file`, `extracted_at`, or `loaded_at` fields. These are client-side metadata:
- `source_file`: The client generates a logical identifier such as `sync_{job_id}`
- `extracted_at`: Set from `metadata.synced_at`
- `loaded_at`: Set to the current timestamp during DuckDB insertion
