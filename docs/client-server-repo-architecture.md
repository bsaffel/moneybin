# client-server-repo-architecture.md

## Goal

Build a **portfolio-friendly public GitHub repo** that supports rapid client + server iteration during proof-of-concept, while preserving the option to later launch a **paid, private “production server”** without awkward refactors, licensing drama, or accidentally open-sourcing differentiators.

Core strategy:

- Publish a **reference ingestion server** (toy server) that is intentionally “adapter-only.”
- Keep **contracts + SDK** as the stable integration surface.
- Later, build a separate **private core/production server** that consumes the same raw data and contracts, but contains all “secret sauce” logic.

---

## Guiding Principles

### 1 Adapter, not brain

The public server exists to:

- connect to aggregators (e.g., Plaid)
- pull data
- store raw payloads and minimal normalized primitives
- expose a small API surface for the client to function

It should **not** contain:

- categorization heuristics
- deduplication rules
- reconciliation logic
- enrichment, matching, “fixes,” edge-case handling
- tax-grade correctness

**Ugly is protective.** If something feels like “smart cleanup,” it probably belongs in the private core server later.

### 2) Contract-first development

The client depends on:

- an API contract (OpenAPI / JSON Schema / Protobuf)
- generated types
- a generated SDK (or thin hand-written client using generated types)

The client must not depend on server implementation details.

### 3) Data pipeline boundary

Public server stops at **raw ingestion**.

- Persist “as reported by aggregator” raw events/transactions.
- Avoid opinionated transformations that are hard to reverse.
- Prefer append-only event storage where possible.

The private core server later:

- reads raw events
- applies transformations + logic
- produces “gold” canonical views

### 4) Make it easy to split later

Even while everything is public during POC, structure the code so the “reference ingestion server” is conceptually separate and can be frozen/deprecated later without breaking the client or contracts.

---

## Recommended Repo Layout (Public Portfolio Repo)

Top-level:

repo/
client/
server-ingestion/
shared/
contracts/
docs/
examples/
scripts/
docker/

### `client/` (public)

- UI / app that users run
- Depends ONLY on the contract + SDK (not server internals)

### `server-ingestion/` (public reference server)

A minimal server whose job is ingestion + persistence.

Responsibilities:

- Aggregator auth handshake (e.g., Plaid Link token → exchange public token)
- Data pull jobs (transactions, accounts, balances)
- Persist raw payloads + minimal indexing
- Serve read endpoints required for the client’s basic functionality

Non-responsibilities (explicitly excluded):

- categorization
- dedupe across sources
- reconciliation across accounts
- enrichment (merchant mapping, fuzzy matching, etc.)
- “correctness” beyond faithfully storing what the aggregator returned

Naming note:

- Call it `server-ingestion` (or `ingestion-server`), NOT `server`.
- This reduces expectation that it is production-grade.

### `contracts/` (public)

- Source-of-truth API definition and/or data schemas
- OpenAPI recommended for HTTP/JSON
- May include JSON Schema for stored raw payload shapes, if helpful

### `shared/` (public)

- Generated types
- SDK (generated or thin wrapper)
- Shared constants, but **no business logic**
- Error codes and response conventions (typed)

### `docs/` (public)

- Architecture docs (this file)
- API docs generated from contracts
- “Reference server is not the product” positioning

---

## Public API Surface (POC-Friendly)

Keep the reference server API small and utilitarian:

Suggested endpoint categories:

- Auth/session
  - `POST /auth/login` (optional if local-only)
  - `POST /auth/logout`
- Aggregator connection
  - `POST /aggregators/plaid/link-token`
  - `POST /aggregators/plaid/exchange-token`
- Ingestion jobs
  - `POST /ingest/transactions/sync`
  - `POST /ingest/accounts/sync`
- Basic reads for UI
  - `GET /accounts`
  - `GET /transactions?start=&end=&account_id=`
  - `GET /balances`

Optional:

- Health + metadata
  - `GET /health`
  - `GET /version`

**Avoid:**

- “categorized transactions” endpoints
- “canonical merchants” endpoints
- “reconciled ledger” endpoints
- advanced analytics endpoints

Those are prime candidates for the private core server.

---

## Data Storage Contract (Public Reference Server)

The public server should store raw data in a durable, inspectable way that can be reused later.

Recommended pattern:

- Raw JSON payloads stored as-is
- Minimal normalized tables for indexing/query

Example storage domains:

- `raw_aggregator_events` (append-only)
- `raw_transactions` (as returned)
- `raw_accounts`
- `sync_runs` (job metadata)

Rules:

- Never delete raw history during POC; prefer append-only + “latest view” derived queries.
- Avoid applying business rules at ingestion time.

The private core server later should be able to read these raw tables/files directly.

---

## “Secret Sauce” Private Server (Future)

When ready, create a separate private repo/project:

Name ideas:

- `server-core`
- `server-prod`
- `moneybin-core`

Responsibilities:

- Read from raw ingestion store
- Apply transformations and “correctness”:
  - dedupe
  - reconciliation
  - categorization
  - enrichment
  - backfills
  - canonical entity modeling
- Produce canonical “gold” tables and product-quality endpoints
- Own operational concerns:
  - rate limiting
  - queuing
  - retries and dead-letter handling
  - tenant isolation
  - billing/auth plans
  - observability

Integration surface stays stable via:

- same `contracts/` (or a superset)
- same client SDK expectations where possible

---

## Guardrails to Prevent Scope Creep (Non-Negotiable)

### Guardrail A: No smart transforms in public

If you find yourself writing logic that:

- “fixes” messy data
- handles real-world edge cases
- improves match quality
- merges across sources

Stop. Capture raw data and leave a TODO referencing the future core server.

### Guardrail B: Shared code must not contain business logic

`shared/` should contain:

- types
- SDK
- error codes
- pagination conventions
- request/response models

It must not contain:

- categorization rules
- fuzzy matching
- “cleanup helpers” that become core logic

### Guardrail C: Public server is explicitly “reference”

Include strong positioning language in `server-ingestion/README.md`:

- “Reference ingestion server for local development and experimentation.”
- “Not intended to be production-grade.”
- “Correctness and advanced logic belong in the core server.”

### Guardrail D: Contract is the boundary

Client changes must be driven by contract changes, not server-internal coupling.

---

## Development Workflow (Agent-Friendly)

### Daily loop (contract-first)

1. Modify `contracts/` (API or schema)
2. Regenerate `shared/` types + SDK
3. Update `server-ingestion/` to satisfy the contract (minimal)
4. Update `client/` to use the SDK/types
5. Run tests + smoke E2E

### One-command dev

Provide a single entrypoint to run everything locally:

- `make dev` or `./scripts/dev`
- Starts:
  - reference ingestion server
  - client
  - dependencies (db, redis) via `docker compose` if needed

### Testing

- Contract compatibility (breaking change detection)
- Basic API tests for reference server
- Minimal E2E: connect aggregator sandbox → sync → show transactions

---

## “Public Now, Private Later” Expectations

Important realities:

- Anything public should be assumed permanently accessible (forks exist).
- Plan for “stop publishing new server code” rather than “erase history.”
- Keeping the public server simple and non-differentiating is the safety mechanism.

Deprecation strategy later:

- Freeze `server-ingestion` at a stable reference version
- Mark as “maintenance mode”
- Keep contracts compatible where possible
- Encourage hosted/private core server for production-grade behavior

---

## Implementation Checklist (for an Agent)

- [ ] Create repo layout: `client/`, `server-ingestion/`, `contracts/`, `shared/`, `docs/`
- [ ] Pick contract system: OpenAPI (recommended) + codegen
- [ ] Create `shared/` generation pipeline (types + SDK)
- [ ] Implement minimal ingestion server endpoints:
  - link-token, exchange-token
  - sync transactions/accounts
  - read endpoints for UI
- [ ] Persist raw payloads (append-only preferred)
- [ ] Add “reference server” disclaimers + guardrails in README
- [ ] Ensure client uses only SDK/types
- [ ] Provide `make dev` (or equivalent) + `docker compose` if needed
- [ ] Add contract-breaking-change checks + basic API/E2E tests

---

## Non-Goals (Explicit)

The public portfolio repo does NOT attempt to solve:

- full correctness guarantees
- sophisticated categorization
- reconciliation and ledger-quality outputs
- enterprise security/compliance posture
- multi-tenant billing and hosted operations

Those belong to the future private core server.

---

## Summary

This architecture optimizes for:

- fast POC iteration (client + server together)
- strong portfolio signal (real integrations + real contracts)
- future commercialization optionality (core logic remains private)

The key is discipline: **public server = ingestion adapter only**; **private server = the brain**.
