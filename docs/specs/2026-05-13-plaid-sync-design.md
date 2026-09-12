# Plaid Sync Phase 1 — Superseded Design Record

> **Superseded.** This 2026-05-13 design no longer defines the Plaid sync
> contract. [`sync-plaid.md`](sync-plaid.md) is the indexed, implemented
> canonical record.

## Status

superseded

The original document claimed precedence over `sync-plaid.md`, which left two
competing specifications. Its surviving client-facing decisions are reconciled
in the canonical spec: the client initiates with `POST /sync/link/initiate`,
checks `GET /sync/link/status?session_id=...`, supports the `widget_flow` link
type, and uses `provider_item_id` for re-authentication. The raw Plaid sign is
preserved until `prep.stg_plaid__transactions` converts it to MoneyBin's
accounting convention.

moneybin-sync remains opaque to the client. Server-side provider details,
credentials, callbacks, and webhook handling are not a MoneyBin client
contract. Consult `sync-plaid.md` and `sync-overview.md` for current behavior.

The complete historical design remains available in Git history before this
supersession record. It must not be used as an implementation authority.
