# ADR-015: Transaction identity is content-derived with alias forwarding; account identity is a surrogate

**Status:** Proposed (decision approved in design; flips to Accepted when
[`account-identity-resolution.md`](../specs/account-identity-resolution.md) is
promoted from `draft` → `ready`)

## Context

[`account-identity-resolution.md`](../specs/account-identity-resolution.md)
(M1S) makes one real-world account resolve to a single canonical `account_id`
across sources, which in turn lets cross-source transaction dedup finally merge
the *same transaction* observed by two sources into one canonical record. That
raised a one-way-door question for the **canonical transaction's identity**
(`core.fct_transactions.transaction_id`): when two observations merge — or when
an account re-mints, or a pending row posts — should that id stay fixed, and how
is it produced?

Three forces pull in different directions:

1. **`transaction_id` is a public-ish contract.** It is exposed through the MCP
   `sql_query` tool and the `moneybin://schema` resource, and it is the FK every
   curation table (`app.transaction_categories` / `_notes` / `_tags` / `_splits`)
   points at. An agent or query that captures an id expects to resolve it later.
2. **`core` is derived from `raw` (medallion, [ADR-001](001-medallion-data-layers.md)).**
   `transaction_id` is today a pure function of `raw` + match decisions —
   rebuildable, no backup/corruption surface. That property is most valuable
   exactly where volume is highest: transactions.
3. **The merge problem is real and ongoing.** A content-hash keyed on the dedup
   group changes whenever group membership changes, which orphans curation on
   every merge, not just at migration.

Prior art (Actual, Firefly III, Maybe, GnuCash, Plaid) is near-unanimous on one
point: **content must not *be* the identity** — every tool keeps a stable id and
a *separate, demoted* dedup/match key (FITID, `imported_id`, `import_hash`,
`plaid_id`); Firefly states the motive directly — source ids and capitalization
mutate, so hashes are brittle. But two nuances decide the MoneyBin-specific
answer:

- Those tools **mutate records in place**, so a surrogate id persists for free.
  MoneyBin **derives** `core`, so a stable surrogate would require a *persisted
  per-transaction identity registry that survives every rebuild* — hot mutable
  app-state for the highest-volume entity, weakening derive-from-raw where it
  matters most.
- **Plaid — the closest analog** (an API serving canonical records from messy
  bank feeds) — deliberately does **not** keep the id stable across the
  pending→posted enrichment. It mints a new `transaction_id` and ships a
  **forwarding pointer** (`pending_transaction_id`) plus a removal signal, so
  consumers relink. External-reference durability is achieved by **resolution,
  not immutability** — and far more cheaply.

This is pattern-establishing: future canonical entities (securities, merchants)
will inherit whichever identity model we choose here, and the "why" (the
derive-from-raw vs. stable-public-id tension) is not recoverable from the code.
It meets the ADR bar in [`design-principles.md`](../../.claude/rules/design-principles.md).

## Decision

**Transaction identity is content-derived with an alias forwarding map — not a
surrogate. Account identity *is* a surrogate. The asymmetry is deliberate.**

For transactions:

1. **`transaction_id` stays content-derived**, keyed on the **immutable source
   identity** (`source_type | source_origin | source_native_key |
   source_transaction_id`), never on the mutable canonical `account_id` and never
   on descriptive text (`description`/`memo` — the brittle field belongs to the
   fuzzy matcher, not to identity).
2. **Priority-anchored, not whole-set-hashed.** A merged group's id is derived
   from its **highest-priority member's** immutable identity (reusing the
   existing golden-record `MatchingSettings.source_priority`, `ofx > plaid >
   tabular`). A lower-priority twin joining (the common forward-order case —
   authoritative bank file first, CSV later) leaves the id **unchanged**; only a
   higher-priority source arriving later flips the anchor. This minimizes churn
   and rides the most stable id available.
3. **An alias map (`old_id → new_id`) records every id-changing merge.** SQL,
   agent, external, and curation-FK references resolve through it — the Plaid
   `pending_transaction_id` model. Brittleness in any one source key (a mutated
   FITID, the description-bearing CSV per-source hash) thus degrades to a
   forwarding pointer, never an orphan.

For accounts (decided in the same spec, recorded here for the contrast): a
**minted opaque surrogate `account_id`** with a `app.account_links` registry —
because accounts are few, long-lived, user- and agent-*referenced* (filtered by,
named; the id is the AX handle), so a stable public id earns its keep and the
registry is tiny.

**The general rule for a canonical merged entity:** use a **surrogate +
registry** when the entity is few, long-lived, and externally *referenced* and a
stable public id is user-facing (accounts). Use a **content-derived id + alias
forwarding** when the entity is high-volume and internal and preserving
derive-from-raw matters (transactions). Identity must never *be* raw content; a
content-derived id is still acceptable when paired with alias forwarding for
reference durability.

## Consequences

- **Derive-from-raw is preserved** for the hot path; the only added state is an
  append-only alias table populated on id-changing merges (and the existing
  `app.match_decisions`), not a per-transaction registry.
- **Curation is never lost on a merge:** the remap step (and consumers) resolve
  `old_id → new_id` via the alias map; the user never sees the id and loses no
  annotation.
- **Reference durability is by resolution, not immutability** — a documented
  contract (like Plaid's): a consumer holding an old id must resolve it through
  the alias map. The `moneybin://schema` / `sql_query` docs must state this.
- **Churn is bounded** to transactions whose dedup-group membership actually
  changes, and minimized further by priority-anchoring.
- **Two open follow-ups** (tracked in the spec, not blocking): hardening the CSV
  per-source content hash (drop `description`; `identifiers.md` territory), and
  the alias-chain-collapse rule across multiple successive merges.
- **The opaque account surrogate** must be reclassified in the privacy taxonomy
  (`ACCOUNT_IDENTIFIER` → record-id tier) so the redaction layer does not mask
  the "stable non-PII handle" the spec promises — tracked in the spec.

## Alternatives considered

- **(B) Stable surrogate `transaction_id` + identity registry** (the Actual /
  GnuCash model). Rejected: it adds hot mutable app-state for the
  highest-volume entity and makes transaction identity backup/recovery-critical,
  weakening derive-from-raw precisely where it is most load-bearing — to buy an
  immutability guarantee that alias forwarding reaches by resolution at a
  fraction of the cost.
- **(A) Plain content-hash with no alias map** (the original draft). Rejected:
  dominated by (C) — external/SQL references break on merge for the want of a
  cheap append-only alias table, and the prior art is unanimous that bare
  content-as-identity is brittle.
