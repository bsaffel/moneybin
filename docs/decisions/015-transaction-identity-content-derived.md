# ADR-015: Transaction identity is content-derived with alias forwarding; account identity is a surrogate

**Status:** Accepted (the governing spec
[`account-identity-resolution.md`](../specs/account-identity-resolution.md) is
`in-progress`)

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

A bare content hash cannot provide stable references: source identity can be
enriched or corrected, while group membership changes as observations merge. But
MoneyBin's derived `core` layer cannot obtain a stable surrogate for free: it
would need a persisted per-transaction registry that survives every rebuild,
adding hot mutable state to the highest-volume entity and weakening
derive-from-raw where it matters most.

The instability is a source-protocol fact, not a hypothetical: Plaid mints a
new `transaction_id` when a pending transaction posts, forwarding the old one
via `pending_transaction_id` — so even a provider-assigned id does not survive
the pending→posted boundary.

The durable alternative is forwarding. When a source record changes identity
as it moves from pending to posted, the old reference must resolve to the new
one. External-reference durability is therefore achieved by **resolution, not
immutability**, through a cheap append-only alias map.

This is pattern-establishing: future canonical entities (securities, merchants)
will inherit whichever identity model we choose here, and the "why" (the
derive-from-raw vs. stable-public-id tension) is not recoverable from the code.
It meets the ADR bar in [`design-principles.md`](../../.claude/rules/design-principles.md).

## Decision

**Transaction identity is content-derived with an alias forwarding map — not a
surrogate. Account identity *is* a surrogate. The asymmetry is deliberate.**

For transactions:

1. **`transaction_id` stays content-derived**, keyed on the **immutable source
   identity** (`source_type | source_origin | source_account_key |
   source_transaction_id`), never on the mutable canonical `account_id` and never
   on descriptive text (`description`/`memo` — the brittle field belongs to the
   fuzzy matcher, not to identity).
2. **Stability-class-anchored, not whole-set-hashed.** A merged group's
   `transaction_id` is derived from its **anchor member's** immutable identity,
   where the anchor is chosen by an intrinsic stability class — not by the mutable
   `MatchingSettings.source_priority` list (which governs golden-record field merging
   only):

   | Class | Rank | Sources | Id basis | Drifts? |
   |---|---|---|---|---|
   | native | 0 | OFX (FITID), Plaid (txn id) | upstream-assigned | never |
   | minted | 1 | manual (`manual_` + uuid4, persisted PK) | minted once | never |
   | hash | 2 | CSV / tabular family; gsheet-live (future) | content hash | yes (re-export) |

   Within a class, the tiebreak is `loaded_at` (first-seen), then the source
   identity tuple for a deterministic final ordering. A lower-stability twin
   joining (the common forward-order case — bank file first, CSV later) leaves the
   id **unchanged**; a more-stable source backfilling history re-anchors the group
   **once** (alias-forwarded), then stays stable. Single and unmatched transactions
   hash their own identity unchanged.

   **Why intrinsic class, not the source-priority list.** Reusing `source_priority`
   for identity is fragile: reordering it (a legitimate field-merge tuning operation)
   would re-key merged `transaction_id`s. More concretely, `gsheet` (field-
   authoritative in the list) is a future content-hash source — an unstable anchor
   candidate — while `ofx` is a native-id source and the naturally stable choice
   regardless of how field priorities are ordered. An intrinsic 3-class rank is a
   fact about how an id is derived; it does not drift as sources are added or
   priorities are retuned.
3. **An alias map (`old_id → new_id`) records every id-changing merge.** SQL,
   agent, external, and curation-FK references resolve through it. Brittleness
   in any one source key (a mutated FITID or a description-bearing CSV
   per-source hash) thus degrades to a forwarding pointer, never an orphan.

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
  contract: a consumer holding an old id must resolve it through the alias map.
  The `moneybin://schema` / `sql_query` docs must state this.
- **Churn is bounded** to transactions whose dedup-group membership actually
  changes, and minimized further by stability-class anchoring: a new lower-stability
  twin joining causes no re-key; only a more-stable source arriving later flips the
  anchor (once, then stable).
- **Two open follow-ups** (tracked in the spec, not blocking): hardening the CSV
  per-source content hash (drop `description`; `identifiers.md` territory), and
  the alias-chain-collapse rule across multiple successive merges.
- **The opaque account surrogate** must be reclassified in the privacy taxonomy
  (`ACCOUNT_IDENTIFIER` → record-id tier) so the redaction layer does not mask
  the "stable non-PII handle" the spec promises — tracked in the spec.

## Alternatives considered

- **(B) Stable surrogate `transaction_id` + identity registry.** Rejected: it
  adds hot mutable app-state for the highest-volume entity and makes transaction
  identity backup/recovery-critical, weakening derive-from-raw precisely where
  it is most load-bearing — to buy an immutability guarantee that alias
  forwarding reaches by resolution at a fraction of the cost.
- **(A) Plain content-hash with no alias map** (the original draft). Rejected:
  dominated by (C) — external and SQL references break on merge for the want of
  a cheap append-only alias table; bare content-as-identity is brittle whenever
  its inputs change.

## Change history

- **2026-06-15 (pre-launch) — Decision 2 amended.** The merged-group anchor is
  chosen by an intrinsic stability class (native › minted › hash) plus first-seen
  tiebreak, not by `MatchingSettings.source_priority`. Rationale: `source_priority`
  governs field merging and is mutable; reusing it for identity would re-key merged
  `transaction_id`s on a legitimate field-merge tuning operation, and a future
  content-hash source (`gsheet`) ranking high on field authority would be an
  unstable anchor regardless of its list position. The intrinsic 3-class rank is a
  fact about how an id is derived and does not drift. Consequences section updated
  accordingly.
