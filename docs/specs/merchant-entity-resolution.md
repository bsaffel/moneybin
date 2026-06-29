# Merchant Entity Resolution

> Last updated: 2026-06-28
> Status: in-progress
> Address: M1T (Ingestion Core)
> Type: Feature
> Owns: the canonical-merchant-identity contract — `app.merchant_links` +
> `app.merchant_link_decisions`, and the `provider merchant id → merchant_id`
> resolution step categorization runs through.
> Mirrors: [`account-identity-resolution.md`](account-identity-resolution.md)
> (the account twin — same two-table + adopt-or-mint pattern, lifted to the
> merchant grain).
> Depends on: Tier-1 (PR #283) — `merchant_entity_id` captured into
> `raw.plaid_transactions` + `prep.stg_plaid__transactions`, backfillable via
> `moneybin sync pull --force`.
> Relates to: Tier-2b (`plaid-tier2b`, Plaid Personal Finance Category
> categorizer) — Tier-2a establishes merchant *identity*; Tier-2b fills merchant
> *category*.
> Unblocks: cross-connection / cross-source merchant dedup; merchant merge
> (future); merchant default-category learning from a stable id.

## One-line goal

One real-world merchant = one canonical `merchant_id`, regardless of the
description text a transaction arrives with — by resolving a transaction's
merchant **by a stable provider id (Plaid `merchant_entity_id`) before falling
back to name matching**, backed by a durable `provider_id → merchant_id` link
registry. Plaid's entity granularity is a *signal*, never the definition of our
merchant entities; users can override every binding.

## Problem statement

Merchant matching today is **purely name-based**. `match_merchants`
(`src/moneybin/services/categorization/matcher.py`) resolves a transaction's
merchant by `oneOf` exemplar set-membership, then `exact`/`contains`/`regex`
pattern against the `description + "\n" + memo` text. `merchant_id` is assigned
during categorization (`orchestrator.py:308`) and stored on
`app.transaction_categories.merchant_id`; merchants live in `app.user_merchants`,
exposed via the `core.dim_merchants` view.

Name-based matching splits one real merchant into several whenever its
description text varies (`SQ *STARBUCKS 0123`, `STARBUCKS STORE 456`,
`Starbucks`), and merges distinct merchants whenever their text collides. Plaid
already hands us a **stable, cross-connection merchant identifier**
(`merchant_entity_id`) plus a clean `merchant_name` — Tier-1 captured both into
`raw`/`prep` — but nothing consumes them. This spec makes that id a first-class
**strong-ref dedup signal**.

## Prior art

- **The account twin — [`account-identity-resolution.md`](account-identity-resolution.md)
  (M1S).** Accounts already solve "external stable id as a strong dedup signal":
  `app.account_links` (`ref_kind='persistent_token'` stores Plaid's
  `persistent_account_id`) + `app.account_link_decisions` (review queue), with an
  `AccountResolver` ladder that auto-adopts a strong ref *before* any fuzzy
  matching. `merchant_entity_id` is the merchant-side twin; this spec lifts the
  **whole** pattern (two tables + adopt-or-mint ladder + review queue) to the
  merchant grain.
- **`identifiers.md`** — Guard "bind on id, not text"; external ids feed dim
  resolution. A provider merchant id is a source-provided id (strategy #1).
- **Sibling passthrough** — the M1S.2 inbox follow-up (account-side
  `persistent_account_id`) is the same passthrough class.

## Decision 1 — `merchant_id` stays the opaque user/system surrogate; the provider id is a signal, not the identity

`core.dim_merchants.merchant_id` remains the canonical, user/system-owned
`uuid4[:12]` it is today (`created_by ∈ {user, ai, rule, plaid, migration}`).
**Plaid does not define our merchant entities.** A `merchant_entity_id` is an
external *reference* that points *at* one of our canonical merchants; one
canonical merchant may own **many** provider ids (N:1) — because Plaid (and
future aggregators) sometimes split one real merchant into several entity ids, or
the user merges merchants that arrived with different ids, and the user's mental
model is one "Amazon." The link table (Decision 2) models that N:1 explicitly; a
single column on `user_merchants` could not, and would force Plaid's granularity
onto the user.

## Decision 2 — Two tables: `merchant_links` (binding) + `merchant_link_decisions` (review queue)

Mirrors `account_links` / `account_link_decisions` wholesale. Both are written
through repos (`MerchantLinksRepo`, `MerchantLinkDecisionsRepo`) so every
mutation emits a paired `app.audit_log` row in the same transaction (Invariant
10, [`app-integrity-invariant.md`](app-integrity-invariant.md)).

Simpler than the account tables in one way: merchants have **no `source_native`
translation role**. Account staging JOINs `account_links` to translate a source
account key → canonical id at transform time; merchant identity is assigned at
*categorization* time, so `merchant_links` carries **only** the strong-ref
binding, never a per-source translation key. No `source_origin` column — a Plaid
`merchant_entity_id` is stable *across* connections by construction, so the
binding is not connection-scoped.

### `app.merchant_links` — the provider-id → canonical-merchant binding

One row per (provider id, canonical merchant). Status is binary: `accepted`
(live) or `reversed` (undone) — no pending state here (that lives in
`merchant_link_decisions`).

```sql
-- app.merchant_links
link_id      TEXT     PRIMARY KEY,   -- uuid4[:12]
merchant_id  TEXT     NOT NULL,      -- canonical merchant this provider id maps to
ref_kind     TEXT     NOT NULL,      -- CHECK (ref_kind IN ('merchant_entity_id'))  [closed, extensible]
ref_value    TEXT     NOT NULL,      -- the provider's stable merchant id (opaque, non-PII)
source_type  TEXT     NOT NULL,      -- issuing provider: plaid (future: mx, simplefin, ...)
status       TEXT     NOT NULL,      -- CHECK (status IN ('accepted', 'reversed'))
decided_by   TEXT     NOT NULL,      -- CHECK (decided_by IN ('auto', 'user', 'system'))
decided_at   TIMESTAMP NOT NULL,
reversed_at  TIMESTAMP,
reversed_by  TEXT
```

**Contracts (repo-enforced guards — DuckDB has no partial unique indexes,
consistent with `AccountLinksRepo`):**

- **One provider id → one canonical merchant.** `(source_type, ref_kind,
  ref_value)` is unique among `accepted` rows. Scoping by `source_type` keeps the
  table **provider-neutral**: a second aggregator is a new `source_type` value,
  zero schema change, no cross-provider id collision.
- **N:1 is allowed.** No uniqueness on `merchant_id` — one merchant may hold many
  provider ids (Plaid variant ids, future merges, multiple aggregators).

### `app.merchant_link_decisions` — the fuzzy-match review queue

`match_decisions`-shaped. One row per (unbound provider id, candidate merchant)
proposal — candidates are relational rows, not JSON. The review queue reads
`pending` rows; this is the **only** place ambiguous state lives.

```sql
-- app.merchant_link_decisions
decision_id           TEXT  PRIMARY KEY,  -- uuid4[:12]
ref_kind              TEXT  NOT NULL,     -- 'merchant_entity_id'
ref_value             TEXT  NOT NULL,     -- the unbound provider id under review
source_type           TEXT  NOT NULL,     -- issuing provider
provider_merchant_name TEXT,              -- provider's merchant_name (reviewer display + match basis)
candidate_merchant_id TEXT  NOT NULL,     -- existing merchant proposed as the binding target
confidence_score      DECIMAL(5, 4),
match_signals         TEXT,               -- JSON: which signal fired + value (per match_decisions convention)
status                TEXT  NOT NULL,     -- CHECK (status IN ('pending','accepted','rejected','reversed'))
decided_by            TEXT  NOT NULL,     -- CHECK (decided_by IN ('auto','user'))
match_reason          TEXT,
decided_at            TIMESTAMP NOT NULL,
reversed_at           TIMESTAMP,
reversed_by           TEXT
```

- **Resolving a decision** (Decision 6): **accept(target=candidate)** writes the
  `merchant_links` binding `ref_value → candidate` (`decided_by='user'`) and marks
  the decision `accepted`; sibling decisions for the same `ref_value` auto-
  `reject`. **reject / mint-new** records the declined pairing (so the resolver
  won't re-propose it) and the resolver mints a new merchant for the id on its
  next pass. **undo** sets `reversed`.

## Decision 3 — Resolution ladder (adopt-or-mint)

The merchant resolver runs at categorization time (Decision 4), mirroring
`AccountResolver.resolve`'s blocking → score → accept/review/mint. For a
transaction carrying a provider `merchant_entity_id`:

| Rung | Condition | Action | Visibility |
|---|---|---|---|
| 1 | provider id already in `merchant_links` (`accepted`) | **adopt** that `merchant_id` (skip name matching) | silent — near-certain |
| 2 | unbound; provider `merchant_name` **exact / exemplar** match to an existing merchant | **auto-bind** the id → that merchant, adopt it | silent — near-certain |
| 3 | unbound; **fuzzy** (`contains`/`regex`) or **multiple** candidate merchants | **propose** — one `pending` `merchant_link_decisions` row per candidate; **do not** bind, **do not** mint | **surfaced** for review |
| 4 | unbound; no candidate | **mint** a new merchant (`canonical_name` = provider `merchant_name`, `created_by='plaid'`), bind the id | silent — novel, safe |

- Rung 1 is the dedup payoff: the *first* transaction with id `E` resolves/mints a
  merchant and binds `E`; every later transaction with `E` — **even with different
  description text** — hits rung 1 and lands on the same merchant. No conflict
  risk on bind: we only reach rungs 2–4 because rung 1 missed, so `E` is provably
  unbound.
- Rung 3 **never blocks categorization.** The transaction still categorizes via
  the normal name/LLM path (rung-3 only holds the durable *binding* for review),
  exactly as a pending account-merge proposal never orphans a transaction.
- Rung 4 makes Plaid a **merchant-identity source**, coherent with it being an
  account-identity source and with the existing `created_by='plaid'` enum value.

This **subsumes** the existing exemplar accumulator for id-bearing transactions:
the accumulator (`orchestrator.py:372`, `created_by='ai'`) still creates merchants
for transactions **without** a provider id (OFX/CSV, or Plaid rows Plaid couldn't
identify). Rungs run first and set `merchant_id`, gating the accumulator off for
id-bearing rows — the two paths cover disjoint inputs, no duplication.

## Decision 4 — Resolve at categorization time; the provider id never enters `core.fct_transactions`

`merchant_id` is assigned today inside the categorization orchestrator (→
`app.transaction_categories.merchant_id`); `core.fct_transactions` joins
`core.dim_merchants` off that FK. The id resolver slots in as a **pre-step
(rung 0) before name matching** in the orchestrator's merchant lookup — keeping
**one** merchant-assignment path. (Resolving at transform time, like
`AccountResolver`, would create a *second* merchant-assignment path beside
categorization — the coherence violation we avoid; the account precedent informs
the *concept*, not the *location*.)

**The provider id stays out of `core`.** Consumers read canonical `merchant_id` /
`merchant_name` from core, and call `merchant_links` if they ever need the
provider id — a provider value has no place in the canonical fact view. The id
lives in exactly two homes: `raw`/`prep` (provider input) and `app.merchant_links`
(the binding).

The resolver reads the per-transaction provider id from the **prep resolution
layer**, where provider values already live and where the gold `transaction_id`
is assigned (`prep.int_transactions__matched` mints the SHA-256 gold key, so
Plaid's native `transaction_id` and the gold key differ; the resolver writes
against the gold key). Implementation choice (plan-time): carry
`merchant_entity_id` one hop to `prep.int_transactions__merged` riding the same
`ARG_MIN`-by-priority merge as `merchant_name`/`location_*` — **stopping at prep,
never `core.fct_transactions`** — vs. a join back through `matched`. The carry is
preferred for coherence (it is "just another source field" through the union),
and the union is positional `SELECT *`, so the field is added to all four source
CTEs (NULL for ofx/manual/tabular).

## Decision 5 — Magic stays visible: silence calibrated to certainty

Per [`design-principles.md`](../../.claude/rules/design-principles.md) "Magic
stays visible." Silent auto-action is allowed **only** on a near-certain signal:

- **Rung 1 (exact id)** and **rung 4 (novel id, new merchant)** — silent. An
  exact provider-id match is the strongest possible signal; a brand-new id minting
  a brand-new merchant carries no error risk. A wrong merchant is cheap and
  self-evident to fix (it shows in the categorization result, re-bindable) —
  unlike a silent *account* merge.
- **Rung 2 (exact / exemplar name match)** — silent, because the name match is
  itself exact, consistent with how the matcher already binds merchant identity on
  an exact match.
- **Rung 3 (fuzzy / ambiguous)** — **never silent.** A `contains`/`regex` match or
  multiple candidates would attach the id to possibly the wrong merchant, so it is
  always surfaced to the review queue. This mirrors accounts auto-adopting only on
  strong signals and routing weak ones to review.

## Decision 6 — Surfaces: `merchants_links_*` + the top-level `review` aggregator

Mirrors `accounts_links_*` (`account-identity-resolution.md` Decision 5). The
object reviewed is "a proposed merchant link," so it lives under the `merchants`
noun. CLI + MCP for parity (functional, not nominal).

| Operation | CLI | MCP |
|---|---|---|
| List pending link proposals (grouped by provider id) | `merchants links pending` | `merchants_links_pending` |
| Resolve one — bind to a candidate, or mint **new** | `merchants links set <id> --into <merchant_id>` / `--new` | `merchants_links_set(decision_id, target_merchant_id=…\|None)` |
| Reverse a prior decision | `merchants links undo <id>` | (CLI-only, matching `matches undo` / `accounts links undo`) — **deferred to M1L** (audit-undo consumer) |
| Decision history | `merchants links history` | `merchants_links_history` |
| Run resolution / backfill over unbound ids | `merchants links run` | `merchants_links_run` |

- **`…set(decision_id, target_merchant_id=Y)`** binds the id to merchant `Y`
  (Y must equal the decision's own `candidate_merchant_id` — a confirming safety
  check, consistent with the account-links twin); auto-rejects siblings.
  `target_merchant_id=None` mints a new merchant for the id. Envelope,
  sensitivity tier, and `actions[]` per [`mcp.md`](../../.claude/rules/mcp.md).
- **Inline discovery.** Sync / categorization results report *"N merchant-link(s)
  need review"* and point at the queue — the least-astonishing discovery path.
- **Aggregate into the top-level `review`.** `ReviewService` gains a
  `merchant_links_pending` count so the domain-neutral `review` sweep (CLI
  `moneybin review`, MCP `review`) can't silently miss the merchant-link backlog.

## Decision 7 — Backfill + re-resolution: harvest, then forward; never flood the queue

After a `sync pull --force` repopulates `merchant_entity_id` on raw and a refresh
carries it to the gold grain, **backfill by harvesting existing categorizations
— do not re-run the fuzzy ladder over history** (that would flood the review
queue).

- **Phase A — harvest (zero review).** For every Plaid transaction with *both* a
  `merchant_entity_id` **and** an already-assigned `merchant_id`, record the
  binding `id → merchant_id` (`decided_by='system'`). This records *established
  facts* (the user's existing assignments are ground truth), generating **no**
  proposals, and embodies Decision 1: the id attaches to *our* existing merchants.
- **Conflicts only → bounded review.** If one provider id maps to **≥2** distinct
  merchants across history (name matching had split one merchant), surface *that*
  as a review item. Bounded by the count of *conflicting ids*, not transactions.
- **Phase B — forward ladder.** On the next `refresh_run`, transactions that
  previously had no merchant now resolve: most hit rung 1 (their id was bound in
  Phase A) → silent adopt; only genuinely-new ambiguous ids reach rung 3. Phase A
  having bound the common ids keeps Phase B's review volume small.
- **Precedence-safe.** Backfill writes *bindings*; it does **not** retro-rewrite
  categorizations. The resolver writes at the existing `plaid`/`auto` priority via
  `write_categorization`'s guard, so it can only fill merchant identity on rows
  that lacked one — never clobbering a `user`/`rule`/`ai` categorization.
- **Idempotent.** `merchant_links`'s `(source_type, ref_kind, ref_value)`
  uniqueness makes re-harvesting a no-op; the harvest is safe to fold into the
  resolver's first pass after `merchant_entity_id` is present and re-run freely.

## Decision 8 — Category seam: Plaid is authoritative for "who," not "what"

A Plaid-minted merchant (rung 4) starts with `category_id = NULL`. This mirrors
Plaid's own model: every Plaid category field is `personal_finance_category.*`
(primary / detailed / confidence), attached **per transaction**, never to the
merchant entity — Plaid hands a merchant only `merchant_entity_id` +
`merchant_name`. So a category-less Plaid merchant is not a gap we introduce.

Tier-2a establishes merchant **identity**; merchant **category** stays the job of
the LLM / rules / **Tier-2b** (which consumes the txn-level PFC
`category_detailed`/`confidence`). Our merchant default-category remains a
*learned* optimization, set over time by user/accumulator/Tier-2b — never
required at mint.

## Privacy

`merchant_entity_id` is an opaque provider id, never an account number — so it is
`RECORD_ID` (Tier LOW), **not** the `ACCOUNT_IDENTIFIER` exception
`account_links.ref_value` carries. Add to `src/moneybin/privacy/taxonomy.py` and
reconcile `test_classification_registry_coverage.py`:

- `("app", "merchant_links")`: `link_id`, `merchant_id`, `ref_value`,
  `source_type` → `RECORD_ID`.
- `("app", "merchant_link_decisions")`: `decision_id`, `ref_value`,
  `candidate_merchant_id`, `source_type` → `RECORD_ID`;
  **`provider_merchant_name` → `MERCHANT_NAME`** (it is a merchant name — medium
  tier, not a bare id).

## Observability

Per [`observability.md`](observability.md) and `metrics/registry.py`, mirroring
the `ACCOUNT_LINK_*` family:

- `MERCHANT_LINK_REVIEW_PENDING` (gauge) — distinct provider ids with `pending`
  decisions; refreshed at the resolver's propose pass and on `merchants_links_set`
  accept/reject (kept honest in both directions, like
  `refresh_account_link_pending_gauge`).
- `MERCHANT_RESOLUTION_OUTCOME_TOTAL` (counter, label `outcome ∈
  {adopted, auto_bound, proposed, minted}`) — one increment per resolved
  transaction, so the ladder's behavior is observable.
- Add an `entity_id` outcome label to the existing
  `CATEGORIZE_MATCH_OUTCOME_TOTAL` for rung-1 hits.

## Testing

- **Unit** — the resolver ladder (each rung), N:1 binding, conflict detection in
  harvest, precedence-safety (resolver write cannot clobber a higher-priority
  categorization), repo audit-pairing (Invariant 10).
- **Migration** — `merchant_links` / `merchant_link_decisions` DDL against
  pre-populated `user_merchants` + `transaction_categories` fixtures (≥3 rows; the
  harvest reads them). Pure-additive tables, but the harvest step touches existing
  data.
- **Scenario** (`make test-scenarios`) — a Plaid fixture where the same
  `merchant_entity_id` arrives with two different descriptions must collapse to one
  `merchant_id`; a fuzzy case must land in the review queue, not auto-bind. Update
  the hardcoded column-list stubs the `merchant_entity_id` carry touches.
- **E2E** — `merchants links pending/set/run` CLI + the MCP equivalents.
- New test files included in per-phase `pyright` (tests are in strict scope).

## Phased implementation outline

1. **Schema + repos** — `app.merchant_links`, `app.merchant_link_decisions`,
   `MerchantLinksRepo`, `MerchantLinkDecisionsRepo` (audit-paired); taxonomy +
   coverage test; `TableRef` constants.
2. **Provider id to the resolution layer** — carry `merchant_entity_id` to
   `prep.int_transactions__merged` (all four union CTEs); update the orchestrator's
   uncategorized fetch.
3. **Resolver ladder** — rung 0 in the merchant lookup; adopt / auto-bind / mint;
   bind-write through `MerchantLinksRepo`.
4. **Review surface** — `merchants links *` CLI + `merchants_links_*` MCP;
   `ReviewService` aggregation; inline-discovery hints.
5. **Backfill** — idempotent harvest + conflict detection in the resolver's first
   pass.
6. **Metrics + docs** — registry entries; CHANGELOG / roadmap / features /
   capabilities-map / `INDEX.md`.

## What this unblocks

- Cross-connection / cross-source merchant dedup (the merchant analog of what M1S
  did for accounts).
- Merchant *merge* (re-pointing one merchant's links onto another) — the substrate
  ships here; the merge surface is a later increment.
- Stable-id merchant default-category learning + Tier-2b PFC categorization keyed
  off a durable merchant.

## Out of scope

- Merchant *category* assignment from Plaid (Tier-2b, `personal_finance_category`).
- A merchant *merge* surface (substrate only here).
- Multi-pattern / automated-discovery merchant evolution beyond id-based identity
  (the broader `merchant-entity-resolution` planned vision; this increment is its
  first concrete step).
- A second aggregator's provider id (the table is provider-neutral; wiring a new
  `source_type` is a future increment).
