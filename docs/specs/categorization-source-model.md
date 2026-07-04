# Categorization Source Model & Plaid PFC Categorizer

> Last updated: 2026-07-04
> Status: implemented
> Address: M1U (Ingestion Core)
> Type: Feature
> Owns: the category **source-model** contract — the `method` vs `source_type`
> split on `app.transaction_categories` and the provider-code → category reverse
> lookup. Ships the first provider-native categorizer (Plaid Personal Finance
> Category) on that contract.

> **Implemented 2026-07-04 (as built).** Provider-code → category resolution
> runs through the **M1V category-source bridge**
> (`core.bridge_category_source_map` — one canonical category per
> `(source_type, source_category_code)`, two-tier detailed→primary reverse
> lookup), which **superseded** the original `dim_categories.plaid_detailed`
> join described in Decision 4 (that column was hard-cut when the bridge
> landed — the `plaid_detailed` tag was many-to-one and produced
> non-deterministic reverse lookups). **Shipped:** Decisions 1–4 (the
> bridge-based `apply_plaid_categories`, wired last into `categorize_pending`)
> + 8 (metrics + the `plaid_unmapped` coverage stat). **Deferred to the
> immediate follow-up** (bundled with the axis-2 category-seed audit): the
> opt-in AI→`provider_native` **upgrade pass** (Decision 4) and the per-source
> **candidates view** (Decision 5) — both additive, neither on the critical
> path (new imports categorize automatically). Original design text is
> preserved below for rationale; mechanism deltas are flagged inline.
> Mirrors: the identity-MDM pattern from
> [`merchant-entity-resolution.md`](merchant-entity-resolution.md) (M1T) and
> [`account-identity-resolution.md`](account-identity-resolution.md) (M1S) —
> per-source assertions + a resolved canonical + a (deferred) review queue, lifted
> from the *identity* grain to the *category* grain.
> Depends on: Tier-1 (PR #283) — `category_detailed` (Plaid PFC `detailed`) +
> `category_confidence` (`confidence_level`) captured into `raw.plaid_transactions`
> + `prep.stg_plaid__transactions`, backfillable via `moneybin sync pull --force`.
> Builds on: [`categorization-matching-mechanics.md`](categorization-matching-mechanics.md)
> (the `categorized_by` source-priority ladder and `categorize_pending` auto-apply)
> and [`merchant-entity-resolution.md`](merchant-entity-resolution.md) (Tier-2a
> merchant identity — Decision 8 left category to "the LLM / rules / Tier-2b").
> Unblocks: multi-aggregator category survivorship, a category conflict-review
> surface, and merchant-scoped categorization rules — all designed here, built as
> later M1U increments (Decision 7).

## One-line goal

Turn category assignment from a **single winner-take-all golden record** into a
**source-aware model** — so a provider's native categorization (Plaid PFC today,
MX/SimpleFIN later) is one clearly-labelled *source* among many, adjudicated by a
precedence policy that is honest about where each opinion came from — and ship the
Plaid PFC categorizer as the first source on that model. Activate Plaid's dormant
`plaid_detailed` seed to assign a real `category_id`, confidence-gated, at its
correct method priority (`provider_native`, priority 6).

## Problem statement

Category assignment today is a **single-golden-record-with-priority-ladder**, and
three properties of it break the moment a serious user's data disagrees with a
single provider:

1. **One row per transaction, no candidates.** `app.transaction_categories` has
   `PRIMARY KEY (transaction_id)` — exactly one category per transaction. Once a
   higher-priority source writes, the loser's opinion is gone from the app layer
   (it survives only in `raw`). There is no place to say "Plaid said Coffee, MX
   said Restaurants."
2. **`categorized_by` conflates *method* and *source*.** The value `'plaid'` means
   both "provider-native categorization" (a *method*) and "the Plaid aggregator"
   (a *source*). A second aggregator has nowhere to go but a new priority integer
   (`'mx': 6.5`), which does not scale and encodes source-trust as precedence.
3. **Merchant defaults record the method, not their provenance (known,
   accepted, deferred).** `apply_merchant_categories` (`orchestrator.py`) reads a
   merchant's stored `category_id` (`app.user_merchants`) and writes it as
   `categorized_by='rule'` (priority 2) — regardless of whether that merchant
   category was set by the **user** or first-touch-guessed by the **LLM**
   (`created_by='ai'`). So a single AI guess about one transaction can become a
   merchant-wide default at priority 2. This spec deliberately does **not** change
   that. `categorized_by` records the *method* that applied the category — an
   invariant the auto-rule override-detection query and `delete_by_rule` both
   depend on — and an attempt to demote merchant matches to their authoring
   provenance was built and then reverted (see Decision 3). Making
   merchant-default provenance precise is a **deferred follow-up** that would have
   to update every reader of `categorized_by` in lockstep.

These map directly onto the three questions this spec must answer:

- **Two aggregators disagree on a category** → needs per-source *lineage* plus a
  survivorship policy richer than one integer.
- **The user needs to be more precise than the aggregator** (e.g. *Amazon* is
  Shopping, but *Amazon AWS* is a business expense) → belongs to **rules**, not
  merchant metadata; a merchant is one entity and must not be forced to carry
  sub-transaction precision on its single `category_id`.
- **General MDM** (golden record, source lineage, conflict review) → *identity*
  already has this (`merchant_links` + `merchant_link_decisions` + a resolved
  canonical); *category* has none of it.

**Non-goal / what stays true:** the consumer contract — **one resolved category
per transaction** on `app.transaction_categories` — does not change. Every
addition here is either a new column, a derived view, or a later additive table.
No consumer query breaks.

## Prior art

- **Identity MDM — [`merchant-entity-resolution.md`](merchant-entity-resolution.md)
  (M1T) and [`account-identity-resolution.md`](account-identity-resolution.md)
  (M1S).** Both solve "many sources assert about one entity" with a per-source
  binding table + a review queue + a resolved canonical. This spec lifts the same
  shape to category: per-source candidates (derived from `raw` for now) + a
  resolved golden record + a deferred review queue.
- **The precedence ladder — [`categorization-matching-mechanics.md`](categorization-matching-mechanics.md).**
  Establishes `categorized_by` source-priority enforcement on write
  (`transaction_categories_repo.py` `upsert_guarded`) and `categorize_pending`
  auto-apply. This spec refines what `categorized_by` *means* (the method /
  `source_type` split) and extends the `categorize_pending` scan to re-check
  `provider_native` rows, without changing the guard mechanism.
- **Tier-2a Decision 8.** Merchant identity was shipped deliberately
  category-free: *"A category-less Plaid merchant is not a gap we introduce."*
  This spec is the sanctioned home for merchant category assignment it pointed to.
- **Auto-rules v2 — [`categorization-auto-rules.md`](categorization-auto-rules.md).**
  Already anticipates sub-merchant precision by amount (*"Starbucks $5 = Coffee,
  Starbucks $25 = Food & Drink… v1 surfaces the conflict; v2 resolves it"*). The
  merchant-scoped/richer-condition rules in Decision 7 extend that line.

## Decision 1 — Split *source* from *method* on `app.transaction_categories`

`categorized_by` is redefined to carry the **method** only; a new `source_type`
column carries the **origin aggregator**. This is essentially free right now:
**no `categorized_by='plaid'` row has ever been written** (the provider-native
method is dormant), so there is no data to migrate and the reserved `'plaid'`
value is renamed before it is ever used.

```sql
-- app.transaction_categories (delta)
-- categorized_by: METHOD ONLY. 'plaid' -> 'provider_native'.
--   CHECK (categorized_by IN
--     ('user','rule','auto_rule','migration','ml','provider_native','ai'))
source_type  TEXT  NOT NULL DEFAULT 'internal'
             -- origin aggregator: 'plaid' | 'mx' | 'simplefin' | ...
             -- 'internal' for user/rule/auto_rule/migration/ml/ai methods
```

- `SOURCE_PRIORITY` (`services/categorization/_shared.py`) key `"plaid"` becomes
  `"provider_native"` at the same priority (6). The `CategorizedBy` `Literal` and
  the repo's generated `CASE` ladder update in lockstep.
- Migration adds `source_type` and backfills every existing row to `'internal'`
  (all existing rows use internal methods). Provider-native writes set it to the
  aggregator (`'plaid'`).
- **Plaid PFC = `method=provider_native, source_type=plaid`.** A future MX PFC =
  `provider_native, source_type=mx` — **no new priority integer, zero schema
  change** (mirrors how `merchant_links.source_type` keeps identity
  provider-neutral).

## Decision 2 — Precedence stays a *method* ladder; source-trust is deferred, not encoded as priority

Precedence is decided by `categorized_by` (method), unchanged in mechanism:

```
user 1 > rule 2 > auto_rule 3 > migration 4 > ml 5 > provider_native 6 > ai 7
```

When two `provider_native` sources disagree — only possible once a **second**
aggregator ingests categories, which is not the case today — the tie breaks by
**confidence, then a source-trust order**, using the `confidence` and
`source_type` columns. That survivorship policy is **designed here but not built**
(Decision 7); with one aggregator it would be dead code. The columns that make it
possible (`source_type`, numeric `confidence`) land now so the later policy is
purely additive.

### Rules and merchants override `provider_native` across runs

The precedence ladder must hold *across* `categorize_pending` runs, not only
within a single write — otherwise a rule or merchant authored **after** the Plaid
import could never displace the `provider_native` categorization Plaid already
wrote. So `categorize_pending`'s scan (`fetch_uncategorized_rows`) treats a row as
*pending* when it is uncategorized **OR** already stamped
`categorized_by='provider_native'`. The deterministic engines (rule, auto_rule,
merchant) therefore re-scan Plaid-categorized rows on every run, and a rule or
merchant added after the import overrides the `provider_native` categorization on
the next run. The write-time guard (`excluded_priority <= existing_priority`)
still does the actual precedence work — it permits rule/merchant (2) over
`provider_native` (6) and rejects the reverse — so the scan change only lets those
engines *see* the row; it does not weaken the guard. `ai` rows stay **excluded**
from the re-scan: an `ai` commit is user-directed, so committed categorizations
remain final.

## Decision 3 — Merchant matches stamp the `rule` method (provenance demotion reverted)

`apply_merchant_categories` stamps a flat `categorized_by='rule'` (priority 2) on
every merchant match — the **method** that applied the category, exactly as on
`origin/main`, not the merchant's authoring provenance. `merchant_id` is still
written on the row for lineage, so "this came via the merchant catalog" stays
queryable.

An earlier slice of this increment shipped a **provenance-aware** variant
(`MERCHANT_PROVENANCE_TO_METHOD` + a `Merchant.created_by` field): a merchant match
was stamped with the *authority of how the merchant's category was set*
(`user`/`rule`/`migration`/`ai`), so an `ai`-derived default landed at priority 7
and would "lose to a fresh `provider_native` read." **It was reverted**, because
the demotion was **inert** and, worse, caused three real defects:

- **Inert.** `apply_plaid_categories` only writes `WHERE tc.transaction_id IS NULL`,
  so `provider_native` never overrides an already-categorized row. The single
  intended benefit — an AI merchant default losing to `provider_native` — could
  never fire.
- **Defect 1 — corrupted auto-rule health.** Machine-applied merchant matches
  stamped `ai`/`user` leaked into the auto-rule override-detection query, which
  counts `categorized_by IN ('user','ai')` as *human* corrections. Working
  auto-rules were silently auto-deactivated.
- **Defect 2 — wrong semantics.** It recorded the categorization *method* as a
  merchant's authoring provenance, breaking the invariant that `categorized_by` =
  the method — which the override-detection query and `delete_by_rule` both rely on.
- **Defect 3 — lost precedence protection.** It let a later `ai` commit overwrite a
  merchant default that had previously been precedence-protected.

So merchant matches went back to `categorized_by='rule'`, restoring the invariant
that `categorized_by` = the METHOD. Merchant-default provenance precision is
**deferred** to a possible follow-up that would update **all** readers of
`categorized_by` in lockstep.

## Decision 4 — The Plaid PFC categorizer (`apply_plaid_categories`)

A new categorizer in `services/categorization/`, wired into `categorize_pending`.

**Reads:** `prep.stg_plaid__transactions.category_detailed` +
`category_confidence` for uncategorized transactions.
**Joins:** `core.dim_categories` on `plaid_detailed = category_detailed` → yields
`category_id`, `category`, `subcategory` (the seed
`sqlmesh/models/seeds/categories.csv` already carries subcategory).

> **As built:** the `plaid_detailed` join was replaced by a reverse lookup
> against `core.bridge_category_source_map` (M1V) keyed on **both** the
> detailed and primary PFC codes (`source_category_code IN (category_detailed,
> plaid_category)`), with `QUALIFY ROW_NUMBER() … ORDER BY (code_level =
> 'detailed') DESC = 1` so each transaction resolves to exactly one category
> (detailed preferred, primary fallback) — deterministic where the old
> many-to-one tag was not. `dim_categories` is joined only to resolve the
> bridge's `category_id` → display `category`/`subcategory` for the write.
>
> **Ship-blocking correction (2026-07-04):** the read source above,
> `prep.stg_plaid__transactions`, was wrong — that view is keyed by the
> **native Plaid transaction_id**, while `app.transaction_categories` (and
> every join onto it, including `core.fct_transactions`'s own categorization
> join) is keyed by the **gold** `transaction_id` that
> `int_transactions__matched` mints. Writing native-id categorizations left
> them permanently orphaned — a silent no-op for the whole feature. Fixed by
> carrying `category_detailed`/`plaid_category`/`category_confidence` through
> `int_transactions__unioned` → `int_transactions__matched` →
> `int_transactions__merged` (ARG_MIN survivorship, mirroring the
> `merchant_entity_id` precedent) and reading `prep.int_transactions__merged`
> instead. See `tests/moneybin/test_categorize_plaid_e2e.py` for the
> full-pipeline regression proof.
**Writes** `app.transaction_categories` via the guarded repo:
`method=provider_native`, `source_type=plaid`, mapped numeric `confidence`,
`category_id` + `subcategory`, and `merchant_id` when the row resolved to one.

```mermaid
flowchart LR
    U[uncategorized rows] --> R[apply_rules<br/>method=rule 2]
    R --> M[apply_merchant_categories<br/>method=rule 2]
    M --> P[apply_plaid_categories<br/>method=provider_native 6<br/>source_type=plaid<br/>gate: confidence >= MEDIUM]
    P --> A[apply_ai<br/>method=ai 7]
    A --> G[(app.transaction_categories<br/>one resolved row / txn)]
    R --> G
    M --> G
    P --> G
```

**Run order — last of the deterministic categorizers, just before AI.** Final
state is identical regardless of order (the precedence guard decides the winner),
so order is chosen for *cost*: Plaid deterministically clears the long tail so the
expensive LLM only ever sees genuinely ambiguous rows. (This intentionally differs
from an earlier "between rules and merchant" placement suggestion — with merchant
matches stamped `rule` (priority 2) and the write-time guard deciding the winner,
run order is a cost choice, not a correctness one.)

**Confidence map + gate.** Plaid emits `VERY_HIGH | HIGH | MEDIUM | LOW | UNKNOWN`.
Map to `DECIMAL(3,2)`: `VERY_HIGH→0.99, HIGH→0.90, MEDIUM→0.70, LOW→0.40,
UNKNOWN→NULL`. **Gate: assign at `MEDIUM` and above; skip `LOW`/`UNKNOWN`**, which
fall through to AI. Rationale: a provider that reports its own low confidence
should not produce an assertion even at priority 6. The numeric values are a
starting point; the **gate at ≥ MEDIUM** is the decision.

**Backfill / re-categorization.** The normal `categorize_pending` path stays
**uncategorized-only** (idempotent, unchanged). A separate **opt-in upgrade pass**
(a `categorize` flag) lets `provider_native` (6) re-categorize existing
**AI-categorized** (7) rows after the Tier-1 backfill lands `category_detailed`
onto historical rows — guard-respecting, so it never touches anything at priority
≤ 6. Explicit action = magic stays visible; no silent churn on every run.

> **Deferred (as built):** the opt-in upgrade pass is **not** in M1U's first
> slice — it lands in the immediate follow-up, together with its CLI/MCP
> surfacing (a surface-design decision) and the axis-2 category-seed audit.

**Old primary-as-text passthrough.** The existing `plaid_category → category`
fallback text in `prep.int_transactions__unioned` is **kept** as a display
fallback for skipped-`LOW` rows; flagged as low-priority cleanup once
provider-native coverage is proven. Minor.

## Decision 5 — Per-source lineage as a derived `core` view, not mutable state

> **Deferred (as built):** the `fct_transaction_category_candidates` view is
> **not** built in M1U's first slice. With only Plaid as a provider-native
> source today it would be thin, and the M1V bridge (the code→category
> mapping) plus the `source_type` column on `app.transaction_categories`
> already provide the lineage foundation. It lands in the immediate follow-up,
> and becomes genuinely useful when a second aggregator arrives. Design
> rationale preserved below.

"What did each source say?" is answered by a read-only view, **not** a new mutable
table — because `raw` already is the per-source assertion store (each aggregator's
native category lives immutably in its own raw table; internal method decisions
are single-authored and already captured by `categorized_by` + `rule_id`).

```sql
-- core.fct_transaction_category_candidates (VIEW)
-- one row per (transaction, source) native category candidate
transaction_id, source_type, native_raw, category_id, confidence, is_winner
```

v1 surfaces Plaid only (derived from `stg_plaid__transactions` +
`dim_categories`); `is_winner` compares against the resolved
`transaction_categories` row. A second aggregator plugs in as another `UNION`
branch — the exact seam the future conflict-review queue reads from. Near-free (a
view), and it is where "robust enough for multi-source" becomes concrete today.

## Decision 6 — Sub-merchant precision lives in rules, not merchant metadata

A merchant is one entity; its `category_id` is a **coarse default**, not a
per-transaction resolver. Precision beyond the merchant (Amazon → Shopping, but
Amazon **AWS** → Business) is the job of `app.categorization_rules` at priority 2,
which already outranks `provider_native`. Two notes:

- **Already possible today:** a global rule `description contains 'AWS' → Business`
  fires at priority 2 and beats Plaid — no new capability required for
  distinctive-substring cases.
- **The real gaps** (deferred, Decision 7): rules cannot be scoped to a merchant
  (`categorization_rules` has no `merchant_id`), and the richer amount/scope
  operators from `categorization-auto-rules.md` v2 are unbuilt. Those matter when
  the discriminator is **not** in the description (same text, differing by amount
  or account).

## Decision 7 — Designed now, built later (additive M1U increments)

Each deferred layer is provably **additive** — the golden-record contract (one row
per transaction) is unchanged whether lineage is a view or a table — so none of
this blocks shipping the Decision 1–6 slice. Registered, built when a real trigger
makes them earn their keep:

- **M1U.x — Category assertion store** (`app.transaction_category_assertions`,
  multi-row per txn; `transaction_categories` becomes derived survivorship). Built
  only if raw-as-source-of-truth proves insufficient for **internal** methods that
  need multi-candidate retention. Trigger: a second aggregator, or an internal
  need to keep rejected candidates.
- **M1U.y — Category conflict-review queue** (the magic-visible analog to
  `merchant_link_decisions`). Built when multi-source or confidence-gating actually
  produces reviewable disagreements. Trigger: a second provider-native source, or a
  decision to surface low-confidence conflicts.
- **M1U.z — Merchant-scoped + richer rules** (`rule.merchant_id`, amount/scope
  operators). Trigger: the works-today AWS-string case proves insufficient for a
  same-text discriminator.

## Decision 8 — Observability & the magic-stays-visible posture

**Metrics** (per [`observability.md`](observability.md), `metrics/registry.py`):
provider-native categorized count, skipped-by-confidence (`LOW`/`UNKNOWN`),
skipped-by-precedence (existing `CATEGORIZE_WRITE_SKIPPED_PRECEDENCE_TOTAL`),
surfaced in `categorization_stats` (`queries.py`).

**No confirm surface for this increment.** Per
[`design-principles.md`](../../.claude/rules/design-principles.md) ("calibrate
visibility to certainty; cheap, self-evident mistakes can lean automatic"), a
`provider_native` write at priority 6 with a MEDIUM+ gate: never overrides a
deliberate signal (user/rule/user-merchant all win), only fills the long tail, is
visible in the categorized result, is trivially overridable (a user fix → priority
1), and is non-destructive. Silent-but-visible is the right calibration. The
confirm/review surface is the deferred conflict-review queue (Decision 7),
activated when inferences actually get uncertain.

## Build scope of this increment (M1U)

**In (shipped):** Decisions 1–4 + 6 + 8 — the `source_type`/method split +
migration; the `provider_native` rename; merchant matches stamped with the `rule`
method (the provenance-aware variant was built and reverted — see Decision 3);
rule/merchant override of `provider_native` across runs (the `categorize_pending`
scan re-checks `provider_native` rows); `apply_plaid_categories`
(**bridge reverse-lookup**, confidence map + ≥MEDIUM gate, subcategory, run-order
last) wired into `categorize_pending`; metrics + the `plaid_unmapped` coverage stat.
**Deferred to the immediate follow-up:** the opt-in upgrade pass (Decision 4) and
the derived candidates view (Decision 5) — additive, off the critical path — bundled
with the axis-2 category-seed audit (seed mis-tag fixes + the ~29-code coverage gap).
**Out (designed, registered, additive):** the assertion store, the conflict-review
queue, and merchant-scoped/richer rules (Decision 7).

## Testing strategy

Per [`testing-scenario-comprehensive.md`](testing-scenario-comprehensive.md) — new
scenarios over synthetic ground truth (`make test-scenarios`), plus unit tests:

- **Precedence:** `provider_native` loses to user/rule/auto_rule/migration and to
  every merchant match (merchant matches stamp `rule`, priority 2); it beats only
  `ai`. Across runs: a rule or merchant authored after the Plaid import overrides a
  `provider_native` row on the next `categorize_pending`, while `ai` rows are not
  re-scanned.
- **Confidence gate:** `MEDIUM+` assigns; `LOW`/`UNKNOWN` fall through to AI.
- **Multi-category merchant:** a general merchant (Amazon-like) with per-transaction
  Plaid categories gets **per-row** categories; its merchant default stays NULL —
  proving the single `category_id` is not forced.
- **Backfill/upgrade:** normal `categorize_pending` scans uncategorized rows plus
  `provider_native` rows (so a later rule/merchant can override Plaid) and is
  idempotent when no higher-precedence rule/merchant applies; the deferred opt-in
  pass upgrades `ai → provider_native` and touches nothing at priority ≤ 6.
- **Lineage view:** `fct_transaction_category_candidates.is_winner` agrees with the
  resolved `transaction_categories` row.

## Open questions

- **Exact confidence numerics.** The `MEDIUM+` gate is decided; the specific
  `0.99/0.90/0.70` values are a starting point to tune against real Plaid data.
- **ADR?** This establishes a category-source-model pattern later increments
  inherit (raw-as-assertion-store over a mutable candidate table; source/method
  split; method-based precedence enforced across runs). It sits near the ADR bar in
  `design-principles.md`. Current call: capture rationale in this spec, no separate
  ADR (default "when in doubt, don't"); revisit if a contributor later proposes a
  mutable assertion store without this context.
