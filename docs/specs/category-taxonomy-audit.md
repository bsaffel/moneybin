# Category Taxonomy Audit — comprehensive-but-manageable category content (axis 2)

> Last updated: 2026-07-04
> Status: draft
> Address: M1W (Ingestion Core)
> Type: Feature
> Owns: the category **taxonomy content** — which MoneyBin categories exist,
> their `class` (income/expense/transfer/debt), and their granularity relative
> to a provider's native codes. The *content* axis, distinct from the M1V
> *mapping* axis.
> Companions: [`category-source-map.md`](category-source-map.md) (M1V — the
> provider-code → category bridge this audit's categories are the target of),
> [`categorization-source-model.md`](categorization-source-model.md) (M1U — the
> provider-native categorizer that consumes the bridge),
> [`architecture-shared-primitives.md`](architecture-shared-primitives.md)
> (layer rules, seed vs app layering), `.claude/rules/database.md` (seed
> content, migration realism, column comments).
> Depends on: M1V (implemented) — the bridge made taxonomy expansion **purely
> additive**: adding, retiring, or reclassing a category is now a change to
> `seeds.categories` (+ the bridge rows that point at it), with no consumer
> query rewrite. This spec is the first pass to exercise that additivity.

## Purpose

Curate MoneyBin's category set into the **most comprehensive but manageable**
taxonomy — one that is coherent, accounting-aligned via the `class` column, and
free of redundant or orphaned categories. The M1V bridge fixed the *data model*
for provider-code → category mapping (axis 1) and reconciled the seed's Plaid
tags against the verified PFC taxonomy. It deliberately left the *content* (axis
2) untouched. This spec is that content pass.

It is a **full audit of all 108 seed categories** against an explicit set of
design principles, not a spot-fix of known issues. The known issues (below)
motivate the principles; the principles then drive a category-by-category
review whose row-by-row curation table is produced in the implementation plan.

## Scope note: internal classification only

This pass reconciles MoneyBin's **internal 4-class scheme**
(`income`/`expense`/`transfer`/`debt`) across every category. A formal
external-standard crosswalk (IRS Schedule C line items, a GAAP chart of
accounts) is **deferred to the `us_tax` reference package (M2M)**, its natural
home — it builds on the investment ledger and is tax-package territory, not an
M1 taxonomy pass. Reconciling `class` now is what makes that later crosswalk
purely additive.

## Problem — three findings from M1U/M1V

The audit is motivated by three concrete findings surfaced while building the
categorizer and the bridge. Note that **none is a correctness hole** — every
Plaid transaction already resolves to at least a primary-level category (all 16
Plaid primaries are mapped in the seed). These are *quality* and *coherence*
gaps, which is why this is a curation pass, not a bug fix.

### 1. Redundant / orphan categories

The category dim contains near-duplicate categories for the same real-world
concept. The clearest is **two mortgage categories**:

- `LNP-MTG` — "Loan Payments / Mortgage", `class=debt`, fed by Plaid's
  `LOAN_PAYMENTS_MORTGAGE_PAYMENT` code (the canonical, Plaid-fed mortgage
  category).
- `HSG-MTG` — "Housing & Utilities / Mortgage", `class=expense`, fed by **no**
  Plaid code (an orphan reachable only via user rules/overrides).

Both framings are individually defensible — a mortgage payment is *partly*
principal repayment (debt) and *partly* interest (an expense) — but two
mortgage categories is ambiguous. The audit resolves this (direction decided in
the implementation plan, in the context of the full class reconciliation) and
sweeps the remaining 108 for other duplicates/orphans.

### 2. Coverage-gap granularity (not correctness)

**29 of Plaid's 104 detailed PFC codes have no MoneyBin category** (75 are
mapped). Because every one of the 16 **primary** codes *is* mapped, each of
these 29 still resolves to its primary's category — nothing falls through to
rules/AI. The gap is therefore purely a **granularity** decision: which of the
29 represents a spending distinction a user would want to budget or report on
separately, and so earns its own finer MoneyBin category, versus which
sensibly stays rolled up under its primary.

Rough shape of the 29 (final triage in the plan):

- **Genuinely-distinct spending categories** — candidates to add: e.g.
  `GENERAL_SERVICES_CHILDCARE` (Childcare), `MEDICAL_VETERINARY_SERVICES`
  (Veterinary), `ENTERTAINMENT_CASINOS_AND_GAMBLING` (Gambling),
  `PERSONAL_CARE_LAUNDRY_AND_DRY_CLEANING` (Laundry), several
  `GENERAL_MERCHANDISE_*` retail subtypes, `INCOME_RETIREMENT_PENSION` /
  `INCOME_UNEMPLOYMENT` (income subtypes).
- **Roll-up (stay at primary)** — the 11 `TRANSFER_IN_*` / `TRANSFER_OUT_*`
  detailed subtypes currently roll up to `TRN`. Finer transfer granularity
  (e.g. distinguishing investment/retirement transfers) matters mostly for the
  investment ledger (M1J, off the current path) — kept rolled up for now, with
  the seam left open.
- **Low-value / niche** — e.g. fee subtypes (`BANK_FEES_*`),
  `LOAN_PAYMENTS_OTHER_PAYMENT` — likely roll up.

### 3. `class` reconciliation

`class` was added by M1V (default `expense` for user categories; assigned at
seed-curation time). It has not been audited end-to-end. The audit assigns each
category's `class` by its accounting nature and fixes any mistags, so that the
`class` column can reliably drive income-statement separation,
transfer-exclusion from spend reporting, and the future tax crosswalk.

## Design principles

The audit judges every category against these. They are the spec's durable
contribution; the row-by-row decisions apply them.

1. **Granularity — earn the split.** A provider's detailed code earns its own
   MoneyBin category only when it is a spending distinction a typical user would
   budget or report on separately. Otherwise it rolls up to its primary's
   category. MoneyBin-finer subcategories a provider cannot distinguish are
   allowed only when a *different* signal (merchant, rule, another provider) can
   populate them — never as an unfillable category. Applies to the 29 unmapped
   codes and is re-checked across all 108.

2. **Class by accounting nature.** Every category carries exactly one `class`:
   - `income` — earned or received inflow (wages, interest income, refunds).
   - `expense` — consumption of goods/services (the default for spend).
   - `transfer` — movement between the user's own accounts; no P&L impact;
     excluded from spend/income reporting.
   - `debt` — principal borrowing or repayment (loan principal, credit-card
     paydown).
   - **Interest paid is an `expense`; principal is `debt`.** Where a provider
     lumps them (e.g. a single mortgage-payment code), the category's `class`
     reflects the dominant/servicing intent, documented per category.

3. **No redundant categories.** One category per real spending concept. A
   second category for the same concept is justified only by a *documented*
   user-facing distinction (e.g. two accounting treatments a user deliberately
   chooses between) — otherwise it is retired. Orphan categories (no provider
   feed and no rule/override rationale) are retired or given a feed.

4. **Provider-neutral.** MoneyBin's categories are its own. A provider's
   taxonomy (Plaid's PFC today) is **one input** to the granularity decision,
   never the structure. The category set must read coherently to a user who has
   never heard of Plaid, and absorb a second aggregator (MX/SimpleFIN) without
   restructuring.

## Validation hardening

The M1V seed-validation test
(`tests/moneybin/test_seeds/test_category_source_map_seed.py`) already enforces,
against the checked-in `tests/moneybin/fixtures/plaid_pfc_v2_taxonomy.csv`
source-of-truth: every seeded code exists in the real PFC taxonomy;
`code_level` matches; ≤1 category per `(source_type, code)`; every mapped
`category_id` exists in `categories.csv`. This audit extends it with the
invariants the content pass introduces:

- **Every category carries a valid `class`** — non-null, ∈
  `{income, expense, transfer, debt}`.
- **Coverage report** — the set of provider detailed codes with no bridge row
  is enumerated (not merely counted), so an intentional roll-up is visibly
  distinguished from an accidental gap. This is the promised-but-deferred M1V
  "coverage query" landing with its first real consumer (this audit).
- **No orphan categories beyond an allowlist** — a category with no bridge feed
  must be justified (the allowlist documents the user-choice rationale, e.g. a
  deliberately rule-only category), so a future accidental orphan fails CI.

The taxonomy fixture (`plaid_pfc_v2_taxonomy.csv`) remains the source of truth
for valid codes; if the audit needs a newer PFC revision, the fixture is
refreshed and `source_taxonomy_version` bumped in the same change.

## Migration & mechanics

Category content lives in SQLMesh seeds (`sqlmesh/models/seeds/categories.csv`,
`category_source_map.csv`), not migration DDL. Because M1V already added the
`class` column and the bridge, this pass is **seed-content-only** for the common
case: add/retire/reclass rows in the two CSVs, refresh the resolved views. A
migration is required only if a category *retirement* must remap existing
`app.transaction_categories` rows that reference the retired `category_id` (a
category cannot be dropped out from under committed user categorizations) — that
remap, if any retirement needs it, is a forward migration authored in the plan
per `.claude/rules/database.md` migration realism (populated fixtures, ≥3 rows,
idempotent). Retirements with zero referencing rows need no migration.

## Scope

**In scope (M1W):**

- Full audit of all 108 seed categories against the four principles.
- Resolve the mortgage duplication and any other duplicates/orphans surfaced.
- Triage the 29 unmapped detailed codes (add the genuinely-distinct; roll up
  the rest); add the corresponding `seeds.category_source_map` rows for any new
  categories.
- End-to-end `class` reconciliation across all 108.
- Validation-test hardening (valid-class invariant, enumerated coverage report,
  orphan allowlist).
- Any retirement remap migration the audit's decisions require.
- Spec + `INDEX.md` + `docs/roadmap.md` + CHANGELOG (`Changed`) updates.

**Out of scope (deferred):**

- **External-standard crosswalk** (IRS Schedule C, chart of accounts) —
  `us_tax` package (M2M).
- **Finer transfer granularity** (splitting `TRANSFER_*` detaileds off `TRN`) —
  lands with the investment ledger (M1J) that actually consumes it.
- `parent_id` N-level category nesting; map-to-null suppression of a seed
  mapping (both remain M1V-deferred).
- The typed-payload `class` field on `CategoryRow` and provider-native
  candidates view — tracked under their own increments.

## Testing strategy

- **Seed-validation unit tests** — the hardened
  `test_category_source_map_seed.py` (valid class, coverage report, orphan
  allowlist) plus the existing four invariants, all green against the fixture.
- **Scenario tests** (`make test-scenarios`) — because this changes data shape
  (added/retired categories, reclassed rows), the categorization and
  spend/income-reporting scenarios re-run against synthetic ground truth; any
  scenario asserting a specific category/`class` is updated in lockstep.
- **Categorizer regression** — the M1U provider-native tests still pass: newly
  added categories resolve through the bridge; retired ones no longer appear.

## Open questions

- **Mortgage resolution direction** — keep `LNP-MTG` only (retire `HSG-MTG`) vs
  keep both with a documented user-choice distinction. Decided in the plan, in
  the context of the full `class` reconciliation.
- **Retirement policy** — for any category with committed
  `app.transaction_categories` rows, confirm the remap target (its primary's
  category) and author the migration; categories with zero references retire
  cleanly.
- **PFC revision** — confirm `plaid_pfc_v2` is still the current published
  revision at implementation, or refresh the fixture + bump
  `source_taxonomy_version`.
