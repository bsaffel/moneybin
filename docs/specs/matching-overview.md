# Transaction Matching — Overview

> Last updated: 2026-05-02 — promoted to implemented (v1)
> Status: Implemented — both v1 pillars shipped (same-record dedup PR #43/#46, transfer detection PR #47). Umbrella doc for the transaction-matching initiative. Child specs listed in [Pillars](#the-three-pillars) are written separately.
> Companions: [`smart-import-overview.md`](smart-import-overview.md) (peer initiative), [`privacy-and-ai-trust.md`](privacy-and-ai-trust.md) (audit log shared), `CLAUDE.md` "Architecture: Data Layers"

## Purpose

Transaction matching is MoneyBin's entity-resolution infrastructure: cross-source deduplication, transfer pairing, and mastered-record merge rules. It is the mechanism that turns the raw/prep layers into a trustworthy core analytics layer. This doc fixes the vision, the data-model contract, the scope boundary, and the build order. Design and implementation details live in the child specs it points to.

## Vision

> **Every transaction appears exactly once in core, with a full trail back to every raw source. Matches resolve automatically when confident, and every decision is explainable and reversible.**

Four commitments:

1. **Uniqueness in core.** `core.fct_transactions` has a grain of one real-world transaction. If three sources report the same $45 coffee purchase, one gold row represents it.
2. **Provenance completeness.** For any gold transaction, the full list of raw rows that contributed to it is queryable — which source, which file, when extracted.
3. **Confidence-gated automation.** Deterministic matches (same source re-import, Plaid retro-mutation by ID) apply automatically. Fuzzy matches are scored; high-confidence merges are auto-applied and logged, medium-confidence merges queue for human review.
4. **Reversibility as a first-class property.** Every auto-merge stores enough pre-merge state that un-merging is exact and lossless. Every decision is logged with who/what decided, when, and why.

## Core as gold analytics layer

Transaction matching is what creates and maintains the gold quality in `core`. Without it, `core.fct_transactions` is a UNION ALL with latent duplicates. With it, core is a proper star schema that any consumer can trust.

### Layer progression

The existing `raw` → `prep` → `core` architecture maps naturally to a layered quality progression:

| Layer | Schema | Role | Quality guarantee |
|---|---|---|---|
| Raw | `raw` | Immutable ingestion records — every row ever loaded | Append-only, never modified, no dedup |
| Staging | `prep` | Cleaned, typed, within-source deduplicated views | One row per (`source_id`, `source_type`) pair |
| Core | `core` | Mastered, cross-source deduplicated, analytics-ready | **One row per real-world transaction**, regardless of how many sources reported it |

### Gold layer contract

- **Star-schema semantics.** `fct_transactions` is a proper fact table. `dim_accounts` is a proper dimension. Consumers — dashboards, ad-hoc SQL, MCP tools, CLI — query core directly. Core is the analytical API, not an internal implementation detail.
- **Analytics-first design.** Columns are chosen for query ergonomics, not app plumbing. Derived columns (`transaction_direction`, `transaction_year_month`, `amount_absolute`) already exist and this continues. Transfer flags, match provenance, and source counts should be queryable on the fact table or via star-schema joins — without requiring app-layer lookups.
- **Independently queryable.** A user can `SELECT * FROM core.fct_transactions WHERE ...` and trust the result without knowing about `app.*`, `raw.*`, or the matching pipeline. Core is self-contained for analytics.
- **Deterministic refresh.** Core is fully rebuildable from raw + match decisions at any time. No state lives only in core — it is derived, never authoritative for mutable data.

### Core artifacts matching introduces

| Table/view | Type | Grain | Purpose |
|---|---|---|---|
| `core.fct_transactions` (modified) | Fact | One real-world transaction | Gains: `is_transfer`, `transfer_pair_id`, `match_confidence`, `canonical_source_type`, `source_count` (number of raw rows that contributed to this gold record) |
| `meta.fct_transaction_provenance` | Fact | One (canonical_txn, contributing_raw_row) pair | Full lineage: which raw rows from which sources contributed to each gold record |
| `core.bridge_transfers` | Bridge | One transfer pair | Links two `fct_transactions` rows as a matched pair; carries direction, date offset, amount delta |

Match decisions themselves — user confirmations, rejections, auto-merge logs — live in `app.*` (user-authored, mutable state). The provenance output derived from those decisions lives in `core.*` (model-derived, analytics-ready, rebuilt on every SQLMesh run).

## The three pillars

| Pillar | Purpose | Default posture | Child spec |
|---|---|---|---|
| **A. Same-record dedup** | When multiple raw rows describe the same real-world transaction, resolve to one canonical gold record | Three tiers: ≥0.95 auto-merge, 0.70–0.95 review queue, <0.70 drop. Thresholds configurable in settings. | `matching-same-record-dedup.md` |
| **B. Transfer detection** | When two rows describe two sides of a money movement across accounts, link them with a transfer pair | v1: every fuzzy pair queues for review (no auto-merge). v2 (deferred): learned promotions for confirmed patterns. | `matching-transfer-detection.md` |
| **C. Golden-record merge rules** | When pillar A merges records, determine which source's fields win per-column | Per-field merge policy (e.g., prefer Plaid description, prefer user-set category, prefer earliest `transaction_date`) | Absorbed into `matching-same-record-dedup.md` (pillars A+C ship together) |

Pillars A and C are tightly coupled — you can't ship dedup without merge rules for the fields of the merged record. They share a build phase.

## Cross-cutting concerns

Not separate pillars, but every pillar must honor these. Detailed design lives in child specs.

### Provenance schema

`meta.fct_transaction_provenance` links every gold record to every contributing raw row. All merges, pair links, and un-matches write rows here. Provenance rows are never deleted, only superseded (a revoked match appends a reversal row, preserving history).

### Reversibility

Every auto-merge stores enough pre-merge state that un-merge is exact and lossless. The decision log records:

- `decided_by` — `auto` (system, above threshold), `user` (explicit confirmation), `system` (deterministic match)
- `decided_at` — timestamp
- `match_reason` — human-readable explanation of why the match was proposed (signals used, scores)
- `reversed_at` / `reversed_by` — populated when a match is revoked; NULL otherwise

Un-merge restores the previously separate gold rows and their individual provenance. Re-running the matcher after un-merge should re-propose the same match (not silently re-merge it) unless the user marks it as a permanent rejection.

### Match review UX

- **CLI** (v1, non-negotiable): `moneybin matches review` — shows pending matches one at a time: `[a]ccept / [r]eject / [s]kip / [q]uit`. `moneybin matches log` — shows recent decisions.
- **MCP** (Phase 2): `list_pending_matches`, `confirm_match`, `reject_match` tools. Lets the AI surface review items conversationally.
- **Match log for audit** (v1, non-negotiable): `app.match_decisions` records every auto-merge and user decision with reasoning. Foundation of the "reversible" promise.

## In scope

- Same-record cross-source dedup (OFX + CSV + Plaid + Smart Import describing the same txn)
- Same-record re-import dedup (same source loaded twice; trivial case of the general rule)
- Transfer detection, same institution (date-aligned, amount-exact)
- Transfer detection, cross-institution (date offset ±N days, amount exact or near)
- Plaid retro-mutation handling (deterministic by transaction_id — not fuzzy)
- Manual entry reconciliation (user-entered txn later discovered in a file import)
- Golden-record merge rules (per-field source-priority policy)
- Un-match / revoke (user splits an auto-merged pair)
- Match review UX (CLI v1, MCP Phase 2)
- One-time historical backfill (scan existing `fct_transactions` for latent duplicates at release)

## Out of scope

Explicitly deferred. Revisit per pillar as the initiative matures.

- **Cross-currency transfer detection** — requires FX conversion + fuzzy amount matching with FX tolerance. Deferred to the multi-currency initiative.
- **Same-record dedup for investment lots/holdings** — depends on investment-tracking spec. Reserved for later.
- **Same-record dedup for accounts** — `dim_accounts` already handles this via `account_id` partitioning. If fuzzy account matching is needed later, it's a small spec of its own.
- **Merchant normalization** — different concern (attribute normalization, not record identity). Owned by `app.merchants` / categorization pipeline.
- **Categorization** — happens after matching. Separate spec.

Two semantic non-goals:

1. **Matching does not modify raw.** Raw is immutable. Matching produces provenance records and match decisions; it never edits staging or raw tables. The user's "preserved for analysis" requirement is a hard constraint.
2. **Matching does not gate ingestion.** The matcher runs *after* rows land in staging. Ingestion always accepts the row; matching is a downstream resolution pass. A file is never rejected because it contains duplicates.

## Default automation posture

### Same-record dedup (pillar A)

Three-tier confidence model:

| Tier | Confidence | Behavior |
|---|---|---|
| High | ≥ 0.95 | Auto-merge; user sees result in import summary; match logged and reversible |
| Medium | 0.70 – 0.95 | Queued for review; not applied until user confirms via CLI or MCP |
| Low | < 0.70 | Dropped (logged for debugging only; not surfaced to user) |

Thresholds are configurable via Pydantic settings (`matching.high_confidence_threshold`, `matching.review_threshold`). Defaults are conservative.

### Transfer detection (pillar B)

v1: **always-review-fuzzy.** Any non-deterministic transfer pair proposal queues for user review. No auto-merging.

v2 (deferred): **learned promotions.** After the user confirms N matches of the same pattern (e.g., "checking → savings, same institution, same day, exact amount"), offer to promote that pattern to auto-merge. Promotions are themselves reversible.

## Default run model

**Hybrid: deterministic sync, fuzzy batch.**

- **Deterministic matches** — same-source re-import (identical `transaction_id`), Plaid retro-mutation (Plaid-provided `transaction_id`) — apply synchronously during the import that produced them. No queue, no review, because these are not decisions — they're mechanical identity matches.
- **Fuzzy matches** — cross-source dedup, transfer pairs — run automatically after import completes. Results go to the match log: auto-merges above threshold are applied and logged; medium-confidence proposals queue for review. Import output reads: *"Import complete: 1,240 rows. Matching: 3 auto-merged, 5 pending review. Run `moneybin matches review` when ready."*
- **Manual trigger** — `moneybin matches run` is always available for running the matcher on-demand, independent of import.

## Adjacent initiatives

Three sibling initiatives feed the matcher. Matching defines the provenance contract; siblings conform to it.

- **Smart Import** (`smart-import-overview.md`) — produces raw rows from CSV, TSV, Excel, Parquet, Feather, and (future) PDF. Every Smart Import pillar must output rows that conform to the provenance schema and `source_type` taxonomy defined here.
- **Plaid sync** (`sync-overview.md` / `sync-plaid.md`) — produces raw rows from Plaid API. Also the source of deterministic retro-mutation handling (Plaid provides stable `transaction_id`).
- **Manual entry** (pending) — produces raw rows from user input. Reconciliation of a manual entry with a later-imported file is a matching case, not a manual-entry case.
- **Multi-currency** (pending) — owns cross-currency transfer amount matching (FX-aware tolerance). This spec defers that case.

## Build order & rationale

1. **Provenance & audit schema** — foundational DDL. Defines the tables every pillar writes to (`app.match_decisions`, `meta.fct_transaction_provenance`, `core.bridge_transfers`). Written first as a schema PR or as part of pillar A's child spec.
2. **Pillars A + C** (`matching-same-record-dedup.md`) — ship together in one spec. Dedup without merge rules leaves the gold record undefined; merge rules without dedup have nothing to merge. This is the build phase that fixes the latent duplicate bug in `fct_transactions`.
3. **Pillar B** (`matching-transfer-detection.md`) — layers on once A/C are solid. Different semantics (link two records, don't collapse them), different review posture (always-review in v1).
4. **[Deferred] Learned promotions for transfer auto-merge** — Phase 2 enhancement. Not a v1 spec.

## Success criteria

- **No double-counting.** Spending totals computed over `core.fct_transactions` equal totals a user would compute manually after reconciling their statements. This is the headline metric.
- **Gold-layer contract.** Any consumer querying `core.fct_transactions` can trust that totals, counts, and aggregations reflect reality without awareness of the matching pipeline. Core is independently queryable.
- **Provenance completeness.** For any gold transaction, the full list of raw rows that contributed to it is queryable via `meta.fct_transaction_provenance`.
- **Reversibility guarantee.** Any auto-merge can be undone. After undo, the previously separate gold rows are restored and re-running the matcher re-proposes (not re-applies) the same match.
- **Transfer integrity.** Spending/income analytics in core exclude transfer pairs by default. Users can opt in to seeing them by joining through `core.bridge_transfers`.
- **Backfill correctness.** At release, a one-pass backfill identifies and resolves the latent duplicates currently in `fct_transactions` from today's UNION-without-dedup logic. No manual re-imports required.
- **Review queue manageability.** After initial backfill, a user importing a typical month of data sees a manageable review queue — the system auto-resolves high-confidence cases, not every match.

## Open questions

Cross-cutting decisions deferred to child specs or to resolve during implementation.

- **Architecture: SQL vs Python vs hybrid.** Where does the matching engine live? SQL-first (declarative SQLMesh models) works for deterministic matches but struggles with fuzzy string similarity. Python-first (matcher service writing to match tables) offers full power but adds a pipeline stage. Hybrid (SQL for candidate blocking, Python for scoring) is likely. Child-spec decision per pillar.
- **Canonical ID strategy.** Introduce a separate `canonical_transaction_id` (new synthetic key for the gold record) or overload the existing `transaction_id` with the "winning" source's ID? A new key is cleaner but changes every downstream query; overloading is simpler but conflates source identity with gold identity.
- **Signal set for fuzzy matching.** Which columns feed the similarity score, and with what weights? (account, date ±N days, amount exact/near, description fuzzy via TF-IDF or edit distance, merchant if available.) Per-pillar decision in A and B child specs.
- **Review queue persistence.** Does the queue live in `app.*` (user-authored state) or `core.*` (model-derived)? `app.*` is more natural for mutable user decisions but doesn't participate in SQLMesh refresh. Likely `app.*` for decisions, `core.*` for derived provenance.
- **Backfill UX at release.** One-shot migration on first upgrade (automatic, potentially slow), or explicit `moneybin matches backfill` command (user-triggered, predictable)?
- **Interaction with Smart Import pillar F.** AI-parsed transactions — should they enter matching with lower default confidence, or be treated the same as any other source?
- **Match metadata on the fact table.** Resolved: analytics-relevant columns (`is_transfer`, `transfer_pair_id`, `match_confidence`, `canonical_source_type`, `source_count`) go directly on `core.fct_transactions` for query ergonomics. Detailed match metadata (decision logs, match reasons, signal scores, reversal history) lives in supplemental tables (`app.match_decisions`, `meta.fct_transaction_provenance`). Child specs define the exact column list per table.
- **`source_type` taxonomy.** This spec owns the taxonomy. Renamed from `source_system` — `source_type` is neutral enough for both file formats and API/sync sources. Current values: `ofx` and `csv`. Smart tabular import adds format-specific values (`csv`, `tsv`, `excel`, `parquet`, `feather`, `pipe`) per `smart-import-tabular.md`. Plaid adds `plaid`. Future: `pdf_statement`, `pdf_ai_parsed`, `manual`. The canonical gold record carries `canonical_source_type` recording which source "won" the merge. See `.claude/rules/database.md` for the column naming rule.
- **`source_origin` — institution/connection scoping.** `source_origin` identifies the specific institution, connection, or profile that produced a row (e.g., `chase_credit`, `fidelity_brokerage`, a Plaid `item_id`). It scopes matching: two rows with the same `source_origin` and `source_type` are "within-source" candidates (Tier 2b — overlapping statements from the same bank), while rows with different `source_origin` or `source_type` values are "cross-source" candidates (Tier 3). Population logic is source-specific: tabular import derives it from `TabularProfile` names, OFX from institution identifiers in the file, Plaid from `item_id`. Child specs define the blocking criteria that use `source_origin`.
