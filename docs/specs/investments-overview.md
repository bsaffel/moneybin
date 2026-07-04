# Investments — Overview

> Last updated: 2026-07-04 — draft → ready reconciliation: HIFO promoted into the
> foundation child's v1 method set (LIFO stays demand-gated); lots-model
> SQL-vs-Python question resolved (Python SQLMesh model); foundation child
> Plaid-validated against the OpenAPI spec.
> Umbrella doc for the investments initiative (milestone M1J). Child specs listed
> in [The four pillars](#the-four-pillars) are written separately; the foundation
> child is [`investments-data-model.md`](investments-data-model.md).
> Status: ready
> Type: Umbrella
> Companions: [`asset-tracking.md`](asset-tracking.md) (the asset/investment dividing
> line), [`reports-net-worth.md`](reports-net-worth.md) (net worth integration target),
> [`matching-overview.md`](matching-overview.md) (peer entity-resolution initiative),
> [`architecture-shared-primitives.md`](architecture-shared-primitives.md) (layer + Invariant 8),
> [`extension-contracts.md`](extension-contracts.md) (`us_tax` package consumes core
> investment tables).

## Purpose

Investments is MoneyBin's largest competitive moat: a personal-finance platform
that produces **cost-basis output reconcilable against a real 1099-B for a full
tax year**, computed independently from the user's own transaction ledger rather
than trusted blindly from a broker feed. This doc fixes the vision, the data-model
contract, the scope boundary, and the build order. Design and implementation
details live in the child specs it points to.

This is the keystone of milestone **M1J**. At least six already-written specs gate
on it: Plaid Investments sync, portfolio/holdings reports, investment-transaction
dedup and transfer detection, investment OFX (`<INVSTMTRS>`) import, and net-worth
holdings valuation. None of them can proceed until the contracts here are fixed.

## Vision

> **Every position, lot, and realized gain is derived from a single investment
> transaction ledger — explainable, reproducible, and tying out to the 1099-B the
> broker sends. What MoneyBin computes, the user can verify.**

Four commitments:

1. **The ledger is the source of truth.** `core.fct_investment_transactions` is the
   one authored/ingested surface. Lots, holdings, and gain/loss are *derived* from
   it in SQLMesh — never snapshotted into mutable `app.*` state (shared-primitives
   Invariant 8).
2. **Cost basis is computed, not just stored.** MoneyBin reproduces realized gain/loss
   from the ledger so it can reconcile against (and detect drift from) broker-reported
   numbers, and project the tax impact of a sale *before* it happens.
3. **Method-agnostic by construction.** The lot grain supports FIFO, specific
   identification, and average cost as computations over the same data. Picking fewer
   methods for v1 costs no rework later.
4. **1099-B is the bar.** M1J does not close until cost-basis output ties to a real
   broker 1099-B for a full tax year end-to-end. Reconciliation, not plausibility,
   is the success metric; a holdings dashboard without the tie-out is not enough.

## The asset / investment dividing line

Inherited verbatim from [`asset-tracking.md`](asset-tracking.md):

> **If the value comes from a market ticker, it's an investment. If it comes from
> an appraisal or estimate, it's an asset.**

Brokerage holdings, ETFs, mutual funds, bonds, and crypto are investments (this
initiative). Houses, cars, and jewelry are assets (`asset-tracking.md`). Both
contribute to net worth, through different daily-valuation pipelines that meet only
at the `reports.net_worth` aggregation.

## Core as the analytics layer

Investments extends the `raw` → `prep` → `core` progression with a securities
dimension and an investment-transaction fact, then derives lots, holdings, and
valuation on top. Consumers (CLI, MCP, the `us_tax` package, reports) read from
`core` only.

| Table/view | Type | Grain | Purpose |
|---|---|---|---|
| `core.dim_securities` | Dimension | One real-world security | Mastered instrument reference; resolves ticker/CUSIP/ISIN/crypto refs to a stable surrogate `security_id` |
| `core.fct_investment_transactions` | Fact | One investment event | The ledger: buys, sells, dividends, reinvests, splits, transfers, fees |
| `core.fct_investment_lots` | Fact (derived) | One open/closed tax lot | Each acquisition opens a lot; disposals consume lots per the elected method; carries cost basis and remaining quantity (no gain/loss columns) |
| `core.fct_realized_gains` | Fact (derived) | One (disposal × consumed lot) pair | The 1099-B grain: proceeds, cost basis, gain/loss, short/long-term split |
| `core.dim_holdings` | Dimension (derived) | One position (account × security) | Current open quantity + cost basis; sum of open lots. *(Name locked by `extension-contracts.md`.)* |
| `core.fct_holdings_daily` | Fact (derived) | One position per day | Daily-valued time series (holdings × price). **Pillar C** — needs price feeds. *(Planned name; illustrative in `architecture-shared-primitives.md`, not yet a locked contract.)* |

Mutable user state lives in `app.*`: the manual-entry security catalog, the
cost-basis-method election, and specific-lot selection overrides. Everything in
`core` is rebuilt deterministically from `app.*` + `raw.*` on every SQLMesh run.

## The four pillars

| Pillar | Purpose | Needs price feed? | Child spec |
|---|---|---|---|
| **A. Investment data model** | The securities dimension + the investment-transaction ledger (raw → prep → core), plus manual entry | No | [`investments-data-model.md`](investments-data-model.md) *(foundation child — A+B)* |
| **B. Cost-basis & gain/loss engine** | Derived lots; FIFO + HIFO + specific-ID + average-cost; realized gain/loss; short-term/long-term split | No | [`investments-data-model.md`](investments-data-model.md) *(ships with A)* |
| **C. Price feeds & valuation** | Yahoo + CoinGecko ingestion → append-only `core.fct_security_prices`; `core.fct_holdings_daily`; unrealized gain/loss | Yes (it *is* the feed) | `investments-price-feeds.md` *(planned)* |
| **D. Net-worth integration** | Holdings valuation into `reports.net_worth` / `fct_balances` | Yes (consumes C) | `investments-net-worth.md` *(planned)* |

### Already-carved children (gated stubs)

These exist as planned specs that gate on the contracts above:

- `sync-plaid-investments.md` — Plaid Investments product (holdings, securities, investment transactions).
- Investment OFX import — `<INVSTMTRS>` handling, a child of `smart-import-financial.md`.
- Portfolio/holdings reports — `reports.portfolio`, `reports.holdings`, gated per `reports-recipe-library.md`.
- Investment-transaction dedup + transfer detection — children of the matching initiative.

### The foundation boundary (why A+B ship first, without a price feed)

The single most important scoping insight: **realized gain/loss needs no market
price.** It is computed entirely from the ledger — a sale's proceeds versus the
cost of the consumed lots, both of which are recorded events. Only *unrealized*
gain/loss (paper value of what you still hold) requires a current price.

Therefore the foundation child (Pillar A + B) reaches the M1J "ties to a real
1099-B end-to-end" bar **without** Pillar C. Price feeds (C) and net-worth
integration (D) become clean follow-on children that add valuation on top of an
already-correct cost-basis engine.

## Cross-cutting concerns

Every pillar honors these. Detailed design lives in the child specs.

### Cost-basis method is a stored election, not hardcoded logic

Lots are the universal grain. The "method" only decides which lots a disposal
draws from and how their basis is summed:

| Method | Rule over lots | Typical use |
|---|---|---|
| FIFO | Oldest lots first | IRS default; most brokerages for stocks |
| Specific identification | User-/broker-selected lots, else FIFO | Tax-loss harvesting; controlling ST/LT |
| Average cost | Pool basis across lots (IRS: funds/ETFs only) | Vanguard/Fidelity mutual-fund default |
| HIFO | Highest-cost lots first | Crypto tax minimization (v1 — sort-key variant of FIFO) |
| LIFO | Last lots first | Future, demand-gated (no PFM ships it for brokerage) |

The **short-term/long-term split walks lots oldest-first under every method** — only
the basis *number* changes. The election is recorded at per-account default
(`app.account_settings`) + per-security override (`app.securities`), so a user can
hold FIFO stocks and average-cost funds in the same account.

### MoneyBin mirrors the broker's method; it does not enforce IRS policy

For v1, MoneyBin reproduces whatever method the broker reported (read from the
1099-B / Plaid / OFX). It does **not** police IRS election rules — no average-cost
lock-in, no "you can't switch back," no double-category sub-variants. That policy
surface is what would balloon average cost into a large feature, and it is not a
reconciliation tool's job.

### Security identity: surrogate key + resolution chain

`core.dim_securities` keys on a stable surrogate `security_id` (truncated UUID4,
mirroring `dim_merchants`). Ticker, CUSIP, ISIN, FIGI, and `coingecko_id` are
*attributes*, not the key — tickers get reused and renamed, CUSIP/ISIN are licensed
and US/international-centric, and crypto has none of them. Incoming references
resolve to the surrogate via a chain (CUSIP/ISIN → ticker+exchange → name fuzzy),
mirroring the institution-resolution chain in `smart-import-financial.md`. Licensed
identifiers (CUSIP/ISIN) that arrive in the user's own data are stored and used, but
MoneyBin ships no CUSIP/ISIN lookup database.

### Prices are an append-only history, not a cache

Pillar C stores daily closes in an append-only `core.fct_security_prices` (daily
grain). A historical close is immutable, so this is a backfill-once / extend-daily
record, not a cache with invalidation logic. Volume is trivial for DuckDB (≈100
securities × 252 trading days × 10 years ≈ 126k rows). Today's live value can come
from a real-time call or short cache; everything past is stored. Raw vs.
split/dividend-adjusted prices is a Pillar C concern (cost basis uses raw ledger
data; charts often use adjusted).

### Stale prices surface, never masquerade as current (Pillar C)

When a current price is unavailable — market closed, feed gap, or a security the
feed does not cover — valuation falls back to the most recent stored close, but
the fallback is always visible. Any priced response carries the `price_date` it
actually used plus a staleness measure; past a configurable threshold the
response envelope adds an explicit staleness warning. Reuse the staleness
vocabulary [`asset-tracking.md`](asset-tracking.md) already establishes
(`days_since_observed` + `staleness_threshold_days`) rather than coining a
parallel `staleness_days`, so prices and physical assets share one shape for the
staleness concept. A stale close is never silently presented as the current
price. This is the price-side application of
"magic stays visible": the fallback itself is fine, silently misrepresenting its
age is not. A shipped competitor already does exactly this (snapshot fallback
plus gap-day staleness marking), confirming the shape.

### Currency: column now, conversion later

A `currency` column lands on the ledger, lots, prices, and holdings in the
foundation child — adding it to `core` later is a breaking migration. v1 assumes a
single reporting currency and does no FX. Multi-currency conversion is **M1K**
(`multi-currency`, future). Same "lock the schema, stage the algorithm" move as the
cost-basis methods.

## In scope (the initiative)

- A securities dimension with multi-source identity resolution.
- An investment-transaction ledger (manual entry in the foundation child; Plaid/OFX
  as separate children).
- Derived lots, holdings, realized + unrealized gain/loss, ST/LT classification.
- FIFO, specific-identification, and average-cost methods.
- Daily price history (Yahoo equities/ETFs/funds; CoinGecko crypto).
- Net-worth integration of holdings valuation.
- CLI + MCP surface under a top-level `investments` group.

## Out of scope

- **Options** (exercise/assignment/expiration) — distinct modeling, not in the M1J
  bar. Reserved for a later child.
- **Wash-sale detection / Schedule D** — owned by the `us_tax` package
  (`extension-contracts.md`), which consumes the core tables defined here.
- **Multi-currency FX conversion** — M1K. The `currency` column lands now; conversion
  does not.
- **IRS election-policy enforcement** — MoneyBin mirrors the broker's method (see
  cross-cutting above).
- **Margin / short positions / derivatives** beyond plain long holdings — future.
- **Brokerage order execution** — MoneyBin records history; it never places trades.

## Build order & rationale

1. **Foundation child A+B** ([`investments-data-model.md`](investments-data-model.md))
   — securities dimension, ledger, lots, and the three-method cost-basis engine with
   manual entry. Reaches the full-tax-year 1099-B bar. Fixes every schema contract
   the gated children wait on.
2. **Pillar C** (`investments-price-feeds.md`) — append-only price history, daily
   valuation, unrealized gain/loss.
3. **Pillar D** (`investments-net-worth.md`) — holdings valuation into
   `reports.net_worth`.
4. **Already-carved children** (Plaid sync, OFX import, portfolio reports, matching)
   — proceed against the now-fixed contracts, in any order. M1J closes when the
   milestone's promise (1099-B reconciliation + holdings in net worth) is met.

## Success criteria

- **1099-B reconciliation (headline).** For a real brokerage account, MoneyBin's
  realized gain/loss per lot — short-term and long-term — ties to the broker's
  1099-B for a full tax year under the broker's reported method.
- **Ledger-derived correctness.** Holdings, lots, and gain/loss are fully rebuildable
  from `core.fct_investment_transactions` + `app.*` on every SQLMesh run; no derived
  state is authoritative.
- **Method coverage.** FIFO, HIFO, specific-ID, and average-cost all produce correct ST/LT
  splits over the same lot ledger.
- **Net worth completeness.** Once Pillar D ships, `reports.net_worth` includes
  market-valued holdings alongside cash and physical assets.
- **Contract stability.** The gated children (sync, import, reports, matching) build
  against the core tables here without schema changes.

## Open questions

Cross-cutting decisions deferred to child specs or to resolve during implementation.

- **Lot-selection override home — resolved.** The canonical lot-selection override
  lives in a **core** `app.*` table (`app.lot_selections` in the foundation child),
  not the `us_tax` package — cost basis is core, and a non-US user with no `us_tax`
  installed still must pick lots to determine realized gain. `extension-contracts.md`
  has been corrected to drop "lot-selection overrides" from the `us_tax` write set
  (it now reads `app.lot_selections` + `core.fct_realized_gains` for Schedule D);
  `us_tax` keeps only tax-specific config (filing status, wash-sale adjustments).
- **`dim_securities` source model.** v1 is manual-only (`app.securities` → `core.dim_securities`).
  When Plaid/OFX importers arrive, `dim_securities` becomes a multi-source union with
  cross-source resolution (mirroring `fct_transactions`). The foundation child fixes
  the surrogate-key contract; the union/dedup mechanics are detailed when the first
  importer lands.
- **Lots model: SQL vs Python — resolved.** The lot-consumption engine is a
  **Python SQLMesh model** (`fct_investment_lots.py` / `fct_realized_gains.py`),
  applying the existing `fct_balances_daily.py` precedent — sequential lot
  consumption (FIFO/HIFO cursor, average-cost running pool, specific-ID lookup)
  is unmaintainable in pure SQL. Applies an existing pattern → no ADR
  (2026-07-04; recorded in the foundation child's Key Decisions).
- **Realized gain/loss surface grain — resolved.** The foundation child uses a
  separate `core.fct_realized_gains` fact (one row per disposal × consumed lot — the
  1099-B grain), rather than overloading columns onto `core.fct_investment_lots`.
- **Adjusted vs raw price storage (Pillar C).** Store raw closes and adjust on read,
  or store both raw and split/dividend-adjusted series. Pillar C decision.
- **Cross-source security resolution thresholds.** The fuzzy step (name match when no
  CUSIP/ticker) needs scoring + a review posture, mirroring matching. Deferred to the
  first importer child.
