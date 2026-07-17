# Feature: Investment Data Model & Cost-Basis Engine

## Status
implemented

## Goal

Establish the securities dimension and the investment-transaction ledger, then
derive lots, holdings, and **realized** gain/loss (short- and long-term) from that
ledger — reproducing broker numbers well enough to reconcile against a real
full-tax-year 1099-B.
This is the foundation child of the investments initiative (Pillars A + B of
[`investments-overview.md`](investments-overview.md)). It deliberately stops short
of market-price valuation: realized gain/loss needs no price feed, so the 1099-B bar
is reachable here, while unrealized value and net-worth integration follow as
separate children.

## Background

Investments are the keystone of milestone M1J and the single most-referenced
unwritten contract in the repo (see [`investments-overview.md`](investments-overview.md)
for the full gate list). This spec fixes the contracts those gated specs wait on:
the security identity key, the ledger shape and its `type` taxonomy, and the
lot/holding derivation.

The crucial scoping insight (from the overview): **realized gain/loss is computed
entirely from the ledger** — a sale's proceeds versus the cost of the lots it
consumes, both recorded events — so it requires no market price. Only *unrealized*
gain/loss needs a current price, which is Pillar C's job.

Related specs:
- [`investments-overview.md`](investments-overview.md) — umbrella; vision, pillars, cross-cutting contracts
- [`asset-tracking.md`](asset-tracking.md) — the asset/investment dividing line; sibling net-worth contributor
- [`architecture-shared-primitives.md`](architecture-shared-primitives.md) — layer conventions; **Invariant 8** (derivations live in SQLMesh, never snapshotted into `app.*`)
- [`identifiers.md`](../../.claude/rules/identifiers.md) — surrogate keys (truncated UUID4) and content hashes
- [`account-management.md`](account-management.md) — `app.account_settings`, extended here with the cost-basis-method default
- [`moneybin-cli.md`](moneybin-cli.md) — promotes the placeholder `accounts investments` to a **top-level `investments` group**
- [`extension-contracts.md`](extension-contracts.md) — `us_tax` package consumes `core.dim_holdings` + `core.fct_investment_transactions`

## Requirements

1. **Security catalog in `app.securities`.** A manually-maintained catalog keyed on a
   stable surrogate `security_id` (truncated UUID4). Ticker, CUSIP, ISIN, FIGI, and
   `coingecko_id` are nullable *attributes*, never the key.
2. **Six security types for v1:** `equity`, `etf`, `mutual_fund`, `bond`, `crypto`,
   `cash`, plus `other`. `cash` covers money-market funds and sweep positions
   (Plaid's `Security.type = 'cash'`), paired with an `is_cash_equivalent`
   flag. New types are added by extending the `CHECK` constraint (a
   lightweight migration via `database-migration.md`); the `other` escape hatch
   absorbs unanticipated instruments without one.
3. **Identity resolution chain.** Free-text or partial security references resolve to
   a `security_id` via CUSIP/ISIN → ticker+exchange → name fuzzy, mirroring the
   institution-resolution chain in `smart-import-financial.md`. Three rules adopted
   from Portfolio Performance's battle-tested `SecurityCache` (127 broker importers):
   ticker resolution tries the full reference as a stored ticker first — so a
   dotted ticker like `BRK.B` resolves by its own ticker — and only then falls back
   to stripping an exchange suffix (`UMAX.AX` → `UMAX`, disambiguated by the
   `exchange` attribute); a name match is rejected when the candidate carries
   a *contradicting* strong identifier (CUSIP/ISIN/ticker); identifier collisions
   raise naming the exact attribute — never auto-merge (identifiers.md Guard 2).
   Note for importer children: Plaid delivers CUSIP/ISIN only to
   CUSIP-Global-Services-licensed customers (null in practice since 2024-03),
   so provider identifiers (`institution_security_id`, provider `security_id`)
   are the practical strong rung for feeds. v1 resolves against
   `app.securities` (manual-only); the contract is multi-source-ready. When the first
   importer child lands, cross-source security identity adopts the established
   binding + review-queue pattern — `app.security_links` / `app.security_link_decisions`
   mirroring the merchant (M1T) and account (M1S) link tables, with the same
   adopt-on-strong-signal / propose-on-fuzzy ladder — purely additive to the schema
   fixed here.
4. **Investment-transaction ledger in `core.fct_investment_transactions`.** One row
   per investment event. The only authored/ingested surface; everything else derives
   from it.
5. **Closed `type` taxonomy** (one-way-door enum), mapped from OFX `<INVTRANLIST>` and
   Plaid (see [Plaid Investments Readiness](#plaid-investments-readiness)) so
   importers slot in cleanly — fourteen values: `buy`, `sell`, `reinvest`,
   `dividend`, `interest`, `capital_gain_distribution`, `transfer_in`,
   `transfer_out`, `deposit`, `withdrawal`, `split`, `fee`, `return_of_capital`,
   `other`. `deposit`/`withdrawal` are *external* cash funding events (NULL
   `security_id`; move net contribution), kept distinct from
   `transfer_in`/`transfer_out`, which are *internal*, basis-preserving position
   moves—collapsing the two breaks net-contribution and time-weighted-return
   math. The `type` is the engine-dispatch contract; two companion columns keep
   one enum from serving mechanics, reporting, and display:
   - **`subtype`** (nullable) — closed per-type refinement vocabulary carrying tax
     character and provenance detail: `qualified`/`non_qualified` on `dividend`;
     `short_term`/`long_term` on `capital_gain_distribution`; `tax_withheld` on
     `fee`; `dividend`/`interest`/`capital_gain` on `reinvest` (funding source).
     Raw preserves the provider's original type string alongside. Extending the
     vocabulary is additive; consumers must tolerate NULL and unknown values.
   - **`event_group_id`** (nullable) — links the rows of one decomposed economic
     event (reinvest pair, merger legs, tax withholding on a dividend). Truncated
     UUID4 minted at entry/staging time.
6. **Sign conventions.** `quantity` is signed: positive for acquisitions
   (`buy`/`reinvest`/`transfer_in`), negative for disposals (`sell`/`transfer_out`),
   NULL for cash-only events (`deposit`, `withdrawal`, `dividend`, `interest`,
   `fee`, …). `amount` follows the existing accounting convention
   (negative = cash out, e.g. a buy; positive = cash in, e.g. a sell or dividend).
   `amount` is the *total* cash effect **including** fees; the `fees` breakout is the
   portion that increases cost basis on acquisitions and reduces proceeds on disposals.
   So a buy's cost basis is `|amount|` and a sell's net proceeds is `amount`.
   **`reinvest` rows carry the acquisition leg only** — quantity, price, and the
   negative `amount` of cash redeployed; the income being reinvested is always its
   own `dividend`/`interest` row sharing the `event_group_id`. This keeps semantics
   identical across manual entry, Plaid (which delivers the pair as two rows), and
   OFX, and means income reports sum income-typed rows only — reinvested income can
   never double-count or silently vanish.
7. **Manual entry via raw, per-provider raw tables.** Manual entry writes
   `raw.manual_investment_transactions` (mirroring `raw.manual_transactions`) →
   `prep.stg_manual__investment_transactions` → `core.fct_investment_transactions`,
   following the CLI-imperative / MCP-declarative-set pattern from
   `transaction-curation.md`. Importer children add their own provider-shaped raw
   tables (`raw.plaid_investment_transactions`, `raw.ofx_investment_transactions`)
   plus staging models, unioned at the core boundary — the same per-provider
   pattern as the cash-transaction pipeline (`stg_plaid__transactions` et al.) and
   the max-data-capture posture, since provider rows carry fields (Plaid
   `subtype`, `institution_security_id`, dual currency codes) a shared generic
   table would flatten. With one source in v1, `core.fct_investment_transactions`
   selects from the single staging model; the union arrives with the second source
   (the same extension pattern as `core.dim_securities`).
8. **Derived lots in `core.fct_investment_lots`** (Invariant 8). Each acquisition
   opens a lot; disposals consume lots per the elected method. Each lot carries a
   stable content-hash `lot_id` so specific-ID overrides can reference it.
9. **Derived realized gains in `core.fct_realized_gains`.** One row per
   (disposal, consumed lot) pair — the 1099-B grain: proceeds, cost basis, gain/loss,
   acquisition/disposal dates, and short-/long-term classification.
10. **Derived holdings in `core.dim_holdings`** — current open quantity + cost basis
    per (account, security); the sum of open lots. *(Name locked by `extension-contracts.md`.)*
11. **Four cost-basis methods:** FIFO, HIFO, specific identification, average cost —
    all computations over the same lot ledger (see [Cost-Basis Engine](#cost-basis-engine)).
12. **Method election** at per-account default (`app.account_settings.default_cost_basis_method`)
    + per-security override (`app.securities.cost_basis_method`). Resolution:
    per-security → per-account → global FIFO. Average cost validates to fund/ETF types;
    `fifo`/`hifo`/`specific` are unrestricted.
13. **Specific-ID overrides in `app.lot_selections`** — core `app.*` state (cost basis
    is core, not the `us_tax` package; resolves the `extension-contracts.md`
    inconsistency the overview's open questions section flagged — see
    [`investments-overview.md`](investments-overview.md#open-questions)).
14. **Mirror, don't enforce.** v1 reproduces the broker's reported method; it does not
    enforce IRS election lock-in or wash-sale rules.
15. **Currency column** on ledger, lots, gains, and holdings now; no FX conversion
    (deferred to M1K). Named **`currency_code`** per `multi-currency.md`'s canonical
    column-name decision (matching `core.fct_transactions` / `core.dim_accounts`).
16. **CLI commands** under a top-level `investments` group (see CLI Interface).
17. **MCP tools** under the `investments_*` namespace (see MCP Interface).
18. **All commands support `--output json`** for non-interactive / agent parity.

## Data Model

> **Precision convention.** Quantity, price, and per-unit-cost columns use
> `DECIMAL(28,10)` rather than `architecture-shared-primitives.md` Layer Rule 6's
> `DECIMAL(18,8)`. This is a deliberate, domain-driven deviation: crypto positions
> need full fractional-unit precision (sub-satoshi quantities, sub-cent token prices),
> which the fiat-oriented `18,8` default truncates. Money *amounts* (`amount`, `fees`,
> `cost_basis*`) keep the standard `DECIMAL(18,2)`.

> **`updated_at` convention.** Every derived `core` model here computes `updated_at`
> per [`core-updated-at-convention.md`](core-updated-at-convention.md) — the `MAX`
> of per-row input timestamps (never `CURRENT_TIMESTAMP` inside a model).

### New table: `app.securities`

Manually-maintained security catalog. Managed via CLI (`investments securities add/set`).

```sql
CREATE TABLE IF NOT EXISTS app.securities (
    security_id VARCHAR NOT NULL PRIMARY KEY,           -- Stable surrogate (truncated UUID4, 12 hex); never derived from ticker
    name VARCHAR NOT NULL,                              -- Human-readable label ("Apple Inc.", "Bitcoin")
    security_type VARCHAR NOT NULL CHECK (security_type IN ('equity', 'etf', 'mutual_fund', 'bond', 'crypto', 'cash', 'other')), -- Instrument classification; 'cash' = money-market/sweep positions
    ticker VARCHAR,                                     -- Exchange ticker ("AAPL"); nullable, not unique (tickers get reused)
    exchange VARCHAR,                                   -- Listing exchange ("NASDAQ"); disambiguates duplicate tickers
    cusip VARCHAR,                                      -- 9-char CUSIP if supplied by user data; licensed — accepted, never redistributed
    isin VARCHAR,                                       -- ISIN if supplied; international identifier
    figi VARCHAR,                                       -- OpenFIGI identifier (open mapping aid); nullable
    coingecko_id VARCHAR,                               -- CoinGecko slug for crypto price lookup (Pillar C); nullable
    is_cash_equivalent BOOLEAN,                         -- Highly liquid, treat-like-cash flag (money-market/sweep); NULL = unknown
    cost_basis_method VARCHAR CHECK (cost_basis_method IN ('fifo', 'hifo', 'specific', 'average')), -- Per-security election override; NULL falls back to account default
    currency_code VARCHAR NOT NULL DEFAULT 'USD',       -- Instrument's denominating currency; canonical name per multi-currency.md; no FX conversion in v1
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, -- When the catalog entry was created
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP  -- When last modified; service must set explicitly on UPDATE (DuckDB has no ON UPDATE trigger)
);
```

### New table: `raw.manual_investment_transactions`

Immutable manual-entry records, mirroring `raw.manual_transactions` (per-provider
raw pattern — importer children add their own provider-shaped raw tables).
Manual entry resolves the account and security interactively at entry time
(identifiers.md Guard 2: resolve free-text at the boundary), so this table
carries resolved IDs plus the user's original reference string for audit.

```sql
CREATE TABLE IF NOT EXISTS raw.manual_investment_transactions (
    source_transaction_id VARCHAR PRIMARY KEY, -- Truncated UUID4 (12 hex), prefixed 'manual_' for source-clarity in joins
    source_type VARCHAR NOT NULL DEFAULT 'manual',  -- Discriminator; constant for this table
    source_origin VARCHAR NOT NULL DEFAULT 'user',  -- Origin tag; always 'user' for manual entries
    import_id VARCHAR NOT NULL,                -- FK to raw.import_log.import_id; one batch per CLI call or MCP bulk call
    account_id VARCHAR NOT NULL,               -- FK to core.dim_accounts; resolved at entry
    security_id VARCHAR,                       -- FK to app.securities; resolved at entry; NULL for cash-only events
    security_ref VARCHAR,                      -- The user-supplied security reference as typed (audit trail for the resolution)
    type VARCHAR NOT NULL,                     -- Core taxonomy value (CLI/MCP validate at entry; manual rows arrive canonical)
    subtype VARCHAR,                           -- Per-type refinement (tax character, reinvest source); nullable
    event_group_id VARCHAR,                    -- Links legs of one economic event (reinvest pair, merger legs); nullable
    trade_date DATE NOT NULL,                  -- Trade date (drives holding period); NOT settlement date
    settlement_date DATE,                      -- Settlement date if supplied; informational
    original_acquisition_date DATE,            -- For transfer_in: shares' original acquisition date (holding period transfers in); NULL otherwise
    quantity DECIMAL(28, 10),                  -- Units (high precision for fractional shares / crypto); signed per Requirement 6
    price DECIMAL(28, 10),                     -- Per-unit price; NULL for non-priced events
    amount DECIMAL(18, 2),                     -- Cash effect; signed per Requirement 6
    fees DECIMAL(18, 2),                       -- Commissions/fees component; folded into cost basis
    currency_code VARCHAR DEFAULT 'USD',       -- Denominating currency as supplied
    description VARCHAR,                       -- Free-text description
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, -- When the row was inserted
    created_by VARCHAR NOT NULL,               -- 'cli' or 'mcp'; future-extensible for multi-user identity
    investment_transaction_id VARCHAR          -- Predicted gold-key (content hash, per raw.manual_transactions.transaction_id precedent); populated at INSERT
);
```

### New table: `app.lot_selections`

Specific-identification overrides. Core `app.*` state (cost basis is core). Records,
for a disposal, which lots to draw from and how much from each.

```sql
CREATE TABLE IF NOT EXISTS app.lot_selections (
    investment_transaction_id VARCHAR NOT NULL, -- FK to the disposal row in core.fct_investment_transactions
    lot_id VARCHAR NOT NULL,                     -- FK to core.fct_investment_lots; the chosen lot
    quantity DECIMAL(28, 10) NOT NULL,          -- Units to draw from this lot for this disposal
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, -- When the selection was recorded
    PRIMARY KEY (investment_transaction_id, lot_id) -- One selection per (disposal, lot) pair
);
```

### Modified table: `app.account_settings`

Adds the per-account cost-basis-method default (per `account-management.md`).

```sql
ALTER TABLE app.account_settings
    ADD COLUMN default_cost_basis_method VARCHAR
        CHECK (default_cost_basis_method IN ('fifo', 'hifo', 'specific', 'average')); -- Per-account default; NULL → global FIFO
```

Delivered the same dual-path way as prior column additions (e.g. V030's Plaid
transaction fields): the column joins the `app_account_settings.sql` DDL for fresh
installs, and a migration applies the ALTER to existing databases.

### SQLMesh model: `core.dim_securities` (VIEW)

v1 projects the manual catalog; structured as a union so importers add CTEs later
(same extension pattern as `core.fct_asset_valuations`).

```sql
MODEL (
  name core.dim_securities,
  kind VIEW
);

SELECT
  security_id,        -- Stable surrogate key
  name,               -- Display name
  security_type,      -- equity | etf | mutual_fund | bond | crypto | cash | other
  ticker,             -- Display/lookup ticker (carry the ID per identifiers.md Guard 1)
  exchange,           -- Listing exchange
  cusip,              -- Licensed identifier; present only if user-supplied
  isin,               -- International identifier
  figi,               -- OpenFIGI mapping
  coingecko_id,       -- Crypto price-lookup slug (Pillar C)
  is_cash_equivalent, -- Treat-like-cash flag (money-market/sweep)
  currency_code       -- Denominating currency
FROM app.securities
-- Future: UNION ALL resolved securities from prep.stg_plaid__securities, etc.
```

### SQLMesh model: `core.fct_investment_transactions` (TABLE)

The canonical ledger. v1 selects from `prep.stg_manual__investment_transactions`
(rows arrive with resolved IDs and canonical taxonomy — see Requirement 7);
importer children add their staging models to a union here, where their raw type
strings map to the taxonomy and unresolved security refs run the resolution chain.

```
Columns:
  investment_transaction_id  VARCHAR          -- Canonical ID (source-provided or content hash)
  account_id                 VARCHAR          -- FK to core.dim_accounts
  security_id                VARCHAR          -- FK to core.dim_securities; NULL for cash-only events (deposit, withdrawal, account fee, cash interest)
  trade_date                 DATE             -- Trade date; drives holding-period classification
  settlement_date            DATE             -- Settlement date; informational
  original_acquisition_date  DATE             -- transfer_in only: original acquisition date; lot uses COALESCE(this, trade_date)
  type                       VARCHAR          -- Closed taxonomy (see Requirement 5)
  subtype                    VARCHAR          -- Per-type refinement (tax character, reinvest source); nullable
  event_group_id             VARCHAR          -- Links legs of one decomposed economic event; nullable
  quantity                   DECIMAL(28,10)   -- Signed units: + acquire, − dispose, NULL cash-only
  price                      DECIMAL(28,10)   -- Per-unit price; NULL for non-priced events
  amount                     DECIMAL(18,2)    -- Signed cash effect: − out (buy), + in (sell/dividend)
  fees                       DECIMAL(18,2)    -- Fee/commission component folded into basis
  currency_code              VARCHAR          -- Denominating currency; no FX in v1
  source_type                VARCHAR          -- Origin tag (manual | ofx | plaid)
  source_origin              VARCHAR          -- Institution/connection scope
  description                VARCHAR          -- Free-text description
  updated_at                 TIMESTAMP        -- Row freshness per core-updated-at-convention
```

### SQLMesh model: `core.fct_investment_lots` (TABLE, derived)

Each acquisition opens a lot; disposals consume open lots per the resolved method.
Implemented as a **Python SQLMesh model** (Key Decision 13) — the consumption logic
(FIFO cursor, average-cost running pool, specific-ID override lookup) is awkward in
pure SQL. The pure engine lives in `moneybin.investments.cost_basis`
(`compute_lots_and_gains`), fed by `moneybin.investments.sqlmesh_loader`; the
`src/moneybin/sqlmesh/models/core/fct_investment_lots.py` model itself is a thin wrapper that
runs the engine and types the output. Lot identity is a content hash so it is
stable across rebuilds and referenceable by `app.lot_selections`.

```
Columns:
  lot_id                  VARCHAR          -- Content hash of (account_id, security_id, acquisition_date, source acquisition txn id); prefix "lot_"
  account_id              VARCHAR          -- FK to core.dim_accounts
  security_id             VARCHAR          -- FK to core.dim_securities
  acquisition_date        DATE             -- Trade date of the opening event; drives ST/LT
  acquisition_type        VARCHAR          -- buy | reinvest | transfer_in
  original_quantity       DECIMAL(28,10)   -- Units when the lot opened
  remaining_quantity      DECIMAL(28,10)   -- Open units after disposals consumed (0 when fully closed)
  cost_basis_total        DECIMAL(18,2)    -- Total basis of original_quantity, including fees
  cost_basis_remaining    DECIMAL(18,2)    -- Basis attributable to remaining_quantity
  cost_basis_method       VARCHAR          -- Resolved method that governed this lot's consumption (fifo | hifo | specific | average)
  currency_code           VARCHAR          -- Denominating currency
  is_open                 BOOLEAN          -- remaining_quantity > 0
  source_transaction_id   VARCHAR          -- FK to the opening core.fct_investment_transactions row
  basis_incomplete        BOOLEAN          -- TRUE when this lot opened with no supplied basis (e.g. transfer_in with unknown cost basis); see "Oversold lots"
  updated_at              TIMESTAMP        -- Row freshness
```

### SQLMesh model: `core.fct_realized_gains` (TABLE, derived)

The 1099-B grain: one row per (disposal, consumed lot) pair. This is the
reconciliation surface.

```
Columns:
  realized_gain_id    VARCHAR          -- Content hash of (disposal txn id, lot_id)
  account_id          VARCHAR          -- FK to core.dim_accounts
  security_id         VARCHAR          -- FK to core.dim_securities
  disposal_txn_id     VARCHAR          -- FK to the disposing core.fct_investment_transactions row
  lot_id              VARCHAR          -- FK to core.fct_investment_lots consumed
  quantity            DECIMAL(28,10)   -- Units drawn from this lot for this disposal
  acquisition_date    DATE             -- Lot acquisition date (holding-period start)
  disposal_date       DATE             -- Disposal trade date (holding-period end)
  proceeds            DECIMAL(18,2)    -- Sale proceeds attributable to this quantity (net of fees)
  cost_basis          DECIMAL(18,2)    -- Cost basis attributable to this quantity (method-dependent)
  gain_loss           DECIMAL(18,2)    -- proceeds − cost_basis (signed; − is a loss)
  term                VARCHAR          -- 'short' (held ≤ 1 year) | 'long' (held > 1 year)
  cost_basis_method   VARCHAR          -- Method that produced this basis
  basis_incomplete    BOOLEAN          -- TRUE when part of this disposal matched no tracked lot (zero-basis slice); see "Oversold lots"
  currency_code       VARCHAR          -- Denominating currency
  updated_at          TIMESTAMP        -- Row freshness
```

### SQLMesh model: `core.dim_holdings` (VIEW, derived)

Current positions — the sum of open lots per (account, security). No date dimension;
it is the "now" snapshot, rebuilt on every run. Daily-valued history
(`core.fct_holdings_daily`) is Pillar C.

```
Columns:
  account_id        VARCHAR          -- FK to core.dim_accounts (grain)
  security_id       VARCHAR          -- FK to core.dim_securities (grain)
  quantity          DECIMAL(28,10)   -- Total open units (Σ remaining_quantity)
  cost_basis        DECIMAL(18,2)    -- Total open basis (Σ cost_basis_remaining)
  average_cost      DECIMAL(28,10)   -- cost_basis / quantity; (28,10) not Rule-6 (18,8) on purpose — crypto fractional-unit precision propagates through the ratio
  currency_code     VARCHAR          -- Denominating currency
  updated_at        TIMESTAMP        -- Row freshness
```

> **Unrealized gain/loss** (`quantity × current_price − cost_basis`) is intentionally
> absent here — it requires a price, which Pillar C (`investments-price-feeds.md`)
> supplies. `dim_holdings` v1 carries cost basis only.

## Cost-Basis Engine

All four methods are computations over `core.fct_investment_lots`. The engine walks
the ledger per (account, security) in trade-date order, opening lots on acquisitions
and consuming them on disposals.

> **The method set is intentionally closed to the four v1 methods.** The
> `cost_basis_method` `CHECK` (on `app.securities` and `app.account_settings`) allows
> only `fifo`, `hifo`, `specific`, `average` on purpose — electing a method the engine
> does not implement would silently miscompute basis, so the constraint is a guard,
> not an oversight. Declaring a method the engine cannot execute is worse than
> omitting it. **LIFO stays out absent real demand.** Mechanically, ordering
> methods are consumption-order variants of the same engine, so adding LIFO later
> is a sort key plus a lightweight `CHECK`-widening migration—the same deliberate
> trade-off as `security_type`. Build order within this child:
> FIFO → HIFO (same machinery, different sort key) → specific-ID → average.

### Short-term / long-term split (shared across all methods)

Holding period is always determined per-lot, oldest-first, regardless of method —
only the *basis number* differs. A disposal held ≤ 1 year from the lot's
`acquisition_date` is `short`; > 1 year is `long`. A single disposal can split across
both terms when it consumes multiple lots.

### Method: FIFO (default)

Disposals consume open lots in ascending `acquisition_date`. Each consumed slice
contributes its actual per-unit basis. The simplest path and the IRS default.
(FIFO-as-default is a deliberate divergence from Beancount's STRICT-by-default
posture: we model the IRS's default assumption for unelected lots, not
hand-entry typo protection — ambiguity guarding lives in the security
resolution chain instead.)

### Method: HIFO

Disposals consume open lots in descending per-unit basis
(`cost_basis_remaining / remaining_quantity`); ties break oldest-first (favors
long-term treatment). Same consumption machinery as FIFO with a different sort
key. Constituency: self-custody crypto tax minimization — brokerage "HIFO" is
reported on 1099-Bs as specific identification, which the specific-ID path
covers, so HIFO carries unit tests but no 1099-B scenario burden.

### Method: Specific identification

Before falling back to FIFO order, the engine reads `app.lot_selections` for the
disposal. Selected lots are consumed in the specified quantities; any unselected
remainder falls back to FIFO. This unlocks tax-loss harvesting and ST/LT control.
Shares the FIFO consumption machinery — it is an override on consumption order, not a
separate engine.

> **Known gap (v1).** `select_lots` (the service method backing both
> `investments lots select` and `investments_lots_select`) does not verify
> that the disposal's resolved cost-basis method is `specific` before
> accepting the write — a selection saved against a FIFO/HIFO/average-elected
> position is silently ignored at the next `sqlmesh run`, since
> `_consumption_plan` only reads `app.lot_selections` under `specific`.
> Tracked as a follow-up: reject the write when the resolved method isn't
> `specific`, pointing the caller at `investments securities set --method
> specific` (or the account default).

### Method: Average cost

Basis per disposed unit = (remaining pooled cost ÷ remaining pooled units) at the
moment of disposal — a running average that every acquisition/reinvestment mutates.
The ST/LT split still walks lots oldest-first (only the basis is averaged). Validated
to `mutual_fund` / `etf` security types. This is the one genuinely distinct
computation; it adds a single derived path, not a parallel system.

**Implementation note:** compute the pool as two running scalars—remaining
pooled cost and remaining pooled units—rescaled multiplicatively on every
disposal. Do not implement average cost as runtime lot-merging inside the
consumption machinery: it couples pooled-basis arithmetic to lot identity and
makes partial disposals harder to reason about. The lots are still traversed
oldest-first for holding-period attribution; only the basis number comes from
the pool.

**`core.fct_realized_gains` grain under average cost.** There is no lot-specific
basis when pooling, but the table keeps its uniform (disposal × consumed lot) grain:
lots are still traversed oldest-first to assign each slice's holding period, and each
resulting row carries the **pooled average** basis rather than the lot's actual cost.
`lot_id` records the lot that supplied the holding-period attribution, not a
lot-specific cost. This reconciles with broker 1099-Bs, which report one blended-basis
figure per disposal, split into short- and long-term portions.

### Mirror, don't enforce (v1)

The engine reproduces the elected method to match broker output. It does **not**
enforce IRS election lock-in, average-cost switching restrictions, or wash-sale
adjustments. Those are out of scope (wash sales belong to the `us_tax` package).

### Corporate actions

Single-security actions are **typed ledger events applied at lot derivation** —
never rewrites of historical rows (the pattern that left Portfolio Performance
unable to model anything beyond splits for a decade) and never user-computed
zero-and-refill pairs (the Beancount recipe whose own docs concede it destroys
holding-period continuity):

- `split` carries the split **multiplier** `M` (new shares per old share — `2`
  for 2:1, `1.5` for 3:2, `0.5` for a 1:2 reverse split) in its `quantity`
  field; `price`/`amount`/`fees` are unused. Every open lot's
  `original_quantity`/`remaining_quantity` scales by `M` while
  `cost_basis_total` is preserved (per-unit basis changes, total does not).
  Deliberately simpler than a ratio-as-added-units encoding (Decision D6).
- `return_of_capital` reduces `cost_basis_remaining` without creating a disposal,
  allocated **pro-rata across the security's open lots by remaining quantity**
  (RoC arrives per-position on broker statements; the engine owns the per-lot
  spread — never the user).
- `reinvest` opens a new lot (acquisition leg only; the income leg is its own
  `dividend`/`interest` row per Requirement 6).
- `transfer_in` opens a lot whose `acquisition_date` is the shares' **original**
  acquisition date (holding period transfers with the shares — it is *not* reset to
  the transfer date), carrying the supplied basis. Manual entry therefore accepts an
  `--acquired DATE` (original acquisition) and `--basis AMOUNT` for transfers; the
  ledger persists the original date in `original_acquisition_date`, and the lot uses
  `COALESCE(original_acquisition_date, trade_date)`. `transfer_out` consumes lots
  without proceeds (no realized gain).

**Two-security actions** (merger, spin-off, crypto-to-crypto trade) are expressed
as **decomposed leg pairs sharing an `event_group_id`** — the shape Rotki ships
(paired SPEND/RECEIVE with a group identifier) and Portfolio Performance's 2026
corporate-action redesign converges on (linked N-ary entry, ratio-derived basis):

- **Merger / share-class conversion**: `transfer_out` of the old security +
  `transfer_in` of the new, basis carried via `--basis`, holding period via
  `--acquired` (per-lot when granularity is known, aggregate otherwise).

**Same-day ordering.** Events sharing a `trade_date` are applied in a
deterministic, economically-ordered sequence — never arbitrary ingestion or
content-hash order: corporate actions (`split`, `return_of_capital`) take effect
at the ex-date, before same-day trades, and acquisitions precede same-day
disposals (so a same-day buy is an available lot for a same-day sell). The
derived lots and 1099-B therefore never depend on the order events were recorded.

- **Spin-off**: `return_of_capital` on the parent (the basis carve-out, pro-rata)
  + `transfer_in` of the spun security carrying that basis and the parent's
  acquisition date.
- **Crypto-to-crypto trade**: `sell` of asset A + `buy` of asset B at fair market
  value — a taxable disposal plus an acquisition, which is the US-tax-correct
  treatment.

v1 supports these via manual entry of the legs (the CLI/MCP accept
`--event-group` to link them); importer children automate the decomposition in
their staging models. No dedicated `exchange`/`spin_off` enum values until real
import experience demands them — the `event_group_id` linkage is the durable
primitive.

### Oversold lots and incomplete acquisitions

The engine **never blocks the rebuild and never silently invents basis** when
data is incomplete, on either side of a lot's life:

- **Disposal side (oversold).** A disposal exceeds the security's tracked open
  lots (incomplete history). The unmatched slice realizes with **zero cost
  basis** (worst case for the user, conservative for taxes) and the affected
  `core.fct_realized_gains` row carries `basis_incomplete = TRUE`.
- **Acquisition side (missing basis).** A `transfer_in` with no supplied basis
  (real-world ACATS transfers of uncovered securities often arrive this way)
  opens a **zero-basis lot** in `core.fct_investment_lots` flagged
  `basis_incomplete = TRUE` — never rejected, since the basis genuinely may be
  unknown at transfer time. If that lot is later sold, the flag carries
  forward onto the resulting `core.fct_realized_gains` row exactly as an
  oversold disposal would.

CLI/MCP surfaces flag both conditions (`investments lots`/`investments gains`
warnings + response-envelope `warnings`). This adopts the pattern Rotki ships
(structured missing-acquisition records surfaced to review) over the
log-and-continue degradation Portfolio Performance uses. Watch their documented
false-positive class: the same economic security arriving under two
identifiers — which the surrogate-key resolution
chain exists to prevent.

## Plaid Investments Readiness

The schema above was pressure-tested against Plaid's Investments product
(2026-07-04, `plaid/plaid-openapi` `2020-09-14.yml` @ `6abd747c`) so
`sync-plaid-investments.md` slots in as a purely additive child — provider raw
tables + staging models, **no `core` migration**. Findings that shaped the
contracts: the `subtype`/`event_group_id` columns, `deposit`/`withdrawal`
types, the `'cash'` security type + `is_cash_equivalent`, and the
provider-identifier resolution rung all exist because of this validation.

> **Amended 2026-07-10** (child spec design): "no `core` migration" holds for
> reshapes but not additions — [`sync-plaid-investments.md`](sync-plaid-investments.md)
> adds nullable `provider_type`/`provider_subtype` columns to
> `core.fct_investment_transactions` (provider-fidelity promotion, the
> `original_description` precedent) and non-authoritative `provider_reported_*`
> reconciliation columns to `core.dim_holdings`. Additive column migrations
> only; no rename/retype. The child also supersedes the `dim_securities`
> "Future: UNION ALL from staging" comment — its resolver mints synced
> securities into `app.securities` (merchant precedent), so the dim stays a
> catalog view. One refinement to Requirement 5's "`event_group_id` …
> truncated UUID4 minted at entry/staging time": staging-synthesized group ids
> are **content hashes**, not UUIDs — a UUID minted inside a staging view
> would churn on every SQLMesh rebuild. Manual entry keeps its minted UUID
> (persisted in raw, so determinism is unaffected). Finally, the Taxonomy
> mapping table below is **one subtype short**: `stock distribution`
> (`InvestmentTransactionSubtype.STOCK_DISTRIBUTION`, verified in the Plaid
> Python SDK 2026-07-10) is a real security-bearing inflow the "48 subtypes,
> no residue" count missed. It maps to `transfer_in` (opens a lot; the child
> spec adds the row and a general "unlisted security-bearing subtype → review,
> never silent `other`" guard). Additive — no existing row changes. Separately,
> `buy to cover` / `sell short` are re-routed out of `buy`/`sell` to `other`
> (with a `system doctor` surface): the long-lot engine can't model short
> positions, so mapping them to `buy`/`sell` would fabricate a long lot or an
> oversold phantom gain. This *does* change two rows in the table below — the sole such
> change, made because the original mapping was a latent correctness bug, not a
> reshape. Short/margin accounting stays future work.

### Taxonomy mapping (Plaid type/subtype → ours)

Plaid's 6 types × 48 subtypes map onto the taxonomy with no residue beyond
`other` and staging-level exclusion:

| Plaid (type/subtype) | → type | → subtype / notes |
|---|---|---|
| buy/{buy, contribution} | `buy` | |
| buy/{dividend, interest, LT/ST capital gain} reinvestment | `reinvest` | subtype records funding source; the paired Plaid income row maps to its income type, linked by `event_group_id` in staging |
| buy/assignment, sell/exercise, transfer/{assignment, exercise, expire} | `other` | options out of scope |
| sell/sell | `sell` | |
| buy/buy to cover, sell/sell short | `other` | short-position legs — the engine models only long lots, so mapping to `buy`/`sell` would open a spurious long lot or realize an oversold phantom gain; routed to `other` (recorded, kept out of the lot engine) with a `system doctor` surface until short accounting is modeled (future work). A deliberate route to `other`, not the accidental security-bearing default the guard forbids |
| sell/distribution | `transfer_out` | in-kind outflow from tax-advantaged account |
| cash-or-fee/{account, legal, management, transfer, trust, fund, miscellaneous} fee, margin expense | `fee` | |
| cash-or-fee/{tax, tax withheld, non-resident tax} | `fee` | subtype `tax_withheld` |
| cash-or-fee/{dividend, qualified dividend, non-qualified dividend} | `dividend` | subtype `qualified`/`non_qualified` |
| cash-or-fee/{interest, interest receivable} | `interest` | |
| cash-or-fee/{LT/ST capital gain, unqualified gain} | `capital_gain_distribution` | subtype `long_term`/`short_term` |
| fee/return of principal | `return_of_capital` | |
| cash/{contribution, deposit} | `deposit` | NULL security |
| cash/withdrawal | `withdrawal` | NULL security |
| transfer/{transfer, send} | `transfer_in`/`transfer_out` | direction by sign |
| transfer/split | `split` | |
| transfer/{merger, spin off, trade} | decomposed leg pairs | see Corporate actions; staging synthesizes, `event_group_id` links |
| transfer/{adjustment}, fee/adjustment, loan payment, rebalance | `other` | |
| cancel (no subtype), cash/{pending credit, pending debit}, transfer/request | **excluded at staging** | cancellation/pending lifecycle is sync-child territory; `cancel_transaction_id` is a deprecated dead field — never build on it |

### Securities, reconciliation, currency, signs

- Plaid `Security` identifiers (`institution_security_id` + `institution_id`,
  provider `security_id`, `ticker_symbol`, `name`) fit the resolution chain;
  CUSIP/ISIN are license-gated (null in practice) and `sedol` is deprecated —
  hence the provider-identifier rung in Requirement 3. Plaid's `security_id`
  can churn on corporate actions (adopt-quality, not immutable).
- `Security.type` is a **prose enum, not schema-enforced** — staging validates
  defensively; `fixed income` → `bond`, `cash` → `cash`, `derivative`/`loan` →
  `other`, `cryptocurrency` → `crypto`.
- `Holding.cost_basis` and `Holding.tax_lots[]` (per-lot broker data where the
  institution provides it) are **store-don't-trust reconciliation references**
  captured in the sync child's raw layer; our ledger-derived basis is never
  overwritten by them.
- `iso_currency_code`/`unofficial_currency_code` are mutually exclusive →
  `COALESCE` into `currency_code` losslessly.
- **Sign trap:** Plaid investment `amount` is positive = cash out — the exact
  opposite of our convention — while Plaid `quantity` already matches ours.
  The future `stg_plaid__investment_transactions` negates `amount` and must
  say so loudly.

## CLI Interface

All commands under a top-level `investments` group (promoting the placeholder
`accounts investments` in [`moneybin-cli.md`](moneybin-cli.md), peer to `accounts`,
`transactions`, and `assets`). All support `--output json`.

### Ledger

```
moneybin investments add --account <id|name> --security <ticker|name> \
    --type buy --date 2024-01-15 --quantity 10 --price 150.00 \
    [--subtype qualified] [--fees 4.95] [--currency USD] \
    [--event-group <id>] [--notes "..."]
```
- Records one event in `raw.manual_investment_transactions`; resolves
  `--security` via the resolution chain, prompting to create a catalog entry if
  unknown. `--event-group` links legs of one economic event (merger pair,
  spin-off legs).
- **Reinvest convenience:** `--type reinvest` records the acquisition leg AND
  atomically writes the paired income row (`dividend` by default;
  `--subtype interest|capital_gain` selects the income type), both sharing a
  minted `event_group_id` — one command, two ledger rows, mirroring how brokers
  and Plaid report the event.

```
moneybin investments list [--account <id|name>] [--security <ticker|name>] \
    [--type buy] [--from DATE] [--to DATE] [--output json|table]
```
- Lists ledger events (the canonical `core.fct_investment_transactions`).

### Positions & lots

```
moneybin investments holdings [--account <id|name>] [--output json|table]
```
- Current positions: quantity, cost basis, average cost. *(Market value / unrealized
  gain appear once Pillar C ships; v1 shows cost basis only and says so.)*

```
moneybin investments lots [--account <id|name>] [--security <ticker|name>] \
    [--open | --all] [--output json|table]
```
- Lots with remaining quantity and basis. Default: open lots only.

```
moneybin investments lots select <disposal_txn_id> --lot <lot_id>:<quantity> [--lot <lot_id>:<quantity> ...]
moneybin investments lots select <disposal_txn_id> --clear
```
- Sets the **full** specific-identification selection for the disposal in
  `app.lot_selections` — a declarative state-set (Shape 1a): the listed `(lot,
  quantity)` pairs **replace** any prior selection for that disposal (delete by
  omission), identical in semantics to the `investments_lots_select` MCP tool.
  `--clear` submits the empty set, removing all overrides for the disposal and
  reverting it to FIFO (the CLI equivalent of the MCP tool's empty `selections=[]`).
  Unselected remainder falls back to FIFO. (Both surfaces are declarative-set here —
  there is no additive variant, to keep CLI and MCP outcomes identical.)

### Gains

```
moneybin investments gains [--account <id|name>] [--security <ticker|name>] \
    [--from DATE] [--to DATE] [--term short|long] [--output json|table]
```
- Realized gain/loss (the 1099-B surface) from `core.fct_realized_gains`.

### Cost-basis method election (no dedicated verb)

The method is a *field*, not its own operation (per `surface-design.md`):
- **Per-account default** → `moneybin accounts set <id> --default-cost-basis-method fifo|hifo|specific|average`.
- **Per-security override** → `moneybin investments securities set <id> --method fifo|hifo|specific|average`
  (`average` validates to fund/ETF security types).

> **Surface extension to `account-management.md`.** The `--default-cost-basis-method`
> flag on `accounts set`, the matching `default_cost_basis_method` parameter on the
> `accounts_set` MCP tool, and the `app.account_settings.default_cost_basis_method`
> column (the ALTER above) are all **added by this spec** — `account-management.md`
> (status `implemented`) does not yet carry them. Implementing this spec updates the
> accounts command, the `accounts_set` tool, and the `account-management.md` surface
> tables accordingly. Clearing follows the existing `accounts set` convention: a
> `--clear-default-cost-basis-method` companion flag (CLI) and `clear_fields` entry
> (MCP) reset the column to NULL → global FIFO.

### Securities catalog

```
moneybin investments securities list [--type equity] [--output json|table]
moneybin investments securities add --name "Apple Inc." --type equity \
    --ticker AAPL [--exchange NASDAQ] [--cusip ...] [--coingecko-id ...]
moneybin investments securities set <security_id> [--name ...] [--ticker ...] \
    [--cusip ...] [--method fifo|hifo|specific|average]
```

### Example output

```
$ moneybin investments holdings --account fidelity_brokerage

Security   Qty      Cost Basis   Avg Cost   Method
AAPL       15.000   $2,475.00    $165.00    fifo
VTSAX      210.450  $24,800.00   $117.84    average
BTC        0.500    $18,000.00   $36,000.00 specific

  ℹ️  Market value and unrealized gain require price feeds (coming in Pillar C).
```

```
$ moneybin investments gains --account fidelity_brokerage --from 2024-01-01

Date         Security  Qty     Proceeds    Basis       Gain/Loss   Term
2024-06-12   AAPL      5.000   $950.00     $750.00     +$200.00    long
2024-09-03   BTC       0.250   $9,500.00   $9,000.00   +$500.00    short

  Realized 2024: +$700.00  (long +$200.00 / short +$500.00)
```

## MCP Interface

Namespace `investments_*`, following [`mcp-architecture.md`](mcp-architecture.md)
conventions (response envelope, sensitivity tiers). Functional parity with the CLI
(same outcomes reachable), not 1:1 naming.

### Read tools

**`investments`** — List ledger events.
- Params: `account` (optional), `security` (optional), `type` (optional), `from`/`to` (optional DATE)
- Sensitivity: `high` (derived from field classification — quantity/price/amount/fees are `TXN_AMOUNT`)

**`investments_holdings`** — Current positions with cost basis.
- Params: `account` (optional)
- Sensitivity: `high` (cost basis / average cost are `BALANCE`-classified)

**`investments_lots`** — Open/closed lots.
- Params: `account` (optional), `security` (optional), `open_only` (BOOLEAN, default true)
- Sensitivity: `high` (cost basis fields are `BALANCE`-classified)

**`investments_gains`** — Realized gain/loss (1099-B surface).
- Params: `account` (optional), `security` (optional), `from`/`to` (optional DATE), `term` (optional)
- Sensitivity: `high` (proceeds/cost basis/gain-loss are `BALANCE`-classified)

**`investments_securities`** — The catalog.
- Params: `security_type` (optional)
- Sensitivity: `low` (reference data, no amounts)

### Write tools

Per `surface-design.md` — one tool per operation shape, no polymorphic `*_set` catch-all.

**`investments_record`** — Shape 3 (discrete batch event). Record one or more investment
events; resolves securities, reports unresolved refs in `error_details`. Accepts
`subtype` and `event_group_id` per event; a `reinvest` event expands to the
acquisition + income row pair exactly like the CLI convenience (same outcomes,
per functional parity). The batch is **atomic**: all events are validated and
resolved before any write, then written in a single transaction under one
`import_log` batch — a hard-validation or infra failure leaves nothing written
so a retry cannot double-insert. The one soft exception is an unresolved or
ambiguous *security* ref: that event is skipped and reported in `error_details`,
and the rest of the batch still commits.

**`investments_securities_set`** — Shape 1b (entity upsert). Create-or-update one
catalog entry, including its `cost_basis_method` per-security override.

**`investments_lots_select`** — Shape 1a (collection state-set). Set the full set of
`(lot_id, quantity)` selections for one disposal (delete by omission); an empty
`selections=[]` clears all overrides and reverts the disposal to FIFO.

The **per-account default** cost-basis method is a field on `accounts_set`
(`default_cost_basis_method`), not a separate tool — same reasoning as the CLI.

### Response envelope

Standard envelope from [`mcp-architecture.md`](mcp-architecture.md), e.g. for
`investments_holdings`:

```json
{
  "summary": {
    "total_count": 3,
    "sensitivity": "high",
    "display_currency": "USD",
    "warnings": ["Market value/unrealized gain unavailable until price feeds ship"]
  },
  "data": [...],
  "actions": ["Use investments_lots for per-lot basis", "Use investments_gains for realized gain/loss"]
}
```

## Testing Strategy

### Tier 1 — Unit tests

- **Security resolution:** CUSIP/ISIN → ticker+exchange → name fuzzy precedence;
  ambiguity raises (per `identifiers.md` Guard 2); unknown ref creates-or-prompts.
- **Ledger ingestion:** raw → staging → core carry-through of `type`/`subtype`/`event_group_id`; sign conventions
  for each type; cash-only events (NULL `security_id`/`quantity`).
- **FIFO:** known buys + sells produce correct consumed lots, basis, and ST/LT split,
  including a disposal that splits across short and long terms.
- **HIFO:** highest per-unit-basis lots consume first; equal-cost ties break
  oldest-first; partial consumption; ST/LT split still walks oldest-first.
- **Specific-ID:** `app.lot_selections` overrides FIFO order; partial selection falls
  back to FIFO for the remainder.
- **Average cost:** running average across buys + reinvestments; disposal basis;
  ST/LT still oldest-first; validation rejects `average` on `equity`.
- **Corporate actions:** split adjusts quantities preserving total basis;
  return_of_capital reduces basis pro-rata across multiple open lots;
  transfer_in carries basis + acquisition date; transfer_out realizes no gain;
  a merger expressed as an event-grouped transfer pair preserves total basis
  and holding period end-to-end.
- **Reinvest pairing:** the reinvest convenience writes both rows sharing an
  `event_group_id`; dividend-income aggregation over income-typed rows counts
  reinvested dividends exactly once (the report-layer trap test).
- **Oversold lots:** a disposal exceeding tracked lots realizes the unmatched
  slice at zero basis, sets `basis_incomplete`, and surfaces a warning — the
  rebuild never fails.
- **Holdings:** Σ open lots equals `dim_holdings` quantity/basis; fully-closed lots
  drop out.
- **Method election resolution:** per-security → per-account → global FIFO.

### Tier 2 — Synthetic data verification

- Scenario tests under `tests/scenarios/` (run via `make test-scenarios`) with a
  persona holding a mix of FIFO stocks and an average-cost fund, verifying
  `fct_investment_lots`, `fct_realized_gains`, and `dim_holdings` against ground truth.
- A **1099-B reconciliation scenario**: a hand-labeled full-tax-year ledger
  exercising the whole taxonomy (multiple buys, a sell splitting across ST/LT and
  across multiple FIFO lots, a reinvested dividend, a split, a return of capital,
  and an oversold disposal), with every expected per-lot gain and per-term total
  hand-derived to the cent before the pipeline runs. This is the headline M1J
  test; a real-broker 1099-B replaces the fixture at the milestone tie-out (still
  open — see `investments-overview.md`).

### Tier 3 — Integration

- End-to-end: `investments securities add` → `investments add` (several events) →
  `sqlmesh run` → `investments holdings` / `gains` reflect the ledger.
- Specific-ID flow: record a sell → `investments lots select` → `sqlmesh run` →
  `gains` reflects the chosen lots.
- Method change: `investments securities set <id> --method average` on a fund →
  `sqlmesh run` → basis recomputes.

## Dependencies

- [`investments-overview.md`](investments-overview.md) — umbrella contracts this child implements
- [`architecture-shared-primitives.md`](architecture-shared-primitives.md) — Invariant 8; layer + `updated_at` conventions
- [`account-management.md`](account-management.md) — `app.account_settings` (extended with the method default)
- [`database-migration.md`](database-migration.md) — new tables + the `app.account_settings` ALTER
- [`privacy-data-protection.md`](privacy-data-protection.md) — investment data encrypted at rest via `Database`
- [`moneybin-cli.md`](moneybin-cli.md) — `investments` top-level group (promotes the placeholder)
- `core.dim_accounts` — ledger and lots FK existing accounts

## Out of Scope

- **Market-price valuation / unrealized gain** — Pillar C (`investments-price-feeds.md`).
- **Holdings in net worth** — Pillar D (`investments-net-worth.md`).
- **Plaid / OFX import** — separate children; each adds its own provider-shaped raw table + staging model into the core union (Requirement 7).
- **Options, margin, short positions, derivatives** — future.
- **Wash sales, Schedule D, qualified-dividend logic** — the `us_tax` package.
- **IRS election-policy enforcement** — v1 mirrors the broker.
- **Multi-currency FX conversion** — M1K (the `currency` column lands now; conversion does not).
- **Investment-transaction dedup / transfer detection** — matching children, against this contract.

## Implementation Plan

### Files to Create

- `src/moneybin/sql/schema/app_securities.sql` — DDL for `app.securities`
- `src/moneybin/sql/schema/raw_manual_investment_transactions.sql` — DDL for the manual-entry raw table
- `src/moneybin/sql/schema/app_lot_selections.sql` — DDL for `app.lot_selections`
- `src/moneybin/sqlmesh/models/core/dim_securities.sql` — securities VIEW
- `src/moneybin/sqlmesh/models/prep/stg_manual__investment_transactions.sql` — staging VIEW (per-provider naming convention)
- `src/moneybin/sqlmesh/models/core/fct_investment_transactions.sql` — canonical ledger
- `src/moneybin/investments/cost_basis.py` — the pure cost-basis engine (FIFO/HIFO/specific/average consumption, corporate actions, oversold handling)
- `src/moneybin/investments/sqlmesh_loader.py` — loads ledger events + method/selection resolvers from the SQLMesh `ExecutionContext` for the engine
- `src/moneybin/sqlmesh/models/core/fct_investment_lots.py` — thin SQLMesh wrapper running the engine; derived lots
- `src/moneybin/sqlmesh/models/core/fct_realized_gains.py` — thin SQLMesh wrapper; derived realized gains
- `src/moneybin/sqlmesh/models/core/dim_holdings.sql` — derived positions VIEW
- `src/moneybin/sql/migrations/V034__add_investment_tables.py` — create the three new tables and apply the `app.account_settings` ALTER for existing databases
- `src/moneybin/repositories/securities_repo.py` — `SecuritiesRepo` for `app.securities` mutation (Invariant 10: paired audit rows via `BaseRepo`)
- `src/moneybin/repositories/lot_selections_repo.py` — `LotSelectionsRepo` for `app.lot_selections` mutation
- `src/moneybin/services/investment_service.py` — security resolution, manual entry, method election, lot selection (composes the repos)
- `src/moneybin/privacy/payloads/investments.py` — response payload shapes backing the field-classification-derived sensitivity tiers (MCP Interface)
- `src/moneybin/cli/commands/investments/` — `investments` command group (`__init__.py` for `add`/`list`/`holdings`/`gains`, `securities.py`, `lots.py`)
- `src/moneybin/mcp/tools/investments.py` — `investments_*` tools
- `tests/moneybin/test_services/test_investment_service.py`, `tests/moneybin/test_cli/test_investments.py`,
  `tests/moneybin/test_investments/test_cost_basis_engine.py`, `tests/moneybin/test_investments/test_sqlmesh_loader.py`,
  `tests/moneybin/test_investments/test_investment_models_transform.py`, `tests/moneybin/test_investments_schema.py`,
  `tests/moneybin/test_mcp/test_investments_tools.py`, `tests/moneybin/test_migration_v034.py`,
  `tests/moneybin/test_repositories/test_lot_selections_repo.py`, `tests/moneybin/test_repositories/test_securities_repo.py`
- `tests/scenarios/_investments_seed.py`, `tests/scenarios/data/investments-1099b-reconciliation.yaml`,
  `tests/scenarios/data/investments-persona.yaml`, `tests/scenarios/test_investments_1099b_reconciliation.py`,
  `tests/scenarios/test_investments_persona_correctness.py` — the 1099-B reconciliation and persona-correctness scenarios

### Files to Modify

- `src/moneybin/cli/main.py` — register the top-level `investments` group
- `src/moneybin/cli/commands/accounts/investments.py` — **removed** the placeholder stub (relocated to the top-level group)
- `src/moneybin/cli/commands/accounts/*` + `src/moneybin/mcp/tools/accounts.py` — add `--default-cost-basis-method` flag to `accounts set` and the matching `accounts_set` parameter
- `src/moneybin/schema.py` — register the new DDL files in `_NON_PROVIDER_SCHEMA_FILES`
- `src/moneybin/sql/schema/app_account_settings.sql` — add the `default_cost_basis_method` column for fresh installs (the migration covers existing databases)
- `src/moneybin/repositories/account_settings_repo.py` — extend `AccountSettingsRepo.set()` and `_ACCOUNT_SETTINGS_COLUMNS` with the new column
- `src/moneybin/tables.py` — add table constants (`DIM_SECURITIES`, `FCT_INVESTMENT_TRANSACTIONS`, `FCT_INVESTMENT_LOTS`, `FCT_REALIZED_GAINS`, `DIM_HOLDINGS`)
- `src/moneybin/privacy/taxonomy.py` — field classifications backing the `investments_*` tools' sensitivity tiers
- `src/moneybin/metrics/registry.py` — instrumentation for ingestion + cost-basis runs (per `observability.md`)
- `docs/specs/account-management.md` — document the `accounts set` / `accounts_set` cost-basis-method extension + the new `app.account_settings` column
- `docs/specs/INDEX.md`, `docs/roadmap.md` — status + milestone rows

### Key Decisions

1. **Lots are the universal grain; method is an election.** Every method is a
   computation over the same lot ledger, so picking fewer methods never forces a
   schema migration. The lot grain removes ambiguity rather than adding flexibility.
2. **Realized gain/loss needs no price feed.** The foundation child hits the 1099-B
   bar from the ledger alone; valuation is a later pillar.
3. **Surrogate `security_id` + resolution chain.** Identity survives ticker churn,
   handles crypto, and respects licensed-identifier constraints — coherent with the
   merchant/institution resolution patterns already in the codebase.
4. **Derived, never snapshotted (Invariant 8).** Lots, gains, and holdings rebuild
   from the ledger + `app.*` on every run. The only `app.*` state is the catalog, the
   method election, and lot selections.
5. **Mirror the broker, don't enforce IRS policy.** Keeps average cost bounded to one
   derived path and keeps tax-policy surface in the `us_tax` package.
6. **Lot-selection override is core, not the tax package.** Cost basis is core, so the
   override that determines it must be too — reconciling the `extension-contracts.md`
   inconsistency noted in the overview.
7. **Currency column now, FX later.** A one-way-door column added pre-emptively to
   avoid a breaking `core` migration in M1K.
8. **Top-level `investments` CLI/MCP group.** A four-pillar domain is a peer of
   `accounts`/`transactions`/`assets`, not a subtree under `accounts`.
9. **Type + subtype + event_group_id, not a wider flat enum.** (2026-07-04
   reconciliation.) The closed `type` set is the engine-dispatch contract;
   `subtype` carries tax character and provenance additively; `event_group_id`
   links decomposed legs. Prevents overloading one enum for mechanics,
   reporting, and display — the failure mode surveyed engines converged away
   from.
10. **Reinvest is acquisition-only; income is always its own row.** Uniform
    semantics across manual/Plaid/OFX; the entry surfaces write the pair
    atomically. Income reports sum income types only.
11. **Two-security corporate actions are event-grouped leg pairs**, not new
    enum values — the shape shipped by Rotki and converged on by Portfolio
    Performance's redesign after their split-as-rewrite dead end.
12. **Per-provider raw tables** (`raw.manual_investment_transactions` now;
    provider tables in importer children) — coherent with the cash-transaction
    pipeline; a shared generic raw table was a documented-pattern miss in the
    draft.
13. **Engine decisions confirmed 2026-07-04, no ADRs**: the cost-basis engine
    is a Python SQLMesh model (applies the `fct_balances_daily.py` precedent)
    and the ledger is a new fact table (applies ADR-001's dim/fact pattern) —
    both recorded here and in the PR per `design-principles.md`'s ADR bar.
14. **Four methods in v1 (FIFO, HIFO, specific-ID, average), LIFO on demand.**
    HIFO is a sort-key variant of the FIFO machinery with a real demand signal;
    average cost is 1099-B-motivated and implemented as a running pool (never
    runtime lot-merging). Build order: FIFO → HIFO → specific → average.
