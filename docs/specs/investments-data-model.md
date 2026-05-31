# Feature: Investment Data Model & Cost-Basis Engine

## Status
draft

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
2. **Five security types for v1:** `equity`, `etf`, `mutual_fund`, `bond`, `crypto`,
   plus `other`. New types are added by extending the `CHECK` constraint (a
   lightweight migration via `database-migration.md`); the `other` escape hatch
   absorbs unanticipated instruments without one.
3. **Identity resolution chain.** Free-text or partial security references resolve to
   a `security_id` via CUSIP/ISIN → ticker+exchange → name fuzzy, mirroring the
   institution-resolution chain in `smart-import-financial.md`. v1 resolves against
   `app.securities` (manual-only); the contract is multi-source-ready.
4. **Investment-transaction ledger in `core.fct_investment_transactions`.** One row
   per investment event. The only authored/ingested surface; everything else derives
   from it.
5. **Closed `type` taxonomy** (one-way-door enum), mapped from OFX `<INVTRANLIST>` and
   Plaid so importers slot in cleanly: `buy`, `sell`, `reinvest`, `dividend`,
   `interest`, `capital_gain_distribution`, `transfer_in`, `transfer_out`, `split`,
   `fee`, `return_of_capital`, `other`.
6. **Sign conventions.** `quantity` is signed: positive for acquisitions
   (`buy`/`reinvest`/`transfer_in`), negative for disposals (`sell`/`transfer_out`),
   NULL for cash-only events. `amount` follows the existing accounting convention
   (negative = cash out, e.g. a buy; positive = cash in, e.g. a sell or dividend).
   `amount` is the *total* cash effect **including** fees; the `fees` breakout is the
   portion that increases cost basis on acquisitions and reduces proceeds on disposals.
   So a buy's cost basis is `|amount|` and a sell's net proceeds is `amount`.
7. **Manual entry via raw.** Manual entry writes `raw.investment_transactions`
   (`source_type = 'manual'`) → `prep.stg_*` → `core.fct_investment_transactions`,
   following the CLI-imperative / MCP-declarative-set pattern from
   `transaction-curation.md`. Future importers (Plaid, OFX) write the same raw table.
8. **Derived lots in `core.fct_investment_lots`** (Invariant 8). Each acquisition
   opens a lot; disposals consume lots per the elected method. Each lot carries a
   stable content-hash `lot_id` so specific-ID overrides can reference it.
9. **Derived realized gains in `core.fct_realized_gains`.** One row per
   (disposal, consumed lot) pair — the 1099-B grain: proceeds, cost basis, gain/loss,
   acquisition/disposal dates, and short-/long-term classification.
10. **Derived holdings in `core.dim_holdings`** — current open quantity + cost basis
    per (account, security); the sum of open lots. *(Name locked by `extension-contracts.md`.)*
11. **Three cost-basis methods:** FIFO, specific identification, average cost — all
    computations over the same lot ledger (see [Cost-Basis Engine](#cost-basis-engine)).
12. **Method election** at per-account default (`app.account_settings.default_cost_basis_method`)
    + per-security override (`app.securities.cost_basis_method`). Resolution:
    per-security → per-account → global FIFO. Average cost validates to fund/ETF types.
13. **Specific-ID overrides in `app.lot_selections`** — core `app.*` state (cost basis
    is core, not the `us_tax` package; see the overview's open-question on reconciling
    `extension-contracts.md`).
14. **Mirror, don't enforce.** v1 reproduces the broker's reported method; it does not
    enforce IRS election lock-in or wash-sale rules.
15. **Currency column** on ledger, lots, gains, and holdings now; no FX conversion
    (deferred to M1K).
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

### New table: `app.securities`

Manually-maintained security catalog. Managed via CLI (`investments securities add/set`).

```sql
CREATE TABLE IF NOT EXISTS app.securities (
    security_id VARCHAR NOT NULL PRIMARY KEY,           -- Stable surrogate (truncated UUID4, 12 hex); never derived from ticker
    name VARCHAR NOT NULL,                              -- Human-readable label ("Apple Inc.", "Bitcoin")
    security_type VARCHAR NOT NULL CHECK (security_type IN ('equity', 'etf', 'mutual_fund', 'bond', 'crypto', 'other')), -- Instrument classification
    ticker VARCHAR,                                     -- Exchange ticker ("AAPL"); nullable, not unique (tickers get reused)
    exchange VARCHAR,                                   -- Listing exchange ("NASDAQ"); disambiguates duplicate tickers
    cusip VARCHAR,                                      -- 9-char CUSIP if supplied by user data; licensed — accepted, never redistributed
    isin VARCHAR,                                       -- ISIN if supplied; international identifier
    figi VARCHAR,                                       -- OpenFIGI identifier (open mapping aid); nullable
    coingecko_id VARCHAR,                               -- CoinGecko slug for crypto price lookup (Pillar C); nullable
    cost_basis_method VARCHAR CHECK (cost_basis_method IN ('fifo', 'specific', 'average')), -- Per-security election override; NULL falls back to account default
    currency VARCHAR NOT NULL DEFAULT 'USD',            -- Instrument's denominating currency; no FX conversion in v1
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, -- When the catalog entry was created
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP  -- When last modified; service must set explicitly on UPDATE (DuckDB has no ON UPDATE trigger)
);
```

### New table: `raw.investment_transactions`

Immutable ingestion records. Manual entry and (future) importers both write here.
Carries unresolved source-text security references — resolution to `security_id`
happens at the `core` boundary.

```sql
CREATE TABLE IF NOT EXISTS raw.investment_transactions (
    source_id VARCHAR NOT NULL,                -- Source-provided ID (OFX FITID, Plaid id) or content hash for manual; unique per source
    source_type VARCHAR NOT NULL,              -- Origin tag: 'manual', 'ofx', 'plaid' (closed vocabulary, per matching-overview)
    source_origin VARCHAR,                     -- Institution/connection scope ("fidelity_brokerage", Plaid item_id); scopes matching
    account_ref VARCHAR NOT NULL,              -- Source account reference; resolved to dim_accounts in staging
    security_ticker VARCHAR,                   -- Raw ticker as supplied; input to resolution chain
    security_cusip VARCHAR,                    -- Raw CUSIP as supplied; input to resolution chain
    security_name VARCHAR,                     -- Raw security name as supplied; input to resolution chain
    txn_type VARCHAR NOT NULL,                 -- Raw transaction type; mapped to the core taxonomy in staging
    trade_date DATE NOT NULL,                  -- Trade date (drives holding period); NOT settlement date
    settlement_date DATE,                      -- Settlement date if supplied; informational
    original_acquisition_date DATE,            -- For transfer_in: shares' original acquisition date (holding period transfers in); NULL otherwise
    quantity DECIMAL(28, 10),                  -- Units (high precision for fractional shares / crypto); signed in core
    price DECIMAL(28, 10),                     -- Per-unit price; NULL for non-priced events
    amount DECIMAL(18, 2),                     -- Cash effect; signed in core
    fees DECIMAL(18, 2),                       -- Commissions/fees component; folded into cost basis
    currency VARCHAR,                          -- Denominating currency as supplied
    description VARCHAR,                        -- Free-text description from source
    source_file VARCHAR,                       -- File path/URL for imported sources; NULL for manual
    extracted_at TIMESTAMP,                    -- When fetched/entered
    loaded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, -- When written to raw
    PRIMARY KEY (source_id, source_type)       -- One row per (source id, source) pair; mirrors transaction raw tables
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
        CHECK (default_cost_basis_method IN ('fifo', 'specific', 'average')); -- Per-account default; NULL → global FIFO
```

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
  security_type,      -- equity | etf | mutual_fund | bond | crypto | other
  ticker,             -- Display/lookup ticker (carry the ID per identifiers.md Guard 1)
  exchange,           -- Listing exchange
  cusip,              -- Licensed identifier; present only if user-supplied
  isin,               -- International identifier
  figi,               -- OpenFIGI mapping
  coingecko_id,       -- Crypto price-lookup slug (Pillar C)
  currency            -- Denominating currency
FROM app.securities
-- Future: UNION ALL resolved securities from prep.stg_plaid__securities, etc.
```

### SQLMesh model: `core.fct_investment_transactions` (TABLE)

The canonical ledger. Resolves `account_ref` → `account_id` and the raw security
refs → `security_id` (resolution chain), maps `txn_type` → the core taxonomy, and
applies the sign conventions.

```
Columns:
  investment_transaction_id  VARCHAR          -- Canonical ID (source-provided or content hash)
  account_id                 VARCHAR          -- FK to core.dim_accounts
  security_id                VARCHAR          -- FK to core.dim_securities; NULL for cash-only events (account fee, cash interest)
  trade_date                 DATE             -- Trade date; drives holding-period classification
  settlement_date            DATE             -- Settlement date; informational
  original_acquisition_date  DATE             -- transfer_in only: original acquisition date; lot uses COALESCE(this, trade_date)
  type                       VARCHAR          -- Closed taxonomy (see Requirement 5)
  quantity                   DECIMAL(28,10)   -- Signed units: + acquire, − dispose, NULL cash-only
  price                      DECIMAL(28,10)   -- Per-unit price; NULL for non-priced events
  amount                     DECIMAL(18,2)    -- Signed cash effect: − out (buy), + in (sell/dividend)
  fees                       DECIMAL(18,2)    -- Fee/commission component folded into basis
  currency                   VARCHAR          -- Denominating currency; no FX in v1
  source_type                VARCHAR          -- Origin tag (manual | ofx | plaid)
  source_origin              VARCHAR          -- Institution/connection scope
  description                VARCHAR          -- Free-text description
  updated_at                 TIMESTAMP        -- Row freshness per core-updated-at-convention
```

### SQLMesh model: `core.fct_investment_lots` (TABLE, derived)

Each acquisition opens a lot; disposals consume open lots per the resolved method.
Likely a **Python SQLMesh model** — the consumption logic (FIFO cursor, average-cost
running aggregate, specific-ID override lookup) is awkward in pure SQL (see Open
Questions). Lot identity is a content hash so it is stable across rebuilds and
referenceable by `app.lot_selections`.

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
  cost_basis_method       VARCHAR          -- Resolved method that governed this lot's consumption
  currency                VARCHAR          -- Denominating currency
  is_open                 BOOLEAN          -- remaining_quantity > 0
  source_transaction_id   VARCHAR          -- FK to the opening core.fct_investment_transactions row
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
  currency            VARCHAR          -- Denominating currency
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
  currency          VARCHAR          -- Denominating currency
  updated_at        TIMESTAMP        -- Row freshness
```

> **Unrealized gain/loss** (`quantity × current_price − cost_basis`) is intentionally
> absent here — it requires a price, which Pillar C (`investments-price-feeds.md`)
> supplies. `dim_holdings` v1 carries cost basis only.

## Cost-Basis Engine

All three methods are computations over `core.fct_investment_lots`. The engine walks
the ledger per (account, security) in trade-date order, opening lots on acquisitions
and consuming them on disposals.

> **The method set is intentionally closed to the three v1 methods.** The
> `cost_basis_method` `CHECK` (on `app.securities` and `app.account_settings`) allows
> only `fifo`, `specific`, `average` on purpose — electing a method the engine does not
> implement would silently miscompute basis, so the constraint is a guard, not an
> oversight. HIFO/LIFO (listed as future methods in `investments-overview.md`) are added
> by widening the `CHECK` when their engine paths ship — a lightweight `app.*` migration,
> the same deliberate trade-off as `security_type`.

### Short-term / long-term split (shared across all methods)

Holding period is always determined per-lot, oldest-first, regardless of method —
only the *basis number* differs. A disposal held ≤ 1 year from the lot's
`acquisition_date` is `short`; > 1 year is `long`. A single disposal can split across
both terms when it consumes multiple lots.

### Method: FIFO (default)

Disposals consume open lots in ascending `acquisition_date`. Each consumed slice
contributes its actual per-unit basis. The simplest path and the IRS default.

### Method: Specific identification

Before falling back to FIFO order, the engine reads `app.lot_selections` for the
disposal. Selected lots are consumed in the specified quantities; any unselected
remainder falls back to FIFO. This unlocks tax-loss harvesting and ST/LT control.
Shares the FIFO consumption machinery — it is an override on consumption order, not a
separate engine.

### Method: Average cost

Basis per disposed unit = (remaining pooled cost ÷ remaining pooled units) at the
moment of disposal — a running average that every acquisition/reinvestment mutates.
The ST/LT split still walks lots oldest-first (only the basis is averaged). Validated
to `mutual_fund` / `etf` security types. This is the one genuinely distinct
computation; it adds a single derived path, not a parallel system.

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

- `split` adjusts open-lot `original_quantity`/`remaining_quantity` by the split ratio
  while preserving `cost_basis_total` (per-unit basis changes, total does not).
- `return_of_capital` reduces `cost_basis_remaining` without creating a disposal.
- `reinvest` opens a new lot (acquisition) and records income.
- `transfer_in` opens a lot whose `acquisition_date` is the shares' **original**
  acquisition date (holding period transfers with the shares — it is *not* reset to
  the transfer date), carrying the supplied basis. Manual entry therefore accepts an
  `--acquired DATE` (original acquisition) and `--basis AMOUNT` for transfers; the
  ledger persists the original date in `original_acquisition_date`, and the lot uses
  `COALESCE(original_acquisition_date, trade_date)`. `transfer_out` consumes lots
  without proceeds (no realized gain).

## CLI Interface

All commands under a top-level `investments` group (promoting the placeholder
`accounts investments` in [`moneybin-cli.md`](moneybin-cli.md), peer to `accounts`,
`transactions`, and `assets`). All support `--output json`.

### Ledger

```
moneybin investments add --account <id|name> --security <ticker|name> \
    --type buy --date 2024-01-15 --quantity 10 --price 150.00 \
    [--fees 4.95] [--currency USD] [--notes "..."]
```
- Records one event in `raw.investment_transactions` (`source_type=manual`); resolves
  `--security` via the resolution chain, prompting to create a catalog entry if unknown.

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
moneybin investments lots select <disposal_txn_id> --lot <lot_id>:<quantity> [--lot <lot_id>:<quantity> ...] [--yes]
moneybin investments lots select <disposal_txn_id> --clear [--yes]
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
- **Per-account default** → `moneybin accounts set <id> --default-cost-basis-method fifo|specific|average`.
- **Per-security override** → `moneybin investments securities set <id> --method fifo|specific|average`
  (`average` validates to fund/ETF security types).

> **Surface extension to `account-management.md`.** The `--default-cost-basis-method`
> flag on `accounts set`, the matching `default_cost_basis_method` parameter on the
> `accounts_set` MCP tool, and the `app.account_settings.default_cost_basis_method`
> column (the ALTER above) are all **added by this spec** — `account-management.md`
> (status `implemented`) does not yet carry them. Implementing this spec updates the
> accounts command, the `accounts_set` tool, and the `account-management.md` surface
> tables accordingly.

### Securities catalog

```
moneybin investments securities list [--type equity] [--output json|table]
moneybin investments securities add --name "Apple Inc." --type equity \
    --ticker AAPL [--exchange NASDAQ] [--cusip ...] [--coingecko-id ...]
moneybin investments securities set <security_id> [--name ...] [--ticker ...] \
    [--cusip ...] [--method fifo|specific|average]
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
- Sensitivity: `medium` (positions/amounts)

**`investments_holdings`** — Current positions with cost basis.
- Params: `account` (optional)
- Sensitivity: `medium`

**`investments_lots`** — Open/closed lots.
- Params: `account` (optional), `security` (optional), `open_only` (BOOLEAN, default true)
- Sensitivity: `medium`

**`investments_gains`** — Realized gain/loss (1099-B surface).
- Params: `account` (optional), `security` (optional), `from`/`to` (optional DATE), `term` (optional)
- Sensitivity: `medium`

**`investments_securities`** — The catalog.
- Params: `security_type` (optional)
- Sensitivity: `low` (reference data, no amounts)

### Write tools

Per `surface-design.md` — one tool per operation shape, no polymorphic `*_set` catch-all.

**`investments_record`** — Shape 3 (discrete batch event). Record one or more investment
events; resolves securities, reports unresolved refs in `warnings`.

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
    "sensitivity": "medium",
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
- **Ledger ingestion:** raw → core mapping of `txn_type` → taxonomy; sign conventions
  for each type; cash-only events (NULL `security_id`/`quantity`).
- **FIFO:** known buys + sells produce correct consumed lots, basis, and ST/LT split,
  including a disposal that splits across short and long terms.
- **Specific-ID:** `app.lot_selections` overrides FIFO order; partial selection falls
  back to FIFO for the remainder.
- **Average cost:** running average across buys + reinvestments; disposal basis;
  ST/LT still oldest-first; validation rejects `average` on `equity`.
- **Corporate actions:** split adjusts quantities preserving total basis;
  return_of_capital reduces basis; transfer_in carries basis + acquisition date;
  transfer_out realizes no gain.
- **Holdings:** Σ open lots equals `dim_holdings` quantity/basis; fully-closed lots
  drop out.
- **Method election resolution:** per-security → per-account → global FIFO.

### Tier 2 — Synthetic data verification

- Scenario tests under `tests/scenarios/` (run via `make test-scenarios`) with a
  persona holding a mix of FIFO stocks and an average-cost fund, verifying
  `fct_investment_lots`, `fct_realized_gains`, and `dim_holdings` against ground truth.
- A **1099-B reconciliation scenario**: a hand-labeled fixture from a real broker
  1099-B for a full tax year (buys, sells, reinvested dividends, and any broker
  adjustments present in the fixture), with expected realized gains per term that
  the engine must match exactly. This is the headline M1J test.

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
- **Plaid / OFX import** — separate children; they write the same `raw.investment_transactions`.
- **Options, margin, short positions, derivatives** — future.
- **Wash sales, Schedule D, qualified-dividend logic** — the `us_tax` package.
- **IRS election-policy enforcement** — v1 mirrors the broker.
- **Multi-currency FX conversion** — M1K (the `currency` column lands now; conversion does not).
- **Investment-transaction dedup / transfer detection** — matching children, against this contract.

## Implementation Plan

### Files to Create

- `src/moneybin/sql/schema/app_securities.sql` — DDL for `app.securities`
- `src/moneybin/sql/schema/raw_investment_transactions.sql` — DDL for the raw ledger
- `src/moneybin/sql/schema/app_lot_selections.sql` — DDL for `app.lot_selections`
- `sqlmesh/models/core/dim_securities.sql` — securities VIEW
- `sqlmesh/models/prep/stg_investment_transactions.sql` — staging VIEW (resolve + map + sign)
- `sqlmesh/models/core/fct_investment_transactions.sql` — canonical ledger
- `sqlmesh/models/core/fct_investment_lots.py` — derived lots (Python model; cost-basis engine)
- `sqlmesh/models/core/fct_realized_gains.py` — derived realized gains
- `sqlmesh/models/core/dim_holdings.sql` — derived positions VIEW
- `src/moneybin/services/investment_service.py` — security resolution, manual entry, method election, lot selection
- `src/moneybin/cli/commands/investments.py` — `investments` command group
- `src/moneybin/mcp/tools/investments.py` — `investments_*` tools
- `tests/test_investment_service.py`, `tests/test_cli_investments.py`, `tests/test_cost_basis_engine.py`
- `tests/scenarios/investments_1099b/` — the 1099-B reconciliation scenario

### Files to Modify

- `src/moneybin/cli/main.py` — register the top-level `investments` group
- `src/moneybin/cli/commands/accounts/investments.py` — **remove** the placeholder stub (relocated to the top-level group)
- `src/moneybin/cli/commands/accounts/*` + `src/moneybin/mcp/tools/accounts*` — add `--default-cost-basis-method` flag to `accounts set` and the matching `accounts_set` parameter
- `src/moneybin/sql/schema.py` — register new DDL + the `app.account_settings` ALTER
- `src/moneybin/tables.py` — add table constants (`DIM_SECURITIES`, `FCT_INVESTMENT_TRANSACTIONS`, `FCT_INVESTMENT_LOTS`, `FCT_REALIZED_GAINS`, `DIM_HOLDINGS`)
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
```
