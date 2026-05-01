# Feature: Asset Tracking

## Status
draft

## Goal

Track non-financial physical assets (real estate, vehicles, valuables) with periodic valuations, and include them in net worth calculations alongside cash account balances. Assets are fundamentally different from accounts — they have appraisals, not transactions — and deserve their own first-class data model.

## Background

Net worth is incomplete without physical assets. The [net worth spec](net-worth.md) provides balance tracking for financial accounts (checking, savings, credit cards, loans), but a house, car, or piece of jewelry has no account, no transactions, and no institution. Its value comes from periodic appraisals or estimates, not from summing debits and credits.

The dividing line between assets and investments: **if the value comes from a market ticker, it's an investment. If it comes from an appraisal or estimate, it's an asset.** Gold ETFs, crypto, and brokerage holdings belong in the future `investment-tracking.md` spec. Houses, cars, and jewelry belong here.

Related specs and docs:
- [`net-worth.md`](net-worth.md) — balance tracking and `agg_net_worth`; this spec extends it to include physical assets
- [`cli-restructure.md`](cli-restructure.md) — target CLI command tree; asset commands live under `track asset`
- [`privacy-data-protection.md`](privacy-data-protection.md) — asset data encrypted at rest via `Database` class
- [`database-migration.md`](database-migration.md) — migration infrastructure for new tables
- [`mcp-architecture.md`](mcp-architecture.md) — tool taxonomy and response envelope conventions

## Requirements

1. **Asset registry in `app.assets`.** Users create assets with a name, type, optional acquisition info, and optional link to a liability account. Assets are user-authored entities — no external system creates them.
2. **Four asset types for v1:** `real_estate`, `vehicle`, `valuable`, `other`. Extensible by adding new type values without migration.
3. **Manual valuations in `app.asset_valuations`.** Users set a value for an asset as of a date. Upsert semantics — setting a value on an existing date replaces it.
4. **External valuation sources (future).** External providers (Zillow, KBB, appraisal services) land in `raw.*` tables, flow through `prep.stg_*` views, and union into `core.fct_asset_valuations` alongside manual valuations. Not built in v1 — the architecture supports it without schema changes.
5. **Union all valuation sources into `core.fct_asset_valuations`** — a SQLMesh VIEW that normalizes every valuation to a common shape: `(asset_id, valuation_date, value, source_type, source_ref)`.
6. **Materialize `core.fct_asset_valuations_daily`** — a SQLMesh TABLE with one row per asset per day. Carries forward the latest valuation until a new one arrives. Simpler than account balance carry-forward — no transaction adjustments needed.
7. **Extend `core.agg_net_worth`** to include asset valuations. Net worth = sum of account balances + sum of asset valuations. Disposed assets stop contributing after their disposal date.
8. **Liability linking.** An asset can optionally reference a liability account in `dim_accounts` (e.g., a mortgage for a house, an auto loan for a car). This link is informational — it enables equity display (`value - liability balance`) but is not used in net worth arithmetic. Both the asset value and the liability balance contribute to net worth independently.
9. **Staleness warnings.** Each valuation in `fct_asset_valuations_daily` tracks days since the last real observation. Warnings surface in CLI output and MCP responses when a valuation exceeds its staleness threshold. Warnings are informational only — stale values are still included in net worth.
10. **Staleness threshold resolution:** per-asset override → per-type default → global config default.
11. **Asset disposal.** Assets can be marked as sold with a date and sale amount. Disposed assets stop contributing to net worth but their history is preserved.
12. **CLI commands** under `track asset` (see CLI Interface section).
13. **MCP tools:** `assets.list`, `assets.detail`, `assets.summary` (see MCP Interface section).
14. **All commands support `--output json`** for non-interactive parity.
15. **Source precedence within a day.** When multiple valuation sources exist for the same asset on the same date: manual (user assertion) > appraisal > automated estimate (Zillow/KBB).

## Data Model

### New table: `app.assets`

User-created asset registry. Managed via CLI (`track asset add/update/sell/remove`).

```sql
CREATE TABLE IF NOT EXISTS app.assets (
    asset_id VARCHAR NOT NULL PRIMARY KEY,              -- Truncated UUID4 (12 hex chars)
    name VARCHAR NOT NULL,                              -- Human-readable label ("123 Main St", "2021 Tesla Model 3")
    asset_type VARCHAR NOT NULL CHECK (asset_type IN ('real_estate', 'vehicle', 'valuable', 'other')), -- Asset classification
    description VARCHAR,                                -- Optional free-text details about the asset
    acquisition_date DATE,                              -- Date the asset was acquired; NULL if unknown
    acquisition_cost DECIMAL(18, 2),                    -- Original purchase price; NULL if unknown; enables gain/loss tracking
    disposal_date DATE,                                 -- Date the asset was sold or disposed; NULL if still owned
    disposal_amount DECIMAL(18, 2),                     -- Sale price or settlement amount; NULL if still owned
    liability_account_id VARCHAR,                       -- Optional FK to core.dim_accounts (mortgage, auto loan); informational only
    staleness_threshold_days INTEGER,                   -- Per-asset staleness override in days; NULL falls back to type/global default
    include_in_net_worth BOOLEAN NOT NULL DEFAULT TRUE, -- Whether this asset contributes to net worth calculations
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, -- When the asset record was created
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP  -- When the asset record was last modified; service layer must set explicitly on UPDATE (DuckDB has no ON UPDATE trigger)
);
```

### New table: `app.asset_valuations`

User-entered valuations. Managed via CLI (`track asset value set/unset`).

```sql
CREATE TABLE IF NOT EXISTS app.asset_valuations (
    asset_id VARCHAR NOT NULL,       -- FK to app.assets; identifies which asset this valuation applies to
    valuation_date DATE NOT NULL,    -- Date the valuation applies to (when the asset was worth this amount)
    value DECIMAL(18, 2) NOT NULL,   -- Estimated value of the asset as of the valuation date
    notes VARCHAR,                   -- Optional context for the valuation ("2024 county appraisal", "Zillow estimate")
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, -- When this valuation record was entered
    PRIMARY KEY (asset_id, valuation_date) -- One manual valuation per asset per date
);
```

### Future: External valuation sources (raw layer)

When external providers ship, each gets its own raw table following existing patterns. Not built in v1 — included to show the extension point.

```sql
-- Example: raw.zillow_valuations (future, not built in v1)
CREATE TABLE IF NOT EXISTS raw.zillow_valuations (
    asset_id VARCHAR NOT NULL,              -- FK to app.assets; links valuation to an asset
    valuation_date DATE NOT NULL,           -- Date the estimate applies to
    value DECIMAL(18, 2) NOT NULL,          -- Zillow's estimated value
    zestimate_range_low DECIMAL(18, 2),     -- Low end of Zestimate confidence range
    zestimate_range_high DECIMAL(18, 2),    -- High end of Zestimate confidence range
    source_file VARCHAR,                    -- Path or URL of the data source
    extracted_at TIMESTAMP,                 -- When the data was fetched from Zillow
    loaded_at TIMESTAMP                     -- When the record was written to the raw table
);
```

Adding a new provider = new raw table, new `prep.stg_*__valuations` view, new CTE + `UNION ALL` in `core.fct_asset_valuations`. Same pattern as balance observation sources in the net worth spec.

### SQLMesh model: `core.fct_asset_valuations` (VIEW)

Unions all valuation sources into a normalized shape. One row per observation.

```sql
MODEL (
  name core.fct_asset_valuations,
  kind VIEW
);

WITH manual_valuations AS (
  SELECT
    asset_id,          -- FK to app.assets
    valuation_date,    -- Date the valuation applies to
    value,             -- Estimated value
    'manual' AS source_type, -- Valuation source: always 'manual' for user-entered
    'user' AS source_ref     -- Source reference: always 'user' for manual entry
  FROM app.asset_valuations
)
-- Future: add CTEs for stg_zillow__valuations, stg_kbb__valuations, etc.
SELECT
  asset_id,        -- FK to app.assets
  valuation_date,  -- Date the valuation applies to
  value,           -- Estimated value
  source_type,     -- Origin of the valuation: manual, zillow, kbb, appraisal, etc.
  source_ref       -- Source reference: user, filename, URL, etc.
FROM manual_valuations
```

### SQLMesh model: `core.fct_asset_valuations_daily` (TABLE)

One row per asset per day. Materialized as a FULL table — recomputed on every `sqlmesh run`. Simpler than `fct_balances_daily` because there are no transactions to adjust for — just carry the latest valuation forward. FULL is acceptable at v1 scale (dozens of assets, years of daily rows). If it becomes a bottleneck, switch to INCREMENTAL with a date-spine watermark — easy to add later since downstream consumers already expect complete daily coverage.

For each asset:
1. Find all valuations from `fct_asset_valuations`.
2. For each day between the first valuation and either `disposal_date` or today, carry forward the most recent valuation.
3. Compute `days_since_observed` for staleness tracking.

```
Columns:
  asset_id             VARCHAR        -- FK to app.assets; identifies the asset
  valuation_date       DATE           -- Calendar date for this row
  value                DECIMAL(18,2)  -- Value as of this date (observed or carried forward)
  is_observed          BOOLEAN        -- TRUE if a real valuation exists for this date; FALSE if carried forward
  observation_source   VARCHAR        -- source_type of the valuation (manual, zillow, etc.); NULL if carried forward
  days_since_observed  INTEGER        -- Days since the last real valuation; powers staleness warnings
```

Only produces rows between the first valuation and either `disposal_date` (for disposed assets) or the current date (for active assets). Assets with zero valuations produce no rows.

**Source precedence within a day:** When multiple sources provide a valuation for the same asset on the same date, precedence is: manual > appraisal > automated estimate (zillow, kbb). Most authoritative source wins. Implementation: use `ROW_NUMBER()` with a `CASE WHEN source_type = 'manual' THEN 1 WHEN source_type = 'appraisal' THEN 2 ELSE 3 END` ordering to make the precedence explicit and stable.

**Implementation note:** The date spine generation and carry-forward logic can use DuckDB's `generate_series` for the date spine and window functions with `IGNORE NULLS` for carry-forward, matching the approach in `fct_balances_daily`.

### Extending `core.agg_net_worth`

The existing net worth view (defined in [`net-worth.md`](net-worth.md)) is extended to include asset valuations:

```sql
MODEL (
  name core.agg_net_worth,
  kind VIEW
);

WITH account_values AS (
  SELECT
    d.balance_date,                          -- Calendar date
    d.balance AS value,                      -- Account balance (positive or negative)
    'account' AS worth_type                  -- Distinguishes accounts from assets in the aggregation
  FROM core.fct_balances_daily AS d
  LEFT JOIN app.account_settings AS s
    ON d.account_id = s.account_id
  WHERE COALESCE(s.include_in_net_worth, TRUE)
),
asset_values AS (
  SELECT
    v.valuation_date AS balance_date,        -- Calendar date (aliased to match account_values)
    v.value,                                 -- Asset valuation (always positive)
    'asset' AS worth_type                    -- Distinguishes assets from accounts in the aggregation
  FROM core.fct_asset_valuations_daily AS v
  INNER JOIN app.assets AS a
    ON v.asset_id = a.asset_id
  WHERE a.include_in_net_worth = TRUE
    AND (a.disposal_date IS NULL OR v.valuation_date <= a.disposal_date)
),
combined AS (
  SELECT balance_date, value, worth_type FROM account_values
  UNION ALL
  SELECT balance_date, value, worth_type FROM asset_values
)
SELECT
  balance_date,                                                              -- Calendar date
  SUM(value) AS net_worth,                                                   -- Total net worth across all accounts and assets
  SUM(CASE WHEN value > 0 THEN value ELSE 0 END) AS total_assets,           -- Sum of positive values (cash + physical assets)
  SUM(CASE WHEN worth_type = 'asset' THEN value ELSE 0 END) AS total_physical_assets, -- Sum of physical asset valuations only
  SUM(CASE WHEN value < 0 THEN value ELSE 0 END) AS total_liabilities,      -- Sum of negative values (credit cards, loans)
  SUM(CASE WHEN worth_type = 'account' THEN 1 ELSE 0 END) AS account_count, -- Number of account rows contributing on this date
  SUM(CASE WHEN worth_type = 'asset' THEN 1 ELSE 0 END) AS asset_count      -- Number of asset rows contributing on this date
FROM combined
GROUP BY balance_date
```

**Double-counting prevention:** The liability linked to an asset (e.g., a mortgage) is already a separate account with a negative balance. The asset's full value and the liability's negative balance both contribute to net worth independently — their sum is the correct equity. The `liability_account_id` link is informational only, used for display ("House: $500k value, $350k mortgage, $150k equity"), not for arithmetic.

## Staleness Configuration

### Default thresholds by asset type

| Asset type | Default staleness (days) | Rationale |
|---|---|---|
| `real_estate` | 180 | Property values shift slowly; semi-annual recheck is reasonable |
| `vehicle` | 90 | Steady depreciation, seasonal market shifts |
| `valuable` | 365 | Collectibles and jewelry — values rarely checked |
| `other` | 180 | Conservative middle ground |

### Global override

A new field in `MoneyBinSettings`:

```python
asset_staleness_default_days: int = (
    180  # Global fallback when no per-type or per-asset threshold is set
)
```

### Resolution order

1. `app.assets.staleness_threshold_days` (per-asset override)
2. Per-type default from the table above
3. `MoneyBinSettings.asset_staleness_default_days` (global config)

### Where staleness surfaces

- **`track asset list`** — warning indicator next to stale assets with days since last valuation
- **`track networth show`** — summary note: "N assets have stale valuations" with asset names
- **MCP tools** — `summary.warnings` array includes stale asset notices

Staleness is informational only — never blocks queries or omits stale assets from net worth. The value is still included; the system tells you it might be outdated.

## CLI Interface

All commands under `track asset`, following [`cli-restructure.md`](cli-restructure.md). All support `--output json` for non-interactive parity.

### Asset management

```
moneybin track asset add --name "123 Main St" --type real_estate \
    [--description "Primary residence"] \
    [--acquisition-date 2020-03-15] [--acquisition-cost 450000] \
    [--liability-account-id <account_id>] [--staleness-days 180]
```
- Creates asset in `app.assets`, returns the generated `asset_id`
- `--type` validates against the four known types (`real_estate`, `vehicle`, `valuable`, `other`)

```
moneybin track asset list [--type real_estate] [--include-disposed] [--output json|table]
```
- Default: active assets only (no `disposal_date`)
- Shows: name, type, latest valuation, days since valuation, staleness warning if applicable

```
moneybin track asset show <asset_id> [--output json|table]
```
- Detail view: all asset fields, valuation history, linked liability balance if present, gain/loss vs acquisition cost

```
moneybin track asset update <asset_id> [--name "..."] [--description "..."] \
    [--liability-account-id <id>] [--staleness-days 90]
```
- Updates any mutable field on the asset

```
moneybin track asset sell <asset_id> --date 2025-06-01 --amount 35000 [--notes "Trade-in"] [--yes]
```
- Sets `disposal_date` and `disposal_amount`
- Asset stops contributing to net worth after disposal date
- History preserved; asset visible with `--include-disposed`

```
moneybin track asset remove <asset_id> [--yes]
```
- Hard delete of asset and all its valuations
- Confirmation required
- For "I entered this by mistake," not for selling

### Valuation management

```
moneybin track asset value set <asset_id> --date 2025-01-15 --value 475000 [--notes "Zillow estimate"]
```
- Inserts or updates a row in `app.asset_valuations` (upsert on `asset_id + valuation_date`)

```
moneybin track asset value history <asset_id> [--from DATE] [--to DATE] [--output json|table]
```
- Shows valuation history for an asset over time

```
moneybin track asset value unset <asset_id> --date 2025-01-15 [--yes]
```
- Removes a specific manual valuation

### Example output

```
$ moneybin track asset list

Name                Type          Value        Last Valued    Status
123 Main St         real_estate   $475,000     2025-01-15     ✅ Current
2021 Tesla Model 3  vehicle       $28,500      2024-09-01     ⚠️ Stale (234 days)
Grandma's Ring      valuable      $12,000      2024-03-01     ✅ Current
```

```
$ moneybin track networth show

Net Worth: $347,250 as of 2025-04-23

  Cash accounts:    $82,750  (3 accounts)
  Physical assets:  $515,500  (3 assets)
  Liabilities:     -$251,000  (1 account)

  ⚠️ 1 asset has a stale valuation: 2021 Tesla Model 3 (234 days)
```

## MCP Interface

### Tools

**`assets.list`** — List assets with current valuations and staleness status.
- Params: `asset_type` (optional VARCHAR), `include_disposed` (optional BOOLEAN, default false)
- Sensitivity: `medium` (asset names and values)
- Returns: assets with latest valuation, days since valuation, staleness warning, linked liability balance

**`assets.detail`** — Full detail for a single asset including valuation history.
- Params: `asset_id` (VARCHAR)
- Sensitivity: `medium` (individual asset details)
- Returns: all asset fields, valuation history, linked liability info, gain/loss vs acquisition cost

**`assets.summary`** — Aggregate asset value by type.
- Params: `as_of_date` (optional DATE)
- Sensitivity: `low` (aggregates only)
- Returns: total value, breakdown by type, count, stale asset warnings

### Response envelope

Follows the standard envelope from [`mcp-architecture.md`](mcp-architecture.md):

```json
{
  "summary": {
    "total_count": 3,
    "sensitivity": "medium",
    "display_currency": "USD",
    "warnings": ["1 asset has a stale valuation: 2021 Tesla Model 3 (234 days)"]
  },
  "data": [...],
  "actions": ["Use assets.detail for full history", "Use 'track asset value set' to update stale valuations"]
}
```

### Write tools

Deferred to v2, same as balance assertion write tools in the net worth spec. The `track asset` CLI handles the low-frequency asset management workflow for v1.

### Net worth tools (existing, extended)

`get_net_worth` and `get_net_worth_history` from the net worth spec automatically include assets via the extended `agg_net_worth` view — no new tools needed. The response gains a `total_physical_assets` field alongside `total_assets` and `total_liabilities`.

## Testing Strategy

### Tier 1 — Unit tests

- **Asset CRUD:** Create, update, sell, remove assets via the service layer. Verify `app.assets` state after each operation.
- **Valuation set/unset:** Verify upsert semantics (set on new date inserts, set on existing date updates). Verify unset removes the correct row.
- **Daily carry-forward:** Given known valuations, verify `fct_asset_valuations_daily` fills gaps correctly. Verify `days_since_observed` is accurate.
- **Staleness threshold resolution:** Verify per-asset → per-type → global fallback chain.
- **Net worth integration:** Verify `agg_net_worth` includes asset valuations. Verify disposed assets are excluded after disposal date. Verify `include_in_net_worth = FALSE` excludes an asset.
- **Liability linking:** Verify equity display calculation (asset value - linked liability balance) without affecting net worth arithmetic.
- **Edge cases:**
  - Assets with no valuations → no `fct_asset_valuations_daily` rows
  - Assets with a single valuation → rows from that date to today (or disposal date)
  - Multiple valuation sources on the same date → precedence applies
  - Disposed asset → rows stop after disposal date
  - Asset excluded from net worth → still in `fct_asset_valuations_daily`, absent from `agg_net_worth`

### Tier 2 — Synthetic data verification

- Scenario tests under `tests/scenarios/` (run via `make test-scenarios`) that include assets with known valuations and verify `fct_asset_valuations_daily` and `agg_net_worth` match expected values.
- Staleness scenarios: assets with old valuations trigger warnings at the correct thresholds.

### Tier 3 — Integration

- End-to-end: `track asset add` → `track asset value set` → `sqlmesh run` → `track networth show` → verify asset included in net worth.
- Disposal flow: `track asset sell` → `sqlmesh run` → verify asset excluded from net worth after disposal date.
- Liability linking: add asset with `--liability-account-id` → `track asset show` → verify equity display.
- Staleness surfacing: set an old valuation → `track asset list` → verify warning appears.

## Dependencies

- [`net-worth.md`](net-worth.md) — `agg_net_worth` view is extended; this spec cannot ship before or independently of net worth
- [`database-migration.md`](database-migration.md) — new tables (`app.assets`, `app.asset_valuations`) require migration infrastructure
- [`privacy-data-protection.md`](privacy-data-protection.md) — asset data is sensitive; encrypted at rest via `Database` class
- [`cli-restructure.md`](cli-restructure.md) — `track asset` namespace defined by the CLI structure spec
- `core.dim_accounts` — liability linking references existing dimension

## Out of Scope

- **Investment holdings** — brokerage accounts, 401k, IRA, crypto, commodities (gold/silver). Market-priced instruments belong in `investment-tracking.md`. The dividing line: if the value comes from a market ticker, it's an investment; if from an appraisal or estimate, it's an asset.
- **Automated valuation integrations** — Zillow, KBB, and similar APIs. The data model supports them (raw → prep → core pipeline), but no provider is built in v1. Adding one is a separate child spec.
- **Asset depreciation schedules** — automatic depreciation calculations (straight-line, declining balance). Users can model this via periodic valuations.
- **Insurance tracking** — policy details, coverage amounts, premium costs. Related but separate domain.
- **Asset categories/tags** — beyond the four types. Could be added later without schema changes.
- **Multi-currency assets** — all valuations assumed single-currency for v1, matching the net worth spec.
- **Asset photos/documents** — attachments like deeds, titles, appraisal PDFs. Separate feature.

## Implementation Plan

### Files to Create

- `src/moneybin/sql/schema/app_assets.sql` — DDL for `app.assets`
- `src/moneybin/sql/schema/app_asset_valuations.sql` — DDL for `app.asset_valuations`
- `sqlmesh/models/core/fct_asset_valuations.sql` — VIEW unioning all valuation sources
- `sqlmesh/models/core/fct_asset_valuations_daily.sql` — TABLE with daily carry-forward logic (may be Python model)
- `src/moneybin/services/asset_service.py` — business logic for asset CRUD and valuation management
- `src/moneybin/cli/commands/asset.py` — `track asset` command group
- `tests/test_asset_service.py` — unit tests for asset service
- `tests/test_cli_asset.py` — CLI integration tests

### Files to Modify

- `src/moneybin/cli/main.py` — register `track asset` command group
- `src/moneybin/sql/schema.py` — register new DDL files for `app.assets` and `app.asset_valuations`
- `src/moneybin/config.py` — add `asset_staleness_default_days` to `MoneyBinSettings`
- `sqlmesh/models/core/agg_net_worth.sql` — extend to include asset valuations (created by net worth spec, modified here)
- `src/moneybin/mcp/tools/` — add `assets.list`, `assets.detail`, `assets.summary` tools
- `docs/specs/INDEX.md` — add entry for this spec

### Key Decisions

1. **Assets are not accounts.** Accounts have transactions and balances; assets have appraisals and valuations. Separate data models, united only at the net worth aggregation layer.
2. **Manual valuations in `app`, external in `raw`.** Follows the existing layer conventions — user-authored state in `app`, external source data in `raw` flowing through `prep` to `core`.
3. **Liability linking is informational.** The FK to `dim_accounts` enables equity display but does not affect net worth arithmetic. No double-counting risk.
4. **Staleness is informational.** Warnings surface in CLI and MCP but never block queries or omit values from net worth.
5. **`sell` not `dispose`.** Natural language for the 90% case. Edge cases (gift, loss) handled with `--amount 0` and `--notes`.
6. **`value set/unset`** — declarative verb pair for valuations. "Set" handles both insert and update. "Unset" is the natural inverse.
7. **Source precedence mirrors net worth.** Manual > appraisal > automated estimate, same philosophy as balance observation precedence.
8. **Cash-only v1 alignment.** This spec extends net worth v1 with physical assets while keeping investments and multi-currency as future work.
