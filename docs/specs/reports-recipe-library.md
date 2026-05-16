# Reports Recipe Library

## Status

- **Type:** Feature
- **Status:** implemented
- **Milestone:** M2C ("brand surface" cluster — recipe library + `moneybin doctor`)

## Goal

Ship the first wave of `reports.*` SQLMesh views — eight curated, named, queryable presentation models that back the `moneybin reports *` CLI surface and the `reports_*` MCP tools. Establish the read-only `reports` schema as a first-class consumer interface (per [`architecture-shared-primitives.md`](architecture-shared-primitives.md)), and make MoneyBin's "show me the SQL" demo land: every number a user or AI sees has a named SQLMesh model file behind it that can be inspected, modified, and re-queried.

Bundle in three migrations that should ship alongside the inaugurating `reports.*` work:

1. **`core.agg_net_worth` → `reports.net_worth`** — the cascading edit deferred from `architecture-shared-primitives.md` §Cascading Edits.
2. **`app.categories` (Python-built view) → `core.dim_categories` (SQLMesh model)** — closes a pre-existing layer-rule violation where `app.categories` is exposed to consumers directly. Same pattern as `core.dim_accounts`.
3. **`app.merchants` (Python-built view) → `core.dim_merchants` (SQLMesh model)** — same fix shape as categories. Forces the categorization-service merchant write path (`INSERT INTO MERCHANTS`, currently aimed at a UNION view) to route correctly to `USER_MERCHANTS`.

The dim migrations are bundled because we are already touching the `core/` SQLMesh layer, the `TableRef` registry, and the schema-discoverability surface in this spec — landing them now avoids re-opening the same surface twice.

## Background

A starter library of named SQL models — one per common question a user or AI consumer will ask — is the read-only counterpart to the `transactions`, `accounts`, and `import` write surfaces. The CLI and MCP `reports` namespaces (per [`cli-restructure.md`](cli-restructure.md) v2) need backing models; without them, those subcommands are stubs.

Beyond surface mechanics, the recipe library is what makes MoneyBin's lineage story land. When an AI consumer says "you spent $4,200 on dining last quarter," MoneyBin can answer with a named model file in `sqlmesh/models/reports/`, traceable through `core.fct_transactions` to the rows in `raw`. Hosted PFMs can't make that move; local-first PFMs without a curated model layer don't either. The view layer is the discoverability surface — `moneybin reports list` becomes the menu the user didn't know they needed, and `moneybin://schema` advertises every model and column to AI consumers automatically.

The `reports.*` schema convention was decided in [`architecture-shared-primitives.md`](architecture-shared-primitives.md):

- `reports.*` is a **view layer**, never tables (line 55, layer table).
- **No shape prefix** — every model is a curated presentation by definition (line 295).
- **One model per CLI/MCP report**, name mirrors the CLI subcommand (line 55).
- **`core.agg_*` is retired** — time-series and cross-entity rollups live in `reports.*` going forward (line 297).
- The legacy `core.agg_net_worth` migrates to `reports.net_worth` (line 409, modulo the spelling-with-underscore decision below).

This spec is the inaugurating implementation of that convention. It exercises the schema end-to-end (registration, `TableRef`, schema-discoverability resource, CLI/MCP surface) so future report specs can follow a worn path instead of paving one.

### Related specs

- [`architecture-shared-primitives.md`](architecture-shared-primitives.md) — gate spec; defines the `reports.*` schema, layer rules, and the `core.agg_net_worth → reports.net_worth` cascading edit. **Carries one small follow-up amendment in this spec** (rename `reports.networth` → `reports.net_worth`); see [Migrations](#migrations).
- [`cli-restructure.md`](cli-restructure.md) — v2 reports CLI namespace (`reports networth`, `reports spending`, `reports cashflow`, `reports budget`, `reports health`). This spec adds five more subcommands (`reports recurring`, `reports merchants`, `reports uncategorized`, `reports large-transactions`, `reports balance-drift`).
- [`mcp-tool-surface.md`](mcp-tool-surface.md) — v2 `reports_*` MCP tools mirror the CLI 1:1.
- [`mcp-sql-discoverability.md`](mcp-sql-discoverability.md) — `moneybin://schema` resource. **Extended** by this spec to include the `reports` schema with `audience: "interface"`.
- [`net-worth.md`](net-worth.md) — owner of the existing `core.agg_net_worth` model, which this spec migrates. The two `NetworthService` SQL references are updated as part of the migration (no behavior change).
- [`transaction-curation.md`](transaction-curation.md) — sibling M2A spec that introduces `app.audit_log`, `app.transaction_tags`, etc. This spec does not depend on its tables; the doctor spec ([`moneybin-doctor.md`](moneybin-doctor.md), drafted next) will.
- [`mcp-architecture.md`](mcp-architecture.md) — sensitivity tiers. All `reports_*` tools are **Tier 1 (Account-Level)** — they expose aggregate financial state and category breakdowns, never raw PII or full transaction descriptions.

### Decisions made during design (cross-references for reviewers)

| Decision | Rationale | Where it lands |
|---|---|---|
| Eight models in v1, not fifteen | Hits the 6–10 range from the brief; every model independently demoable; future iterations add narrower models (income_sources, recurring-detail) | §Models |
| `recurring_subscriptions` ships with `confidence` column (0.0–1.0) | A survey of PFM subscription detection (Actual Budget, Copilot, Monarch, Rocket Money, Firefly III) found none exposes a confidence score; tools are binary suggested/accepted. Surfacing the score lets users and AI consumers apply their own thresholds rather than accepting a hidden classifier. | §Models §`reports.recurring_subscriptions` |
| Wide-grain principle: prefer powerful views consumers can aggregate over narrow single-purpose ones | Drove `top_merchants → merchant_activity` (top-N is just `ORDER BY total_spend DESC LIMIT N` against the wider view) and `year_over_year_spending → spending_trend` (one model supports YoY, MoM, and 3-month-trailing comparisons). Future report specs apply the same lens. | §Models |
| `reports.net_worth` (with underscore), not `reports.networth` | Reintroduces space/underscore for readability. Overrides the gate spec's name; landed via [Migrations](#migrations) below. | §Migrations |
| Sequenced before `moneybin-doctor.md` | Doctor's `balance_drift` traffic-light comes from `reports.balance_drift`. Recipe-library lands first; doctor reads the view. | §Sequencing |
| Bundle `core.dim_categories` and `core.dim_merchants` migrations | Today `app.categories` and `app.merchants` are Python-built views exposed directly to consumers, violating the `architecture-shared-primitives.md` rule that consumers read only `core.*`/`reports.*`. The recipe library is already touching the `core/` SQLMesh layer, `TableRef`, and the schema-discoverability surface — bundling now avoids reopening the same surface twice. | §Migrations |

## Architectural Pattern

The `reports.*` schema is a **read-only presentation layer** (`architecture-shared-primitives.md` §Data Layer). Every model is a SQLMesh view (`kind VIEW`) reading exclusively from `core.*` and (where applicable) `app.*` joined into `core.dim_*` resolved views. **No `reports.*` model writes to `app.*`, no service writes to `reports.*`, no consumer reads from `prep.*` or `raw.*` directly.**

The pattern is small and uniform:

1. **Naming.** `reports.<entity>` mirrors the CLI subcommand `moneybin reports <entity>` and the MCP tool `reports_<entity>_get`. One model, one CLI subcommand, one MCP tool — three names, three surfaces, identical concept.
2. **Grain.** Each model picks the widest grain its consumers can collapse from. CLI defaults aggregate or rank for the human eye; SQL/MCP consumers re-rank, pivot, or filter as needed.
3. **Comments.** Every column carries an inline `/* */` comment per `.claude/rules/database.md` — these comments are surfaced verbatim by the `moneybin://schema` MCP resource. The model-level comment (top of the SQL file) explains the view's purpose, grain, and any heuristic it uses.
4. **Confidence over false certainty.** Where a model is heuristic (`reports.recurring_subscriptions` is the only one in v1), it exposes a `confidence` column instead of a binary classification. Users and AI consumers can apply their own thresholds.
5. **No cross-currency assumptions.** All amounts in v1 are in profile currency. Multi-currency rollups are owned by the future `multi-currency.md` (M3); see `architecture-shared-primitives.md` §Open Architectural Questions (b).

## Models

Eight models in v1. Each section: purpose, grain, source, columns (with comments matching the SQL), CLI/MCP surface.

### `reports.net_worth`

**Purpose:** Daily cross-account net worth snapshot, replacing `core.agg_net_worth`. Powers `reports networth show` (point-in-time) and `reports networth history` (time series).

**Grain:** One row per `balance_date`. Aggregates across all accounts where `include_in_net_worth = TRUE AND archived = FALSE`.

**Source:** `core.fct_balances_daily` joined with `core.dim_accounts`. Identical query body to today's `core.agg_net_worth` — this is a rename, not a redesign.

**Columns:**

| Column | Type | Comment |
|---|---|---|
| `balance_date` | `DATE` | Calendar date |
| `net_worth` | `DECIMAL(18,2)` | Total balance across all included accounts |
| `total_assets` | `DECIMAL(18,2)` | Sum of positive balances |
| `total_liabilities` | `DECIMAL(18,2)` | Sum of negative balances (kept negative) |
| `account_count` | `INTEGER` | Number of accounts contributing on this date |

CLI: `moneybin reports networth show [--as-of DATE]`, `moneybin reports networth history [--from DATE] [--to DATE] [--interval daily|weekly|monthly]`.
MCP: `reports_networth_get` (point-in-time), `reports_networth_history_get` (time series). Tier 1.

### `reports.cash_flow`

**Purpose:** Monthly inflow/outflow/net per account × category. Powers `reports cashflow show` and any drill-down the consumer wants (by account, by category, totals).

**Grain:** One row per `(year_month, account_id, category)`. Wide-grain — consumers `GROUP BY` further, or aggregate over the whole table for a single net number.

**Source:** `core.fct_transactions` joined with `core.dim_accounts` for `account_name`. Category is the denormalized `category` text already on `core.fct_transactions` (resolved at categorization time from `app.transaction_categories`); no join to a categories dim is needed for any v1 report. Excludes transactions where `is_transfer = TRUE` (transfers are intra-portfolio movements, not cash flow). Excludes transactions in archived accounts.

**Columns:**

| Column | Type | Comment |
|---|---|---|
| `year_month` | `DATE` | First-of-month for the calendar month |
| `account_id` | `VARCHAR` | Owning account (joinable to core.dim_accounts) |
| `account_name` | `VARCHAR` | Account display name (resolved from app.account_settings if overridden) |
| `category` | `VARCHAR` | Spending category text from core.fct_transactions; NULL for uncategorized |
| `inflow` | `DECIMAL(18,2)` | Sum of positive amounts in this (month, account, category) cell |
| `outflow` | `DECIMAL(18,2)` | Sum of negative amounts in this cell (kept negative) |
| `net` | `DECIMAL(18,2)` | inflow + outflow |
| `txn_count` | `INTEGER` | Number of non-transfer transactions in this cell |

CLI: `moneybin reports cashflow show [--from MONTH] [--to MONTH] [--by account|category|account-and-category]`.
MCP: `reports_cashflow_get`. Tier 1.

### `reports.spending_trend`

**Purpose:** Monthly spending per category with month-over-month and year-over-year deltas. Subsumes the brief's `year_over_year_spending` — the wider grain supports YoY, MoM, and 3-month-trailing comparisons from a single view.

**Grain:** One row per `(year_month, category)`. Outflow-only — this is a spending lens, not a cashflow lens. Consumers comparing income trends should use `reports.cash_flow` and aggregate.

**Source:** `core.fct_transactions` filtered to `amount < 0 AND is_transfer = FALSE`, grouped by `(date_trunc('month', txn_date), category)`. Window functions (`LAG`) compute deltas within each category. Category is the denormalized text on `core.fct_transactions`; no join needed.

**Columns:**

| Column | Type | Comment |
|---|---|---|
| `year_month` | `DATE` | First-of-month |
| `category` | `VARCHAR` | Spending category text; NULL for uncategorized |
| `total_spend` | `DECIMAL(18,2)` | Sum of absolute outflow this month in this category |
| `txn_count` | `INTEGER` | Outflow transaction count |
| `prev_month_spend` | `DECIMAL(18,2)` | Spend in the previous calendar month for the same category |
| `mom_delta` | `DECIMAL(18,2)` | total_spend - prev_month_spend |
| `mom_pct` | `DECIMAL(8,4)` | mom_delta / prev_month_spend; NULL if prev_month_spend = 0 |
| `prev_year_spend` | `DECIMAL(18,2)` | Spend in the same calendar month one year prior |
| `yoy_delta` | `DECIMAL(18,2)` | total_spend - prev_year_spend |
| `yoy_pct` | `DECIMAL(8,4)` | yoy_delta / prev_year_spend; NULL if prev_year_spend = 0 |
| `trailing_3mo_avg` | `DECIMAL(18,2)` | Rolling 3-month average ending this month, same category |

CLI: `moneybin reports spending show [--from MONTH] [--to MONTH] [--category SLUG] [--compare yoy|mom|trailing]`.
MCP: `reports_spending_get`. Tier 1.

### `reports.recurring_subscriptions`

**Purpose:** Heuristic detection of likely-recurring outflow charges (subscriptions, memberships, regular bills). Surfaces candidates with a `confidence` score; does **not** auto-classify or write back to any table.

**Grain:** One row per `(merchant_normalized, amount_bucket, cadence)` cluster. Multiple rows per merchant are normal (e.g., Apple One $14.95/mo + AppleCare $7.99/mo).

**Algorithm:**

1. Filter `core.fct_transactions` to `amount < 0 AND is_transfer = FALSE`.
2. Group by `(merchant_normalized, ROUND(amount, 0))` to bucket near-equal charges.
3. For each group with `≥ 3` occurrences in the last 18 months:
   - Compute consecutive `interval_days` between transactions, `interval_days_avg`, `interval_days_stddev`.
   - Classify cadence:
     - `weekly` if `5 ≤ avg ≤ 9` and `stddev < 2`
     - `biweekly` if `12 ≤ avg ≤ 16` and `stddev < 3`
     - `monthly` if `27 ≤ avg ≤ 33` and `stddev < 4`
     - `quarterly` if `85 ≤ avg ≤ 95` and `stddev < 7`
     - `yearly` if `355 ≤ avg ≤ 375` and `stddev < 14`
     - else `irregular` (still surfaced if other criteria met, with reduced confidence)
4. Compute `confidence = LEAST(1.0, occurrence_count / 6.0) * GREATEST(0.0, 1.0 - LEAST(1.0, interval_days_stddev / 14.0))`.
   - Bounded `[0, 1]`.
   - Saturates at 1.0 at six occurrences with zero variance.
   - Approaches 0 as variance grows or count drops.
   - Exact formula is in the model file's docstring; users can override by writing their own derived view.
5. Compute `status = CASE WHEN today - last_seen ≤ 60 THEN 'active' ELSE 'inactive' END`.
6. Compute `annualized_cost` based on cadence:
   - weekly → `avg_amount * 52`
   - biweekly → `avg_amount * 26`
   - monthly → `avg_amount * 12`
   - quarterly → `avg_amount * 4`
   - yearly → `avg_amount * 1`
   - irregular → `avg_amount * (365.25 / interval_days_avg)` (NULL if `interval_days_avg ≤ 0`)

**Columns:**

| Column | Type | Comment |
|---|---|---|
| `merchant_normalized` | `VARCHAR` | Normalized merchant string (joinable to core.dim_merchants) |
| `avg_amount` | `DECIMAL(18,2)` | Average absolute charge amount across this cluster |
| `cadence` | `VARCHAR` | One of: weekly, biweekly, monthly, quarterly, yearly, irregular |
| `interval_days_avg` | `DECIMAL(8,2)` | Mean days between consecutive charges |
| `interval_days_stddev` | `DECIMAL(8,2)` | Stddev of inter-arrival intervals |
| `occurrence_count` | `INTEGER` | Number of matching charges in the last 18 months |
| `first_seen` | `DATE` | Earliest charge in this cluster |
| `last_seen` | `DATE` | Most recent charge in this cluster |
| `status` | `VARCHAR` | 'active' if last_seen within 60 days, else 'inactive' |
| `annualized_cost` | `DECIMAL(18,2)` | Estimated yearly cost based on avg_amount and cadence |
| `confidence` | `DECIMAL(4,3)` | 0.0-1.0 score; see model docstring for formula |

**Posture:** `reports.recurring_subscriptions` is a **candidate generator**, not authoritative state. The acceptance/rejection loop (where users confirm "yes this is a subscription, track it") belongs to a future spec — see [Out of Scope](#out-of-scope).

CLI: `moneybin reports recurring show [--min-confidence FLOAT] [--status active|inactive|all] [--cadence weekly|biweekly|monthly|quarterly|yearly]`.
MCP: `reports_recurring_get`. Tier 1.

### `reports.uncategorized_queue`

**Purpose:** Surface uncategorized transactions ranked by curator-impact (large + old first). Backs the curation workflow; complements (does not replace) the `transactions categorize` review surface.

**Grain:** One row per uncategorized transaction.

**Source:** `core.fct_transactions` filtered to `category IS NULL AND is_transfer = FALSE`.

**Columns:**

| Column | Type | Comment |
|---|---|---|
| `transaction_id` | `VARCHAR` | Joinable to core.fct_transactions |
| `account_id` | `VARCHAR` | Owning account |
| `account_name` | `VARCHAR` | Account display name |
| `txn_date` | `DATE` | Transaction date |
| `amount` | `DECIMAL(18,2)` | Signed amount |
| `description` | `VARCHAR` | Original description (for inspection; categorization usually keys on merchant_normalized) |
| `merchant_normalized` | `VARCHAR` | Normalized merchant string |
| `age_days` | `INTEGER` | Days since txn_date |
| `priority_score` | `DECIMAL(18,2)` | ABS(amount) * age_days; higher = higher curator priority |
| `source_type` | `VARCHAR` | Source system that contributed this transaction |
| `source_id` | `VARCHAR` | Provenance reference within the source system |

CLI default sort: `ORDER BY priority_score DESC`. The view does not bake the sort in — SQL/MCP consumers can re-rank by recency, by amount, by account, etc.

CLI: `moneybin reports uncategorized show [--min-amount DECIMAL] [--account NAME] [--limit N]`.
MCP: `reports_uncategorized_get`. Tier 1.

### `reports.merchant_activity`

**Purpose:** Per-merchant lifetime aggregations. Subsumes the brief's `top_merchants` — top-N is just `ORDER BY total_spend DESC LIMIT N` against this view. The wider grain also powers a hypothetical "merchant detail" surface without a second model.

**Grain:** One row per `merchant_normalized`.

**Source:** `core.fct_transactions` excluding transfers, grouped by `merchant_normalized`. NULL merchants (rare; usually reflects a parsing failure upstream) are bucketed into a single `'(unknown)'` row to keep the view consumable.

**Columns:**

| Column | Type | Comment |
|---|---|---|
| `merchant_normalized` | `VARCHAR` | Normalized merchant string; '(unknown)' when source merchant is NULL |
| `total_spend` | `DECIMAL(18,2)` | Lifetime absolute outflow |
| `total_inflow` | `DECIMAL(18,2)` | Lifetime sum of positive amounts |
| `total_outflow` | `DECIMAL(18,2)` | Lifetime sum of negative amounts (kept negative) |
| `txn_count` | `INTEGER` | Total transaction count |
| `avg_amount` | `DECIMAL(18,2)` | Mean signed amount across all transactions |
| `median_amount` | `DECIMAL(18,2)` | Median signed amount (DuckDB MEDIAN aggregate) |
| `first_seen` | `DATE` | Earliest transaction with this merchant |
| `last_seen` | `DATE` | Most recent transaction |
| `active_months` | `INTEGER` | Distinct year-month count with at least one transaction |
| `top_category` | `VARCHAR` | Mode of category text across this merchant's transactions; NULL if all uncategorized |
| `account_count` | `INTEGER` | Distinct accounts on which this merchant appears |

CLI: `moneybin reports merchants show [--top N] [--sort spend|count|recent]`.
MCP: `reports_merchants_get`. Tier 1.

### `reports.large_transactions`

**Purpose:** Surface unusually large transactions, by both absolute amount and statistical outlier flags. Demo-rich; useful for catching expense anomalies and recurring-charge surprises.

**Grain:** One row per non-transfer transaction. The view does not pre-filter — consumers `WHERE` by their definition of "large".

**Source:** `core.fct_transactions` excluding transfers. Per-account or per-category z-scores computed via window functions.

**Columns:**

| Column | Type | Comment |
|---|---|---|
| `transaction_id` | `VARCHAR` | Joinable to core.fct_transactions |
| `account_id` | `VARCHAR` | Owning account |
| `account_name` | `VARCHAR` | Account display name |
| `txn_date` | `DATE` | Transaction date |
| `amount` | `DECIMAL(18,2)` | Signed amount |
| `description` | `VARCHAR` | Original description |
| `merchant_normalized` | `VARCHAR` | Normalized merchant string |
| `category` | `VARCHAR` | Spending category text; NULL if uncategorized |
| `amount_zscore_account` | `DECIMAL(8,3)` | Z-score of |amount| relative to median + MAD within this account |
| `amount_zscore_category` | `DECIMAL(8,3)` | Z-score of |amount| relative to median + MAD within this category text (NULL if uncategorized or category has fewer than 5 txns) |
| `is_top_100` | `BOOLEAN` | TRUE if this transaction is in the top 100 by ABS(amount) across the whole table |

CLI default: `ORDER BY ABS(amount) DESC LIMIT 25`, with z-scores shown as columns for context. CLI flags expose anomaly mode: `--anomaly account` filters to `amount_zscore_account > 2.5`; `--anomaly category` analogous.

CLI: `moneybin reports large-transactions show [--top N] [--anomaly account|category|none]`.
MCP: `reports_large_transactions_get`. Tier 1.

### `reports.balance_drift`

**Purpose:** Per-(account, assertion_date) reconciliation deltas: asserted vs computed balance. Feeds `moneybin doctor` (next spec) directly via the `status` column traffic-light.

**Grain:** One row per `(account_id, assertion_date)` from `app.balance_assertions`.

**Source:** `app.balance_assertions` left-joined with `core.fct_balances_daily` on `(account_id, assertion_date)`. NULL `computed_balance` (assertion exists for a date with no daily balance row) becomes `drift_status = 'no-data'`.

**Columns:**

| Column | Type | Comment |
|---|---|---|
| `account_id` | `VARCHAR` | Joinable to core.dim_accounts |
| `account_name` | `VARCHAR` | Account display name |
| `assertion_date` | `DATE` | User-asserted balance date |
| `asserted_balance` | `DECIMAL(18,2)` | User-entered balance for this date |
| `computed_balance` | `DECIMAL(18,2)` | Carried-forward balance from core.fct_balances_daily; NULL if missing |
| `drift` | `DECIMAL(18,2)` | asserted_balance - computed_balance |
| `drift_abs` | `DECIMAL(18,2)` | ABS(drift); for default sort |
| `drift_pct` | `DECIMAL(8,4)` | drift / asserted_balance; NULL if asserted_balance = 0 |
| `days_since_assertion` | `INTEGER` | today - assertion_date |
| `status` | `VARCHAR` | 'clean' if `drift_abs < 1.00`, 'warning' if `< 10.00`, 'drift' if `≥ 10.00`, 'no-data' if computed_balance IS NULL |

Thresholds (`$1`, `$10`) are hardcoded in v1 with a docstring noting they are intentional defaults; future iterations may move them to `MoneyBinSettings.reports.balance_drift_thresholds`.

CLI: `moneybin reports balance-drift show [--account NAME] [--status drift|warning|clean|no-data] [--since DATE]`.
MCP: `reports_balance_drift_get`. Tier 1.

`moneybin doctor` (next spec) will read this view to compute its reconciliation traffic-light: any row with `status = 'drift'` flips the doctor section red.

## Data Model

This spec creates **eight SQLMesh views** and **zero new tables**. The migration of `core.agg_net_worth` removes one existing view and replaces it with `reports.net_worth`.

### Files to create

```
sqlmesh/models/reports/
├── net_worth.sql                 -- migrated from core/agg_net_worth.sql
├── cash_flow.sql
├── spending_trend.sql
├── recurring_subscriptions.sql
├── uncategorized_queue.sql
├── merchant_activity.sql
├── large_transactions.sql
└── balance_drift.sql

sqlmesh/models/core/
├── dim_categories.sql            -- migrated from app.categories Python-built view
└── dim_merchants.sql             -- migrated from app.merchants Python-built view
```

### Files to modify

**SQLMesh layer:**
- `sqlmesh/models/core/agg_net_worth.sql` — **delete** (content moves to `sqlmesh/models/reports/net_worth.sql`).

**Schema and registry:**
- `src/moneybin/schema.py` — add `reports` to the schema list. Currently registers `raw, prep, core, app, meta, seeds, synthetic`; this spec adds the eighth.
- `src/moneybin/tables.py` —
  - Replace `TableRef.AGG_NET_WORTH` with `TableRef.REPORTS_NET_WORTH`.
  - Repoint `CATEGORIES` from `("app", "categories")` to `("core", "dim_categories")`.
  - Repoint `MERCHANTS` from `("app", "merchants")` to `("core", "dim_merchants")`.
  - Add `TableRef` constants for all eight new `reports.*` views with `audience = "interface"` so they appear in `moneybin://schema`.
  - Drop the `# view: ...` comments above `CATEGORIES` and `MERCHANTS` (now self-evident from the `core.dim_*` schema).

**Resolution layer (Python-built views retired):**
- `src/moneybin/seeds.py` — drop the categories and merchants view-creation branches in `refresh_views()`. If no view branches remain after both are removed, delete `refresh_views()` and any callers (the implementation will check).

**Consumers:**
- `src/moneybin/services/networth_service.py` — three SQL references update from `AGG_NET_WORTH` to `REPORTS_NET_WORTH`. No behavior change.
- `src/moneybin/services/categorization_service.py` — fix the latent bug at line 426: route `INSERT INTO {MERCHANTS.full_name}` to `INSERT INTO {USER_MERCHANTS.full_name}`. Existing read paths via `MERCHANTS` keep working (they auto-pick up `core.dim_merchants` via the constant). Same for any read-side `CATEGORIES.full_name` usages.
- `src/moneybin/services/schema_catalog.py` — swap `app.categories` and `app.merchants` interface entries for `core.dim_categories` and `core.dim_merchants`; extend with the eight new `reports.*` views and curated example queries (see [Schema Discoverability](#schema-discoverability)).

**Specs:**
- `docs/specs/architecture-shared-primitives.md` — small text amendment per [Migration 1](#1-amendment-to-architecture-shared-primitivesmd).

**CLI:**
- Extend `src/moneybin/cli/commands/reports/` with subcommands for the five new reports (`recurring`, `merchants`, `uncategorized`, `large_transactions`, `balance_drift`). Existing `networth`, `cashflow`, `spending` subcommands either already exist or get wired up to the new views.
- The existing `categories list` and `merchants list` CLI commands are unaffected because they go through services that already use `TableRef.CATEGORIES`/`MERCHANTS`. After migration, those services read from `core.dim_*`.

**MCP:**
- Extend `src/moneybin/mcp/tools/reports_*.py` with the five new tools. The existing `networth` tool's source query updates to read from `reports.net_worth`.

**Tests:**
- Per-model SQLMesh audits (see [Testing Strategy](#testing-strategy)).
- New scenario `tests/scenarios/reports_recipe_library/` for the eight `reports.*` views.
- Update any tests that hard-coded `app.categories` or `app.merchants` schema-qualified names to use the `TableRef` constants instead — those constants now resolve to `core.dim_*`.

### Privacy middleware

The privacy middleware's managed-write validation enforces that writes target only `app.*` and `raw.*`. **`reports.*` is read-only by design and never appears in `_WRITABLE_SCHEMAS`** (per `architecture-shared-primitives.md` §Cascading Edits line 412). No middleware changes needed.

## Schema Discoverability

The `moneybin://schema` MCP resource (`mcp-sql-discoverability.md`) currently exposes interface tables from `core.*` and select `app.*`. This spec extends it to include all eight `reports.*` views with full column comments and example queries.

Convention for `reports.*` example queries (per `mcp-sql-discoverability.md` patterns):

- Each view's `TableRef` constant carries an `example_queries` list with 2-3 representative queries, surfaced verbatim in the MCP resource.
- Example queries should demonstrate the wide-grain principle: a "total" rollup, a "by category" / "by account" drilldown, and a "ranked" pattern.
- Format examples in canonical SQL (uppercase keywords, two-space indent) so AI consumers see the project's house style.

Example for `reports.cash_flow`:

```sql
-- Monthly net cash flow, all accounts and categories
SELECT year_month, SUM(net) AS total_net
FROM reports.cash_flow
GROUP BY year_month
ORDER BY year_month;

-- Spending by category, last 12 months
SELECT category, SUM(outflow) AS total_outflow
FROM reports.cash_flow
WHERE year_month >= date_trunc('month', current_date - INTERVAL '12 months')
GROUP BY category
ORDER BY total_outflow ASC;

-- Per-account monthly summary
SELECT year_month, account_name, SUM(net) AS account_net
FROM reports.cash_flow
GROUP BY year_month, account_name
ORDER BY year_month, account_net DESC;
```

These are part of the spec's success criteria — without curated example queries, the recipe library doesn't function as a discoverability surface. The example queries are reviewed alongside the model SQL.

## CLI Interface

Extends `cli-restructure.md` v2's `reports` namespace. Five new subcommands added; three existing subcommands (`networth`, `cashflow`, `spending`) backed by the new/migrated views.

```
moneybin reports
+-- networth
|   +-- show [--as-of DATE]                          (existing — now backed by reports.net_worth)
|   +-- history [--from DATE] [--to DATE] [--interval daily|weekly|monthly]
+-- cashflow show [--from MONTH] [--to MONTH] [--by account|category|account-and-category]
+-- spending show [--from MONTH] [--to MONTH] [--category SLUG] [--compare yoy|mom|trailing]
+-- recurring show [--min-confidence FLOAT] [--status active|inactive|all] [--cadence ...]
+-- merchants show [--top N] [--sort spend|count|recent]
+-- uncategorized show [--min-amount DECIMAL] [--account NAME] [--limit N]
+-- large-transactions show [--top N] [--anomaly account|category|none]
+-- balance-drift show [--account NAME] [--status drift|warning|clean|no-data] [--since DATE]
```

All commands support `--output json` per `cli-restructure.md`. JSON output uses `ResponseEnvelope` shape per `architecture-shared-primitives.md` §MCP/CLI/SQL Symmetry.

CLI function naming follows `<group>_<verb>` convention (`reports_recurring_show`, etc.) per `.claude/rules/cli.md`.

## MCP Interface

Mirrors CLI 1:1. Eight new or updated tools, all Tier 1 (Account-Level):

| Tool | Sensitivity | Purpose |
|---|---|---|
| `reports_networth_get` | Tier 1 | Point-in-time net worth (existing tool, repointed source) |
| `reports_networth_history_get` | Tier 1 | Net worth history |
| `reports_cashflow_get` | Tier 1 | Monthly cash flow |
| `reports_spending_get` | Tier 1 | Spending trend with deltas |
| `reports_recurring_get` | Tier 1 | Recurring subscription candidates |
| `reports_merchants_get` | Tier 1 | Merchant activity |
| `reports_uncategorized_get` | Tier 1 | Uncategorized queue |
| `reports_large_transactions_get` | Tier 1 | Large transactions |
| `reports_balance_drift_get` | Tier 1 | Balance reconciliation drift |

Tier 1 rationale (per `mcp-architecture.md`): these expose aggregate financial state, category breakdowns, and merchant-level totals. They never expose raw account numbers, full descriptions of one-off purchases without aggregation, or PII-bearing fields. The `uncategorized_queue` and `large_transactions` views surface individual `description` columns — these are still Tier 1 because the data is the user's own, exposed to consumers the user has explicitly granted MCP access to. Tier 2 (Transaction-Level) is reserved for tools that expose full unaggregated transaction lists (`transactions_list`).

All tools return `ResponseEnvelope` with `data`, `summary`, and `display_currency` fields (per `architecture-shared-primitives.md` §MCP/CLI/SQL Symmetry).

## Migrations

Four small schema migrations land in this spec's first PR. All four are derivation-only — no row data moves, every affected object is a view derivable from existing tables. Listed in the order SQLMesh will execute them.

### 1. Amendment to `architecture-shared-primitives.md`

The gate spec uses `reports.networth` (no underscore) throughout. This spec uses `reports.net_worth` (with underscore) for readability consistency with `core.fct_balances_daily`, `app.transaction_notes`, etc.

The PR includes a small text edit to `architecture-shared-primitives.md` replacing `reports.networth` with `reports.net_worth` at all three sites (§Data Layer, §SQLMesh Layer Conventions, §Cascading Edits) and updating the §Cascading Edits paragraph to point ownership of the agg-net-worth migration at this spec:

> **`core.agg_net_worth` → `reports.net_worth`.** New `reports` schema is added to `src/moneybin/schema.py`. The SQLMesh model at `sqlmesh/models/core/agg_net_worth.sql` moves to `sqlmesh/models/reports/net_worth.sql`. `TableRef.AGG_NET_WORTH` is replaced by `TableRef.REPORTS_NET_WORTH`. `NetworthService` updates its three SQL references. **This migration is owned by `reports-recipe-library.md`** (the inaugurating implementation of the `reports.*` schema) and lands as part of that spec's first PR.

Not a redesign — a rename plus an ownership transfer of a deferred migration to the spec that actually executes it.

### 2. `core.agg_net_worth` → `reports.net_worth`

Rename of one SQLMesh view. Same SELECT body. Destructive in SQLMesh terms only (DROP + CREATE on a view); no source-of-truth data is involved because the view is fully derivable from `core.fct_balances_daily`.

Steps on first `moneybin transform apply` after this spec ships:

1. SQLMesh sees `core.agg_net_worth` is no longer in the model set and that `reports.net_worth` is new.
2. SQLMesh drops the old view and creates the new one.
3. The first `NetworthService` call post-migration reads from `reports.net_worth` and returns identical results.

The CHANGELOG entry calls out the rename so users running `transform apply` understand the prompt.

### 3. `app.categories` (Python-built view) → `core.dim_categories` (SQLMesh model)

The current setup is a layer-rule violation that predates this spec: `app.categories` is a Python-built view (created in `src/moneybin/seeds.py:refresh_views()` as `CREATE OR REPLACE VIEW app.categories AS ...`) that consumers (the `categories list` CLI, `schema_catalog.py`, `CategorizationService`) read directly. Per [`architecture-shared-primitives.md`](architecture-shared-primitives.md):

- §Layer Rules item 2: "Consumers read from `core` and `reports`, never from `prep`, `app`, `meta`, `seeds`, `raw` directly."
- §Layer Rules item 3: "Core dimensions are the single source of truth. When `app.*` metadata refines or overrides a `core.dim_*` entity, the join lives in the dim model itself."
- §Architecture Invariants item 8: "Derivations live in SQLMesh, not in services."

The view is a derivation (`seeds.categories ∪ app.user_categories \ app.category_overrides`); it should be a SQLMesh model.

**Migration:**

- Create `sqlmesh/models/core/dim_categories.sql` with the same SELECT body as today's `seeds.py`-built view. Model header: `MODEL (name core.dim_categories, kind VIEW);`.
- The user-state tables (`app.user_categories`, `app.category_overrides`) **stay where they are** — they're mutable user state, not derivations. Only the resolution view moves.
- `seeds.py:refresh_views()` drops its categories branch entirely. The merchants branch is handled by migration 4 below.
- `TableRef.CATEGORIES` repoints from `("app", "categories")` to `("core", "dim_categories")`. The constant name stays the same so existing call sites (`{CATEGORIES.full_name}`) continue to work post-migration; only the resolved schema-qualified name changes.
- `schema_catalog.py` swaps its `app.categories` interface entry for `core.dim_categories`. Example queries are updated.
- `app.categories` view is dropped (no backward-compat alias — single-process local-first tool, all callers are in this PR's diff).

### 4. `app.merchants` (Python-built view) → `core.dim_merchants` (SQLMesh model)

Same architectural fix as migration 3. The Python-built `app.merchants` view (in `seeds.py:refresh_views()`) was originally a union of `app.user_merchants` and three seed catalogs with `app.merchant_overrides` applied — exposed directly to consumers, same layer-rule violation.

> **Amendment 2026-05-15:** seed merchant catalogs and `app.merchant_overrides` were retired. `core.dim_merchants` is now a thin SELECT over `app.user_merchants` only.

**Migration:**

- Create `sqlmesh/models/core/dim_merchants.sql` with the same SELECT body as today's `seeds.py`-built view. Same SQLMesh model shape as `dim_categories`.
- User-state tables (`app.user_merchants`, `app.merchant_overrides`) stay in `app.*`.
- `seeds.py:refresh_views()` drops its merchants branch (and at this point the function may be deletable entirely if no other view branches remain — the implementation will check).
- `TableRef.MERCHANTS` repoints from `("app", "merchants")` to `("core", "dim_merchants")`.
- `schema_catalog.py` swaps its `app.merchants` interface entry for `core.dim_merchants`.
- `app.merchants` view is dropped.

**Forcing-function fix:** `categorization_service.py:426` currently reads `INSERT INTO {MERCHANTS.full_name}` — i.e., `INSERT INTO app.merchants`, which is a UNION view and therefore not insertable in DuckDB. This is a pre-existing latent bug. After migration, `MERCHANTS.full_name` is `core.dim_merchants`, which is a `core.*` view (read-only by design). The INSERT obviously breaks; the fix is to route the write to `USER_MERCHANTS` (the actual user-state table where new merchants always should have landed). The migration PR includes this fix.

### Aggregate impact

| Object | Before | After |
|---|---|---|
| `core.agg_net_worth` | SQLMesh view in `core` | dropped |
| `reports.net_worth` | (does not exist) | SQLMesh view in `reports` (same body as old `core.agg_net_worth`) |
| `app.categories` | Python-built view in `app` | dropped |
| `core.dim_categories` | (does not exist) | SQLMesh view in `core` (same body as old `app.categories`) |
| `app.merchants` | Python-built view in `app` | dropped |
| `core.dim_merchants` | (does not exist) | SQLMesh view in `core` (same body as old `app.merchants`) |
| `app.user_categories`, `app.category_overrides` | tables (mutable user state) | unchanged |
| `app.user_merchants` | tables (mutable user state) | unchanged |
| `app.merchant_overrides` | table (retired 2026-05-15, V012 drops) | removed |
| `seeds.categories` | SQLMesh seed | unchanged |
| `seeds.merchants_global/us/ca` | SQLMesh seeds (retired 2026-05-15) | removed |

Privacy middleware's `_WRITABLE_SCHEMAS` is unchanged — `app.*` and `raw.*` remain the only writable schemas. Writes against the user-state tables continue to work; reads from the resolution views move to `core.dim_*`.

## Testing Strategy

Three layers, mirroring how `core.fct_*` and the existing `core.agg_net_worth` are tested.

### 1. SQLMesh audits per model

Each model file declares audits for grain integrity and key non-null:

```sql
MODEL (
  name reports.net_worth,
  kind VIEW,
  audits (
    not_null(columns := (balance_date, net_worth)),
    unique_combination_of_columns(columns := (balance_date))
  )
);
```

Specific audits per model:

- `reports.net_worth` — `not_null(balance_date, net_worth)`, `unique_combination_of_columns(balance_date)`.
- `reports.cash_flow` — `unique_combination_of_columns(year_month, account_id, category)`. (NULL category is permitted; DuckDB's grouping handles it.)
- `reports.spending_trend` — `unique_combination_of_columns(year_month, category)`.
- `reports.recurring_subscriptions` — `not_null(merchant_normalized, cadence, confidence)`. Confidence range is asserted in scenario tests, not as an audit (DuckDB SQLMesh audits don't natively support range checks; would be a custom audit).
- `reports.uncategorized_queue` — `not_null(transaction_id, priority_score)`.
- `reports.merchant_activity` — `unique_combination_of_columns(merchant_normalized)`, `not_null(total_spend, txn_count)`.
- `reports.large_transactions` — `not_null(transaction_id, amount)`.
- `reports.balance_drift` — `unique_combination_of_columns(account_id, assertion_date)`, `not_null(asserted_balance)`.

### 2. Scenario tests

Add a new scenario `tests/scenarios/reports_recipe_library/` exercising all eight models against the standard synthetic profile (see `testing-synthetic-data.md` for personas). Each model gets:

- A row-count assertion: "this model returns ≥ N rows for the standard profile"
- A column-presence assertion: "every column in the spec exists in the materialized view"
- 2-3 sentinel-value assertions: e.g., for `reports.recurring_subscriptions`, assert that the synthetic Netflix subscription is detected with `cadence = 'monthly'` and `confidence > 0.8`.

The scenario also asserts each of the four migrations from §Migrations:

1. **`core.agg_net_worth` → `reports.net_worth`:** after applying this spec's transform, `reports.net_worth` exists, `core.agg_net_worth` does not, and the row count + final `net_worth` value match what `core.agg_net_worth` would have returned on the same data. (The synthetic generator is deterministic per `testing-synthetic-data.md`, so this is reproducible.)
2. **`app.categories` → `core.dim_categories`:** `core.dim_categories` exists, `app.categories` does not, the unified row set (seeds + user_categories minus inactive overrides) matches the pre-migration shape, and `is_default`/`is_active`/all original columns are preserved.
3. **`app.merchants` → `core.dim_merchants`:** analogous to categories — `core.dim_merchants` exists, `app.merchants` does not, the unioned row set across user_merchants + three regional seeds matches pre-migration with overrides applied.
4. **Categorization-service merchant write fix:** an explicit unit test under `tests/moneybin/test_services/test_categorization_service.py` exercises `_create_merchant()` and asserts that the new row lands in `app.user_merchants` (now that the bug at line 426 is fixed). A regression test would have caught this earlier; add it now as part of the migration.

Scenario fixtures use the YAML format owned by `testing-scenario-comprehensive.md`.

### 3. CLI/MCP smoke tests

Per `e2e-testing.md`, add subprocess-based smoke tests for the five new CLI commands and update the three existing ones to verify they return `ResponseEnvelope`-shaped JSON with a non-empty `data` array on the standard fixture.

For MCP, add per-tool unit tests under `tests/moneybin/mcp/test_tools/` asserting:

- Tool registration (decorator picks up the function)
- Sensitivity tier is Tier 1
- Successful call returns `ResponseEnvelope` with the expected schema for each report
- Empty-data path returns an envelope with `data = []` and an explanatory `summary.message`

### Test layer expectations

Per `.claude/rules/shipping.md` "Test Layer Check": this feature touches CLI, MCP, and SQLMesh layers. All three layers must have tests before status moves to `implemented`. Unit tests alone are insufficient; the scenario tier is mandatory.

## Synthetic Data Requirements

The standard synthetic profile (`testing-synthetic-data.md`) already produces enough data to exercise most of these models. Two specific patterns the recipe library needs to validate against:

1. **Recurring subscription patterns.** The synthetic generator should produce at least 3 distinct merchant/amount clusters with monthly cadence (e.g., Netflix, Spotify, gym membership) and at least 1 with yearly cadence (e.g., domain registration). These already exist in the persona library; this spec just declares them as ground truth for `reports.recurring_subscriptions` scenario assertions.
2. **One inactive subscription.** A merchant that charged for 6+ months and then stopped. Currently the persona library may or may not include this; if not, add a "cancelled gym 8 months ago" pattern to a persona to validate `status = 'inactive'` detection.
3. **Year-over-year data.** For `reports.spending_trend` YoY assertions, the synthetic profile must span ≥ 13 months. Default profiles span 24 months, so this is satisfied.

If gaps are discovered during implementation, file a follow-up to `testing-synthetic-data.md` (don't pad fixtures inline).

## Sequencing

This spec ships before [`moneybin-doctor.md`](moneybin-doctor.md) (next M2C spec):

1. **PR 1 (this spec):** All eight `reports.*` views; the four migrations from §Migrations (gate-spec amendment, `core.agg_net_worth` → `reports.net_worth`, `app.categories` → `core.dim_categories`, `app.merchants` → `core.dim_merchants`); `schema.py`/`tables.py`/`seeds.py`/`schema_catalog.py` updates; the categorization-service merchant write-path fix; CLI subcommands; MCP tools; tests at all three layers.
2. **PR 2 (doctor spec):** `moneybin doctor` command + `system_doctor` MCP tool, reading from the existing services and from `reports.balance_drift`.

The two PRs can be reviewed in parallel once both specs are written, but PR 1 must merge first because PR 2's reconciliation traffic-light depends on `reports.balance_drift`. The bundled dim migrations land atomically with the rest of PR 1 — splitting them into a separate "architectural cleanup" PR was considered and rejected because it would re-open the same `tables.py` / `schema_catalog.py` / `seeds.py` surface twice.

## Out of Scope

- **Subscription acceptance/rejection state.** The suggest-then-confirm pattern in Actual/Copilot/Monarch needs a user-state table (e.g., `app.recurring_subscriptions` with accepted/rejected/snooze states). That is a future spec — likely an extension of `transaction-curation.md` or its own `subscription-curation.md`. `reports.recurring_subscriptions` is a candidate generator only.
- **Income recurring detection.** Mirror of `recurring_subscriptions` for inflows (paychecks, recurring deposits). Worth adding once subscription-curation lands and the patterns are validated; deferred to keep v1 focused.
- **Multi-currency rollups.** Owned by future `multi-currency.md` (M3). All `reports.*` v1 models assume profile currency. See `architecture-shared-primitives.md` §Open Architectural Questions (b).
- **Forecast/projection models.** "What will my net worth be in 6 months?" is a separate concern (forecasting); recipe library is descriptive analytics only.
- **Cancellation-as-a-service workflow.** Rocket Money-style cancellation concierge is out of scope and will likely never be in scope (it's their business model, not ours).
- **Portfolio/holdings reports.** Gated on `investment-tracking.md`. Will land as `reports.portfolio`, `reports.holdings`, etc. once the schema decisions there are made.
- **Tax reports.** Owned by `tax-*.md` specs (separate top-level CLI group per `cli-restructure.md` v2; `tax` is not nested under `reports`).

## Dependencies

- **Required:** [`architecture-shared-primitives.md`](architecture-shared-primitives.md) — schema convention. **Met** (PR #118 merged).
- **Required:** `core.fct_balances_daily`, `core.fct_transactions`, `core.dim_accounts`, `seeds.categories`, `app.balance_assertions`. **All exist** (shipped via `net-worth.md`, `account-management.md`, base scaffolding).
- **Helpful:** Synthetic data persona library with subscription patterns. **Mostly met** — may need one persona update for `reports.recurring_subscriptions` "inactive" assertion.
- **None blocking:** This spec does not depend on `transaction-curation.md` (Wave M2A). Doctor spec will, for its curator-stats section.

## Open Questions

1. **Should `reports.balance_drift` `status` thresholds be configurable in v1?** Trade-off: hardcoded keeps the spec simple and the SQL clean; configurable means users with different scales (a $10 drift is noise on a $1M balance, catastrophic on a $50 balance) get sensible defaults. Recommendation: hardcode in v1, add `MoneyBinSettings.reports.balance_drift_thresholds` if/when a real user complains. Note in model docstring.
2. **Should `reports.recurring_subscriptions` use `core.dim_merchants` for normalization, or rely on `core.fct_transactions.merchant_name` (denormalized at categorization time)?** The denormalized text is consistent with how other reports normalize and avoids a join. The dim picks up curator overrides applied after a transaction was categorized. Recommendation: use the text on `core.fct_transactions`; decide concretely during implementation by checking whether overrides flow back into the fact via re-categorization. (Independent of this question, all consumer reads of merchants now go through `core.dim_merchants`, not `app.merchants`, after migration 4.)
3. **Should `reports.spending_trend` exclude transfers explicitly, or trust `core.fct_transactions.is_transfer = FALSE`?** Same answer as for `cash_flow`: trust `is_transfer` (it's the canonical signal). Document in model comment.

## References

### Authoritative

- [`architecture-shared-primitives.md`](architecture-shared-primitives.md) — gate spec for `reports.*` schema convention.
- [`cli-restructure.md`](cli-restructure.md) v2 — `reports` CLI namespace.
- [`mcp-tool-surface.md`](mcp-tool-surface.md) v2 — `reports_*` MCP tools.
- [`mcp-sql-discoverability.md`](mcp-sql-discoverability.md) — `moneybin://schema` resource extended by this spec.
- [`mcp-architecture.md`](mcp-architecture.md) — sensitivity tiers.
- [`net-worth.md`](net-worth.md) — owner of the migrated `agg_net_worth` model.

### Rules

- `.claude/rules/database.md` — column comment convention, sign convention, parameterized SQL.
- `.claude/rules/cli.md` — CLI function naming (`<group>_<verb>`).
- `.claude/rules/security.md` — PII in logs, parameterized SQL.
- `.claude/rules/shipping.md` — README/CHANGELOG/roadmap update flow on shipping.

### Source files (reference points)

- `sqlmesh/models/core/agg_net_worth.sql` — the model being migrated; same SELECT body lands in `sqlmesh/models/reports/net_worth.sql`.
- `src/moneybin/services/networth_service.py` — three SQL references update.
- `src/moneybin/schema.py` — schema list extends from 7 to 8.
- `src/moneybin/tables.py` — `TableRef` constants for the eight new views.
