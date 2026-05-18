<!-- Last reviewed: 2026-05-17 -->
# Data Model

The user-facing data model. Tables in `core.*`, `reports.*`, `app.*`, `meta.*`, and `seeds.*` are the surfaces consumers (CLI, MCP, your own SQL) read from. This page covers each table's grain, key columns, and what they mean. For the pipeline that fills them, see [`docs/guides/data-pipeline.md`](../guides/data-pipeline.md).

Schema is stable but not yet frozen — see [`docs/architecture.md`](../architecture.md) for the pre-v1 evolution posture. Tables here are verified against their SQLMesh model in [`sqlmesh/models/`](../../sqlmesh/models/) (`core.*`, `reports.*`, `meta.*`, `seeds.*`) or DDL in [`src/moneybin/sql/schema/`](../../src/moneybin/sql/schema/) (`app.*`, `raw.*`); per-table file links are omitted since file names match table names.

## Schema layers

| Schema | Purpose | Materialization | Read? | Write? |
|---|---|---|---|---|
| `raw` | Source-specific tables, preserved as imported. Loaders write here. | Tables | Internal | Loaders only |
| `prep` | Light staging — type casts, renames, the matched/merged intermediate. | Views | **No** | SQLMesh only |
| `core` | Canonical analytical tables — `fct_*`, `dim_*`, `bridge_*`. | Views and tables | Yes | **Blocked** |
| `app` | User state — categorizations, notes, tags, splits, budgets, settings. | Tables | Yes | Via services / MCP write tools |
| `meta` | Cross-source provenance + lineage. | Views | Yes | SQLMesh / system |
| `seeds` | Reference data shipped with MoneyBin (categories). | CSV-backed tables | Yes | SQLMesh |
| `reports` | Curated presentation views, one per CLI/MCP report. | Views | Yes | **Blocked** |

Writes to `core.*`, `reports.*`, and `meta.*` are blocked by managed-write middleware. Mutations are scoped to `app.*` (through services or MCP write tools) and loader-only `raw.*`. The general SQL surface is read-only.

## Cross-cutting conventions

These conventions apply across every table below. Read them once; the per-table notes assume them.

### Sign conventions across surfaces

The signed amount lives on `core.fct_transactions.amount`: **negative = expense, positive = income**. `core.fct_transactions.amount_absolute` is provided to skip sign handling in aggregations.

`reports.*` views are *not* sign-uniform. They preserve, invert, or take the absolute value depending on what the report is for. The defaults you will hit:

| View / column | Sign of money column |
|---|---|
| `reports.cash_flow.inflow` | Positive (sum of positive `amount`). |
| `reports.cash_flow.outflow` | **Negative** (sum of negative `amount`, preserved). |
| `reports.cash_flow.net` | Signed (`inflow + outflow`). |
| `reports.spending_trend.total_spend` | **Positive** (`SUM(ABS(amount))`). |
| `reports.merchant_activity.total_spend` | **Positive** (absolute outflow). |
| `reports.merchant_activity.total_outflow` | **Negative** (preserved). |
| `reports.large_transactions.amount` | Signed (preserved from source). |
| `reports.uncategorized_queue.amount` | Signed (preserved from source). |
| `reports.net_worth.total_liabilities` | **Negative** (preserved). |

If you sum `outflow` from `cash_flow` and `total_spend` from `spending_trend` in the same query, one is negative and the other is positive. Don't.

### Currency handling

`core.fct_transactions.currency_code` and `core.dim_accounts.iso_currency_code` are ISO 4217 strings; both default to `'USD'`. The `reports.*` views aggregate without filtering or converting by currency — **they assume single-currency**. Multi-currency users get mixed-currency sums and should treat the numbers as approximations until FX-conversion ships ([`docs/roadmap.md`](../roadmap.md)). The MCP/CLI envelope's `summary.display_currency` is presentation-only; rows are not FX-converted.

### Pending and posted

`core.fct_transactions.is_pending = TRUE` whenever any contributing source row is still flagged pending. When the source posts, the next refresh flips it to `FALSE`; `transaction_id` is stable across that transition because the content-hash inputs (date, amount, account) don't change on post, and Plaid's `transaction_id` is `plaid_<provider_transaction_id>`. Practical filter: most analytics queries should add `WHERE is_pending = FALSE`.

### Dates and timezones

- **Date columns** (`transaction_date`, `authorized_date`, `balance_date`, `assertion_date`, `txn_date`) — institution-local calendar dates; no timezone conversion.
- **Timestamp columns** (`extracted_at`, `loaded_at`, `created_at`, `updated_at`, `applied_at`, `categorized_at`) — UTC. Plaid writes `Datetime(time_zone="UTC")`; OFX and tabular loaders write naive timestamps treated as UTC.
- **Calendar parts** (`transaction_year`, `transaction_month`, `transaction_day`, `transaction_day_of_week`) — derived from `transaction_date` (institution-local). Day-of-week: `0 = Sunday`.

### Money types

`DECIMAL(18,2)` for money columns — never `FLOAT`. The values are **major units** (dollars, euros), not minor units (cents). Polars uses `pl.Decimal(18, 2)`; Python uses `decimal.Decimal`. `DECIMAL(18,8)` is reserved for fractional shares, NAV, prices, and exchange rates (not yet materialized). Every money column in `reports.*` inherits `DECIMAL(18,2)` from the underlying `core` source.

### Merchant normalization (`merchant_normalized`)

`merchant_normalized` in `reports.*` is just `core.fct_transactions.merchant_name`, with NULL bucketed as the literal `'(unknown)'` in `merchant_activity` and `recurring_subscriptions`. `merchant_name` itself is `COALESCE(core.dim_merchants.canonical_name, <source description>)`. There is no algorithmic string normalization (no lowercasing, no whitespace collapse, no POS-prefix stripping). The "normalization" is whatever curation (user, AI, rule, or Plaid bootstrap) recorded into `app.user_merchants.canonical_name`; uncurated transactions surface their raw source description.

### `reports.*` refresh cadence

`reports.*` are SQLMesh views (`kind VIEW`), not materialized tables. They reflect the current state of `core.*` at read time. `core.*` is updated by `moneybin refresh`.

## `core.*` — canonical analytical tables

### `core.fct_transactions`

The canonical transaction fact. Grain: one row per `transaction_id` (gold key — a deterministic SHA-256 hash unique per real-world transaction).

This is a `VIEW` over `prep.int_transactions__merged` joined to category, merchant, transfer, and curation overlays. The fact already aggregates per-transaction `notes`, `tags`, and `splits` as nested `LIST(STRUCT(...))` columns — consumers should not query `app.transaction_notes` etc. directly.

| Column | Type | Description |
|---|---|---|
| `transaction_id` | VARCHAR | Gold key. Deterministic, stable across re-imports. |
| `account_id` | VARCHAR | FK → `core.dim_accounts.account_id`. |
| `transaction_date` | DATE | Posted/settled date; earliest across sources for merged records. |
| `authorized_date` | DATE | Authorization date from highest-priority source. NULL when not provided. |
| `amount` | DECIMAL(18,2) | Signed: **negative = expense, positive = income**. |
| `amount_absolute` | DECIMAL(18,2) | `ABS(amount)`; avoids sign handling in aggregations. |
| `transaction_direction` | VARCHAR | `'expense'` \| `'income'` \| `'zero'` (derived from sign). |
| `description` | VARCHAR | Payee or merchant description from highest-priority source. |
| `merchant_name` | VARCHAR | `COALESCE(core.dim_merchants.canonical_name, source value)`. |
| `memo` | VARCHAR | Additional notes from highest-priority source. |
| `category` | VARCHAR | Fallback order: `category_id` → `core.dim_categories.category`; else `app.transaction_categories.category` snapshot; else source text. |
| `subcategory` | VARCHAR | Same fallback chain. |
| `categorized_by` | VARCHAR | `'rule'` \| `'ai'` \| `'user'` \| NULL. |
| `payment_channel` | VARCHAR | `online` / `in store` / `other`. |
| `transaction_type` | VARCHAR | Source-specific type code. |
| `check_number` | VARCHAR | NULL for non-check transactions. |
| `is_pending` | BOOLEAN | See the "Pending and posted" callout above. |
| `pending_transaction_id` | VARCHAR | ID of the pending row this record resolved. |
| `location_address` / `_city` / `_region` / `_postal_code` / `_country` | VARCHAR | Merchant address parts; NULL when not provided. |
| `location_latitude` / `_longitude` | DECIMAL(18,8) | Merchant coordinates; NULL when not provided. |
| `currency_code` | VARCHAR | ISO 4217. |
| `source_type` | VARCHAR | Winning record's source: `ofx`, `csv`, `tsv`, `excel`, `plaid`, `manual`, ... |
| `source_count` | INTEGER | Contributing source rows (1 for unmatched, 2+ for merged). |
| `match_confidence` | DECIMAL | NULL for unmatched; `0.0`–`1.0` for matched. |
| `source_extracted_at`, `loaded_at` | TIMESTAMP | Source-parse / DB-write times (UTC). |
| `updated_at` | TIMESTAMP | `GREATEST(loaded_at, categorized_at, notes_latest, tags_latest, splits_latest)`. Does not advance on idempotent SQLMesh re-applies. |
| `is_transfer` | BOOLEAN | TRUE if part of a confirmed transfer pair. |
| `transfer_pair_id` | VARCHAR | FK → `core.bridge_transfers.transfer_id`. NULL if not a transfer. |
| `transaction_year`, `transaction_month`, `transaction_day`, `transaction_day_of_week` | INTEGER | Calendar parts. Day-of-week: 0 = Sunday. |
| `transaction_year_month`, `transaction_year_quarter` | VARCHAR | `YYYY-MM` / `YYYY-QN` period-grouping keys. |
| `notes` | LIST(STRUCT) | `(note_id, text, author, created_at)`; chronological. NULL when no notes — filter via `note_count > 0`. |
| `note_count`, `tag_count` | INTEGER | NULL when no notes / tags. |
| `tags` | LIST(VARCHAR) | Sorted; `'namespace:value'` or bare `'value'`. NULL when no tags — filter via `'x' = ANY(tags)` or `tag_count > 0`. |
| `splits` | LIST(STRUCT) | `(split_id, amount, category, subcategory, note)`; ordered by `ord`. NULL when no splits. |
| `split_count`, `has_splits` | INTEGER / BOOLEAN | NULL / FALSE when no splits. |

Logical grain key: `transaction_id` (declared via the `MODEL ... grain transaction_id` annotation; not a physical PK constraint since the table is a view).

### `core.dim_accounts`

Canonical accounts dimension. Grain: one row per `account_id` (`FULL` model). Joins `app.account_settings` so consumers see one resolved view — no consumer joins `app.account_settings` directly.

| Column | Type | Description |
|---|---|---|
| `account_id` | VARCHAR | Stable across imports; FK target for `fct_transactions.account_id`. |
| `routing_number` | VARCHAR | ABA routing number; NULL when not provided. |
| `account_type` | VARCHAR | Source-supplied classification (`CHECKING`, `SAVINGS`, `CREDITLINE`, ...). |
| `institution_name` | VARCHAR | Human-readable institution. |
| `institution_fid` | VARCHAR | OFX FID; NULL for tabular sources. |
| `source_type`, `source_file` | VARCHAR | Source of the winning record after dedup; source file path. |
| `extracted_at`, `loaded_at`, `updated_at` | TIMESTAMP | Source-parse / DB-write times (UTC). `updated_at = GREATEST(loaded_at, account_settings.updated_at)`. |
| `display_name` | VARCHAR | User override → derived default (`institution_name + account_type + …<last4>`) → bare `account_id`. |
| `official_name` | VARCHAR | User-set or Plaid-supplied formal name. |
| `last_four` | VARCHAR | User-set or Plaid mask. |
| `account_subtype` | VARCHAR | Plaid-style subtype (`checking`, `savings`, `credit card`, `mortgage`, ...). |
| `holder_category` | VARCHAR | `personal` / `business` / `joint`. |
| `iso_currency_code` | VARCHAR | ISO-4217; defaults to `'USD'`. |
| `credit_limit` | DECIMAL(18,2) | User-asserted; drives utilization metrics. |
| `archived` | BOOLEAN | Hides from default lists and `reports.net_worth`. |
| `include_in_net_worth` | BOOLEAN | Independent toggle; archiving forces FALSE. |

Logical grain key: `account_id` (unique after dedup by `ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY extracted_at DESC)`).

### `core.dim_merchants`

Resolved merchant dimension. Grain: one row per `merchant_id`. `VIEW` over `app.user_merchants` joined to `core.dim_categories`. MoneyBin does not ship a curated merchant catalog — every merchant is user-created or system-created on the user's behalf.

| Column | Type | Description |
|---|---|---|
| `merchant_id` | VARCHAR | 12-char UUID hex (`uuid.uuid4().hex[:12]`). |
| `raw_pattern` | VARCHAR | Match pattern; NULL for exemplar-only merchants (`match_type='oneOf'`). |
| `match_type` | VARCHAR | `contains` \| `exact` \| `regex` \| `oneOf`. |
| `canonical_name` | VARCHAR | Display name. |
| `category_id` | VARCHAR | FK → `core.dim_categories.category_id`; NULL for merchants without a default. |
| `category`, `subcategory` | VARCHAR | Resolved via FK with `app.user_merchants` fallback for orphaned rows. |
| `created_by` | VARCHAR | `user` \| `ai` \| `rule` \| `plaid` \| `migration`. |
| `exemplars` | VARCHAR[] | Exact match values for `oneOf` lookup. |
| `created_at`, `updated_at` | TIMESTAMP | UTC. |

Logical grain key: `merchant_id`.

### `core.dim_categories`

Resolved category dimension. Unifies `seeds.categories` (16 primary, ~100 subcategories from Plaid PFC v2) with `app.user_categories`, applying `app.category_overrides`. `UNION` (not `UNION ALL`) collapses accidental ID collisions.

| Column | Type | Description |
|---|---|---|
| `category_id` | VARCHAR | Seed-supplied semantic ID or 12-char UUID hex for user categories. |
| `category` | VARCHAR | Top-level name. |
| `subcategory` | VARCHAR | NULL for top-level-only entries. |
| `description` | VARCHAR | Human-readable description. |
| `plaid_detailed` | VARCHAR | Plaid PFC detailed mapping; NULL for user-defined. |
| `is_default` | BOOLEAN | TRUE for seeded, FALSE for user-created. |
| `is_active` | BOOLEAN | FALSE if user has soft-deleted a default via `app.category_overrides`. |
| `created_at`, `updated_at` | TIMESTAMP | NULL for seed rows (query `meta.model_freshness` for seed freshness); set for user-created (UTC). |

Logical grain key: `category_id`.

### `core.bridge_transfers`

Confirmed transfer pairs linking two `fct_transactions` rows. Grain: one row per `transfer_id`. `VIEW` derived from `app.match_decisions` where `match_type = 'transfer'` and `match_status = 'accepted'`.

| Column | Type | Description |
|---|---|---|
| `transfer_id` | VARCHAR | UUID; also FK to `app.match_decisions.match_id`. |
| `debit_transaction_id` | VARCHAR | Outgoing side (negative amount). FK → `core.fct_transactions.transaction_id`. |
| `credit_transaction_id` | VARCHAR | Incoming side (positive amount). FK → `core.fct_transactions.transaction_id`. |
| `date_offset_days` | INTEGER | Days between the two post dates (0 = same day). |
| `amount` | DECIMAL(18,2) | Absolute transfer amount. |

To walk from a `fct_transactions` row to its counterparty, join on `transfer_pair_id = transfer_id`, then pick the opposite side's `transaction_id`. See the "Common joins" section below.

### `core.fct_transaction_lines`

Split-expanded grain. One row per unsplit transaction; N rows per split transaction. `VIEW` over `core.fct_transactions` with `UNNEST(splits)`. Grain: `(transaction_id, line_id)`.

| Column | Type | Description |
|---|---|---|
| `transaction_id` | VARCHAR | FK → `core.fct_transactions.transaction_id`. |
| `line_id` | VARCHAR | `'whole'` for unsplit, `split_id` for split children. |
| `line_amount` | DECIMAL(18,2) | Per-line amount; equals `parent.amount` for unsplit. |
| `line_category`, `line_subcategory` | VARCHAR | Per-line; falls through to parent for unsplit. |
| `line_note` | VARCHAR | NULL on unsplit rows; per-split note when present. |
| `line_kind` | VARCHAR | `'whole'` \| `'split'`. |
| `account_id`, `transaction_date`, `merchant_name`, `description`, `is_pending`, `is_transfer`, `transfer_pair_id`, `source_type`, `source_count`, `transaction_year`, `transaction_month`, `transaction_year_month`, `transaction_year_quarter` | various | Carried from the parent fact row. |

Logical grain key: `(transaction_id, line_id)`.

**Don't double-count.** Pick one grain per query. If you sum from `fct_transactions`, do not also sum from `fct_transaction_lines` — the lines view sums to the same totals (whole or split). Mixing both produces 2× the answer.

### `core.fct_balances`

Observation-grain balance view: OFX statement balances, tabular running balances, and user-entered assertions, unioned. `VIEW`.

| Column | Type | Description |
|---|---|---|
| `account_id` | VARCHAR | Source-system account identifier. |
| `balance_date` | DATE | Date the balance was observed (institution-local). |
| `balance` | DECIMAL(18,2) | Observed balance. |
| `source_type` | VARCHAR | `ofx` \| `tabular` \| `assertion`. |
| `source_ref` | VARCHAR | File path or `'user'` for assertions. |
| `updated_at` | TIMESTAMP | Underlying observation's `loaded_at` / `created_at` (UTC). |

### `core.fct_balances_daily`

Per-account daily balance spine. Grain: one row per `(account_id, balance_date)` from each account's first observation to its last. `FULL` Python model.

Observed days use the most authoritative source (per-day precedence: `user assertion > ofx > plaid > tabular`). Gaps are filled by carrying the last balance forward, adjusted by intervening transactions from `core.fct_transactions`.

| Column | Type | Description |
|---|---|---|
| `account_id` | VARCHAR | FK → `core.dim_accounts.account_id`. |
| `balance_date` | DATE | Calendar date (institution-local). |
| `balance` | DECIMAL(18,2) | End-of-day balance. |
| `is_observed` | BOOLEAN | TRUE if an authoritative observation exists for this date. |
| `observation_source` | VARCHAR | Winning observation's source (`ofx`, `tabular`, `assertion`, `plaid`); NULL when interpolated. |
| `reconciliation_delta` | DECIMAL(18,2) | `observed_balance − transaction_derived_balance`. Positive when the observed balance exceeds what transactions alone would predict; negative when below. NULL on interpolated days and the first observation. |

Logical grain key: `(account_id, balance_date)`.

## `reports.*` — curated presentation views

All `reports.*` are `VIEW` kind. Consumers (CLI `moneybin reports …`, MCP `reports_*` tools) read these directly.

### Which view should I use?

| Question | View | Notes |
|---|---|---|
| What did I spend, by category, over time? | `reports.spending_trend` | Time-series with MoM / YoY / trailing-3mo windows. Outflow-only, positive values (`SUM(ABS(amount))`). |
| Where did I spend, by merchant? | `reports.merchant_activity` | Lifetime per-merchant aggregates. Top-N is `ORDER BY total_spend DESC LIMIT N`. |
| Income vs. spend by account × category, by month? | `reports.cash_flow` | Signed `inflow` / `outflow` / `net`. Outflow stays negative. |
| What's my net worth? | `reports.net_worth` | Daily snapshot from `fct_balances_daily`. |
| Which transactions are unusually large? | `reports.large_transactions` | Modified z-scores against account and category baselines + `is_top_100`. |
| Which subscriptions am I paying for? | `reports.recurring_subscriptions` | Heuristic candidates with confidence scores; does not auto-classify. |
| What's not categorized yet? | `reports.uncategorized_queue` | Ranked by curator-impact (`ABS(amount) × age_days`). |
| Are my balances drifting from reality? | `reports.balance_drift` | Per-assertion deltas vs computed balance; feeds `moneybin doctor`. |

When `cash_flow`, `spending_trend`, and `merchant_activity` overlap (e.g., "spend by category last month"), pick the one whose **grain** matches the question: `cash_flow` for `(month, account, category)`, `spending_trend` for `(month, category)` with windowed comparisons, `merchant_activity` for lifetime-per-merchant.

### `reports.net_worth`

Cross-account daily net-worth rollup. Grain: one row per `balance_date`. Excludes accounts where `archived = TRUE` or `include_in_net_worth = FALSE`.

| Column | Type | Description |
|---|---|---|
| `balance_date` | DATE | Calendar date. |
| `net_worth` | DECIMAL(18,2) | `SUM(balance)` across included accounts. |
| `account_count` | INTEGER | Distinct accounts contributing. |
| `total_assets` | DECIMAL(18,2) | `SUM(balance WHERE balance > 0)`. |
| `total_liabilities` | DECIMAL(18,2) | `SUM(balance WHERE balance < 0)`; **kept negative**. |

### `reports.cash_flow`

Monthly inflow/outflow/net per account × category. Grain: one row per `(year_month, account_id, category)`. Excludes transfers (`is_transfer = FALSE`) and archived accounts.

| Column | Type | Description |
|---|---|---|
| `year_month` | VARCHAR | `'YYYY-MM'`. |
| `account_id` | VARCHAR | Joinable to `core.dim_accounts.account_id`. |
| `account_name` | VARCHAR | Resolved `dim_accounts.display_name`. |
| `category` | VARCHAR | NULL for uncategorized. |
| `inflow` | DECIMAL(18,2) | Sum of positive amounts. |
| `outflow` | DECIMAL(18,2) | Sum of negative amounts; **kept negative**. |
| `net` | DECIMAL(18,2) | `inflow + outflow`. |
| `txn_count` | INTEGER | Non-transfer transactions in this cell. |

### `reports.spending_trend`

Monthly spending per category with MoM, YoY, and trailing-3mo windows. Grain: one row per `(year_month, category)`. Outflow-only — restricts to `amount < 0 AND NOT is_transfer AND NOT archived`.

| Column | Type | Description |
|---|---|---|
| `year_month` | VARCHAR | `'YYYY-MM'`. |
| `category` | VARCHAR | Grouping key; NULL for uncategorized. |
| `total_spend` | DECIMAL(18,2) | `SUM(ABS(amount))`; **positive**. |
| `txn_count` | INTEGER | Outflow count. |
| `prev_month_spend` | DECIMAL(18,2) | Spend in the previous month, same category. |
| `mom_delta` | DECIMAL(18,2) | `total_spend − prev_month_spend`. |
| `mom_pct` | DECIMAL | `mom_delta / prev_month_spend`; NULL when prev = 0. |
| `prev_year_spend` | DECIMAL(18,2) | Same calendar month one year prior. |
| `yoy_delta` | DECIMAL(18,2) | `total_spend − prev_year_spend`. |
| `yoy_pct` | DECIMAL | `yoy_delta / prev_year_spend`; NULL when prev_year = 0. |
| `trailing_3mo_avg` | DECIMAL(18,2) | Rolling 3-month average ending this month. |

### `reports.recurring_subscriptions`

Heuristic detection of likely-recurring outflows. Grain: one row per `(merchant_normalized, amount_bucket)` cluster with ≥3 occurrences. Default window: last 18 months. Surfaces candidates with a confidence score; does not auto-classify.

| Column | Type | Description |
|---|---|---|
| `merchant_normalized` | VARCHAR | `'(unknown)'` for NULL merchants. |
| `avg_amount`, `annualized_cost` | DECIMAL(18,2) | Mean absolute charge; estimated yearly cost. |
| `cadence` | VARCHAR | `weekly` \| `biweekly` \| `monthly` \| `quarterly` \| `yearly` \| `irregular`. |
| `interval_days_avg`, `interval_days_stddev` | DECIMAL | Inter-arrival statistics. |
| `occurrence_count` | INTEGER | Charges in the last 18 months. |
| `first_seen`, `last_seen` | DATE | Earliest / most recent charge. |
| `status` | VARCHAR | `'active'` if `last_seen` within `max(60 days, 2× cadence)`, else `'inactive'`. |
| `confidence` | DECIMAL | `0.0`–`1.0`; saturates at `1.0` with ≥6 occurrences and zero variance. |

### `reports.uncategorized_queue`

Uncategorized transactions ranked by curator-impact. Grain: one row per uncategorized transaction. Excludes transfers and archived accounts.

| Column | Type | Description |
|---|---|---|
| `transaction_id` | VARCHAR | Joinable to `core.fct_transactions.transaction_id`. |
| `account_id` | VARCHAR | Owning account. |
| `account_name` | VARCHAR | Resolved display name; NULL only if `dim_accounts.display_name` itself is NULL (uncommon). |
| `txn_date` | DATE | Transaction date. |
| `amount` | DECIMAL(18,2) | Signed (source sign preserved). |
| `description` | VARCHAR | Source description. |
| `merchant_normalized` | VARCHAR | Resolved merchant; NULL when no `dim_merchants` match and no source merchant value. |
| `age_days` | INTEGER | `CURRENT_DATE − txn_date`. |
| `priority_score` | DECIMAL(18,2) | `ABS(amount) × age_days` — default sort key. |
| `source_type` | VARCHAR | Provenance source. |
| `source_id` | VARCHAR | **NULL placeholder today.** Reserved column pending `source_id` surfacing on `fct_transactions`. Don't filter or join on it. |

### `reports.merchant_activity`

Per-merchant lifetime aggregations. Grain: one row per `merchant_normalized`. NULL merchants bucketed as `'(unknown)'`. Excludes transfers and archived accounts. Subsumes "top merchants" — top-N is `ORDER BY total_spend DESC LIMIT N`.

| Column | Type | Description |
|---|---|---|
| `merchant_normalized` | VARCHAR | `'(unknown)'` for NULL merchants. |
| `total_spend` | DECIMAL(18,2) | Lifetime absolute outflow; **positive**. |
| `total_inflow` | DECIMAL(18,2) | Lifetime sum of positive amounts. |
| `total_outflow` | DECIMAL(18,2) | Lifetime sum of negative amounts; **kept negative**. |
| `txn_count`, `account_count`, `active_months` | INTEGER | Counts (transactions / distinct accounts / distinct year-months). |
| `avg_amount`, `median_amount` | DECIMAL(18,2) | Signed mean / median. |
| `first_seen`, `last_seen` | DATE | Date range. |
| `top_category` | VARCHAR | Modal category; NULL if all uncategorized. |

### `reports.large_transactions`

All non-transfer transactions with z-scores against account and category baselines. Grain: one row per non-transfer transaction. Uses median + MAD (more outlier-robust than mean + stddev). Consumers filter by their own definition of "large" — top-N, `|z| > 2.5`, etc.

| Column | Type | Description |
|---|---|---|
| `transaction_id` | VARCHAR | Joinable to `core.fct_transactions.transaction_id`. |
| `account_id` | VARCHAR | Owning account. |
| `account_name` | VARCHAR | Resolved display name. |
| `txn_date` | DATE | Transaction date. |
| `amount` | DECIMAL(18,2) | Signed (source sign preserved). |
| `description` | VARCHAR | Source description. |
| `merchant_normalized` | VARCHAR | Resolved merchant; NULL when not curated. |
| `category` | VARCHAR | Spending category; NULL if uncategorized. |
| `amount_zscore_account` | DECIMAL | Modified z-score relative to account median + MAD. NULL when MAD = 0. |
| `amount_zscore_category` | DECIMAL | Modified z-score relative to category median + MAD; NULL when category has fewer than 5 transactions or MAD = 0. |
| `is_top_100` | BOOLEAN | TRUE if in the top 100 by `ABS(amount)` overall. |

### `reports.balance_drift`

Per-`(account, assertion_date)` reconciliation deltas: asserted vs computed balance. Grain: one row per balance assertion. Feeds `moneybin doctor`.

| Column | Type | Description |
|---|---|---|
| `account_id` | VARCHAR | Joinable to `core.dim_accounts.account_id`. |
| `account_name` | VARCHAR | Resolved display name. |
| `assertion_date` | DATE | User-asserted balance date. |
| `asserted_balance` | DECIMAL(18,2) | User-entered value. |
| `computed_balance` | DECIMAL(18,2) | Carry-forward from `core.fct_balances_daily`; NULL if missing. |
| `drift` | DECIMAL(18,2) | `asserted_balance − computed_balance`. |
| `drift_abs` | DECIMAL(18,2) | For default sort. |
| `drift_pct` | DECIMAL | `drift / asserted_balance`; NULL when asserted is zero. |
| `days_since_assertion` | INTEGER | `CURRENT_DATE − assertion_date`. |
| `status` | VARCHAR | `clean` (< $1) \| `warning` (< $10) \| `drift` (≥ $10) \| `no-data` (computed NULL). |

## Common joins

The three patterns below cover the vast majority of consumer queries.

### Transaction enriched with account + resolved category

```sql
SELECT
  t.transaction_id,
  t.transaction_date,
  t.amount,
  t.description,
  t.merchant_name,
  a.display_name AS account_name,
  c.category,
  c.subcategory
FROM core.fct_transactions AS t
INNER JOIN core.dim_accounts AS a
  ON t.account_id = a.account_id
LEFT JOIN core.dim_categories AS c
  ON t.category = c.category   -- t.category is already the resolved text
WHERE NOT a.archived
  AND NOT t.is_transfer
  AND t.is_pending = FALSE;
```

`t.category` is already resolved on the fact (fallback chain documented above). The `dim_categories` join only adds `description` / `plaid_detailed` if you need them.

### Transfer pair lookup

Given a `transaction_id`, find its counterparty:

```sql
SELECT
  t.transaction_id   AS my_side,
  CASE WHEN t.amount < 0 THEN b.credit_transaction_id
                          ELSE b.debit_transaction_id
  END                AS counterparty_id,
  b.date_offset_days,
  b.amount           AS transfer_amount
FROM core.fct_transactions AS t
INNER JOIN core.bridge_transfers AS b
  ON t.transfer_pair_id = b.transfer_id
WHERE t.transaction_id = ?;
```

Convention: `debit_transaction_id` is the outgoing (negative) side, `credit_transaction_id` is the incoming (positive) side.

### Tag-namespace filter

`tags` is `LIST(VARCHAR)` with strings shaped `'namespace:value'` or bare `'value'`. To filter for a namespace:

```sql
SELECT *
FROM core.fct_transactions
WHERE tag_count > 0
  AND EXISTS (
    SELECT 1
    FROM UNNEST(tags) AS u(tag)
    WHERE u.tag LIKE 'project:%'
  );
```

For an exact tag, `'project:side-hustle' = ANY(tags)` is faster.

## Canonical queries

Patterns that any analytics consumer will recreate. Verified against the schemas above.

### Monthly spending by category, last 12 months, excluding transfers

```sql
SELECT
  year_month,
  category,
  total_spend                  -- positive (SUM(ABS(amount)))
FROM reports.spending_trend
WHERE year_month >= STRFTIME(CURRENT_DATE - INTERVAL '12' MONTHS, '%Y-%m')
ORDER BY year_month, total_spend DESC;
```

`spending_trend` already filters `amount < 0 AND NOT is_transfer AND NOT archived`. Don't re-derive from `fct_transactions` unless you need a non-monthly grain.

### Net-worth snapshot

```sql
SELECT balance_date, net_worth, total_assets, total_liabilities
FROM reports.net_worth
ORDER BY balance_date DESC
LIMIT 1;
```

Use `reports.net_worth` for the snapshot. Reach down to `core.fct_balances_daily` only when you need per-account detail or want to apply non-default account filters (e.g., include archived accounts).

### Splits-sum invariant

Splits should sum to the parent amount. The invariant isn't enforced in SQL; use this assertion query to find violations:

```sql
SELECT
  t.transaction_id,
  t.amount               AS parent_amount,
  SUM(l.line_amount)     AS lines_sum,
  t.amount - SUM(l.line_amount) AS variance
FROM core.fct_transactions AS t
INNER JOIN core.fct_transaction_lines AS l
  USING (transaction_id)
WHERE t.has_splits
GROUP BY t.transaction_id, t.amount
HAVING t.amount <> SUM(l.line_amount);
```

### Top merchants in a date window

`reports.merchant_activity` is lifetime-only. For a bounded window (last 90 days, current year, etc.), query `core.fct_transactions` directly with the same filters `merchant_activity` applies — `amount < 0 AND NOT is_transfer AND NOT a.archived` — bucket NULL `merchant_name` to `'(unknown)'`, group by `merchant_name`, `ORDER BY SUM(ABS(amount)) DESC LIMIT N`.

## Anti-patterns

What not to do, and why.

- **Don't `SUM(amount) FROM core.fct_transactions` without filtering `is_transfer = FALSE`.** Transfers appear as a debit on one account and credit on another. They cancel in aggregate over the whole table, but they double-count within any account-level slice.
- **Don't aggregate both `core.fct_transactions.amount` and `core.fct_transaction_lines.line_amount` in the same query.** Pick one grain. The lines view sums to the same totals as the fact (whole = parent.amount, split lines sum to parent.amount); joining both yields 2×.
- **Don't read from `prep.*`.** It's internal staging — column shapes can change without notice and no catalog comments are emitted. Use `core.*`.
- **Don't `SUM(amount)` across mixed currencies.** `reports.*` and any cross-account aggregate over `fct_transactions` add `amount` without FX conversion. For single-currency users this is correct; for multi-currency users it's wrong. Filter by `currency_code` or `iso_currency_code` until multi-currency support ships.
- **Don't filter on `reports.uncategorized_queue.source_id`.** It's a NULL placeholder today.
- **Don't mix sign conventions.** If you join `cash_flow.outflow` (negative) and `spending_trend.total_spend` (positive) in the same expression, the math is wrong. Pick one view per question.
- **Don't query `app.transaction_notes` / `app.transaction_tags` / `app.transaction_splits` directly when you need them per-transaction.** They're already aggregated as nested `LIST(STRUCT(...))` columns on `core.fct_transactions`. Direct queries miss the resolved shape and bypass the audit-emitting service layer for writes.

## `app.*` — user-state surface

Tables here capture state that cannot be re-derived from raw sources: categorization choices, notes, tags, splits, budgets, account settings. **Writes happen through services / MCP tools, not raw SQL.** Each row carries `updated_at` (and usually a `created_by` / `actor` field) so the audit log can be reconstructed.

| Table | Grain | Purpose |
|---|---|---|
| `app.account_settings` | One row per `account_id` | User-controlled account fields (`display_name`, `archived`, `include_in_net_worth`, etc.). Surfaced via `core.dim_accounts`. |
| `app.balance_assertions` | One row per `(account_id, assertion_date)` | User-entered balance anchors. Feeds `core.fct_balances` and `reports.balance_drift`. |
| `app.transaction_categories` | One row per `transaction_id` | Category assignments. Carries `category_id`, `categorized_by` (`rule` / `ai` / `user`), `confidence`, `merchant_id`, `rule_id`. |
| `app.transaction_notes` | One row per `note_id` | Free-form notes. Joined into `core.fct_transactions.notes`. Max 2000 chars (service-enforced). |
| `app.transaction_tags` | One row per `(transaction_id, tag)` | Slug-flavored tags (`^[a-z0-9_-]+(:[a-z0-9_-]+)?$`). Joined into `core.fct_transactions.tags`. |
| `app.transaction_splits` | One row per `split_id` | Split children. Sum should equal `parent.amount` (not SQL-enforced — see the assertion query). Joined into `core.fct_transactions.splits`. |
| `app.categorization_rules` | One row per `rule_id` | Pattern-based auto-categorization rules. |
| `app.proposed_rules` | One row per `proposed_rule_id` | Auto-rule proposals staged for review. |
| `app.rule_deactivations` | One row per `deactivation_id` | Audit trail for rule deactivations. |
| `app.user_merchants` | One row per `merchant_id` | Mutable merchant entries. Surfaced via `core.dim_merchants`. |
| `app.user_categories` | One row per `category_id` | User-created categories. Combined with seeds via `core.dim_categories`. |
| `app.category_overrides` | One row per `category_id` | User soft-deletions on seed categories. |
| `app.budgets` | One row per `budget_id` | Monthly spending targets by category over a `start_month`–`end_month` window. |
| `app.imports` | One row per labeled `import_id` | User-applied labels on import batches. FK → `raw.import_log.import_id`. |
| `app.audit_log` | One row per mutation | Unified audit log; emitted synchronously in the same transaction as the mutation. |
| `app.match_decisions` | One row per `match_id` | Matcher + user-review decisions. `match_type` ∈ `{dedup, transfer}`. Source for `core.bridge_transfers`. |
| `app.tabular_formats` | One row per format `name` | Saved column mappings for tabular imports (Chase, Citi, Tiller, Mint, YNAB built-ins + auto-detected). |

**Internal `app.*` tables (do not query directly):** `app.seed_source_priority`, `app.metrics`, `app.versions`, `app.schema_migrations`. These are ops plumbing — source-priority ranking, Prometheus snapshots, component versions, migration history.

MCP-visible app tables are tagged `audience="interface"` in [`src/moneybin/tables.py`](../../src/moneybin/tables.py); internal-only tables are reachable via the read-only SQL surface but not advertised on the `moneybin://schema` resource.

## `meta.*` — provenance and lineage

| Table | Grain | Purpose |
|---|---|---|
| `meta.fct_transaction_provenance` | One row per (gold record × contributing source row) | Links every `core.fct_transactions.transaction_id` to every source row that contributed. Unmatched records have exactly one provenance row (`match_id = NULL`). Matched groups have one row per contributing source. |
| `meta.model_freshness` | One row per registered SQLMesh model | Public-contract wrapper over `sqlmesh._snapshots`. Exposes `last_changed_at` (when the current content version was first materialized) and `last_applied_at` (when SQLMesh last wrote to any snapshot row). |

`meta.fct_transaction_provenance` columns:

| Column | Description |
|---|---|
| `transaction_id` | FK to `core.fct_transactions.transaction_id`. |
| `source_transaction_id` | Source-native ID, joinable to raw/prep. |
| `source_type` | Import pathway / origin system. |
| `source_origin` | Institution / connection / format. |
| `source_file` | File that produced the source row. |
| `source_extracted_at` | When the row was parsed (UTC). |
| `match_id` | FK to `app.match_decisions.match_id`; NULL for unmatched records. |

## `seeds.*` — reference data

| Table | Grain | Backing |
|---|---|---|
| `seeds.categories` | One row per `category_id` | CSV-backed (`sqlmesh/models/seeds/categories.csv`). 16 primary categories with ~100 subcategories, based on Plaid Personal Finance Category v2. Columns: `category_id`, `category`, `subcategory`, `description`, `plaid_detailed`. SQLMesh detects CSV changes automatically. |

Surfaced via `core.dim_categories` alongside `app.user_categories`.

## Identifier conventions

Identifiers across the model use a small set of strategies — source IDs where available, deterministic content hashes when not, truncated UUID4 hex for opaque keys. Full rule and rationale: [`.claude/rules/identifiers.md`](../../.claude/rules/identifiers.md).

## See also

- [`docs/guides/data-pipeline.md`](../guides/data-pipeline.md) — how rows reach these tables (raw → prep → core), refresh semantics, dedup rules.
- [`docs/specs/architecture-shared-primitives.md`](../specs/architecture-shared-primitives.md) — the 12 shared primitives consumers rely on.
- [`docs/guides/sql-access.md`](../guides/sql-access.md) — opening the encrypted database from external clients.
- [`src/moneybin/tables.py`](../../src/moneybin/tables.py) — `TableRef` constants; the canonical list of advertised table names.
