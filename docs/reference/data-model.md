<!-- Last reviewed: 2026-09-02 -->
# Data Model

The user-facing data model. Tables in `core.*`, `reports.*`, and `app.*` are the surfaces consumers (CLI, MCP, your own SQL) read from for analysis; `raw.*` and `prep.*` are readable for inspection through the agent-safe SQL paths. This page covers each table's grain, key columns, and what they mean. For the pipeline that fills them, see [`docs/guides/data-pipeline.md`](../guides/data-pipeline.md).

Schema is stable but not yet frozen — see [`docs/architecture.md`](../architecture.md) for the pre-v1 evolution posture. Tables here are verified against their SQLMesh model in [`src/moneybin/sqlmesh/models/`](../../src/moneybin/sqlmesh/models/) (`core.*`, `reports.*`, `meta.*`, `seeds.*`) or DDL in [`src/moneybin/sql/schema/`](../../src/moneybin/sql/schema/) (`app.*`, `raw.*`); per-table file links are omitted since file names match table names.

## Schema layers

| Schema | Purpose | Materialization | Read? | Write? |
|---|---|---|---|---|
| `raw` | Source-specific tables, preserved as imported. Loaders write here. | Tables | Inspection only | Loaders only |
| `prep` | Light staging — type casts, renames, the matched/merged intermediate. | Views | Inspection only | SQLMesh only |
| `core` | Canonical analytical tables — `fct_*`, `dim_*`, `bridge_*`. | Views and tables | Yes | **Blocked** |
| `app` | User state — categorizations, notes, tags, splits, budgets, settings. | Tables | Yes | Via services / MCP write tools |
| `meta` | Cross-source provenance + lineage. | Views | **Refused** | SQLMesh / system |
| `seeds` | Reference data shipped with MoneyBin — six registries: categories, provider-category map, account types, exchange MICs, institutions, price sources. | CSV-backed tables | **Refused** | SQLMesh |
| `reports` | Curated presentation views, one per CLI/MCP report. | Views | Yes | **Blocked** |

"Read?" answers for the agent-safe SQL surface — the `sql_query` MCP tool and `moneybin sql query`. Those admit `core`, `app`, `reports`, `raw`, and `prep`, and refuse `meta` and `seeds` by `DESCRIBE` as by `SELECT`. `raw` and `prep` are an inspection exception, not a widening of the analysis contract: their shapes change without notice, and they carry 34 column declarations with every other value masked by a value-shape scan rather than by a declared class. `moneybin db shell` and `moneybin db query` are raw operator access with no privacy middleware; they read every schema and mask nothing.

Mutations use service-backed MCP or CLI write paths for `app.*` and loader-only `raw.*`. The general MCP SQL surface is read-only.

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
| `core.uncategorized_queue.amount` | Signed (preserved from source). |
| `reports.net_worth.total_liabilities` | **Negative** (preserved). |

If you sum `outflow` from `cash_flow` and `total_spend` from `spending_trend` in the same query, one is negative and the other is positive. Don't.

### Currency handling

`core.fct_transactions.currency_code`, `core.fct_investment_transactions.currency_code` and `core.dim_accounts.currency_code` are ISO 4217 strings. `fct_transactions.currency_code` resolves to the transaction's own captured currency (from OFX `CURDEF` or Plaid), else its account's `currency_code`, else `NULL`, and `fct_investment_transactions.currency_code` resolves the same way from the event's own currency (typed at `investments add`, or reported by Plaid); `dim_accounts.currency_code` resolves to the user's `accounts set --currency` override, else the currency the account's own source reported (OFX `CURDEF`, Plaid `iso_currency_code`, the tabular `currency` column), else `NULL`. There is no `'USD'` fallback: an account nobody stated a currency for is unknown, and `moneybin system doctor` reports it rather than guessing.

Every `reports.*` view that sums money carries a `currency_code` column and groups by it, so a mixed-currency profile gets one sub-total per currency rather than one combined number. A `NULL` currency is its own segment — never resolved to the home currency, because that guess is one nothing downstream could flag. All unknown-currency rows share that one segment and are summed together, since nothing distinguishes two unknowns; `moneybin system doctor` fails on any of them, and `accounts set --currency` is the fix. `reports.net_worth` is one row per `(balance_date, currency_code)`; a consumer that re-aggregates it must keep `currency_code` in its own `GROUP BY` or it re-blends what the view separated. `reports.balance_drift` projects `currency_code` without grouping by it — asserted and computed balances belong to the same account, so the comparison is single-currency by construction.

`moneybin profile set home_currency <ISO 4217>` records which currency a profile treats as home, and reports price into it by default; `--display-currency` overrides it per call. Conversion is presentation-only — nothing writes a converted amount, and every `core.*` column keeps its original. Three reports convert: `core:large_transactions`, `core:balance_drift`, and `core:networth`, each of whose rows carries one amount and one date to price it on. Every other report aggregates per `currency_code`, so pricing a row would leave several rows sharing one grain key; those stay segmented and say why in `summary.degraded_reason`. One row that cannot be priced segments the whole result rather than converting part of it, and `summary.display_currency` then names the currency the rows are already in.

### Pending and posted

`core.fct_transactions.is_pending = TRUE` whenever any contributing source row is still flagged pending. When the source posts, the next refresh flips it to `FALSE`; `transaction_id` is stable across that transition because the content-hash inputs (date, amount, account) don't change on post, and Plaid's `transaction_id` is `plaid_<provider_transaction_id>`. Practical filter: most analytics queries should add `WHERE is_pending = FALSE`.

### Dates and timezones

- **Date columns** (`transaction_date`, `authorized_date`, `balance_date`, `assertion_date`, `txn_date`) — institution-local calendar dates; no timezone conversion.
- **Timestamp columns** (`extracted_at`, `loaded_at`, `created_at`, `updated_at`, `applied_at`, `categorized_at`) — UTC. Plaid writes `Datetime(time_zone="UTC")`; OFX and tabular loaders write naive timestamps treated as UTC.
- **Calendar parts** (`transaction_year`, `transaction_month`, `transaction_day`, `transaction_day_of_week`) — derived from `transaction_date` (institution-local). Day-of-week: `0 = Sunday`.

### Money types

`DECIMAL(18,2)` for money columns — never `FLOAT`. The values are **major units** (dollars, euros), not minor units (cents). Polars uses `pl.Decimal(18, 2)`; Python uses `decimal.Decimal`. Investment quantities and unit prices (share counts, cost basis per share, security closes) use `DECIMAL(28,10)` — see [Investments](#investments) below. Exchange rates use `DECIMAL(18,8)` in `raw.exchange_rates` and `app.exchange_rate_overrides`: a rate is a ratio, not an amount, so it never rounds to cents. Every money column in `reports.*` inherits `DECIMAL(18,2)` from the underlying `core` source.

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
| `original_description` | VARCHAR | Raw, unmodified bank-statement description (Plaid `original_description`); NULL when the source has no separate raw form. Distinct from `description` (cleaned) and `memo` (supplementary). |
| `merchant_name` | VARCHAR | `COALESCE(core.dim_merchants.canonical_name, source value)`. Display only. |
| `merchant_id` | VARCHAR | FK → `core.dim_merchants.merchant_id`. NULL when no canonical merchant has been resolved. GROUP/PARTITION on this, not `merchant_name` (identifiers.md Guard 1). |
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
| `location_latitude` / `_longitude` | DOUBLE | Merchant coordinates; NULL when not provided. |
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
| `account_type` | VARCHAR | Canonical classification, normalized across all sources via `seeds.account_type_map`: `depository`, `credit`, `loan`, `investment`, `other`. `NULL` when the source spelling is unrecognized. |
| `institution_name` | VARCHAR | Human-readable institution. For OFX, resolved from `<FI><FID>` via `seeds.institutions`, falling back to the raw `<ORG>` for an unregistered FID — `<ORG>` is a routing code (Chase publishes `B1`), not a name. |
| `institution_slug` | VARCHAR | Canonical institution slug, the value account matching compares. Resolved from `seeds.institutions` for every source: OFX by exact `<FID>`, tabular and Plaid by normalizing their institution text (case and punctuation stripped) against both the registry's `slug` and its `display_name`. Falls back to the source's own text when the institution is unregistered. Slugifying a display name does not reproduce a curated slug (`U.S. Bank` → `u-s-bank`, not `us_bank`), so a consumer that matches on `institution_name` drops candidates. |
| `institution_fid` | VARCHAR | OFX FID; NULL for tabular sources. |
| `source_type`, `source_file` | VARCHAR | Source of the winning record after dedup; source file path. |
| `extracted_at`, `loaded_at`, `updated_at` | TIMESTAMP | Source-parse / DB-write times (UTC). `updated_at = GREATEST(loaded_at, account_settings.updated_at)`. |
| `display_name` | VARCHAR | User override → derived default (`institution_name + account_subtype + …<last4>`; the subtype is preferred over the canonical type because "checking" reads to a human where "depository" does not) → `institution_name + …<last4>` for a typeless account → `institution_name + account_subtype` → `institution_name` alone → `account_subtype + …<last4>` for an account with no institution → the subtype alone → `…<last4>` alone → the literal `Unnamed account`. A last four outranks the category beside it at every level, because the category is shared and the last four is what tells two accounts apart. The terminal is never the `account_id`: for an account with no accepted link that id is the institution's own account number. |
| `display_name_is_user_set` | BOOLEAN | Provenance flag, never for display: TRUE when `display_name` came from the user-set override or the source's own account label, FALSE for a generated fallback. `AccountResolver`'s weak name-match signal requires TRUE, since two accounts' generated descriptors coinciding is not evidence a person named either one. |
| `official_name` | VARCHAR | User-set or Plaid-supplied formal name. |
| `last_four` | VARCHAR | User-set or Plaid mask. |
| `account_subtype` | VARCHAR | Plaid-style subtype (`checking`, `savings`, `credit card`, `mortgage`, ...). User override, else the provider's own subtype, else derived from the source spelling by `seeds.account_type_map`. |
| `holder_category` | VARCHAR | `personal` / `business` / `joint`. |
| `currency_code` | VARCHAR | ISO-4217. User override, else the currency the account's own source reported; `NULL` when nobody stated one — there is no `'USD'` default. See "Currency handling" above. |
| `credit_limit` | DECIMAL(18,2) | User-asserted; drives utilization metrics. |
| `archived` | BOOLEAN | Hides from default lists and `reports.net_worth`. |
| `include_in_net_worth` | BOOLEAN | Independent toggle; archiving forces FALSE. |

Logical grain key: `account_id`.

The three staging views union into one set, then group on the canonical id — the `app.account_links` id when the source record is bound to one, its own source-native key when it is not. Each column is then merged across the group on its own, not taken wholesale from one winning row:

- **Structured bank fields** (`routing_number`, `institution_fid`, `last_four`, and the displayed `source_type` / `source_file`) — first non-null by source strength, then recency: `ARG_MIN` over `(source_rank, -EPOCH_US(extracted_at))`, with `ofx` = 0, `plaid` = 1, everything else 2.
- **`institution_slug`** — resolved-first, then recency. A slug `seeds.institutions` resolved outranks raw text however recently the raw text arrived; ranking by recency alone would let one unregistered spelling in a later spreadsheet overwrite the canonical slug and stop the account matching itself on the next import.
- **Descriptive fields** (`institution_name`, `account_type`, `official_name`, `account_label`, `account_subtype`, and the source-reported currency) — first non-null by recency: `ARG_MAX` over `extracted_at`.
- **`extracted_at` / `loaded_at`** — `MAX` over the group, which keeps `updated_at` monotone.

The per-field merge is what stops a later, weaker source's `NULL` from clobbering a value a stronger source already supplied.

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

Resolved category dimension. Unifies `seeds.categories` (17 primary, ~95 subcategories, based on Plaid PFC v2) with `app.user_categories`, applying `app.category_overrides`. `UNION` (not `UNION ALL`) collapses accidental ID collisions.

| Column | Type | Description |
|---|---|---|
| `category_id` | VARCHAR | Seed-supplied semantic ID or 12-char UUID hex for user categories. |
| `category` | VARCHAR | Top-level name. |
| `subcategory` | VARCHAR | NULL for top-level-only entries. |
| `description` | VARCHAR | Human-readable description. |
| `class` | VARCHAR | Accounting class: `income` \| `expense` \| `transfer` \| `debt`. Derived from the `category_id` prefix. |
| `is_default` | BOOLEAN | TRUE for seeded, FALSE for user-created. |
| `is_active` | BOOLEAN | FALSE if user has soft-deleted a default via `app.category_overrides`. |
| `created_at`, `updated_at` | TIMESTAMP | NULL for seed rows (query `meta.model_freshness` for seed freshness); set for user-created (UTC). |

Logical grain key: `category_id`.

### `core.bridge_category_source_map`

Resolved provider-code → canonical-category bridge. Grain: one row per `(source_type, source_category_code)`. `VIEW` that unions the `seeds.category_source_map` defaults with `app.category_source_map` user overrides — a user row for a given code always wins over the seed default. This is the reverse-lookup key for turning an aggregator's category code (e.g. Plaid PFC) into a MoneyBin `category_id`, with no schema change per new aggregator.

| Column | Type | Description |
|---|---|---|
| `source_type` | VARCHAR | Aggregator / import pathway the code comes from (e.g. `plaid`). |
| `source_category_code` | VARCHAR | The provider's category code (e.g. `FOOD_AND_DRINK_COFFEE`). |
| `code_level` | VARCHAR | `detailed` \| `primary`. Detailed wins over primary on reverse lookup. |
| `category_id` | VARCHAR | FK → `core.dim_categories.category_id`. Exactly one per code (canonical-by-PK). |
| `source_taxonomy_version` | VARCHAR | Provider taxonomy version the mapping was derived against (drift marker; not part of the key). |
| `is_default` | BOOLEAN | TRUE for seed rows, FALSE for user overrides from `app.category_source_map`. |

Two-tier reverse lookup — match a transaction's detailed and primary codes and let detailed win: `WHERE source_category_code IN (detailed, primary) ORDER BY code_level = 'detailed' DESC LIMIT 1`.

### `core.bridge_merchant_entities`

The source system's own merchant-entity reference for a gold transaction. Grain: one row per `transaction_id`. `VIEW` over `prep.int_transactions__merged`, filtered to rows that carry an entity id — sources that issue none (OFX, tabular, manual) are simply absent, so consumers LEFT JOIN.

| Column | Type | Description |
|---|---|---|
| `transaction_id` | VARCHAR | FK → `core.fct_transactions.transaction_id`. |
| `merchant_entity_id` | VARCHAR | The source system's stable merchant id. Opaque — never an account number. |
| `merchant_entity_source_type` | VARCHAR | `source_type` of the merge member that issued the id — NOT the merge-winner `fct_transactions.source_type`. The two differ whenever an entity-bearing row deduped against a higher-priority source. |
| `source_merchant_name` | VARCHAR | Merchant name as the source stated it. Distinct from `fct_transactions.merchant_name`, which has already been replaced by the resolved canonical name. |

`(merchant_entity_source_type, merchant_entity_id)` is the pair `app.merchant_links` binds to a canonical `core.dim_merchants` row. Key on the pair, never on the id alone: two sources may mint the same id string for different merchants.

### `core.bridge_transfers`

Confirmed transfer pairs linking two `fct_transactions` rows. Grain: one row per `transfer_id`. `VIEW` derived from `app.match_decisions` where `match_type = 'transfer'`, `match_status = 'accepted'`, and `reversed_at IS NULL` — a later-reversed match drops out even though its `match_status` still reads `'accepted'`.

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
| `currency_code` | VARCHAR | ISO 4217, inherited from the parent fact row — every line of a transaction shares its denomination. `GROUP BY` it whenever you sum `line_amount`; NULL means the currency is genuinely unknown. |
| `account_id`, `transaction_date`, `merchant_name`, `description`, `is_pending`, `is_transfer`, `transfer_pair_id`, `source_type`, `source_count`, `transaction_year`, `transaction_month`, `transaction_year_month`, `transaction_year_quarter` | various | Carried from the parent fact row. |

Logical grain key: `(transaction_id, line_id)`.

**Don't double-count.** Pick one grain per query. If you sum from `fct_transactions`, do not also sum from `fct_transaction_lines` — the lines view sums to the same totals (whole or split). Mixing both produces 2× the answer.

### `core.fct_balances`

Observation-grain balance view: OFX statement balances, tabular running balances, and user-entered assertions, unioned. `VIEW`.

| Column | Type | Description |
|---|---|---|
| `account_id` | VARCHAR | Source-system account identifier. |
| `balance_date` | DATE | Date the balance was observed (institution-local). |
| `balance` | DECIMAL(18,2) | Observed balance. **Liabilities negative.** OFX and tabular sources arrive pre-signed from the institution; Plaid's `current_balance` (always reported positive) is negated at ingest for `credit` / `loan` account types. A Plaid row whose account type can't be resolved is dropped rather than defaulted positive. |
| `source_type` | VARCHAR | `ofx` \| `tabular` \| `assertion` \| `plaid`. |
| `source_ref` | VARCHAR | File path (ofx/tabular), `'user'` for assertions, or the Plaid item id. |
| `updated_at` | TIMESTAMP | Underlying observation's `loaded_at` / `created_at` (UTC). |
| `currency_code` | VARCHAR | ISO 4217; the observation's own captured currency, else inherited from `core.dim_accounts.currency_code`. |

### `core.fct_balances_daily`

Per-account daily balance spine. Grain: one row per `(account_id, balance_date)` from each account's first observation to its last. `FULL` Python model.

Observed days use the most authoritative source (per-day precedence: `user assertion > {ofx, plaid} > tabular`; `ofx` and `plaid` tie and are broken by freshest `updated_at`, then `source_type` ascending). Gaps are filled by carrying the last balance forward, adjusted by intervening transactions from `core.fct_transactions`.

Only transactions denominated in the **currency being carried** adjust the balance. A transaction in any other currency is left out rather than added as though the units matched. This spine converts nothing: exchange rates live in `raw.exchange_rates` and conversion runs at the report layer, after `core` is built, so no rate reaches this model. The excluded movement is not lost: it appears in the next observation's `reconciliation_delta`, and therefore as drift in `reports.balance_drift`. `moneybin system doctor` warns whenever a profile holds more than one currency and names this consequence.

| Column | Type | Description |
|---|---|---|
| `account_id` | VARCHAR | FK → `core.dim_accounts.account_id`. |
| `balance_date` | DATE | Calendar date (institution-local). |
| `balance` | DECIMAL(18,2) | End-of-day balance. |
| `is_observed` | BOOLEAN | TRUE if an authoritative observation exists for this date. |
| `observation_source` | VARCHAR | Winning observation's source (`ofx`, `tabular`, `assertion`, `plaid`); NULL when interpolated. |
| `reconciliation_delta` | DECIMAL(18,2) | `observed_balance − transaction_derived_balance`. Positive when the observed balance exceeds what transactions alone would predict; negative when below. NULL on interpolated days, on the first observation, and whenever the observed currency differs from the one carried into that day — the prior balance is then in another unit, and this model converts nothing, so no comparison is defined. |
| `currency_code` | VARCHAR | ISO 4217; carried forward from the winning observation (or its interpolated predecessor) on each day. |

Logical grain key: `(account_id, balance_date)`.

### `core.uncategorized_queue`

Uncategorized transactions ranked by curator-impact. Grain: one row per uncategorized transaction. Excludes transfers and archived accounts. Service-internal — its only reader is the categorization surface (`moneybin transactions categorize pending` / MCP `reviews(kind="categorization", status="pending")`), not a standalone `reports.*` view.

| Column | Type | Description |
|---|---|---|
| `transaction_id` | VARCHAR | Joinable to `core.fct_transactions.transaction_id`. |
| `account_id` | VARCHAR | Owning account; RECORD_ID (opaque, unmasked — spec D6), same as everywhere else. |
| `account_name` | VARCHAR | Resolved display name; NULL only if `dim_accounts.display_name` itself is NULL (uncommon). |
| `txn_date` | DATE | Transaction date. |
| `amount` | DECIMAL(18,2) | Signed (source sign preserved). |
| `currency_code` | VARCHAR | ISO 4217 the amount is denominated in; NULL when unknown. |
| `description` | VARCHAR | Source description. |
| `merchant_id` | VARCHAR | FK → `core.dim_merchants.merchant_id`. NULL when no canonical merchant was resolved. |
| `merchant_normalized` | VARCHAR | Resolved merchant; NULL when no `dim_merchants` match and no source merchant value. |
| `age_days` | INTEGER | `CURRENT_DATE − txn_date`. |
| `priority_score` | DECIMAL(18,2) | `ABS(amount) × age_days` — default sort key. Compares nominal magnitudes, so it only ranks meaningfully within one `currency_code`. |
| `source_type` | VARCHAR | Provenance source. |
| `source_id` | VARCHAR | **NULL placeholder today.** Reserved column pending `source_id` surfacing on `fct_transactions`. Don't filter or join on it. |

### Investments

`core.fct_investment_transactions` is the only authored/ingested investment surface — lots, holdings, and realized gains all derive from it (Invariant 8: never write to a derived table directly). Sign convention mirrors `fct_transactions.amount`: negative = cash out, positive = cash in.

### `core.dim_securities`

Canonical securities dimension. Grain: one row per `security_id`. `VIEW` over `app.securities` — a catalog view, not a curated seed list (same pattern as `core.dim_merchants`): MoneyBin ships no security catalog, every security is user- or provider-created.

| Column | Type | Description |
|---|---|---|
| `security_id` | VARCHAR | Stable surrogate (truncated UUID4, 12 hex); never derived from ticker. |
| `name` | VARCHAR | Display name. |
| `security_type` | VARCHAR | `equity` \| `etf` \| `mutual_fund` \| `bond` \| `crypto` \| `cash` \| `other`. `cash` covers money-market/sweep positions. |
| `ticker` | VARCHAR | Display/lookup ticker; not unique (tickers get reused) — carry `security_id` for joins/aggregation (identifiers.md Guard 1). |
| `exchange` | VARCHAR | Listing exchange; disambiguates duplicate tickers. |
| `cusip` | VARCHAR | Licensed identifier. User-entered, or passed through from a provider that sends one — Plaid has gated CUSIP behind a license since 2024-03, so Plaid-sourced rows are NULL in practice. |
| `isin` | VARCHAR | International identifier. |
| `figi` | VARCHAR | OpenFIGI mapping. |
| `coingecko_id` | VARCHAR | Crypto price-lookup slug. |
| `is_cash_equivalent` | BOOLEAN | Treat-like-cash flag (money-market/sweep); NULL = unknown. |
| `currency_code` | VARCHAR | Denominating currency. Stored amounts are never converted; `--display-currency` prices them at read time and leaves this column alone. |

`created_by`, `created_at`, `updated_at`, and the per-security `cost_basis_method` override live on `app.securities` but are not projected through this view.

Logical grain key: `security_id`.

### `core.dim_holdings`

Current positions: the sum of open lots per `(account_id, security_id)` — a "now" snapshot with no date dimension, rebuilt on every run. `VIEW`. Grain: `(account_id, security_id)`.

| Column | Type | Description |
|---|---|---|
| `account_id` | VARCHAR | FK → `core.dim_accounts` (grain). |
| `security_id` | VARCHAR | FK → `core.dim_securities` (grain). |
| `quantity` | DECIMAL(28,10) | Total open units (`Σ remaining_quantity` across open lots). |
| `cost_basis` | DECIMAL(18,2) | Total open basis (`Σ cost_basis_remaining`). Under average cost the pooled remaining basis can exceed a single lot's own total — this, not `cost_basis_total`, is the meaningful figure. |
| `average_cost` | DECIMAL(28,10) | `cost_basis / quantity`; NULL when `quantity` is 0. |
| `currency_code` | VARCHAR | Denominating currency for the position. |
| `market_value` | DECIMAL(18,2) | `quantity × resolved close`. **NULL — never zero** — when unpriced, withheld, or source-overlapped (see `valuation_status`); a zero would be indistinguishable from a worthless position and silently understate any aggregate that sums it. |
| `unrealized_gain` | DECIMAL(18,2) | `market_value − cost_basis`. NULL whenever `market_value` is NULL, and also on an otherwise-valued row when any contributing open lot has `basis_incomplete` (an unknown-basis `transfer_in` stores a placeholder 0.00 that would overstate the gain). |
| `price_date` | DATE | Date of the close used (may be earlier than today). NULL exactly when `market_value` is NULL. |
| `price_source` | VARCHAR | `source_type` that supplied the close (see `core.fct_security_prices`). NULL exactly when `price_date` is NULL. |
| `days_since_observed` | INTEGER | `CURRENT_DATE − price_date`. NULL exactly when `price_date` is NULL. |
| `valuation_status` | VARCHAR | `valued` (priced as of today) \| `carried_forward` (priced, but the close predates today) \| `unpriced` (no usable close resolved) \| `withheld` (the figure cannot be trusted — either a known-wrong share count, from a broker snapshot contradiction, an unreconciled split, or a fresh snapshot that dropped a position the ledger still carries; or open lots that disagree on currency, including one lot with a known currency beside one with none, which leaves no single close to value the combined quantity against) \| `source_overlap` (the account's investment ledger arrives from more than one source at once — a broker file import beside a connector sync — so the two ledgers interleave, lots double-count, and cost basis mixes two accountings). A `withheld` or `source_overlap` row publishes **no** pricing at all: `market_value`, `unrealized_gain`, `price_date`, `price_source`, `days_since_observed` are all NULL even if a fresh close did resolve — the close itself is not lost, it stays queryable in `core.fct_security_prices`. The two stay distinct because their remedies do: `withheld` wants the share count reconciled, while `source_overlap` wants one of the two feeds removed (`import_revert` on the redundant batch, or `sync_disconnect` on the duplicate connection), and `system doctor`'s `investment_source_overlap` check fails until one is. |
| `provider_reported_quantity` | DECIMAL(28,10) | **NON-AUTHORITATIVE.** The broker's claimed open units from its newest holdings snapshot; reconciliation reference only, never blended into `quantity`. NULL when the broker's newest snapshot doesn't report this position. |
| `provider_reported_cost_basis` | DECIMAL(18,2) | NON-AUTHORITATIVE broker claim; same NULL rule. |
| `provider_reported_value` | DECIMAL(18,2) | NON-AUTHORITATIVE broker claim; MoneyBin's own `market_value` is never derived from this. |
| `provider_reported_as_of` | TIMESTAMP | Oldest `extracted_at` among the snapshots summed into the three columns above (MIN, not MAX) — a position spanning multiple broker connections is only as fresh as its stalest contributor. |
| `updated_at` | TIMESTAMP | Latest of the position's open-lot timestamps, the resolved close's freshness, and (for a broker-reported position) the newest snapshot receipt time. Does not advance on idempotent SQLMesh re-applies. |

Logical grain key: `(account_id, security_id)`.

### `core.fct_investment_transactions`

The canonical investment-transaction ledger. Grain: one row per `investment_transaction_id`. `FULL` model (materialized table, not a view — unlike `fct_transactions`). Unions manual entry, Plaid transactions, and the Plaid opening-lot bootstrap (a reconstruction of pre-import-window lots, carrying the non-user-authorable `subtype = 'opening_bootstrap'` so it's always distinguishable from a real transfer).

| Column | Type | Description |
|---|---|---|
| `investment_transaction_id` | VARCHAR | Canonical ID (source-provided or content hash). |
| `account_id` | VARCHAR | FK → `core.dim_accounts`. |
| `security_id` | VARCHAR | FK → `core.dim_securities`. NULL for cash-only events (deposit, withdrawal, account fee, cash interest) and for a synced security with no accepted binding. |
| `trade_date` | DATE | Trade date; drives holding-period (short/long-term) classification. |
| `settlement_date` | DATE | Settlement date; informational. |
| `original_acquisition_date` | DATE | `transfer_in` only: original acquisition date (the opened lot uses `COALESCE(this, trade_date)`). |
| `type` | VARCHAR | Closed taxonomy, fourteen values: `buy` \| `sell` \| `reinvest` \| `dividend` \| `interest` \| `capital_gain_distribution` \| `transfer_in` \| `transfer_out` \| `deposit` \| `withdrawal` \| `split` \| `fee` \| `return_of_capital` \| `other`. Lot-affecting: `buy`, `sell`, `transfer_in`, `transfer_out`, `reinvest`, `split`, `return_of_capital`. The rest are cash-only (`quantity` NULL). |
| `subtype` | VARCHAR | Per-type refinement (tax character, reinvest funding source); nullable. `'opening_bootstrap'` marks a reconstructed pre-window lot. |
| `event_group_id` | VARCHAR | Links legs of one decomposed economic event (e.g. a reinvest pair); nullable. |
| `quantity` | DECIMAL(28,10) | Signed units: positive = acquire, negative = dispose, NULL for cash-only events. |
| `price` | DECIMAL(28,10) | Per-unit price; NULL for non-priced events. |
| `amount` | DECIMAL(18,2) | Signed cash effect: negative = out (buy), positive = in (sell/dividend). Every branch arrives in this convention already — never re-flip a provider's sign downstream. |
| `fees` | DECIMAL(18,2) | Fee/commission component, folded into cost basis. |
| `currency_code` | VARCHAR | Denominating currency; no FX in v1. The event's own captured currency, else its account's `currency_code`, else `NULL` — the same resolution `fct_transactions` uses, and there is no `'USD'` fallback. |
| `provider_type`, `provider_subtype` | VARCHAR | Provider's original type/subtype strings (e.g. Plaid's), preserved verbatim for audit. NULL for manual and bootstrap rows. Never a ledger input — `type` is the closed taxonomy. |
| `source_type` | VARCHAR | Origin tag: `manual` \| `plaid`. |
| `source_origin` | VARCHAR | Institution/connection scope. |
| `description` | VARCHAR | Free-text description. |
| `updated_at` | TIMESTAMP | The row's own staging `created_at`. Does not advance on idempotent SQLMesh re-applies, nor when an inherited `currency_code` changes because the account's own currency was edited. |

Logical grain key: `investment_transaction_id`.

### `core.fct_investment_lots`

Tax lots derived from `core.fct_investment_transactions`: each acquisition opens a lot; each disposal consumes open lots per the resolved cost-basis method (`fifo` \| `hifo` \| `specific` \| `average`). Grain: one row per `lot_id`. `FULL` Python model — rebuilt in full on every run by the pure cost-basis engine (`moneybin.investments.cost_basis`).

| Column | Type | Description |
|---|---|---|
| `lot_id` | VARCHAR | Content hash of `(account_id, security_id, acquisition_date, opening transaction id)`; prefix `lot_`. |
| `account_id` | VARCHAR | FK → `core.dim_accounts`. |
| `security_id` | VARCHAR | FK → `core.dim_securities`. |
| `acquisition_date` | DATE | Trade date of the opening event; drives short/long-term classification. |
| `acquisition_type` | VARCHAR | `buy` \| `reinvest` \| `transfer_in`. |
| `original_quantity` | DECIMAL(28,10) | Units when the lot opened. |
| `remaining_quantity` | DECIMAL(28,10) | Open units after disposals consumed (0 when fully closed). |
| `cost_basis_total` | DECIMAL(18,2) | Total basis of `original_quantity`, including fees. |
| `cost_basis_remaining` | DECIMAL(18,2) | Basis attributable to `remaining_quantity`. |
| `cost_basis_method` | VARCHAR | Resolved method that governed this lot's consumption. |
| `currency_code` | VARCHAR | Denominating currency. |
| `is_open` | BOOLEAN | `remaining_quantity > 0`. |
| `source_transaction_id` | VARCHAR | FK → the opening `core.fct_investment_transactions` row. |
| `basis_incomplete` | BOOLEAN | TRUE when the lot opened with no supplied basis (e.g. an ACATS-style `transfer_in` with unknown cost basis) — `cost_basis_total`/`cost_basis_remaining` are 0.00, not a real zero. |
| `updated_at` | TIMESTAMP | Latest of the position's ledger-row timestamps (max over the `(account_id, security_id)` group). Does not advance on idempotent SQLMesh re-applies. |

Logical grain key: `lot_id`.

### `core.fct_realized_gains`

Realized gains at the 1099-B grain: one row per `(disposal transaction, consumed lot)` pair. Grain: one row per `realized_gain_id`. `FULL` Python model, sharing the cost-basis engine and inputs with `core.fct_investment_lots`.

| Column | Type | Description |
|---|---|---|
| `realized_gain_id` | VARCHAR | Content hash of `(disposal_txn_id, lot_id)`. |
| `account_id` | VARCHAR | FK → `core.dim_accounts`. |
| `security_id` | VARCHAR | FK → `core.dim_securities`. |
| `disposal_txn_id` | VARCHAR | FK → the disposing `core.fct_investment_transactions` row. |
| `lot_id` | VARCHAR | FK → the consumed `core.fct_investment_lots` row. |
| `quantity` | DECIMAL(28,10) | Units drawn from this lot for this disposal. |
| `acquisition_date` | DATE | Lot acquisition date (holding-period start). |
| `disposal_date` | DATE | Disposal trade date (holding-period end). |
| `proceeds` | DECIMAL(18,2) | Sale proceeds attributable to this quantity, net of fees. |
| `cost_basis` | DECIMAL(18,2) | Cost basis attributable to this quantity (method-dependent). |
| `gain_loss` | DECIMAL(18,2) | `proceeds − cost_basis`; signed, negative is a loss. |
| `term` | VARCHAR | `short` (held ≤ 1 year) \| `long` (held > 1 year). |
| `cost_basis_method` | VARCHAR | Method that produced this basis. |
| `basis_incomplete` | BOOLEAN | TRUE when part of this disposal matched no tracked lot (a zero-basis slice). |
| `currency_code` | VARCHAR | Denominating currency. |
| `updated_at` | TIMESTAMP | Latest of the position's ledger-row timestamps. Does not advance on idempotent SQLMesh re-applies. |

Logical grain key: `realized_gain_id`.

### `core.fct_security_prices`

The resolved price series: one close per `(security_id, price_date, quote_currency)`, with the winning source carried as provenance. Grain: `(security_id, price_date, quote_currency)`. `FULL` model. Only `price_basis = 'raw'` observations are eligible — an adjusted series states a price relative to corporate actions known when fetched, so it stops being correctly adjusted after the next split; adjusted observations stay visible in `prep.stg_security_prices` but are excluded here rather than silently valued.

| Column | Type | Description |
|---|---|---|
| `security_id` | VARCHAR | FK → `core.dim_securities` (grain). |
| `price_date` | DATE | The date this close applies to (grain). |
| `quote_currency` | VARCHAR | ISO 4217 the close is expressed in (grain); this model converts nothing. |
| `close` | DECIMAL(28,10) | The winning close for one unit, in `quote_currency`; always > 0. |
| `source_type` | VARCHAR | Which source supplied the winning close. Five sources are registered in `seeds.price_source_map`, in precedence order: `override` (your own mark, `moneybin investments prices set`), `plaid` (the close carried on a broker snapshot), `tiingo` (equities, ETFs, mutual funds, bonds), `coingecko` (crypto), `trade_implied` (derived from an executed trade in `core.fct_investment_transactions`). Rank decides first, then freshness. The registry is a closed set for providers: a provider observation whose `source_type` is absent from it is dropped in `prep.stg_security_prices` rather than ranked — see `src/moneybin/sqlmesh/models/core/fct_security_prices.sql`. |
| `price_basis` | VARCHAR | Always `'raw'` here. |
| `updated_at` | TIMESTAMP | When the winning observation was served by its provider (the source's own `extracted_at`). |

`core.dim_holdings` looks up the most recent close at or before today per `(security_id, quote_currency)` — as-of, not equality, so weekends/holidays/outages don't blank a position.

## `reports.*` — curated presentation views

All `reports.*` are `VIEW` kind. Consumers (CLI `moneybin reports …`, MCP `reports(report_id=...)`) read these directly.

### Which view should I use?

| Question | View | Notes |
|---|---|---|
| What did I spend, by category, over time? | `reports.spending_trend` | Time-series with MoM / YoY / trailing-3mo windows. Outflow-only, positive values (`SUM(ABS(amount))`). |
| Where did I spend, by merchant? | `reports.merchant_activity` | Lifetime per-merchant aggregates. Top-N is `ORDER BY total_spend DESC LIMIT N`. |
| Income vs. spend by account × category, by month? | `reports.cash_flow` | Signed `inflow` / `outflow` / `net`. Outflow stays negative. |
| What's my net worth? | `reports.net_worth` | Daily snapshot from `fct_balances_daily`. |
| Which transactions are unusually large? | `reports.large_transactions` | Modified z-scores against account and category baselines + `is_top_100`. |
| Which subscriptions am I paying for? | `reports.recurring_subscriptions` | Heuristic candidates with confidence scores; does not auto-classify. |
| Are my balances drifting from reality? | `reports.balance_drift` | Per-assertion deltas vs computed balance; query it directly; `moneybin system doctor` does not read it. |

What's not categorized yet is answered by `core.uncategorized_queue` (above) rather than a `reports.*` view — it's service-internal, reached via `moneybin transactions categorize pending` / MCP `reviews(kind="categorization", status="pending")`, not a standalone report.

When `cash_flow`, `spending_trend`, and `merchant_activity` overlap (e.g., "spend by category last month"), pick the one whose **grain** matches the question: `cash_flow` for `(month, account, category)`, `spending_trend` for `(month, category)` with windowed comparisons, `merchant_activity` for lifetime-per-merchant.

### `reports.net_worth`

Cross-account daily net-worth rollup. Grain: one row per `balance_date`. Excludes accounts where `archived = TRUE` or `include_in_net_worth = FALSE`.

| Column | Type | Description |
|---|---|---|
| `balance_date` | DATE | Calendar date. |
| `account_count` | INTEGER | Distinct accounts contributing. |
| `total_assets` | DECIMAL(18,2) | `SUM(balance WHERE balance > 0)`. |
| `total_liabilities` | DECIMAL(18,2) | `SUM(balance WHERE balance < 0)`; **kept negative**. |
| `net_worth` | DECIMAL(18,2) | `SUM(balance)` across included accounts. |

### `reports.cash_flow`

Monthly inflow/outflow/net per account × category. Grain: one row per `(year_month, account_id, category)`. Excludes transfers (`is_transfer = FALSE`) and archived accounts.

| Column | Type | Description |
|---|---|---|
| `account_id` | VARCHAR | Joinable to `core.dim_accounts.account_id`. |
| `account_name` | VARCHAR | Resolved `dim_accounts.display_name`. |
| `category` | VARCHAR | NULL for uncategorized. |
| `year_month` | VARCHAR | `'YYYY-MM'`. |
| `txn_count` | INTEGER | Non-transfer transactions in this cell. |
| `inflow` | DECIMAL(18,2) | Sum of positive amounts. |
| `outflow` | DECIMAL(18,2) | Sum of negative amounts; **kept negative**. |
| `net` | DECIMAL(18,2) | `inflow + outflow`. |

### `reports.spending_trend`

Monthly spending per category with MoM, YoY, and trailing-3mo windows. Grain: one row per `(year_month, category)`. Outflow-only — restricts to `amount < 0 AND NOT is_transfer AND NOT archived`.

| Column | Type | Description |
|---|---|---|
| `category` | VARCHAR | Grouping key; NULL for uncategorized. |
| `year_month` | VARCHAR | `'YYYY-MM'`. |
| `txn_count` | INTEGER | Outflow count. |
| `total_spend` | DECIMAL(18,2) | `SUM(ABS(amount))`; **positive**. |
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
| `cadence` | VARCHAR | `weekly` \| `biweekly` \| `monthly` \| `quarterly` \| `yearly` \| `irregular`. |
| `status` | VARCHAR | `'active'` if `last_seen` within `max(60 days, 2× cadence)`, else `'inactive'`. |
| `first_seen`, `last_seen` | DATE | Earliest / most recent charge. |
| `interval_days_avg`, `interval_days_stddev` | DECIMAL | Inter-arrival statistics. |
| `confidence` | DECIMAL | `0.0`–`1.0`; saturates at `1.0` with ≥6 occurrences and zero variance. |
| `occurrence_count` | INTEGER | Charges in the last 18 months. |
| `avg_amount`, `annualized_cost` | DECIMAL(18,2) | Mean absolute charge; estimated yearly cost. |

### `reports.merchant_activity`

Per-merchant lifetime aggregations. Grain: one row per `merchant_normalized`. NULL merchants bucketed as `'(unknown)'`. Excludes transfers and archived accounts. Subsumes "top merchants" — top-N is `ORDER BY total_spend DESC LIMIT N`.

| Column | Type | Description |
|---|---|---|
| `merchant_normalized` | VARCHAR | `'(unknown)'` for NULL merchants. |
| `top_category` | VARCHAR | Modal category; NULL if all uncategorized. |
| `first_seen`, `last_seen` | DATE | Date range. |
| `txn_count`, `active_months`, `account_count` | INTEGER | Counts (transactions / distinct year-months / distinct accounts). |
| `total_inflow` | DECIMAL(18,2) | Lifetime sum of positive amounts. |
| `total_outflow` | DECIMAL(18,2) | Lifetime sum of negative amounts; **kept negative**. |
| `avg_amount`, `median_amount` | DECIMAL(18,2) | Signed mean / median. |
| `total_spend` | DECIMAL(18,2) | Lifetime absolute outflow; **positive**. |

### `reports.large_transactions`

All non-transfer transactions with z-scores against account and category baselines. Grain: one row per non-transfer transaction. Uses median + MAD (more outlier-robust than mean + stddev). Consumers filter by their own definition of "large" — top-N, `|z| > 2.5`, etc.

| Column | Type | Description |
|---|---|---|
| `transaction_id` | VARCHAR | Joinable to `core.fct_transactions.transaction_id`. |
| `account_id` | VARCHAR | Owning account. |
| `account_name` | VARCHAR | Resolved display name. |
| `merchant_normalized` | VARCHAR | Resolved merchant; NULL when not curated. |
| `description` | VARCHAR | Source description. |
| `category` | VARCHAR | Spending category; NULL if uncategorized. |
| `txn_date` | DATE | Transaction date. |
| `amount_zscore_account` | DECIMAL | Modified z-score relative to account median + MAD. NULL when MAD = 0. |
| `amount_zscore_category` | DECIMAL | Modified z-score relative to category median + MAD; NULL when category has fewer than 5 transactions or MAD = 0. |
| `is_top_100` | BOOLEAN | TRUE if in the top 100 by `ABS(amount)` overall. |
| `amount` | DECIMAL(18,2) | Signed (source sign preserved). |

### `reports.balance_drift`

Per-`(account, assertion_date)` reconciliation deltas: asserted vs computed balance. Grain: one row per balance assertion. Query it directly; `moneybin system doctor` does not read it.

| Column | Type | Description |
|---|---|---|
| `account_id` | VARCHAR | Joinable to `core.dim_accounts.account_id`. |
| `account_name` | VARCHAR | Resolved display name. |
| `status` | VARCHAR | `clean` (< $1) \| `warning` (< $10) \| `drift` (≥ $10) \| `no-data` (computed NULL). |
| `assertion_date` | DATE | User-asserted balance date. |
| `days_since_assertion` | INTEGER | `CURRENT_DATE − assertion_date`. |
| `asserted_balance` | DECIMAL(18,2) | User-entered value. |
| `computed_balance` | DECIMAL(18,2) | Interpolated daily balance, or observed balance minus its reconciliation adjustment; NULL if the daily row is missing or is the first observation. |
| `drift_abs` | DECIMAL(18,2) | For default sort. |
| `drift_pct` | DECIMAL | `drift / asserted_balance`; NULL when asserted is zero. |
| `drift` | DECIMAL(18,2) | `asserted_balance − computed_balance`. |

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

`t.category` is already resolved on the fact (fallback chain documented above). The `dim_categories` join only adds `description` / `class` if you need them.

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
- **Don't analyze from `prep.*` or `raw.*`.** The agent-safe SQL paths read both, but for inspection only — column shapes change without notice, no catalog comments are emitted, and masking there is a value-shape scan rather than a declared class. Answer questions from `core.*`; reach for `prep.*` / `raw.*` only when `core.*` cannot say what an importer actually produced.
- **Don't `SUM(amount)` across mixed currencies.** The `reports.*` views already group by `currency_code`, but a query of your own over `core.fct_transactions`, `core.fct_transaction_lines`, or `core.fct_balances` does not — nothing converts, so adding dollars to euros yields a number in no currency. Group by `currency_code`, or filter to one.
- **Don't drop `currency_code` when you re-aggregate a `reports.*` view.** Every money-summing view is one row per grain **per currency**; a `GROUP BY` that omits it silently re-blends the currencies the view separated.
- **Don't filter on `core.uncategorized_queue.source_id`.** It's a NULL placeholder today.
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
| `app.user_merchants` | One row per `merchant_id` | Mutable merchant entries. Surfaced via `core.dim_merchants`. |
| `app.user_categories` | One row per `category_id` | User-created categories. Combined with seeds via `core.dim_categories`. |
| `app.category_overrides` | One row per `category_id` | User soft-deletions on seed categories. |
| `app.category_source_map` | One row per `(source_type, source_category_code)` | User overrides for provider-code → `category_id` mappings. Combined with `seeds.category_source_map` via `core.bridge_category_source_map`. |
| `app.budgets` | One row per `budget_id` | Monthly spending targets by category over a `start_month`–`end_month` window. |
| `app.imports` | One row per labeled `import_id` | User-applied labels on import batches. FK → `raw.import_log.import_id`. |
| `app.audit_log` | One row per mutation | Unified audit log; emitted synchronously in the same transaction as the mutation. |
| `app.match_decisions` | One row per `match_id` | Matcher + user-review decisions. `match_type` ∈ `{dedup, transfer}`. Source for `core.bridge_transfers`. |
| `app.tabular_formats` | One row per format `name` | Saved column mappings for tabular imports (Chase, Citi, Tiller, Mint, YNAB built-ins + auto-detected). |
| `app.securities` | One row per `security_id` | Security catalog (ticker, CUSIP, ISIN, FIGI, `cost_basis_method` override). Entries are user-created or minted by `SecurityResolver` during a Plaid sync; `created_by` records which. Surfaced via `core.dim_securities`. |
| `app.lot_selections` | One row per `(investment_transaction_id, lot_id)` | Specific-identification overrides: which lots a disposal draws from and how much; unselected remainder falls back to FIFO. |

**Internal `app.*` tables (do not query directly):** `app.seed_source_priority`, `app.metrics`, `app.versions`, `app.schema_migrations`. These are ops plumbing — source-priority ranking, Prometheus snapshots, component versions, migration history.

MCP-visible app tables are tagged `audience="interface"` in [`src/moneybin/tables.py`](../../src/moneybin/tables.py); internal-only tables are reachable via the read-only SQL surface but not advertised on the `moneybin://schema` resource.

## `meta.*` — provenance and lineage

| Table | Grain | Purpose |
|---|---|---|
| `meta.fct_transaction_provenance` | One row per (gold record × contributing source row) | Links every `core.fct_transactions.transaction_id` to every source row that contributed. Unmatched records have exactly one provenance row (`match_id = NULL`). Matched groups have one row per contributing source. |
| `meta.model_freshness` | One row per registered SQLMesh model | Public-contract wrapper over `sqlmesh._snapshots` and `sqlmesh._intervals`. Exposes `last_changed_at` (when the current content version was first materialized), `last_applied_at` (when SQLMesh last wrote to any snapshot row — promotions included), `last_executed_at` (when the model was last actually backfilled), and `model_kind`. Use `last_executed_at` for "how old is this data?"; it is NULL for symbolic kinds (`EXTERNAL`, `EMBEDDED`), which never execute, and frozen at the first build for `VIEW` / `SEED`, which SQLMesh does not re-run once their interval is complete. |

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
| `seeds.categories` | One row per `category_id` | CSV-backed (`src/moneybin/sqlmesh/models/seeds/categories.csv`). 17 primary categories with ~95 subcategories, based on Plaid Personal Finance Category v2. Columns: `category_id`, `category`, `subcategory`, `description`, `class`. SQLMesh detects CSV changes automatically. |
| `seeds.category_source_map` | One row per `(source_type, source_category_code)` | CSV-backed (`src/moneybin/sqlmesh/models/seeds/category_source_map.csv`). Default provider-code → `category_id` mappings (Plaid PFC). Surfaced via `core.bridge_category_source_map`. |
| `seeds.account_type_map` | One row per `alias` | CSV-backed (`.../seeds/account_type_map.csv`). Source-spelling → canonical `(account_type, account_subtype)` registry (OFX `<ACCTTYPE>`, Plaid, tabular). Lookup is on `UPPER(alias)`. Consumed by `core.dim_accounts`. |
| `seeds.exchange_mic_map` | One row per `alias` | CSV-backed (`.../seeds/exchange_mic_map.csv`). Alias → canonical ISO-10383 MIC registry for exchange-identity resolution. An alias absent from the table is treated as unknown, not a mismatch. |
| `seeds.institutions` | One row per `fid` | CSV-backed (`.../seeds/institutions.csv`). OFX `<FI><FID>` → `(slug, display_name)`. Consumed by `core.dim_accounts.institution_name`; `slug` also feeds `source_origin` at import time (renaming an existing slug re-keys transaction ids and needs a migration). |
| `seeds.price_source_map` | One row per `source_type` | CSV-backed (`.../seeds/price_source_map.csv`). The price-source registry: `source_rank` (declared precedence — append ranks, never reorder them, since inserting a source ahead of an incumbent silently revalues every date where both hold a close), `ref_kind` (the `app.security_links` reference the source resolves through; NULL for the two sources derived at model build), `ref_role` (`feed_key` binds a feed, `identity` merges two catalog rows), and `security_types` (pipe-delimited; which securities `PriceService` fetches from this source, and the only column retiring a source touches). Consumed by `prep.stg_security_prices` and `core.fct_security_prices`. |

`seeds.categories` is surfaced via `core.dim_categories` alongside `app.user_categories`; `seeds.category_source_map` via `core.bridge_category_source_map` alongside `app.category_source_map`.

## Identifier conventions

Identifiers across the model use a small set of strategies — source IDs where available, deterministic content hashes when not, truncated UUID4 hex for opaque keys. Full rule and rationale: [`.claude/rules/identifiers.md`](../../.claude/rules/identifiers.md).

## See also

- [`docs/guides/data-pipeline.md`](../guides/data-pipeline.md) — how rows reach these tables (raw → prep → core), refresh semantics, dedup rules.
- [`docs/specs/architecture-shared-primitives.md`](../specs/architecture-shared-primitives.md) — the 12 shared primitives consumers rely on.
- [`docs/specs/investments-data-model.md`](../specs/investments-data-model.md) — the closed `type` taxonomy, cost-basis methods, and corporate-action handling behind the investments tables.
- [`docs/guides/sql-access.md`](../guides/sql-access.md) — opening the encrypted database from external clients.
- [`src/moneybin/tables.py`](../../src/moneybin/tables.py) — `TableRef` constants; the canonical list of advertised table names.
