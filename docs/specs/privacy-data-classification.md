# Feature: Privacy Data Classification

## Status
in-progress

## Goal
Establish a typed, source-of-truth registry that maps every column in
`core.*` and `app.*` to a `DataClass` (and via that, a privacy `Tier`).
The registry is the foundation that later PRs build redaction
middleware, consent gates, and SQL lineage on top of. Surface the
classification in DuckDB's catalog (comment sigil) and enforce
completeness in CI.

## Background
- `privacy-and-ai-trust.md` — framework spec describing tiers, consent,
  and the redaction engine (revision pending parallel MCP rename work).
- `privacy-data-protection.md` — implemented: encryption at rest, log
  sanitizer.
- `architecture-shared-primitives.md` — names the `core.*` / `app.*`
  layer split this spec classifies.
- `.claude/rules/identifiers.md` — distinguishes content-hash and
  truncated-UUID record IDs (`RECORD_ID`) from source-provided
  account-bound IDs (`ACCOUNT_IDENTIFIER`).
- `.claude/rules/security.md` — sqlglot identifier-quoting requirement
  used by the comment sync.

## Requirements
1. A `DataClass` StrEnum defines every privacy class MoneyBin
   recognizes. Adding a new class is a one-line change.
2. Each `DataClass` member maps to exactly one `Tier`
   (LOW=1, MEDIUM=2, HIGH=3, CRITICAL=4). Tier ordering supports
   `max(tier)` aggregation in future PRs. The framework spec defines
   the four tiers semantically; this spec introduces the numeric
   ordering for downstream aggregation.
3. A `CLASSIFICATION` dict keyed by `(schema, table) -> {column:
   DataClass}` covers every column in `core.*` and `app.*` that exists
   at startup.
4. A `sync_classification_comments(db)` function writes
   `[class: <name>]` as a suffix on the existing comment for every
   classified column. Re-running is idempotent (zero
   `COMMENT ON COLUMN` statements executed when the catalog already
   matches the registry). If a column's entry is removed from
   `CLASSIFICATION`, the next sync strips its sigil and leaves the
   original human comment intact.
5. The sync runs after `init_schemas` (for app/raw DDL comments) and
   after `sqlmesh_context()` (for SQLMesh-managed core comments).
6. A pytest test enumerates `duckdb_columns()` and fails if any
   `core.*` / `app.*` column has no entry in `CLASSIFICATION`. The
   failure message names every missing column.
7. A reverse test fails if `CLASSIFICATION` contains an entry for a
   column or table that no longer exists.

## Data Model
No new tables. The registry lives in Python; the catalog change is a
suffix on existing `COMMENT ON COLUMN` strings.

## Classification Audit

The columns below were reviewed individually because their name or
origin made the classification non-obvious — every `_id`-suffixed
column in `core.*` and `app.*` is listed, plus any other column whose
class required judgment beyond the priority rules. The justification
column records the reasoning so a future contributor can argue with it
from context. The full registry lives in
`src/moneybin/privacy/taxonomy.py`; this table only covers the entries
that required a call.

The audit was conducted by static enumeration of `sqlmesh/models/core/`
and `src/moneybin/sql/schema/app_*.sql` — the keychain was unavailable
in the sandbox, so a live `duckdb_columns()` enumeration could not be
performed. Migrations through V013 were reviewed and do not add columns
beyond what the schema files declare.

### `_id`-suffixed columns

| (schema, table) | column | class | justification |
|---|---|---|---|
| (app, account_settings) | account_id | ACCOUNT_IDENTIFIER | account-bound external ID; PK that ties user settings to a real institution account. |
| (app, audit_log) | audit_id | RECORD_ID | internal full UUID4 hex per `.claude/rules/identifiers.md`; not externally meaningful. |
| (app, audit_log) | parent_audit_id | RECORD_ID | self-FK to `audit_id`; same class as the target. |
| (app, audit_log) | target_id | RECORD_ID | references arbitrary internal IDs (transaction, rule, merchant); all are RECORD_ID-class in their home tables. |
| (app, balance_assertions) | account_id | ACCOUNT_IDENTIFIER | account-bound external ID; same value as `dim_accounts.account_id`. |
| (app, budgets) | budget_id | RECORD_ID | 12-char truncated UUID4 per identifiers.md; app-created entity with no natural key. |
| (app, categorization_rules) | account_id | ACCOUNT_IDENTIFIER | optional account-bound external ID restricting the rule. |
| (app, categorization_rules) | rule_id | RECORD_ID | 12-char truncated UUID4; app-created entity. |
| (app, category_overrides) | category_id | CATEGORY | matches `seeds.categories.category_id` which is a semantic slug (e.g. `INC-SAL`); rule 9 classifies semantic-slug category IDs as CATEGORY. |
| (app, imports) | import_id | RECORD_ID | FK to `raw.import_log.import_id`, a content-hash internal identifier. |
| (app, match_decisions) | account_id | ACCOUNT_IDENTIFIER | account-bound external ID. |
| (app, match_decisions) | account_id_b | ACCOUNT_IDENTIFIER | second account in a transfer pair; same class as account_id. |
| (app, match_decisions) | match_id | RECORD_ID | internal UUID PK for the decision row. |
| (app, match_decisions) | source_transaction_id_a | RECORD_ID | transaction-level external ID (FITID, Plaid transaction_id, content hash); transaction-bound, not account-bound — rule 1 third bullet. |
| (app, match_decisions) | source_transaction_id_b | RECORD_ID | same as `_a`. |
| (app, proposed_rules) | proposed_rule_id | RECORD_ID | 12-char truncated UUID4 per identifiers.md. |
| (app, proposed_rules) | sample_txn_ids | RECORD_ID | LIST of internal transaction IDs; transaction-bound, not account-bound. |
| (app, rule_deactivations) | deactivation_id | RECORD_ID | 12-char truncated UUID4. |
| (app, rule_deactivations) | rule_id | RECORD_ID | soft reference to `categorization_rules.rule_id`. |
| (app, transaction_categories) | merchant_id | RECORD_ID | 12-char truncated UUID4 from `app.user_merchants`. |
| (app, transaction_categories) | rule_id | RECORD_ID | optional FK to `categorization_rules.rule_id`. |
| (app, transaction_categories) | transaction_id | RECORD_ID | content-hash gold key per identifiers.md; transaction-bound. |
| (app, transaction_notes) | note_id | RECORD_ID | 12-char truncated UUID4. |
| (app, transaction_notes) | transaction_id | RECORD_ID | content-hash gold key. |
| (app, transaction_splits) | split_id | RECORD_ID | 12-char truncated UUID4. |
| (app, transaction_splits) | transaction_id | RECORD_ID | content-hash gold key. |
| (app, transaction_tags) | transaction_id | RECORD_ID | content-hash gold key. |
| (app, user_categories) | category_id | RECORD_ID | 12-char UUID hex assigned at creation; rule 9 says UUID4 `category_id` → RECORD_ID (distinct from the slug variant in `seeds.categories`). |
| (app, user_merchants) | merchant_id | RECORD_ID | 12-char UUID hex from `uuid.uuid4().hex[:12]`. |
| (core, bridge_transfers) | credit_transaction_id | RECORD_ID | FK to `fct_transactions.transaction_id`; transaction-bound. |
| (core, bridge_transfers) | debit_transaction_id | RECORD_ID | FK to `fct_transactions.transaction_id`; transaction-bound. |
| (core, bridge_transfers) | transfer_id | RECORD_ID | UUID; also FK to `app.match_decisions.match_id`. |
| (core, dim_accounts) | account_id | ACCOUNT_IDENTIFIER | account-bound external ID issued by the upstream institution; ties to a real account. |
| (core, dim_categories) | category_id | CATEGORY | UNIONs seed (semantic slug) and `user_categories` (UUID4) rows; classified as CATEGORY because the view's role is categorical reference regardless of the underlying generator and the dominant population is slug-keyed seeds. |
| (core, dim_merchants) | merchant_id | RECORD_ID | UUID hex per identifiers.md; thin view over `app.user_merchants`. |
| (core, fct_balances) | account_id | ACCOUNT_IDENTIFIER | account-bound external ID. |
| (core, fct_balances_daily) | account_id | ACCOUNT_IDENTIFIER | FK to `dim_accounts.account_id`. |
| (core, fct_transaction_lines) | account_id | ACCOUNT_IDENTIFIER | FK to `dim_accounts.account_id`. |
| (core, fct_transaction_lines) | line_id | RECORD_ID | `'whole'` sentinel or `split_id` (truncated UUID4); never account-bound. |
| (core, fct_transaction_lines) | transaction_id | RECORD_ID | FK to `fct_transactions.transaction_id`. |
| (core, fct_transaction_lines) | transfer_pair_id | RECORD_ID | FK to `bridge_transfers.transfer_id`. |
| (core, fct_transactions) | account_id | ACCOUNT_IDENTIFIER | same value as `dim_accounts.account_id`; "one concept, one column name" — same class everywhere. |
| (core, fct_transactions) | pending_transaction_id | RECORD_ID | source-provided ID of the pending transaction this row resolved; transaction-bound, not account-bound. |
| (core, fct_transactions) | transaction_id | RECORD_ID | content-hash gold key per identifiers.md. |
| (core, fct_transactions) | transfer_pair_id | RECORD_ID | FK to `bridge_transfers.transfer_id`. |

### Other judgment calls

| (schema, table) | column | class | justification |
|---|---|---|---|
| (app, account_settings) | archived, include_in_net_worth | TXN_TYPE | account-level state flags; no `BOOLEAN_FLAG` class exists, TXN_TYPE is the closest LOW-tier categorical bucket. Same applies to the mirror columns on `core.dim_accounts`. |
| (app, account_settings) | display_name | USER_NOTE | user-supplied free-text label; treat as user input rather than institution-supplied metadata. Same on `core.dim_accounts`. |
| (app, account_settings) | holder_category | TXN_TYPE | `'personal' / 'business' / 'joint'` — low-cardinality categorical classifier. Same on `core.dim_accounts`. |
| (app, account_settings) | last_four | INSTITUTION_ACCOUNT_NUMBER | last four digits of the account number; protecting the visible portion of the account number under the account-number class is the conservative read. Same on `core.dim_accounts`. |
| (app, account_settings) | official_name | INSTITUTION | mirrors Plaid `official_name` (e.g. "Adv Plus Banking") — institution-side branding for the account, not a number; not actionable as an account-lookup key. |
| (app, audit_log) | before_value, after_value | TXN_AMOUNT | JSON snapshots of mutated rows; can carry amounts, balances, descriptions — classified by the highest-sensitivity content they may contain. HIGH tier is conservative; revisit if a tighter scoping per-action emerges. |
| (app, audit_log) | context_json | DESCRIPTION | freeform JSON for AI-call provenance and operational extras; treated as MEDIUM free-text. |
| (app, audit_log) | target_schema | RECORD_ID | catalog identifier (`'app'`, `'core'`), not a transaction type. |
| (app, audit_log) | target_table | RECORD_ID | catalog identifier (e.g., `'fct_transactions'`), not a transaction type. |
| (app, balance_assertions) | notes | USER_NOTE | optional free-text note attached to a balance assertion. |
| (app, budgets) | monthly_amount | TXN_AMOUNT | budget target dollar amount; not a transaction per se but matches the same HIGH-tier monetary sensitivity. |
| (app, categorization_rules) | merchant_pattern | MERCHANT_NAME | pattern matched against transaction description; reveals which merchants the user tracks. Same on `proposed_rules` and `user_merchants.raw_pattern`. |
| (app, categorization_rules) | name | USER_NOTE | human-readable rule label; user-authored free text. |
| (app, imports) | labels | USER_NOTE | LIST of user-applied slug labels; user-authored, treat as USER_NOTE for parity with `transaction_tags.tag`. |
| (app, match_decisions) | match_reason | USER_NOTE | human-readable explanation; may contain merchant or description hints. |
| (app, metrics) | * | AGGREGATE / TXN_TYPE / TIMESTAMP_OBSERVABILITY | operational telemetry — Prometheus snapshots; numeric/label fields are AGGREGATE, type discriminator is TXN_TYPE, recorded_at is observability. |
| (app, tabular_formats) | field_mapping, header_signature, skip_trailing_patterns | DESCRIPTION | JSON parse-configuration text; not transaction descriptions but free-text-shaped — MEDIUM tier is conservative. |
| (app, transaction_notes) | text | USER_NOTE | by definition the user's free-form note. |
| (app, transaction_splits) | amount | TXN_AMOUNT | per-split signed amount; rule 3 (`*_amount` on transaction tables). |
| (app, transaction_splits) | note | USER_NOTE | optional per-split free-text note. |
| (app, transaction_tags) | tag | USER_NOTE | user-authored slug; treated as USER_NOTE rather than CATEGORY since tags are not the canonical taxonomy. |
| (app, versions) | component | TXN_TYPE | categorical identifier (`'moneybin'`, `'sqlmesh'`); low-cardinality bookkeeping label. |
| (app, versions) | version | AGGREGATE | configuration value, not a record identifier; unified with `schema_migrations.version`. |
| (app, versions) | previous_version | AGGREGATE | same reasoning as `version`. |
| (core, dim_accounts) | institution_fid | INSTITUTION | OFX financial-institution identifier; identifies the institution, not the account. |
| (core, dim_accounts) | source_file | RECORD_ID | path of the source file; internal provenance, not an external identifier. |
| (core, dim_categories) | description, plaid_detailed | CATEGORY | category metadata (definition text, Plaid PFC mapping); travels with the category, not with user transactions. |
| (core, fct_transactions) | check_number | DESCRIPTION | a check number identifies a payment instrument, not an account; parked at MEDIUM until PR 2 introduces `PAYMENT_INSTRUMENT` (CRITICAL). Knowingly underclassified — check numbers are not account numbers. |
| (core, fct_transactions) | location_* (address, city, region, postal_code, country, latitude, longitude) | MERCHANT_NAME | merchant geographic detail; classified under MERCHANT_NAME because they describe the merchant the user transacted with, and inherit the same MEDIUM sensitivity. |
| (core, fct_transactions) | memo | DESCRIPTION | additional source-provided notes on the transaction; rule 6 (free-text on transaction tables). |
| (core, fct_transactions) | splits | TXN_AMOUNT | LIST of split STRUCTs; contains per-split `amount` (HIGH-tier). Classify by the highest-sensitivity component. |
| (core, fct_transactions) | notes, tags | USER_NOTE | nested LIST aggregations of `app.transaction_notes` / `transaction_tags`; carry user-authored content. |

### Follow-ups for PR 2

The audit surfaced several class-shape gaps that PR 1 punts on. None
affect privacy correctness (tiers are conservative), but the class
names are misleading enough that PR 2's redaction logic should
introduce new members before locking in formatting behavior:

- **`BOOLEAN_FLAG`** (LOW) — for `is_active`, `is_pending`, `archived`,
  `is_observed`, `is_default`, `include_in_net_worth`, `success`,
  `multi_account`. Currently routed to `TXN_TYPE`.
- **`STATE_ENUM`** (LOW) — for `status`, `match_status`, `match_tier`,
  `metric_type`, `reason`, `action` (audit log). Currently `TXN_TYPE`.
- **`ACTOR` / `PROVENANCE`** (LOW) — for `actor`, `author`, `*_by`
  columns across `audit_log`, `transaction_notes`, `transaction_tags`,
  `match_decisions`, `proposed_rules`, `categorization_rules`,
  `imports`, `transaction_categories`, `fct_transactions`,
  `transaction_splits`, `user_merchants`, `dim_merchants`. These are
  principal identifiers and may be free-text emails or agent names;
  they need distinct redaction from generic transaction-type enums.
  Currently `TXN_TYPE`.
- **`JSON_SNAPSHOT`** (HIGH) — for `audit_log.before_value` /
  `after_value` (mixed-content blobs). Currently `TXN_AMOUNT` (correct
  tier, wrong class — PR 2's redaction will format these as money).
- **`PAYMENT_INSTRUMENT`** (CRITICAL) — for `check_number`. Currently
  parked at `DESCRIPTION` (MEDIUM) — a knowing underclassification
  pending the new class. Check numbers aren't account numbers, so
  `INSTITUTION_ACCOUNT_NUMBER` was rejected.
- **`LOCATION`** (MEDIUM or HIGH) — for `fct_transactions.location_*`
  (especially `location_latitude`/`location_longitude`). Currently
  `MERCHANT_NAME` (tier is right; semantic is off — geolocation
  reveals movement patterns).

## Implementation Plan

The step-by-step task breakdown is tracked separately (ephemeral). The
durable design decisions that flow out of this work and into later PRs:

### Key Decisions

- **Sigil format.** Append ` [class: <DataClass value>]` as a suffix on
  the existing DuckDB column comment. The class value is the lowercase
  snake-case form of the enum member name (e.g., `account_identifier`,
  `record_id`). A trailing-anchor regex strips the sigil before
  reapplication so re-syncing never duplicates the marker.
- **Sync ordering.** Classification sync runs *after* both existing
  comment-writing paths: `schema._apply_comments` (per-startup DDL
  comments for `app.*` and `raw.*`) and SQLMesh's `register_comments`
  (per-run comments for `core.*` models). Human descriptions are the
  prefix; the class sigil is the suffix.
- **Suffix, not replace.** The sync never rewrites the human
  description — it strips any prior sigil and appends the current one.
  Removing a column's entry from the registry restores the original
  comment on the next sync.
- **Source of truth is Python, not the catalog.** The DuckDB sigil is a
  mirror for `DESCRIBE` / DBeaver convenience. Downstream privacy
  controls (redaction, consent gates, lineage) read `CLASSIFICATION`
  directly. The catalog is observable, not authoritative.

## Testing Strategy
- Completeness test: registry covers every live column.
- Reverse test: every registry entry corresponds to a live column.
- Idempotency test: second sync run produces zero updates.
- Description-preservation test: human comment stays as the prefix; sigil
  is the suffix; stripping the registry entry restores the original.

## Dependencies
None new. Uses existing `Database`, `duckdb_columns()`, sqlglot, pytest.

## Out of Scope
- `Annotated[..., DataClass.X]` propagation on service return types (PR 2).
- Redaction engine (`redact_typed`, `redact_polars_frame`) (PR 2).
- `privacy.log` JSONL writer (PR 2).
- `app.ai_consent_grants` schema + `moneybin privacy grant/revoke/status`
  CLI + consent MCP tools (PR 3).
- sqlglot lineage on `sql_query` (PR 4).
- Presidio integration for unstructured-text scrubbing (deferred).
- MCP elicitation fallback when consent is missing (deferred).
- Per-tool consent granularity (schema supports, UX deferred).
- Revisions to `privacy-and-ai-trust.md` (blocked on parallel MCP rename
  work; this PR does not touch the MCP layer).

## Performance validation (PR 2)

Privacy middleware introduces redaction + log-write overhead on every
MCP/CLI egress. PR 2's acceptance gate caps the regression budget at
≤50 ms p50, ≤200 ms p99, ≤20% total-flow wall-clock vs the
pre-middleware baseline captured before introspection code landed.

### Persona

| Property | Value |
|---|---|
| Fixture | `family.yaml` |
| Accounts | 4 |
| Transactions (generated) | ~2700 over 3 years |
| Total DB size after seeding | ~8.1 MB |

Generated via `moneybin synthetic generate family --seed 8229 --years 3`.

The plan's original target of 5000+ transactions was relaxed to ~2700
after enumerating the available persona library — `family.yaml` is the
largest existing fixture and growing it was out of scope for this PR.
The baseline is still load-bearing for the regression-budget gate;
absolute latency values may be smaller than they would be on a denser
fixture, but the delta-vs-baseline measurement is unaffected.

### Measured flows

Each flow runs ≥30 iterations to produce stable percentiles. Baseline
stored at `tests/scenarios/fixtures/perf_baseline_pre_privacy.json`,
post-middleware assertion at `tests/scenarios/test_privacy_middleware_perf.py`.

| Tool / command | Service method | Tier | Shape |
|---|---|---|---|
| `transactions_get` | `TransactionService.get(limit=100)` | medium | ~100-row list |
| `reports_spending` | `SpendingService.by_category()` | low | aggregate |
| `accounts` | `AccountService.list_accounts()` | medium | ~4-row list (CRITICAL fields) |
| `reports_budget` | `BudgetService.status()` | low | aggregate + per-budget rows |
| `reports_networth_history` | `NetworthService.history()` | medium | time-series |

Concrete numbers are populated by Phase 9 after the post-middleware run.
