# Materialization Freshness Contract

> Last updated: 2026-10-15
> Status: implemented
> Address: M0D.1 (Foundation — shared primitives)
> Type: Architecture
> Owns: which SQLMesh model kinds may read which inputs, the classification of
> every `app.*` input, and the one restate-after-write path every writer uses
> Refines: [`architecture-shared-primitives.md`](architecture-shared-primitives.md)
> (SQLMesh layer conventions) and
> [`core-updated-at-convention.md`](core-updated-at-convention.md)
> Constrained by: [`observability.md`](observability.md)

## One-line goal

Make transaction reads fast by materializing the expensive identity layers,
and make every materialized model provably current after any write, by
treating a model's kind as a freshness contract.

## Decision summary

1. **A model's kind is a freshness contract.** A `FULL` model may read only
   values whose every writer restates it. Curation — an `app.*` write that runs
   no transform — is read only by `VIEW` models. A model that needs both splits
   into a `FULL` base and a thin `VIEW` over it.
2. **Every `app.*` input is classified**, per table or, where one table carries
   both kinds of value, per column: *restates on write* or *read-through*.
3. **One restate path.** Every writer of a restating input calls
   `restate_after_write` with the table it wrote. SQLMesh restates every model
   downstream of that table, so no writer names a model.
4. **The hybrid materialization.** `prep.int_transactions__merged` and
   `core.bridge_transfers` become `FULL`; `core.fct_transactions` stays a
   `VIEW`, so notes, tags, splits and categories stay read-through and cost a
   curation write nothing extra.
5. **`core.dim_accounts` splits** into a `FULL` base, `prep.int_accounts__merged`,
   carrying the source merge and the settings materialized models read, and a
   `VIEW` adding the display-only settings. A rename no longer needs a refresh to
   appear, and no longer has to restate anything.
6. **Materialized `updated_at` folds only restating timestamps.** Two new
   per-field stamps, `app.account_settings.currency_code_changed_at` and
   `default_cost_basis_method_changed_at`, join the existing
   `investment_source_type_changed_at` and `archived_changed_at`; `FULL` models
   fold those instead of the row-wide `updated_at`, which a rename also
   advances.
7. **Two guards.** A structural test checks the classification against the
   model graph and column lineage; a behavioural test performs every write and
   proves a full apply afterwards changes nothing.

## Why this exists

`moneybin accounts links pending` took 10–14 s on a real profile of about a
thousand transactions. The command read `core.fct_transactions` twice per
candidate pair. Each read paid for the whole view chain under it — union,
match, merge, transfer detection, and the curation joins — about 100–135 ms a
read, so the cost grew with the candidate count rather than the ledger.

Batching the reads fixed that command (13.6 s → 2.0 s, of which about 1 s is
process start). It did not fix the chain: every report and every agent read of
transactions pays the same 100–155 ms before its own work starts.

Materializing `core.fct_transactions` itself makes a read nearly free
(0.1–8 ms), but it would turn about 25 curation write paths into restates. The
fact joins every curation table: a note, a tag, a split, a category, a merchant
rename. Each would pay a SQLMesh restate (2.4–2.9 s, plus 1.3–1.6 s to open the
SQLMesh context), and categorization, which runs after the transform in
refresh, would read a stale fact.

The cost sits below the curation joins. Materializing the identity layers —
the merge and transfer detection — brings a read of the unchanged fact view to
about 7 ms on a 2,886-transaction synthetic profile and 8–15 ms at ten times
that size, row-identical to today.

The audit for that change found the same defect already present in eight of
the ten existing `FULL` models: each read some `app.*` value whose writer ran no
transform, so the materialized row stayed stale until the next refresh.

| Model | Stale after |
|---|---|
| `core.dim_accounts` | any `accounts set` field — a rename did not appear until refresh |
| `core.fct_balances_daily` | archive and net-worth toggles; balance assertions |
| `core.fct_investment_lots`, `core.fct_realized_gains` | a security's cost-basis method; lot selections |
| `core.fct_security_prices` | price marks; security links |
| `core.fct_investment_transactions` | security links |
| FX models | their `updated_at` folded fact, account and audit timestamps that curation advances |
| everything downstream of the merge, once `FULL` | dedup match decisions; only transfer decisions restated |

The rule below makes that class of bug structural instead of per-model.

## The rule

> **A model's kind is a freshness contract.** A `FULL` model may read only
> values whose every writer restates it: raw loads, which refresh transforms;
> identity decisions; and the settings classified *restates on write*. Curation
> state — written by an app write that runs no transform — is joined only by
> `VIEW` models, or the model splits into a `FULL` base plus a thin `VIEW`.
> Changing a model's kind needs an inventory of every writer of every input.

The rule lives in `.claude/rules/database.md`, the layer table in
`architecture-shared-primitives.md`, and
`.claude/references/data-layer-touchpoints.md`. The guards below enforce it.

## Input classes

`src/moneybin/sqlmesh_freshness.py` holds the registry. Every `app.*` table
declared in `external_models.yaml` has exactly one entry, and an `app.*` table
with no entry is read by no model.

| Class | Meaning | Writer obligation |
|---|---|---|
| `RESTATES` | A `FULL` model may read it | Call `restate_after_write` after the commit |
| `READ_THROUGH` | Only `VIEW` models read it | None; the next read sees the write |
| `TARGET_SCOPED` | `app.audit_log` only | A `FULL` model may read only rows whose `target_table` fully restates, because those rows commit in the same transaction as the restating write |

A table may override its class per column. `app.account_settings` is the one
such table: its display fields read through and everything else restates.

| Table | Class |
|---|---|
| `app.match_decisions` | `RESTATES` |
| `app.seed_source_priority` | `RESTATES` (seeded from config before every apply) |
| `app.account_links`, `app.account_link_decisions` | `RESTATES` |
| `app.account_settings` | `READ_THROUGH`: `display_name`, `official_name`, `last_four`, `account_subtype`, `holder_category`, `credit_limit`, `updated_at`. Every other column `RESTATES` |
| `app.balance_assertions` | `RESTATES` |
| `app.securities`, `app.security_links`, `app.security_price_overrides` | `RESTATES` |
| `app.lot_selections` | `RESTATES` (newly declared as an external model) |
| `app.exchange_rate_overrides`, `app.profile_settings` | `RESTATES` |
| `app.audit_log` | `TARGET_SCOPED` |
| `app.transaction_categories`, `app.transaction_notes`, `app.transaction_tags`, `app.transaction_splits` | `READ_THROUGH` |
| `app.user_categories`, `app.category_overrides`, `app.user_merchants`, `app.category_source_map` | `READ_THROUGH` |

`raw.*` inputs restate through refresh, and a writer outside refresh restates
them itself; `seeds.*` rebuild on every apply.

`app.profile_settings` restates as a whole, display targets included: the
currency lots derive the home currency's change time from that row's audit
history, which every setting extends, and splitting a one-row table per column
would leave the audit rows unscoped. It is a rare write.

## Model kinds

| Model | Before | After |
|---|---|---|
| `prep.int_transactions__merged` | VIEW | **FULL** |
| `core.bridge_transfers` | VIEW | **FULL** |
| `prep.int_accounts__merged` | — | **FULL** (new) |
| `core.dim_accounts` | FULL | **VIEW** over `prep.int_accounts__merged` |
| `core.fct_transactions` | VIEW | VIEW |

`prep.int_transactions__matched` stays a `VIEW`: it is one join over
`app.match_decisions`, and `merged`, now materialized, absorbs its cost.
`core.bridge_merchant_entities`, `meta.fct_transaction_provenance` and the
investment evidence views stay views; each is read by one engine path, not by
reports.

### `core.dim_accounts`

`prep.int_accounts__merged` holds everything `dim_accounts` derived from the
sources (including the raw `account_label`, `last_four_derived` and
`official_name` the display ladder needs), plus the restating settings resolved
onto it: `currency_code` (the override, else the source's), `archived`,
`include_in_net_worth`, `archived_at`, `default_cost_basis_method`,
`investment_source_type`, and the four change stamps. Its `updated_at` is
`GREATEST(loaded_at, <the four change stamps>)`.

`core.dim_accounts` keeps its columns, order and comments. It selects the base
and joins the display ladder, `official_name`, `last_four`, `account_subtype`,
`holder_category` and `credit_limit` from `app.account_settings`. Its
`updated_at` is `GREATEST(base.updated_at, settings.updated_at)`.

Materialized models and the views that feed them read
`prep.int_accounts__merged`, not `core.dim_accounts`: the base is a pipeline
stage, like `prep.int_transactions__merged` under the fact. Consumers still
read the dim.

## Restate after write

`src/moneybin/services/restate.py`:

```python
def restate_after_write(
    db: Database,
    inputs: Collection[TableRef],
    *,
    committed_change: CommittedChange,
) -> None: ...
```

- Keeps only inputs that restate: a read-through or unclassified `app.*` table
  is dropped, so a caller that wrote a mixed set may pass all of it. Inputs
  outside `app` always restate.
- Returns without work when the warehouse has never been built.
- Restates the named tables through `TransformService.restate_models`. SQLMesh
  accepts an external model as a restate root and restates every downstream
  model, so no writer names a model and a new `FULL` model is covered the day
  it lands.
- Every restatement plan passes a fixed `start` of 2000-01-01. Without one,
  SQLMesh takes the first plan's day as the start and refuses a plan whose end,
  the current time, precedes it. A write made under an earlier clock hits that
  refusal. No model reads the plan's dates, so the start changes no data.
- On failure raises the coded `REFRESH_MODEL_FAILED` error: the write
  committed, the reports that read it did not rebuild, run `moneybin refresh`.

`restate_after_match_effects` and `restate_after_match_run` wrap it for match
decisions: models read only accepted, unreversed decisions, so they restate
when a decision moved into or out of accepted, or a run auto-merged, accepted
or retired one. A pending decision rejected changes nothing downstream.

`fx_accounting_refresh.py`, which restated FX roots for transfer decisions and
currency settings only, is removed; its callers use this module.

### Writers

| Input | Writers |
|---|---|
| `app.match_decisions` | `MatchingService.run`, `set_status`, `undo`, `accept_previewed`, `accept_all_pending`; `ReviewDecisionsService.apply_ordinary`; audit undo |
| `app.account_settings` (restating columns) | `AccountService.settings_update` (a display-only edit restates nothing); audit undo when a restating column moved |
| `app.balance_assertions` | `BalanceService.assert_balance`, `delete_assertion` |
| `app.securities`, `app.lot_selections` | `InvestmentService.upsert_security` / `set_security`, `select_lots` |
| `app.security_links` (and what a merge re-points) | `SecurityLinksService.accept_feed_key`, `accept_merge`; a batched review accept restates once after its commit |
| `app.security_price_overrides` | `PriceService.set_mark`, `delete_mark` |
| `app.exchange_rate_overrides`, `raw.exchange_rates` | `CurrencyService.set_override`, `delete_override`, `_store`; the refresh rates step |
| `app.profile_settings` | `ProfileSettingsService.set_setting` |
| `app.account_links` and the purged `raw.plaid_*` rows | `SyncService.purge_confirmed` |
| `raw.manual_transactions` | `TransactionService.create_manual_batch`, after its category step commits |

An account merge moves links, decisions, lot selections and settings in one
transaction and then runs `refresh(match, transform)`, which rebuilds every
model; it restates nothing separately. A caller that applies right after
matching passes `MatchingService.run(restate=False)`: refresh's match step does
when the transform step follows, and `transactions matches run --skip-transform`
does on request.

Writes that touch only pending decisions — the account-link backfill, a
proposed pair, a standalone reject — restate nothing, because no model reads a
pending decision. The behavioural guard confirms each write it covers.

An import or sync run with `refresh=False`, `import confirm`, and MCP
`gsheet_pull` write raw rows together with the links and settings that describe
them, and leave the warehouse pending exactly as any unrefreshed raw load does.

Migrations run at database open, before any transform. V075 adds the two
change stamps and backfills them; the models that read them change in the same
release, so the next apply rebuilds them.

## `updated_at` in materialized models

`core-updated-at-convention.md` holds: a row's `updated_at` advances when a real
input to the row changes. A `FULL` model that folds a read-through timestamp
breaks the converse: the timestamp moves at write time and the row does not
move until the next refresh, so a consumer polling `updated_at > :cursor` sees a
change that has not happened yet.

- `AccountService.settings_update` stamps `currency_code_changed_at` and
  `default_cost_basis_method_changed_at` on a real change; repeating the current
  value is a no-op for the field. A merge stamps the fields it moves, and an
  undo advances them through the settings repo's existing `_CHANGE_STAMPS`
  mechanism. V075 backfills both from `updated_at`, the value the models folded
  before, so nothing rewinds.
- `FULL` models fold `prep.int_accounts__merged.updated_at` where they folded
  `core.dim_accounts.updated_at`, the merge's `loaded_at` plus the account
  base's stamps where they folded `core.fct_transactions.updated_at`, and
  `default_cost_basis_method_changed_at` where the currency lots folded the
  settings row's `updated_at` and its audit watermark.
- `app.audit_log` reads stay only where their `target_table` fully restates.

An FX conversion, currency lot, realized FX gain or investment ledger row no
longer advances when an account is renamed or a transaction is annotated or
categorized. That is a change to shipped `core` column semantics, disclosed in
the CHANGELOG.

## Alias liveness

`matching/aliasing.py` checks that a transaction id is still live. It read
`core.fct_transactions`; it now reads `prep.int_transactions__matched`, whose
distinct id set equals the fact's on every profile measured and which is
current the moment a decision commits, before the restate that follows. The
matched view has one row per source row, so the component and stranded-curation
queries join its distinct ids.

## Surface changes

- `investments prices set` and `delete` drop `--refresh` and the JSON
  `refreshed` / `refresh_error` fields: the mark values holdings when saved.
- `accounts set` and `accounts balance assert` drop the "reports read this after
  a rebuild" note, and `sync disconnect --purge` drops its refresh hint, for the
  same reason.
- `transactions matches run` / `backfill` rebuild through the matching service
  rather than a separate apply; `--skip-transform` keeps its meaning.

## Guards

**Structural** (`tests/integration/test_materialization_freshness.py`) builds
every model, records each Python model's `fetchdf` SQL by wrapping
`PythonModel.render`, and checks:

1. Every `app.*` external model has a registry entry, and every entry names one.
2. Every `app.*` table a model depends on is a declared external model.
3. A Python model's `depends_on` names every relation its SQL reads.
4. For every `FULL` model, the columns it reads — a SQL model's projected
   lineage, a Python model's qualified column references — reach no
   `READ_THROUGH` column through any chain of `VIEW` models, by SQLMesh's own
   column lineage. A materialized parent is guarded on its own.
5. Every `app.audit_log` read in a Python model or in a view upstream of a
   `FULL` model filters `target_table` to tables that fully restate.

Each check was proven to fail: folding the fact's `updated_at` back into the
currency lots, and scoping an audit read to `account_settings` or
`transaction_notes`, each turn it red.

Column lineage follows projected expressions, not `WHERE` or `JOIN`
predicates, so a model that filters on a read-through column passes the
structural test. The behavioural test closes that.

**Behavioural** (`tests/integration/test_materialization_freshness_writes.py`)
builds a one-year family profile with one account moved to EUR, accepted and
pending transfers, and FX conversions. For each of 17 writes — note, tag,
split, categorize, merchant, rename, currency, archive, net-worth toggle,
cost-basis method, balance assertion, home currency, FX override, security,
match accept, match undo, and an audit undo of a currency change — it
fingerprints every `core.*` and `reports.*` relation after the service write,
runs `TransformService.apply()`, and asserts the fingerprints are unchanged and
that the write moved something. Removing the balance-assertion restate turns its
case red on `core.fct_balances_daily` and the three net-worth reports.

The behavioural guard also found that `reports.large_transactions.is_top_100`
ranked equal amounts in scan order, so a rebuild could flag different rows at
the cutoff; `transaction_id` now breaks the tie.

## Observability

- `moneybin_materialized_restate_total{input, outcome}` counts restates by the
  table that triggered them; `outcome` is `applied`, `failed` or `unbuilt`.
- `moneybin_sqlmesh_run_duration_seconds{model="transform_restate"}` keeps
  timing each restate.

## Performance

| Read | Before | After |
|---|---|---|
| `core.fct_transactions`, synthetic, 2,886 rows | 103–135 ms | 7–12 ms |
| same, 28,860 rows | — | 8–15 ms |
| `accounts links pending`, real profile | 13.6 s | 2.0 s |

A dedup decision, a balance assertion and a ledger setting now pay one restate
of their downstream models, 2.4–4.5 s including the SQLMesh context. A note,
tag, split, category or rename pays nothing.

## Known limits

- The investment lots' and gains' `updated_at` is the ledger's; a cost-basis
  method or lot-selection change restates their values without advancing it.
  This predates this spec and is unchanged by it.
- A change to `matching.source_priority` in config takes effect on the next
  apply, which reseeds `app.seed_source_priority` before it rebuilds.
- Materialized models resolve an account's settings through
  `prep.int_accounts__merged`, which holds only accounts a source delivered. A
  settings row for an account no source describes no longer reaches the lots'
  cost-basis method; `accounts set` refuses such an account, so only rows
  orphaned by a later raw delete are affected, and `system doctor`'s FK
  invariants already flag those.

## Out of scope

- Disabling SQLMesh's optimized-query cache, which costs 0.4 s warm and 1.5 s
  cold per SQLMesh operation.
- Deferring the polars import in the realized-FX loader (0.15–0.19 s).
- Reusing a SQLMesh context across MCP calls without holding a connection.
