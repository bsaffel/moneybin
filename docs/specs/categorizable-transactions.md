# Categorizable Transactions

> Last updated: 2026-10-13
> Status: draft
> Address: M1C.1 (Categorization engine — one definition of the categorization population)
> Type: Feature
> Owns: `core.fct_transactions.is_categorizable` and the removal of
> `core.uncategorized_queue`
> Refines: [`reports-foundation.md`](reports-foundation.md) (which moved the
> queue into `core`) and [`multi-currency.md`](multi-currency.md)
> Requirement 5 (the per-currency impact sort, unchanged here)
> Constrained by: [`privacy-data-classification.md`](privacy-data-classification.md),
> [`observability.md`](observability.md)

## One-line goal

State once, as a column on `core.fct_transactions`, which transactions
categorization works on, and remove the separate `core` view and the
hand-copied predicate that state it today.

## Why this exists

The core schema has a standing rule: an extra `core` relation must meet a very
high bar, and otherwise the concept belongs as a filterable attribute on an
existing table. `core.uncategorized_queue` does not meet it. It is a view over
`core.fct_transactions` with one filter and four derived columns:

- the filter: `category IS NULL`, not a confirmed transfer leg, and an account
  that resolves in `core.dim_accounts` and is not archived;
- the columns: `account_name` (a join to `dim_accounts.display_name`),
  `age_days`, `priority_score`, and a `source_id` that is always NULL.

The population half of that filter — everything except `category IS NULL` —
is also written out by hand in `services/categorization/queries.py` as
`_CATEGORIZABLE_POPULATION`, because the coverage denominator needs the rows
the view filtered out. A test exists only to keep the two copies equal. That is
two definitions of one concept held together by a test, which
`design-principles.md` names as the largest single source of codebase rot.

Neither name has shipped: no release tag exists, and every changelog mention
sits under `[Unreleased]`. Pre-launch posture permits the change.

## Decision

1. Add `is_categorizable BOOLEAN` to `core.fct_transactions`.
2. Delete `core.uncategorized_queue`.
3. Read the categorization population from `is_categorizable` everywhere it is
   read today: the pending list, the pending count, coverage, and the
   categorized-by breakdown.

The name is deliberately narrow. Five report models
(`reports/cash_flow.sql`, `large_transactions.sql`, `merchant_activity.sql`,
`recurring_subscriptions.sql`, `spending_trend.sql`) repeat
`NOT t.is_transfer AND NOT a.archived`, but "excluded from spending analysis"
and "needs a category" are separate questions that happen to share a predicate
today. Folding the reports onto this column would bind them together; that is
a separate decision (see Out of scope).

## Requirements

**R1 — The column.** `core.fct_transactions.is_categorizable` is TRUE exactly
when all three hold:

- the row is not a confirmed transfer leg (`NOT is_transfer`);
- its `account_id` resolves to a `core.dim_accounts` row;
- that account is not archived.

It is never NULL.

**R2 — An unresolved account is FALSE.** `fct_transactions` LEFT JOINs
`dim_accounts` (`fct_transactions.sql:107`), so `a.archived` is NULL for a row
whose account never resolved. `NOT a.archived` alone would then make the column
NULL rather than FALSE. The expression must test the join explicitly:

```sql
NOT is_transfer AND NOT a.account_id IS NULL AND NOT a.archived
```

`NOT x IS NULL` is sqlmesh-format's canonical form (as at
`fct_transactions.sql:83`). Today's queue INNER JOINs `dim_accounts`, so this
reproduces its exclusion exactly. A version that treated an unresolved account
as categorizable would grow the queue by every such row.

**R3 — Placement.** The column follows `is_transfer` and `transfer_pair_id` in
the final projection, so the transfer pair stays adjacent and the column that
depends on it reads next to it. Its comment states the three conditions and
names the categorization service as its reader.

**R4 — The queue is removed.** Delete
`sqlmesh/models/core/uncategorized_queue.sql`, the `CORE_UNCATEGORIZED_QUEUE`
`TableRef` (`tables.py:46`), its `("core", "uncategorized_queue")` block in
`privacy/taxonomy.py`, and its `schema_catalog.py` example. Add a
`fct_transactions` example to the catalog in its place, showing the pending
filter (R5).

**R5 — One predicate, one place.** Delete `_CATEGORIZABLE_POPULATION` and the
test that compared it with the view. Each query below filters
`core.fct_transactions` on the column:

| Query (`categorization/queries.py`) | Today | After |
|---|---|---|
| `list_uncategorized_transactions` | `FROM core.uncategorized_queue` | `fct_transactions` `WHERE is_categorizable AND category IS NULL`, joined to `dim_accounts` for `account_name` |
| `count_uncategorized` | `COUNT(*)` over the queue | `COUNT(*)` over the same filter |
| `coverage` | `_CATEGORIZABLE_POPULATION` | `WHERE is_categorizable` |
| `categorization_stats` breakdown | `_CATEGORIZABLE_POPULATION` | `WHERE is_categorizable` |

The `plaid_unmapped` count stays unscoped, as it deliberately is today.

**R6 — The pending list keeps its response shape.** The pending list computes
`age_days` and `priority_score` in its own query, with the expressions the view
uses today:

```sql
CAST(CURRENT_DATE - t.transaction_date AS INT) AS age_days,
ABS(t.amount) * CAST(CURRENT_DATE - t.transaction_date AS INT) AS priority_score
```

It returns the same keys, in the same order, with the same values:
`transaction_id`, `account_id`, `account_name`, `txn_date`, `amount`,
`currency_code`, `description`, `merchant_id`, `merchant_normalized`,
`age_days`, `priority_score`, `source_type`, `source_id`, plus the
`pending_transfer_match` flag it adds in Python. `source_id` stays as a NULL
literal: removing it is a change to the MCP and CLI payloads, which this spec
does not make. The `date` and `impact` sorts, the per-currency ranking for
`impact`, the `min_amount` filter and the `account_id` filter are unchanged.

**R7 — The upgrade window reports schema drift, not "no data".** After the code
upgrades and before the first refresh, the database still holds the previous
`fct_transactions` view, which has no `is_categorizable`. DuckDB raises
`BinderException` for the missing column.

- `list_uncategorized_transactions` catches `BinderException` alongside
  `CatalogException`. When `fct_transactions` exists, it raises the existing
  `UserError(code=INFRA_SCHEMA_DRIFT, hint="refresh_run")`, with
  `missing_object` naming `core.fct_transactions.is_categorizable`. When
  `fct_transactions` is absent, it returns `None` as it does today.
- `categorization_stats` catches `BinderException` alongside
  `CatalogException` around `coverage()` and around the breakdown, so a
  pre-refresh database reports zeros rather than raising out of `categorize
  stats`.
- `count_uncategorized` already catches every exception and returns 0. The
  doctor's `categorization_coverage` already catches every exception and
  reports `skipped`. Both keep that behavior; the doctor's `detail` text names
  only `fct_transactions`, since coverage no longer joins `dim_accounts`.

`_transactions_with_pending_matches` (`queries.py:609`) already catches
`BinderException` for the same window, so this is the existing pattern, not a
new one.

**R8 — The removed view leaves the database on the first refresh.** No
migration is needed or allowed:

- `fct_transactions` is `kind VIEW`, so the new column needs no DDL.
- Refresh runs `ctx.plan()` and then `ctx.apply()` against the production
  environment (`transform_service.py:334-341`). In SQLMesh 0.236.2, a model
  present in the existing environment and absent from the new plan goes into
  the plan's demoted set (`sqlmesh/core/plan/stages.py:548-553`). Demotion
  "deletes the target view in the virtual layer"
  (`sqlmesh/core/snapshot/evaluator.py:1830-1835`), which drops
  `core.uncategorized_queue`.
- The demoted set is filtered to models whose virtual environment mode is
  `full` (`stages.py:506-512`). That is the default, and
  `sqlmesh/config.py` does not override it.
- `refresh_views()` (`seeds.py:132`) rebuilds only `dim_categories` and
  `dim_merchants`, so it cannot recreate the view.
- A migration could not drop the view regardless: migrations are statically
  forbidden to write SQLMesh-owned schemas
  (`test_migration_schema_ownership.py`).

The September removal of `prep.stg_ofx__institutions` (#613) documented its
view as left in place. That PR reasoned from the migration restriction alone
and did not check SQLMesh's demotion. This spec relies on demotion and states
the evidence. The integration test in R11 confirms it end to end.

**R9 — Privacy.** `is_categorizable` is classified `DataClass.TXN_TYPE`, the
class of `is_transfer`, from which it is derived. The pending list's columns
keep their current classes in the typed payload
(`privacy/payloads/categorize.py`). The comment there that cites
`CLASSIFICATION[("core", "uncategorized_queue")]["age_days"]` moves to cite the
reasoning directly: `age_days` is bijective with `txn_date` because
`CURRENT_DATE` is public, so it is `TXN_DATE`. The `dim_holdings.sql:444` and
`taxonomy.py:940` comments cite the same precedent and are updated to cite the
payload instead.

**R10 — The `sql_query` surface.** `core.uncategorized_queue` has `interface`
audience, so `sql_query` reads it and `sql_schema` lists it with an example.
After this change an agent writes
`SELECT … FROM core.fct_transactions WHERE is_categorizable AND category IS NULL`,
which is the example R4 adds. This removes a readable relation, so a `removed`
changelog fragment records it next to an `added` fragment for the column.

## Observability

No metric reads the queue or the copied predicate. The four counts above feed
`import_inbox` (`mcp/tools/import_inbox.py:38`), `review_service`
(`services/review_service.py:58`), `categorize stats`, and the doctor's
`categorization_coverage` check. Their values do not change, because the
population does not change. This spec adds no metric: it changes where one
existing predicate is written, and adds no operation whose rate, latency or
failure a metric would show.

## Testing

**R11 — Tests.**

- **The column, per exclusion.** One fixture row for each reason a transaction
  is not categorizable — a confirmed transfer leg, an archived account, an
  unresolved account — and one row that is categorizable. Assert TRUE only on
  the last, and assert the column is never NULL. Each fixture row fails exactly
  one condition (`testing.md` → "A Fixture That Trips Two Guards Isolates
  Neither"); the unresolved-account row is the one R2 exists for.
- **Equivalence, measured.** The existing queue, count and coverage tests keep
  their assertions and run against the new queries. Before the model file is
  deleted, one test builds both the old view and the new column on the same
  fixture and asserts the pending list returns identical rows. It is deleted
  with the view, and its result is recorded in the PR.
- **The upgrade window (R7).** A `fct_transactions` fixture without the column
  makes the pending list raise `INFRA_SCHEMA_DRIFT`, and makes `categorize
  stats` return zeros without raising.
- **Removal reaches an existing database (R8).** An integration test copies
  the SQLMesh project to a temporary directory, writes the deleted view's
  model SQL into the copy as a fixture, and builds a database from it through
  `sqlmesh_context(db, sqlmesh_root=…)` (`database.py:1728`). It then deletes
  that file from the copy, plans and applies again, and asserts that
  `core.uncategorized_queue` no longer exists and that
  `core.fct_transactions.is_categorizable` does.
- **Fixtures.** `tests/moneybin/db_helpers.py` stops creating the queue view,
  and the `fct_transactions` fixture DDL gains the column. That file is a
  shared primitive, so the gate includes integration and scenarios.

**Gate.** `make check`, `make format-sql`, `make test`,
`make test-integration`, `make test-scenarios`, and
`uv run pytest tests/test_documentation_policy.py`. This change reshapes core
data, alters a categorization query, and edits `tables.py`, `privacy/` and
`db_helpers.py`.

## Documentation

- `docs/reference/data-model.md`: add the column to `fct_transactions` and
  remove the queue's entry.
- `docs/guides/data-pipeline.md` and `docs/guides/observability.md`: replace
  the queue with the column.
- `reports-foundation.md`: a dated amendment where it records the move into
  `core` (§ near line 206), pointing here. Its historical narrative stays as
  written.
- Other specs that name the queue in present tense (`reports-recipe-library.md`,
  `cli-output-coherence.md`, `moneybin-doctor.md`,
  `categorization-source-model.md`, `category-source-map.md`,
  `multi-currency.md`, `sync-plaid.md`, `reports-overview.md`,
  `privacy-data-classification.md`): update the present-tense references and
  leave history alone.
- Code comments that use the queue to explain `category IS NULL`
  (`fct_transactions.sql:129`, `int_transactions__unioned.sql:136`,
  `stg_manual__transactions.sql:9`, `stg_tabular__transactions.sql:10`,
  `cli/render.py:1011`, `services/_validators.py:46`,
  `categorization/applier.py:630`, `mcp/tools/reviews.py:261,1071`,
  `mcp/tools/transactions_categorize.py:340`,
  `categorization/__init__.py:908`, `doctor_service.py:4556`) now name the
  pending filter instead.
- Migrations V054 and V055 mention the queue in comments. They stay as written:
  editing an applied migration changes its checksum, and `check_drift()` then
  flags it on every database that ran it.

## Out of scope

- **The five report models** that repeat `NOT is_transfer AND NOT archived`.
  Whether "excluded from spending analysis" should become its own column, or
  reuse this one, is a separate decision about what those reports mean.
- **Removing `source_id`** from the pending-list payload: it has been NULL
  since the queue moved to `core`, and dropping it changes the MCP and CLI
  surface (R6).
- **The other relations** flagged by the same core-schema audit:
  `core.fct_exchange_rates`, `core.bridge_merchant_entities`,
  `core.dim_holdings_broker_reported`, and `core.fct_exchange_rates_daily`.

## Alternatives considered

- **Keep the view and point `_CATEGORIZABLE_POPULATION` at a second view of the
  population.** That adds a third `core` relation to remove one copy of a
  predicate, which is the opposite of the rule this spec applies.
- **Store `age_days` and `priority_score` on `fct_transactions` too.** Both
  depend on `CURRENT_DATE` and are meaningful only for the pending list, and
  `priority_score` only within one currency. Putting them on the canonical fact
  would publish two columns whose values change daily for one consumer's sort.
- **A general `is_spending` name shared with the reports.** Rejected for now;
  see Decision and Out of scope.
