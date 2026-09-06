# Feature: Net Worth on the SQL Surface

## Status
draft

Flips to `ready` once §Open decision is answered; nothing else in it is
unsettled.

## Goal

Make net worth answerable by a query the caller can read and rerun — at the
account, currency, and profile grain, in one currency, with the freshness of
every contributing number visible as a column. Today the arithmetic is SQL but
the currency conversion is not, so `sql_query` and `moneybin sql query` see a
per-currency segmented relation with no rates in reach, and the only surface
that can convert is the Python report path.

M2B.2, extending the balance spine that M2B.1 shipped
([`reports-net-worth.md`](reports-net-worth.md)). That spec stays in force: this
one changes nothing about how a balance is observed, interpolated, or
reconciled.

## Background

### What shipped

[`reports-net-worth.md`](reports-net-worth.md) established *accurate or absent*
and built the spine underneath it: `core.fct_balances` unions every balance
observation, `core.fct_balances_daily` (kind FULL) carries each account forward
one row per day, and `reports.net_worth` aggregates the included accounts per
`(balance_date, currency_code)`. Two reports read it — `core:networth` and
`core:networth_history` — both `ServiceReportSpec`, both predating the `@report`
framework.

[`multi-currency.md`](multi-currency.md) M1K.2 then added display conversion:
`raw.exchange_rates` caches what a provider published,
`app.exchange_rate_overrides` records user corrections, and `convert_records`
prices a report's rows at read time when the caller asks for a display currency.

### What this changes

Conversion lives entirely in Python, above the SQL layer. `reports.net_worth`
therefore publishes one row per currency held with no way to relate them, and
the agent-safe SQL surface — which AGENTS.md routes agents and humans to for
exactly this kind of question — cannot answer *"what am I worth?"* for a
multi-currency profile at all. `core:networth_history` cannot answer it in any
form: `_HISTORY_SEMANTICS` (`src/moneybin/reports/service_reports.py:170`) never
sets `fx_date`, so the history report returns segmented rows regardless of the
requested display currency. A multi-currency profile has no single net-worth
trend line by construction.

Nothing in `src/moneybin/sqlmesh/` reads an exchange rate. `raw.exchange_rates`
has no `prep.stg_*` model and no `core` model, because its only consumers have
ever been Python (`ExchangeRateRepo`, `CurrencyService`, `rate_backfill`). Prices
took the other path — `prep.stg_security_prices` → `core.fct_security_prices` —
and are queryable as a result. Rates are the same kind of reference data with
none of the same reach.

This spec closes that: a rate spine in `core`, three report views at three
grains, conversion and data quality as columns, and the two Python-backed
reports retired onto the SQL path that `.claude/rules/reports.md` now makes
the default.

## Defects in the shipped surface

Verified against the tree at the time of writing. The disposition column says
which are this spec's to close.

| # | Defect | Evidence | Disposition |
|---|---|---|---|
| 1 | A margin account's net worth is overstated by the size of its loan. The sync server sends `margin_loan_amount`; `SyncBalance` does not declare it, and Pydantic's default `extra='ignore'` discards it silently. `grep -rn 'margin_loan' src/moneybin/` returns nothing. | `src/moneybin/connectors/sync_models.py:210-219` | **Not this spec.** A connector bug producing a wrong number today; fix independently and first. See §Out of Scope. |
| 2 | Archiving an account rewrites net-worth history. `archived` is a plain BOOLEAN with no date, and the filter applies to every `balance_date`, so closing an account in 2026 retroactively removes it from 2022. | `src/moneybin/sql/schema/app_account_settings.sql:13`, `src/moneybin/sqlmesh/models/reports/net_worth.sql:21` | **Closed here** — Requirement 9. |
| 3 | `core:networth_history` cannot convert currency at all. | `src/moneybin/reports/service_reports.py:170` vs `:122` | **Closed here** — Requirements 1 and 3. |
| 4 | Staleness is invisible on every net-worth surface. `fct_balances_daily` carries `is_observed`, `observation_source`, and `reconciliation_delta`; only `observation_source` reaches a report, rendered as a bare blank cell, and `reconciliation_delta` reaches none. No `system doctor` check covers balance staleness. | `src/moneybin/services/networth_service.py:111-118`, `src/moneybin/cli/render.py:632-633` | **Closed here** — Requirement 8. The doctor check is out of scope. |
| 5 | The double-count invariant that Pillar D must uphold has no guard. Safe today only because no holding is wired into net worth. | `investments-overview.md` §Pillar D states the two tests in future tense | **Not this spec.** Belongs with Pillar D; named here so it is not lost. |
| 6 | An investment account with priced holdings and no balance observation contributes exactly zero to net worth — Requirement 9 of M2B.1 emits no rows without an anchor, and nothing detects the gap. | `investments-overview.md` §Open, `doctor_service.py` invariant list | **Not this spec.** Pillar D / a doctor check. |

## Requirements

1. **Three grains, three relations.** `reports.net_worth_accounts` (account ×
   day), `reports.net_worth` (currency × day), and `reports.net_worth_summary`
   (day) each answer a distinct question with one `SELECT` against one relation
   and no post-processing. §Data Model states the question each one answers.
2. **No rung reads another.** Each is an independent projection of the same
   `core.*` facts at a different `GROUP BY` level. This is not a preference:
   `assert_acyclic` (`src/moneybin/privacy/report_materialization.py:68`)
   rejects any read of `reports.*` by any model under privacy-class derivation,
   because `reports.*` columns *are* derivation's own output. A stacked ladder
   fails CI.
3. **Home-currency conversion happens in SQL.** Rungs 1 and 2 carry converted
   measures beside the originals; rung 3 is home-denominated by construction.
   A caller with no display-currency machinery — `sql_query`, `moneybin sql
   query`, a saved report, a human at a DuckDB prompt — gets a comparable number.
4. **Original currency stays canonical, and nothing converted is persisted at
   row grain.** All three rungs are `kind VIEW`, so a converted column is
   computed on read and stores nothing. This is what keeps the design inside
   [`multi-currency.md`](multi-currency.md) §"The core decision (one-way door)",
   which names deriving through SQLMesh as the sanctioned mechanism. No
   materialized model may carry a converted amount.
5. **A rate is never manufactured.** Outside a pair's own observation window the
   rate spine has no row, so a join misses rather than matching an invented
   rate. Requirement 12 of `multi-currency.md` forbids substituting today's rate
   or 1.0; this extends the same rule to the shape of the table.
6. **Carry-forward is bounded and visible.** A rate row states the day the
   provider actually priced it (`published_date`) beside the day it is being
   applied to (`effective_date`), plus the distance between them. Carry-forward
   spans genuine non-publication days only.
7. **An unpriced currency makes the summary NULL, not smaller.**
   `reports.net_worth_summary.net_worth` is NULL when any contributing currency
   is unpriced on that date, with the unpriced currency count beside it. A
   populated-but-partial total is the failure mode this rung exists to prevent —
   see §Key Decision 4.
8. **Balance staleness is a column.** The accounts rung publishes `is_observed`,
   `observation_source`, `days_since_observed`, and `reconciliation_delta`, so a
   carried-forward balance is distinguishable from an observed one in the data
   rather than in a rendering convention.
9. **Account exclusion is date-scoped where it applies to a stock measure.**
   `app.account_settings` gains `archived_at DATE`; the three net-worth rungs
   exclude an archived account only for dates after it, preserving history that
   was true when it was recorded. Flow reports keep the present-tense filter —
   see §Key Decision 5.
10. **`core:networth` and `core:networth_history` become SQL-backed.** Both
    become `@report` runners over the new views, and `ServiceReportSpec` and its
    executor branch are deleted. This is `.claude/rules/reports.md` §"A new
    report is SQL-backed" applied to the two reports that rule names as
    predating it.
11. **A single-currency profile joins identically.** Identity rows (`USD` → `USD`
    at 1.0) are materialized, so no rung needs a branch for the common case and
    no single-currency user sees a NULL converted column.
12. **Observability.** The rate spine's row and coverage counts join the existing
    `FX_RATE_*` family in `src/moneybin/metrics/registry.py:334-380`; the two
    migrated reports keep the report-execution metrics every catalog report
    already emits.

## Data Model

### The three rungs

Each rung answers a question the others cannot, which is why three exist rather
than one wide relation. All three are `kind VIEW` in the `reports` schema.

| Rung | Grain | The question it answers |
|---|---|---|
| `reports.net_worth_accounts` | `(account_id, balance_date)` | *Which accounts hold my money, how fresh is each number, and what is each worth in one comparable unit?* |
| `reports.net_worth` | `(currency_code, balance_date)` | *How is my position split across the currencies I hold?* |
| `reports.net_worth_summary` | `(balance_date)` | *What am I worth, and what was I worth over time?* |

They read the same three sources — `core.fct_balances_daily`,
`core.dim_accounts`, `core.fct_exchange_rates_daily` — at three `GROUP BY`
levels. The eligibility filter and the rate join are therefore written three
times. That duplication is deliberate and forced by Requirement 2; if it becomes
painful, the remedy is one `core.*` **VIEW** carrying the filter and join that
all three read. It must not be a TABLE: `include_in_net_worth`, `archived`, and
`archived_at` are user-reversible, and baking a reversible filter into
materialized rows makes a later un-archive silently wrong. The same constraint
binds M2P.3 — see §Key Decision 6.

Column order follows Rule B of `.claude/rules/column-ordering.md`: grain keys →
identifying labels → dimensions → dates → provenance → measures, headline
measure last, with statement order (assets, liabilities, net) inside each
measure block. Rule B leaves `AGGREGATE` placement explicitly unchecked, so the
counts below sit where a reviewer should judge them rather than where a guard
put them: counts of the grain are grouped at the head of the measure block, and
`reconciliation_delta` is a measure rather than provenance because its
`money_kind` is what the guard ranks on.

#### `reports.net_worth_accounts`

```
account_id            VARCHAR        -- Grain. FK to core.dim_accounts
account_name          VARCHAR        -- dim_accounts.display_name
currency_code         VARCHAR        -- The account's own denomination; NULL is the unknown segment
home_currency_code    VARCHAR        -- app.profile_settings.home_currency
account_type          VARCHAR        -- depository / credit / loan / investment / other
balance_date          DATE           -- Grain
rate_published_date   DATE           -- The day the rate applied here was actually published
is_observed           BOOLEAN        -- FALSE means carried forward
observation_source    VARCHAR        -- ofx / tabular / assertion / plaid; NULL when interpolated
days_since_observed   INTEGER        -- 0 on an observed day
rate_source           VARCHAR        -- override / provider / identity; NULL when unpriced
reconciliation_delta  DECIMAL(18,2)  -- Observed minus transaction-derived; NULL on interpolated days
account_balance       DECIMAL(18,2)  -- In currency_code
account_balance_home  DECIMAL(18,2)  -- In home_currency_code; NULL when the pair is unpriced
```

`days_since_observed` reuses the name and meaning already established by
[`asset-tracking.md`](asset-tracking.md) and implemented by
[`investments-price-feeds.md`](investments-price-feeds.md); it is the same
concept and must not acquire a second spelling.

#### `reports.net_worth`

```
currency_code            VARCHAR        -- Grain. NULL is the unknown-currency segment
home_currency_code       VARCHAR
balance_date             DATE           -- Grain
rate_published_date      DATE
rate_source              VARCHAR
account_count            INTEGER        -- Accounts contributing on this date in this currency
carried_forward_count    INTEGER        -- How many of them are carried forward, not observed
total_assets             DECIMAL(18,2)
total_liabilities        DECIMAL(18,2)  -- Kept negative
net_worth                DECIMAL(18,2)  -- This currency's segment, in its own unit
total_assets_home        DECIMAL(18,2)
total_liabilities_home   DECIMAL(18,2)
net_worth_home           DECIMAL(18,2)  -- NULL when this currency is unpriced on this date
```

The existing six columns keep their names, types, and meanings; the additions
are additive, which is what M2B.1 Key Decision 5 anticipated when it said
multi-currency would slot in without breaking changes. Their *positions* do
change, because the new columns interleave rather than append — and reordering a
shipped `reports` column is a public-contract change under
`design-principles.md`'s trigger list. Pre-launch posture permits it;
`.claude/rules/column-ordering.md` requires disclosing it in the CHANGELOG
rather than shipping it silently.

`net_worth_home` is the headline: the reason this rung exists on the SQL surface
rather than only in Python is to make the segments comparable, and *"how much of
my position is in EUR"* is unanswerable without a common unit.

#### `reports.net_worth_summary`

```
home_currency_code       VARCHAR
balance_date             DATE           -- Grain
account_count            INTEGER
carried_forward_count    INTEGER
currency_count           INTEGER        -- Distinct currencies held on this date
unpriced_currency_count  INTEGER        -- How many of them had no rate; 0 means complete
total_assets             DECIMAL(18,2)  -- NULL when unpriced_currency_count > 0
total_liabilities        DECIMAL(18,2)  -- NULL when unpriced_currency_count > 0
net_worth                DECIMAL(18,2)  -- NULL when unpriced_currency_count > 0
```

No suffixes: every measure here is in `home_currency_code` by construction, and
no per-rate provenance either: several currencies contribute to one row, each
with its own rate and its own publication day. Rate provenance belongs on the
rungs where a row has exactly one rate; here the coverage counts carry it.

This rung is the reason the other two are not enough. A caller who sums
`reports.net_worth.net_worth_home` themselves gets a plausible wrong answer when
one currency is unpriced, because SQL's `SUM()` skips NULL and silently returns
the priced subset. Rung 3 does the aggregation once, and fails closed.

### Rate models

Three models, each mirroring an existing sibling exactly rather than inventing a
shape.

#### `prep.stg_exchange_rates` (VIEW)

Normalizes `raw.exchange_rates`. Mirrors `prep.stg_security_prices`.

#### `core.fct_exchange_rates` (VIEW)

Observation grain `(from_currency, to_currency, rate_date)`, where `rate_date`
keeps the meaning `raw.exchange_rates` already gives it: **the business day the
provider published the rate for**. Unions the staged provider rows with
`app.exchange_rate_overrides` and resolves precedence — an override outranks
every provider row for its own pair and date. Mirrors `core.fct_balances`
(a VIEW unioning observation sources) and the provider/override precedence
already resolved by `core.fct_security_prices`.

#### `core.fct_exchange_rates_daily` (TABLE, kind FULL)

Grain `(from_currency, to_currency, effective_date)`. Mirrors
`core.fct_balances_daily`: a dense daily spine over an observation model.

```
from_currency         VARCHAR        -- Grain. ISO 4217, upper
to_currency           VARCHAR        -- Grain
effective_date        DATE           -- Grain. The calendar day this rate is applied ON
published_date        DATE           -- The day the provider priced it (= fct_exchange_rates.rate_date)
rate                  DECIMAL(18,8)  -- Multiply a from_currency amount by this
rate_source           VARCHAR        -- override / provider / identity
days_since_published  INTEGER        -- effective_date - published_date; 0 on a publication day
```

**`effective_date` and `rate_date` are deliberately different names for
different things.** `raw.exchange_rates.rate_date` is the publication day, and
its own DDL comment explains why storing the requested day there would be wrong.
This model needs both, so the applied day takes a distinct name. Reusing
`rate_date` for the applied day would leave two columns with one name and
opposite meanings across two layers.

Four properties define it:

- **Window-bounded fill.** For a real pair, rows exist only for
  `effective_date` between that pair's first and last observation. Before the
  first quote and after the last there is no row at all, so a join misses
  visibly rather than matching a manufactured rate. An unbounded fill would
  invent a rate for a delisted pair, a discontinued peg, or a provider that
  stopped publishing.
- **Carry-forward across non-publication days only.** Within the window, a day
  with no observation takes the last published rate, and `published_date` /
  `days_since_published` state that it did. This is precisely the stored
  requested-to-published mapping that
  `src/moneybin/services/currency_service.py:197-205` names as the way to close
  the weekday-holiday gap — and it does so without the widening that same
  comment forbids, because the hop is recorded per row rather than performed at
  lookup time.
- **Identity rows are materialized.** For every currency appearing in
  `core.dim_accounts`, an `X → X` row at 1.0 with `rate_source = 'identity'` and
  `days_since_published = 0`, spanning the date domain of
  `core.fct_balances_daily`. Requirement 11: one join path, no branch, and a
  single-currency profile never sees a NULL converted column.
- **Kind FULL, recomputed every `sqlmesh run`.** A retroactively corrected rate
  or a newly entered override is picked up by the next run with no incremental
  bookkeeping and no staleness marker. This matches `fct_balances_daily` and
  `fct_security_prices`.

The identity arm reads `core.dim_accounts` and `core.fct_balances_daily` for its
date domain, which couples this model to the balance spine. That is accepted:
the coupling is one arm of one model, and the alternative — a manufactured 1.0
written into three report views — is the substitution Requirement 5 forbids.

### `app.account_settings` — new column

```sql
ALTER TABLE app.account_settings ADD COLUMN archived_at DATE;
-- The date the account stopped being part of the position. NULL while active.
-- archived (BOOLEAN) remains the present-tense flag driving list visibility.
```

Net worth is a stock measure and its history should be immutable; archiving is a
present-tense action. Applying a present-tense flag across a historical series
rewrites the past. The three rungs exclude an account for
`balance_date > archived_at` and include it before, so a closed account's 2022
balance stays in 2022's net worth. Backfill sets `archived_at` to the archival
audit-log date where one exists, and otherwise leaves it NULL, which preserves
today's behavior for that account rather than guessing a cutoff.

## Implementation Plan

### Files to Create

SQLMesh models:
- `src/moneybin/sqlmesh/models/prep/stg_exchange_rates.sql`
- `src/moneybin/sqlmesh/models/core/fct_exchange_rates.sql`
- `src/moneybin/sqlmesh/models/core/fct_exchange_rates_daily.sql`
- `src/moneybin/sqlmesh/models/reports/net_worth_accounts.sql`
- `src/moneybin/sqlmesh/models/reports/net_worth_summary.sql`

Report runners:
- `src/moneybin/reports/definitions/networth.py` — `@report` runner for
  `core:networth`
- `src/moneybin/reports/definitions/networth_history.py` — `@report` runner for
  `core:networth_history`

Migration:
- `src/moneybin/sql/migrations/V0NN__add_account_settings_archived_at.py`

Tests: unit tests for each new model's shape and null behavior, a scenario test
comparing the three rungs against generator ground truth, and the two guard
tests named in §Testing Strategy.

### Files to Modify

- `src/moneybin/sqlmesh/models/reports/net_worth.sql` — add the converted
  measures, the rate provenance columns, and `carried_forward_count`; switch the
  eligibility filter to the date-scoped form.
- `src/moneybin/sql/schema/app_account_settings.sql` — declare `archived_at`.
- `src/moneybin/sqlmesh/models/core/dim_accounts.sql` — resolve `archived_at`
  alongside `archived`. Two column comments there still name the retired
  `agg_net_worth` model (`:362-363`); correct them while in the file.
- `src/moneybin/reports/_framework/convert.py` — `convert_records` prices every
  money-classed column using the row's original currency. Pointed at a column
  that is *already* home-converted it applies the rate a second time, silently,
  and passes every existing test. `OutputColumn` needs a currency-basis notion
  before any converted column reaches a report. **Write that test first.**
- `src/moneybin/cli/commands/reports/__init__.py` — the generated CLI
  registration must skip the two migrated specs, whose hand-written
  `networth` / `networth-history` commands stay for their richer summary
  rendering. Filter by spec name with a comment saying why.
- `src/moneybin/metrics/registry.py` — rate-spine coverage counters.
- `docs/specs/INDEX.md`, `docs/roadmap.md` — status and milestone entries.
- `.claude/rules/column-ordering.md` — the "service-backed report is the
  exception" passages, and Enforcement item 3's `_SNAPSHOT_COLUMN_TYPES`
  tripwire, both retire with the deletion below.

### Files to Delete

Approved as part of this work: `src/moneybin/reports/service_reports.py`,
`src/moneybin/services/networth_service.py`,
`src/moneybin/privacy/payloads/networth.py`, the `ServiceReportSpec` class, the
service executor branch in `_framework/catalog.py`, and the two `sql_unavailable`
/ `service_backed` arms in `_framework/explain.py`.

`src/moneybin/services/demo_service.py` is the real cost: it calls
`NetworthService(db).current()` and types `DemoResult.per_currency` with
`NetWorthCurrencySegment`. It moves onto the report catalog, and `DemoResult`'s
type changes with it.

`test_service_report_privacy_maps_match_independent_contract` dies with the kind.
That is the point — CI derivation replaces a hand-reviewed privacy map.

### Key Decisions

1. **Three relations, not one wide one.** A single relation carrying account
   rows and totals rows distinguished by which half is NULL is the shape M2B.1
   shipped, and it forces every consumer to branch on row kind. One grain per
   relation is what makes each rung a plain `SELECT` for a human or an agent.
2. **Conceptual ladder, physical siblings.** The rungs read as a progression but
   are independent projections. Forced by `assert_acyclic`; also better —
   nothing cascades, and each view is comprehensible alone.
3. **Converted columns on rungs 1 and 2, none on rung 3.** Rung 3 is
   single-currency by construction so a suffix would be noise. Rungs 1 and 2
   need both because the original denomination is canonical and the converted
   one is what makes rows comparable.
4. **Counts, not a status enum.** Data quality is expressed as
   `unpriced_currency_count`, `carried_forward_count`, and
   `days_since_observed` rather than a three-valued quality enum. A count is
   strictly more informative than a status label, and counts roll up under `SUM`
   while enums need a combining rule. The trust bit is already carried by
   NULL-versus-populated on the measure itself.
5. **Date-scoped archival for stock measures only.** `NOT archived` also appears
   in `cash_flow`, `spending_trend`, `merchant_activity`, `large_transactions`,
   `recurring_subscriptions`, and `balance_drift`. For a flow report over a
   chosen window, excluding an archived account outright is defensible. For a
   balance history it is not. Only the three net-worth rungs change.
6. **No rung may be materialized without moving the filter to read time.**
   `include_in_net_worth`, `archived`, and `archived_at` are user-reversible.
   Materializing a rung would freeze one evaluation of a reversible filter into
   stored rows, and un-archiving an account would leave the stored total wrong
   with nothing to detect it. This binds M2P.3
   ([`reports-overview.md`](reports-overview.md)), whose whole subject is
   materializing report views: these three are eligible only if the eligibility
   predicate moves to read time over granular rows.
7. **Python conversion stays.** `--display-currency` / `display_currency` to an
   arbitrary currency remains `convert_records`' job. SQL materializes the home
   currency only. Two mechanisms, one each for a distinct job — not two ways to
   do one job.
8. **Valuation-date vocabulary is deferred, deliberately.** A valuation can be
   priced at each row's own date or at the period's end, and MoneyBin has no
   word for the distinction. It does not bite here: every row in a balance spine
   is already dated at the grain, so both readings coincide and the ladder is
   unambiguous. The distinction arrives with Pillar D, where a position acquired
   earlier is valued on a later date. Name it then, once, rather than inventing
   an enum now that holdings would have to re-litigate.

## CLI Interface

No new commands. `moneybin reports networth` and `moneybin reports
networth-history` keep their names and flags; under §Open decision Option A
their rendering is untouched too, and only the backing implementation changes.
`--display-currency` continues to work on both, and now answers from a converted
column rather than a read-time pass when the requested currency is the home
currency.

The three views are reachable through the existing generic surface —
`moneybin sql query "SELECT * FROM reports.net_worth_summary ORDER BY
balance_date DESC LIMIT 12"` — which is the point of the change.

One asymmetry to close while here: `--interval` is a bare `str` with no `Choice`
constraint (`src/moneybin/cli/commands/reports/networth.py:130-132`), so a bad
value fails at the framework's `Literal` check rather than as a Typer usage
error.

## MCP Interface

`reports(report_id="core:networth")` and
`reports(report_id="core:networth_history")` keep their ids and parameters.
Three observable changes:

- `reports explain` returns the actual query for both, in place of today's
  `sql_unavailable` reason, and graduation returns a real verdict rather than
  `service_backed`.
- Both gain the columns their backing view gains. Adding a column to a shipped
  report's result is additive; no existing column changes name, type, or
  meaning.
- `core:networth_history` gains a `net_worth` in the home currency, which it
  cannot produce today in any form.

### One report, one view — and the allocation that follows

A report binds to exactly one view. `ReportSpec.view` is a single `TableRef`
(`src/moneybin/reports/_framework/contract.py:370`), `reports_class_map()` keys
`(schema, name) → classes` from it
(`src/moneybin/privacy/sql_lineage.py:1759-1805`), and a second spec on the same
view silently overwrites the first. The declaring report must also cover every
column its view exposes, checked by
`test_declared_classes_match_derivation`.

So three views and two reports do not divide evenly, and the leftover view is
runner-less — covered by the generated `_derived_classes.py`, exactly as
`reports.net_worth` is covered today. Which view is the leftover is the one
decision this spec does not settle. See §Open decision.

## Open decision

**Does `core:networth` keep returning account rows?**

Today it returns two row kinds in one result — per-currency totals and a
per-account breakdown, told apart by which half is NULL. §Key Decision 1 argues
against that shape for a relation. The same argument applies to a report
envelope, but changing one is a breaking change to a shipped surface, so it is
put here rather than assumed.

**Option A — preserve the envelopes.** `core:networth` binds to
`reports.net_worth` and its runner unions the account rows in from
`reports.net_worth_accounts`; `core:networth_history` binds to
`reports.net_worth_summary`; `reports.net_worth_accounts` is the runner-less
view. Every id, parameter, row kind, and CLI rendering survives untouched, and
the whole change is additive.
*Cost:* the two-row-kind envelope outlives the reason for it. An agent calling
`reports(report_id="core:networth")` still branches on row kind even though the
underlying views no longer make it, so the grain discipline stops at the report
boundary. The runner's spec must declare the union of both views' columns.

**Option B — one grain per report as well as per view.** `core:networth` slims
to per-currency totals over `reports.net_worth`; a new
`core:networth_accounts` serves the breakdown over
`reports.net_worth_accounts`; `core:networth_history` binds to
`reports.net_worth_summary`. Three views, three reports, one grain each, and no
consumer branches on row kind anywhere.
*Cost:* a shipped report stops returning rows it returns today — breaking for
any existing caller — plus a new public report id, and rework of the CLI's
hand-written summary rendering, all inside a change that already deletes a
report kind, adds three rate models, and migrates a schema.

**Recommendation: A now, B as its own change.** B is the better end state and
should not be dropped. It is also independently valuable, independently
reviewable, and much easier to judge once the three views exist and there is
usage to point at. Landing it inside this migration means one PR that changes a
public response shape, deletes a report kind, and introduces a rate spine — with
no way to revert one without the others.

Under either option, no new report id is created for
`reports.net_worth_summary`'s own grain beyond what is listed above; it reaches
callers through `core:networth_history` and through the SQL surface.

## Testing Strategy

### Tier 1 — Unit

- **Rate spine window bounds.** A pair with observations only in a closed
  interval produces no row outside it — asserted on both edges. This is the
  guard for Requirement 5, and the one most likely to regress into an unbounded
  fill.
- **Carry-forward provenance.** A non-publication day inside the window carries
  the prior rate with `published_date` set to the publication day and
  `days_since_published` equal to the gap.
- **Identity rows.** A single-currency profile gets a populated
  `net_worth_home` on every date with no NULL and no special case.
- **Fail-closed summary.** With one unpriced currency held,
  `net_worth_summary.net_worth` is NULL and `unpriced_currency_count` is 1 —
  and the priced subset is *not* returned in the measure.
- **Double conversion.** A converted column passed through `convert_records`
  must not be priced twice. Write this before any converted column exists; it
  fails silently today and passes every existing test.
- **Date-scoped archival.** An account archived on date D contributes to
  `balance_date <= D` and not after, on all three rungs.
- **Grain integrity.** Each rung's declared grain is unique.

### Tier 2 — Synthetic scenarios

`make test-scenarios` compares all three rungs against generator ground truth,
including the multi-currency persona, which already contains an account in a
currency outside the rate provider's published set — so the unpriced path is
reachable from a shipped fixture rather than a hand-built one.

### Tier 3 — Integration

- The privacy-class derivation must accept all three views and reject a stacked
  variant; the second half is a guard on Requirement 2, not a hypothetical.
- CLI and MCP parity on both migrated reports, before and after, over the same
  fixture.

## Synthetic Data Requirements

The `international` persona already supplies the shapes needed: several
currencies, one of them unpriced. Two additions:

- A persona account archived partway through its history, so the date-scoped
  exclusion is exercised end to end rather than only in unit tests.
- A rate-observation gap of more than one non-publication day inside a pair's
  window, so `days_since_published` takes a value greater than 1.

Ground truth needs expected net worth per day in the home currency, plus the
expected NULL dates for the unpriced currency.

## Dependencies

- [`reports-net-worth.md`](reports-net-worth.md) — the balance spine this reads.
  Unchanged by this spec.
- [`multi-currency.md`](multi-currency.md) — Requirements 5, 8, 9, 12, 17, and
  the one-way door on persisted conversion all bind here. `raw.exchange_rates`
  and `app.exchange_rate_overrides` are its tables.
- `.claude/rules/reports.md` — the `@report` contract, declared privacy
  classes, and the SQL-backed default.
- `.claude/rules/column-ordering.md` — Rule B for all three rungs.
- [`database-migration.md`](database-migration.md) — `archived_at`.
- [`reports-overview.md`](reports-overview.md) — M2P.3 materialization, bound by
  Key Decision 6.

## Out of Scope

- **The margin-loan defect** (Defect 1). A wrong headline number today, one
  field wide, depending on nothing here. It should ship on its own, before or
  independent of this work, with the guard its neighbouring docstring already
  implies: a test that fails when a wire field the server sends is undeclared on
  the client model. Burying it inside this refactor would hide it.
- **Investment holdings in net worth** (Pillar D) and the daily position spine
  `core.fct_holdings_daily` (Pillar C.3) — both designed in
  [`investments-price-feeds.md`](investments-price-feeds.md). C.3 is the fourth
  grain beneath this ladder (`account × security × day`) and needs no redesign.
  When it lands, an investment account's value becomes a *component* of its
  balance row rather than a second addend, which is the structural form of the
  invariant M2B.1 records: the provider's reported balance already is the total
  position value.
- **Return metrics** — TWR, IRR, MWR. These are transaction-replay problems,
  not aggregations over any balance grain however fine. No rung of this ladder
  reaches them, and none should grow a column that pretends to.
- **`net_contribution`** — separating "grew from deposits" from "grew from
  market" needs a flow measure joined beside the balance, which no grain of a
  pure balance ladder reaches. A real gap; a different spec.
- **Asset-class, sector, or region breakdowns** — many-to-many against holdings,
  so a `bridge_*` model orthogonal to this ladder rather than a rung of it.
- **Named account subsets** — a filter layered over the ladder, not a grain.
- **Per-lot cost basis** — the grain below account × security, belonging to the
  investments ledger.
- **A `system doctor` check for balance staleness** (Defect 4's other half).
  This spec makes staleness queryable; turning it into an invariant with a
  threshold is a doctor change.
- **Balance forecasting** — unchanged from M2B.1.
- **Arbitrary display-currency conversion in SQL** — Key Decision 7.
