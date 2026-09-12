# Feature: Net Worth on the SQL Surface

## Status
in-progress

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
one row per day, and `reports.net_worth` — today the per-currency view, renamed
in §Report allocation — aggregates the included accounts per
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

Verified against `main` at `4b3412ee`. The disposition column says which are
this spec's to close.

| # | Defect | Evidence | Disposition |
|---|---|---|---|
| 1 | A margin account's net worth was overstated by the size of its loan. The sync server sends `margin_loan_amount`, `SyncBalance` did not declare it, and Pydantic's default `extra='ignore'` discarded it silently. | `src/moneybin/connectors/sync_models.py:232` now declares the field; it reaches the spine through `prep/stg_plaid__balances.sql` and `core/fct_balances.sql`, on migration `V058`. | **Already closed**, by #565, before this spec. Kept in the table because the ladder's correctness depends on the balance it reads, and a reader checking that dependency should find it answered rather than absent. |
| 2 | Archiving an account rewrites net-worth history. `archived` is a plain BOOLEAN with no date, and the filter applies to every `balance_date`, so closing an account in 2026 retroactively removes it from 2022. | `src/moneybin/sql/schema/app_account_settings.sql:13`, `src/moneybin/sqlmesh/models/reports/net_worth.sql:21` | **Closed here** — Requirement 9, behind the prerequisite that requirement names. |
| 3 | `core:networth_history` cannot convert currency at all. | `src/moneybin/reports/service_reports.py:170` vs `:122` | **Closed here** — Requirements 1 and 3. |
| 4 | Staleness is invisible on every net-worth surface. `fct_balances_daily` carries `is_observed`, `observation_source`, and `reconciliation_delta`; only `observation_source` reaches a report, rendered as a bare blank cell, and `reconciliation_delta` reaches none. No `system doctor` check covers balance staleness. | `src/moneybin/services/networth_service.py:111-118`, `src/moneybin/cli/render.py:632-633` | **Closed here** — Requirement 8. The doctor check is out of scope. |
| 5 | The double-count invariant that Pillar D must uphold has no guard. Safe today only because no holding is wired into net worth. | `investments-overview.md` §Pillar D states the two tests in future tense | **Not this spec.** Belongs with Pillar D; named here so it is not lost. |
| 6 | An investment account with priced holdings and no balance observation contributes exactly zero to net worth — Requirement 9 of M2B.1 emits no rows without an anchor, and nothing detects the gap. | `investments-overview.md` §Open, `doctor_service.py` invariant list | **Not this spec.** Pillar D / a doctor check. |

## Requirements

1. **Three grains, three relations.** `reports.net_worth` (day),
   `reports.net_worth_currencies` (currency × day), and
   `reports.net_worth_accounts` (account × day) each answer a distinct question
   with one `SELECT` against one relation and no post-processing. §Data Model
   states the question each one answers.
2. **No rung reads another.** Each is an independent projection of the same
   `core.*` facts at a different `GROUP BY` level. This is not a preference:
   `assert_acyclic` (`src/moneybin/privacy/report_materialization.py:68`)
   rejects any read of `reports.*` by any model under privacy-class derivation,
   because `reports.*` columns *are* derivation's own output. A stacked ladder
   fails CI.
3. **Home-currency conversion happens in SQL.** The currencies and accounts
   rungs carry converted measures beside the originals; `reports.net_worth` is
   home-denominated by construction. A caller with no display-currency
   machinery — `sql_query`, `moneybin sql query`, a saved report, a human at a
   DuckDB prompt — gets a comparable number.
4. **Original currency stays canonical, and nothing converted is persisted at
   row grain.** All three rungs are `kind VIEW`, so a converted column is
   computed on read and stores nothing. This is what keeps the design inside
   [`multi-currency.md`](multi-currency.md) §"The core decision (one-way door)",
   which names deriving through SQLMesh as the sanctioned mechanism. No
   materialized model may carry a converted amount.
5. **A rate is never manufactured.** Outside a pair's own observation window the
   rate spine has no row, so a join misses rather than matching an invented
   rate. Requirement 12 of `multi-currency.md` forbids substituting today's rate
   or 1.0; this extends the same rule to the shape of the table. The trailing
   edge is the one place the window runs past the last observation, and only as
   far as `_last_publication_day` already hops: a Friday quote prices Saturday
   and Sunday, because no reference rate is published on a weekend. A weekday
   past the last observation is ambiguous — a holiday, a stopped feed, or a date
   nobody has fetched yet — so the rows stop there and the total goes NULL
   rather than carrying a stale quote forward. See §Rate models.
6. **Carry-forward is bounded and visible.** A rate row states the day the
   provider actually priced it (`published_date`) beside the day it is being
   applied to (`effective_date`), plus the distance between them. Carry-forward
   spans genuine non-publication days inside the window and the weekend hop at
   its trailing edge, never an unbounded gap.
7. **An unpriced currency makes the total NULL, not smaller.**
   `reports.net_worth.net_worth` is NULL when any contributing currency
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
   see §Key Decision 5. **This requirement has a prerequisite that is not this
   spec's to build** — retiring the archive cascade in `AccountService`, without
   which `archived_at` preserves nothing. See §`app.account_settings` and
   §Prerequisites.
10. **The net-worth reports become SQL-backed.** They become `@report` runners
    over the new views, and `ServiceReportSpec` and its executor branch are
    deleted. This is `.claude/rules/reports.md` §"A new report is SQL-backed"
    applied to the two reports that rule names as predating it.
11. **A single-currency profile joins identically.** Identity rows (`USD` → `USD`
    at 1.0) are materialized, so no rung needs a branch for the common case and
    no single-currency user sees a NULL converted column.
12. **Observability.** The rate spine's row and coverage counts join the existing
    `FX_RATE_*` family in `src/moneybin/metrics/registry.py:397-445`; the two
    migrated reports keep the report-execution metrics every catalog report
    already emits.
13. **A report's id, its view, and its CLI command share one name.** The name
    half of `report_id` is the view's name; `ReportSpec.cli_name` already
    derives the Typer command from it by swapping underscores for hyphens
    (`src/moneybin/reports/_framework/contract.py:408-410`). Applies to every
    report, not only these three — see §Report allocation.

## Data Model

### The three rungs

Each rung answers a question the others cannot, which is why three exist rather
than one wide relation. All three are `kind VIEW` in the `reports` schema.

| Rung | Grain | The question it answers |
|---|---|---|
| `reports.net_worth` | `(balance_date)` | *What am I worth, and what was I worth over time?* |
| `reports.net_worth_currencies` | `(currency_code, balance_date)` | *How is my position split across the currencies I hold?* |
| `reports.net_worth_accounts` | `(account_id, balance_date)` | *Which accounts hold my money, how fresh is each number, and what is each worth in one comparable unit?* |

They read the same three sources — `core.fct_balances_daily`,
`core.dim_accounts`, `core.fct_exchange_rates_effective` — at three `GROUP BY`
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

#### `reports.net_worth_currencies`

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
multi-currency would slot in without breaking changes — though the view itself
is renamed, since `reports.net_worth` becomes the day-grain rung. Their
*positions* also change, because the new columns interleave rather than append,
and reordering a shipped `reports` column is a public-contract change under
`design-principles.md`'s trigger list. Pre-launch posture permits it;
`.claude/rules/column-ordering.md` requires disclosing it in the CHANGELOG
rather than shipping it silently.

`net_worth_home` is the headline: the reason this rung exists on the SQL surface
rather than only in Python is to make the segments comparable, and *"how much of
my position is in EUR"* is unanswerable without a common unit.

#### `reports.net_worth`

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
`reports.net_worth_currencies.net_worth_home` themselves gets a plausible wrong
answer when one currency is unpriced, because SQL's `SUM()` skips NULL and
silently returns the priced subset. This rung does the aggregation once, and
fails closed.

### Rate models

Four models. The first three mirror an existing sibling exactly rather than
inventing a shape; the fourth exists because override precedence has to stay at
read time.

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
Densifies the provider observations in `prep.stg_exchange_rates` — published
rates and identity rows only. User overrides are applied above it, at read time,
by `core.fct_exchange_rates_effective`; see that model for why. `rate_source`
here is therefore `provider` or `identity`, never `override`. The
observation-grain model above keeps resolving precedence for its own consumers;
this one takes the half that is safe to materialize.

**It densifies the staged rows rather than `core.fct_exchange_rates`, and the
choice of upstream is load-bearing.** That view resolves precedence at
observation grain, so on a date carrying both a provider quote and an override
it emits the override alone: the provider row is gone, and no provider arm
survives on that date to densify. The spine would carry an older quote across
it. Nothing looks wrong while the override stands, because
`core.fct_exchange_rates_effective` wins that day at read time regardless.
Delete the override, though, and the effective view falls back to a spine whose
`rate` and `published_date` for that day were never the provider's, and it stays
wrong until the next `sqlmesh run` rebuilds this table. An override applies the
moment it is written; its deletion has to take effect just as immediately, and
only a spine built from the unresolved provider rows does that.

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

- **Window-bounded fill, with a weekend hop at the trailing edge.**
  For a real pair, rows start at that pair's first observation — before the
  first quote there is no row at all, so a join misses visibly rather than
  matching a manufactured rate. An unbounded fill would invent a rate for a
  delisted pair, a discontinued peg, or a provider that stopped publishing.

  Ending the rows *on* the last observation would be wrong too, but only by the
  two days a weekend costs. `CurrencyService.resolve_rate` misses the exact day
  and then tries `_last_publication_day`, which hops a weekend and nothing else,
  "because no reference rate is ever published on one." So a Saturday balance is
  priced from Friday's quote today, and a spine that stopped on Friday would
  take that away and NULL the headline `net_worth` on a healthy profile.

  So the effective range runs the two days past a last observation that falls on
  a Friday, and stops on the observation otherwise, carrying the published rate
  with `days_since_published` counting up.

  **It runs no further, and `_covers_window_end` is not a precedent for running
  further.** That function (`src/moneybin/services/rate_backfill.py:484-507`)
  tolerates a trailing gap of `MAX_BACKWARD_RESOLUTION_DAYS` to decide whether
  to *warn* that a feed has stopped; it prices nothing. The only place that
  constant bounds a rate is `currency_service.py:549`, which rejects a
  provider's own response dated too far before the day asked about. Borrowing it
  as a carry-forward window would price an ordinary Tuesday from a quote up to
  14 days old — the substitution `currency_service.py:197-205` forbids by name.
- **Carry-forward across non-publication days only.** Within the window, a day
  with no observation takes the last published rate, and `published_date` /
  `days_since_published` state that it did. Interior gaps carry safely because
  observations bracket them on both sides: the provider was publishing before
  and after, so the gap is a closure rather than a date nobody fetched. That is
  the stored requested-to-published mapping
  `src/moneybin/services/currency_service.py:197-205` names as the way to close
  the weekday-holiday gap — without the widening that same comment forbids,
  because the hop is recorded per row rather than performed at lookup time. Past
  the last observation nothing brackets the gap, which is why the trailing edge
  above stops at the weekend rather than carrying on.
- **Identity rows are materialized.** For every currency appearing in
  `core.dim_accounts`, an `X → X` row at 1.0 with `rate_source = 'identity'` and
  `days_since_published = 0`, spanning the date domain of
  `core.fct_balances_daily`. Requirement 11: one join path, no branch, and a
  single-currency profile never sees a NULL converted column.
- **Kind FULL, recomputed every `sqlmesh run`.** A retroactively corrected
  provider rate is picked up by the next run with no incremental bookkeeping and
  no staleness marker. This matches `fct_balances_daily` and
  `fct_security_prices`. A user override is *not* in that set — it must apply
  the moment it is written, which is why it is not materialized here.

The identity arm reads `core.dim_accounts` and `core.fct_balances_daily` for its
date domain, which couples this model to the balance spine. That is accepted:
the coupling is one arm of one model, and the alternative — a manufactured 1.0
written into three report views — is the substitution Requirement 5 forbids.

#### `core.fct_exchange_rates_effective` (VIEW)

Same grain, and **the model the three rungs actually join**. It applies override
precedence at read time to the *observation each row carried forward from*, not
only to the calendar day the override is filed under — it densifies the winning
observation rather than overlaying corrections at their recorded dates.

That distinction is the whole correctness of the model. `resolve_rate` consults
`_stored_rate` twice — once for the exact day, once for `_last_publication_day`
of it — and `_stored_rate` is override-first both times. A user who corrects
Friday's quote is therefore already pricing Saturday today. An overlay matched on
`effective_date` alone would leave Saturday carrying the *provider's* Friday
rate, so the SQL reports would ignore the correction on exactly the days
carry-forward exists to cover.

Three rules, in this precedence, reproduce `_stored_rate`:

1. **An override on the `effective_date` itself wins** — `_stored_rate`'s
   exact-day check. `published_date` becomes that day and
   `days_since_published` is 0: the user priced the day itself.
2. **Otherwise an override on the row's `published_date` wins** — the same check
   reached through the carry-forward. A corrected Friday prices the Saturday and
   Sunday carrying from it, and a corrected quote prices every interior
   non-publication day carrying from it. `published_date` and
   `days_since_published` keep the hop they already recorded.
3. **An override on a pair and date the spine does not cover contributes its own
   row** — `_stored_rate` answers from the override table whether or not a
   provider ever priced that day, so a correction is never invisible because the
   provider was silent. Rows carry forward from it under the same rules as an
   observation.

Every row an override wins reads `rate_source = 'override'`.

The split exists because the two halves have opposite freshness requirements. A
materialized override is a regression against the path this spec replaces:
`CurrencyService.resolve_rate` consults `_stored_rate`, which checks the
override table **first** — "a correction outranks every cached provider rate for
its own pair and date, and this is the single place that ordering is expressed"
(`src/moneybin/services/currency_service.py:346-372`). A `kind FULL` model
carrying overrides would serve the pre-override rate until the next
`sqlmesh run`, so a user who ran `fx set` to correct a wrong number would keep
reading the wrong number — and the new SQL reports would be *less* fresh than
today's Python path. Correcting a rate is a mutation with an audit row; a
surface that ignores it until a scheduled rebuild is not one a user can trust.

Dense carry-forward is expensive and safely cacheable; override precedence is a
cheap join and must be live. The seam goes between them. This is the same
reasoning as Key Decision 6 — a reversible, user-controlled input is never
frozen into materialized rows — and leaves the ordering expressed in exactly two
places, `_stored_rate` and this view, which the parity test in §Testing Strategy
holds together.

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

**The column alone preserves nothing.** `AccountService.settings_update` forces
`include_in_net_worth=False` in the same write as `archived=True`
(`src/moneybin/services/account_service.py:714-718`), and `archived=False`
deliberately does not restore it. That flag carries no date, so every historical
row of an archived account still fails the `include_in_net_worth` half of the
eligibility filter and the history this column exists to preserve is excluded
anyway. Adding the date predicate on top of the cascade is inert.

The cascade is also redundant with the filter it defends. `reports.net_worth`
already reads `a.include_in_net_worth AND NOT a.archived`
(`src/moneybin/sqlmesh/models/reports/net_worth.sql:21`); the `NOT a.archived`
half enforces "an archived account never contributes" on its own, at read time.
The write buys exactly one behavior the predicate does not — an account stays
excluded after archive-then-unarchive — and pays for it by overwriting a
user-authored preference with a derived one.

So the fix is to retire the cascade and let `archived_at` carry the exclusion,
date-scoped: `include_in_net_worth AND (archived_at IS NULL OR balance_date <=
archived_at)`. That is a change to a service write path, an `app.*` column
semantic, and a backfill that reconstructs pre-archive intent from
`before_value.include_in_net_worth` on the `archived` FALSE→TRUE audit row.
`.claude/rules/design-principles.md` puts `app.*` schema semantics on the
one-way-door trigger list, and a change of that shape earns its own review
rather than approval alongside three report views. It is therefore a
**prerequisite**, sequenced ahead of this spec exactly as the margin-loan defect
was — see §Prerequisites.

Nothing about that reconstruction decays while it waits: `app.audit_log` is
append-only, with no prune, retention, or delete path, so each archive write
keeps its full prior row state indefinitely.

## Report allocation

### One name per report

A report's `report_id` is a hand-declared `namespace:name` string
(`src/moneybin/reports/_framework/contract.py:367`) with nothing binding its
name half to `spec.view`. Across the eight shipped reports, two match and six do
not. That was drift, not a convention, and this spec ends it:

> **The name half of `report_id` is the view's name.**

`ReportSpec.cli_name` already derives the Typer command from that same name by
swapping underscores for hyphens (`contract.py:408-410`), so one name fixes all
three surfaces at once and there is no independent CLI name to choose.

The reason is `reports explain`. It hands a caller the query, which names the
view; the caller pastes that into `sql_query` and reruns it. Two spellings put a
mapping step on every hop between the report surface and the SQL surface, for
exactly the audience — an agent — that the SQL surface exists to serve.

Direction: the **view name wins**. SQL objects across `prep`, `core`, and
`reports` already use full snake_case domain names (`fct_balances_daily`,
`cash_flow`, `merchant_activity`); the compressed spellings live only in the
report ids and the commands derived from them.

### The three net-worth reports

| Report id | View | CLI | Grain |
|---|---|---|---|
| `core:net_worth` | `reports.net_worth` | `moneybin reports net-worth` | day |
| `core:net_worth_currencies` | `reports.net_worth_currencies` | `moneybin reports net-worth-currencies` | currency × day |
| `core:net_worth_accounts` | `reports.net_worth_accounts` | `moneybin reports net-worth-accounts` | account × day |

Each takes the same optional `from_date` / `to_date` / `interval`. **The
snapshot-versus-history split does not survive.** It was an artifact of how the
two reports were built, not a real distinction: every rung is a daily series,
and "now" is `WHERE balance_date = MAX(balance_date)`. `core:networth_history`
is therefore retired — it is `core:net_worth` with a range — and
`change_abs` / `change_pct` become runner-computed columns on `core:net_worth`
when one is given.

Two consequences worth stating plainly:

- **`reports.net_worth` changes meaning.** It becomes the day-grain scalar; the
  per-currency view it names today is renamed `reports.net_worth_currencies`.
  This is the right end state — `reports.net_worth` should be net worth, the
  number — but a reader of the old name gets a different relation, so the
  rename must be disclosed rather than absorbed.
- **The surviving id points somewhere new.** `core:net_worth` keeps its
  question but answers it at a cleaner grain and no longer returns account
  rows. A caller reading `currency_code` off today's totals rows wants
  `core:net_worth_currencies`.

### The other six reports

The naming rule is repo-wide, so the remaining six move in the same change.
`design-principles.md`'s coherence rule reserves the deprecation-marker path for
a migration that *cannot* land in one PR; this one can, so it lands whole rather
than leaving a second naming pattern standing beside the first. Four of the six
rename; two already satisfy the rule.

| Today | Becomes | CLI becomes |
|---|---|---|
| `core:cashflow` → `reports.cash_flow` | `core:cash_flow` | `cash-flow` |
| `core:spending` → `reports.spending_trend` | `core:spending_trend` | `spending-trend` |
| `core:recurring` → `reports.recurring_subscriptions` | `core:recurring_subscriptions` | `recurring-subscriptions` |
| `core:merchants` → `reports.merchant_activity` | `core:merchant_activity` | `merchant-activity` |
| `core:large_transactions` → `reports.large_transactions` | unchanged | unchanged |
| `core:balance_drift` → `reports.balance_drift` | unchanged | unchanged |

No view is renamed for any of the six — only the id, and the command derived
from it.

### Scope, honestly

This makes the change touch every report rather than only net worth, and
`networth_history` alone appears 86 times across 30 files, four of them public
guides. Most of that is mechanical, and `docs/guides/cli-reference.md` and the
MCP reference regenerate from code. The hand-written prose in
`docs/guides/reports.md` and `docs/guides/getting-started.md` is the part a
person rewrites.

Pre-launch posture is what makes this affordable: `design-principles.md` says to
iterate aggressively until the shape is right, and locks the surface at the
earlier of the M3E hosted launch or the first tagged release adopted by a
non-author user. Neither has happened, so the rename costs a search and replace
now and a deprecation cycle later.

## Implementation Plan

### Files to Create

SQLMesh models:
- `src/moneybin/sqlmesh/models/prep/stg_exchange_rates.sql`
- `src/moneybin/sqlmesh/models/core/fct_exchange_rates.sql`
- `src/moneybin/sqlmesh/models/core/fct_exchange_rates_daily.sql`
- `src/moneybin/sqlmesh/models/core/fct_exchange_rates_effective.sql`
- `src/moneybin/sqlmesh/models/reports/net_worth_accounts.sql`
- `src/moneybin/sqlmesh/models/reports/net_worth_currencies.sql` — today's
  `net_worth.sql`, renamed and widened

Report runners:
- `src/moneybin/reports/definitions/net_worth.py` — `core:net_worth`
- `src/moneybin/reports/definitions/net_worth_currencies.py` —
  `core:net_worth_currencies`
- `src/moneybin/reports/definitions/net_worth_accounts.py` —
  `core:net_worth_accounts`

Migration:
- `src/moneybin/sql/migrations/V0NN__add_account_settings_archived_at.py`

Tests: unit tests for each new model's shape and null behavior, a scenario test
comparing the three rungs against generator ground truth, and the two guard
tests named in §Testing Strategy.

### Files to Modify

- `src/moneybin/sqlmesh/models/reports/net_worth.sql` — becomes the day-grain
  rung. Its current per-currency body moves to `net_worth_currencies.sql` and
  gains the converted measures, the rate provenance columns, and
  `carried_forward_count`; both switch the eligibility filter to the
  date-scoped form.
- The four report definitions being renamed — `cash_flow`, `spending_trend`,
  `recurring_subscriptions`, `merchant_activity` — plus every test, guide, and
  fixture naming an old id or command. Mechanical, but repo-wide; see
  §Report allocation → Scope.
- `src/moneybin/sql/schema/app_account_settings.sql` — declare `archived_at`.
- `src/moneybin/sqlmesh/models/core/dim_accounts.sql` — resolve `archived_at`
  alongside `archived`. Two column comments there still name the retired
  `agg_net_worth` model (`:362-363`); correct them while in the file.
- `src/moneybin/reports/_framework/convert.py` — `convert_records` prices every
  money-classed column using the row's original currency. Pointed at a column
  that is *already* home-converted it applies the rate a second time, silently,
  and passes every existing test. `OutputColumn` needs a currency-basis notion
  before any converted column reaches a report. **Write that test first.**
- `src/moneybin/cli/commands/reports/__init__.py` — the hand-written
  `networth` / `networth-history` commands go away. If `render_summary` is kept
  for `net-worth`, that one command stays hand-written and the generated
  registration skips its spec by name, with a comment saying why; otherwise the
  filter disappears entirely.
- `src/moneybin/metrics/registry.py` — rate-spine coverage counters.
- `docs/specs/INDEX.md`, `docs/roadmap.md` — status and milestone entries.
- `.claude/rules/column-ordering.md` — the "service-backed report is the
  exception" passages retire with the deletion below, including the
  parallel-positions list that names `_SNAPSHOT_COLUMN_TYPES_BY_NAME`,
  `_HISTORY_COLUMNS`, and `types_by_name` — every symbol it cites disappears
  in this deletion too. (Enforcement item 3's `_SNAPSHOT_COLUMN_TYPES`
  tripwire already retired separately, when the snapshot path was keyed by
  name — issue #511.)

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
3. **Converted columns on the currencies and accounts rungs, none on
   `reports.net_worth`.** That rung is single-currency by construction, so a
   suffix there would be noise. The other two need both, because the original
   denomination is canonical and the converted one is what makes rows
   comparable.
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

Three commands, named in §Report allocation, all generated from their specs:
`moneybin reports net-worth`, `... net-worth-currencies`, and
`... net-worth-accounts`. `moneybin reports networth` and `networth-history` are
both replaced — the second by `net-worth --from … --to … --interval monthly`.

Whether the hand-written `render_summary` presentation survives as the default
rendering for `net-worth` is an implementation choice, not a contract: the
generated command renders from the spec, and the richer summary block can be
kept by leaving that one command hand-written. Prefer generated unless the
summary rendering measurably reads better.

`--display-currency` continues to work on all three, and now answers from a
converted column rather than a read-time pass when the requested currency is the
home currency.

The three views are also reachable through the generic surface —
`moneybin sql query "SELECT * FROM reports.net_worth ORDER BY balance_date DESC
LIMIT 12"` — which is the point of the change.

One asymmetry to close while here: `--interval` is a bare `str` with no `Choice`
constraint (`src/moneybin/cli/commands/reports/networth.py:130-132`), so a bad
value fails at the framework's `Literal` check rather than as a Typer usage
error.

## MCP Interface

Three reports, listed in §Report allocation, each reached as
`reports(report_id=..., parameters=...)` with `from_date`, `to_date`, and
`interval` optional on all three. Changes an existing caller sees:

- **`core:networth` becomes `core:net_worth`**, answers at the day grain in the
  home currency, and no longer returns account rows. The per-account breakdown
  moves to `core:net_worth_accounts`, the per-currency split to
  `core:net_worth_currencies`.
- **`core:networth_history` is retired.** Its answer is `core:net_worth` with a
  date range, now in one currency rather than segmented per currency.
- **`reports explain` returns the actual query** for all three, in place of
  today's `sql_unavailable` reason, and graduation returns a real verdict
  rather than `service_backed`.
- **The two-row-kind envelope is gone.** Nothing branches on which half of a
  row is null. That supersedes three paragraphs of `reports-net-worth.md`
  §MCP Interface — which fields populate on which row kind, why totals lead,
  and why the currency collapse runs before `limit`. Those paragraphs describe
  a surface that will no longer exist, so shipping this needs either a scoped
  pointer note at the top of that spec or a decision to leave it as a
  historical record. `INDEX.md`'s "enhancement" row says the original stays
  untouched; its "full redesign" row says the original gets a pointer. This
  change is the first against the data model and the second against the report
  layer, and the taxonomy has no row for that.

The cost is a round trip: an agent asking "what am I worth, and what is in it"
now makes two calls. That buys a relation per question, each with one grain and
no null-branching, which is the trade `.claude/rules/surface-design.md` and
AGENTS.md's AX bias both point at.

## Testing Strategy

### Tier 1 — Unit

- **Rate spine window bounds.** A pair last quoted on a Tuesday produces no row
  for the Wednesday after it, and no row at all before its first observation —
  asserted on both edges. This is the guard for Requirement 5, and the one most
  likely to regress into an unbounded fill; assert the Wednesday specifically,
  because a 14-day allowance is the exact regression that has to stay failing.
- **Healthy trailing edge.** A pair last quoted on a Friday still prices a
  Saturday and Sunday balance, with `days_since_published` at 1 and 2 and a
  non-NULL `net_worth`, and produces no row for the Monday after. The companion
  to the bound above: together they pin the trailing edge to exactly the weekend
  `_last_publication_day` hops.
- **Override precedence is live.** Writing an `app.exchange_rate_overrides` row
  changes `core.fct_exchange_rates_effective` and the three rungs on the next
  query, with **no `sqlmesh run` in between**, and `rate_source` reads
  `override`. This is the regression guard for the FULL-table shape this spec
  rejected; without it, materializing the override is an easy and invisible
  simplification for a later author to make.
- **An override carries forward with the day it corrects.** Correcting a Friday
  quote changes the Saturday and Sunday rows that carry from it, and correcting
  the observation before an interior gap changes every day in that gap. Assert
  the carried days, not only the corrected one: an overlay keyed on
  `effective_date` alone passes on the corrected day and silently serves the
  provider's rate on every day carrying from it.
- **An override on an unpriced day is still visible.** A correction for a pair
  and date the provider never priced produces a row, because `_stored_rate`
  answers from the override table before consulting the cache.
- **Override precedence agrees with `resolve_rate`.** For the same pair and
  date, `core.fct_exchange_rates_effective` returns what
  `CurrencyService.resolve_rate` returns — the parity check that keeps the two
  places expressing this ordering from drifting apart. Assert it on an exact
  publication day and on a weekend date — the two lookups `_stored_rate`
  performs. An interior weekday gap is deliberately *not* a parity case: the
  spine answers it from the recorded hop while `resolve_rate` re-fetches, which
  is the weekday-holiday gap `currency_service.py:197-205` describes as open.
  Assert that intended divergence by name, so a later author does not read it as
  a parity failure and close it by widening the Python lookup.
- **Carry-forward provenance.** A non-publication day inside the window carries
  the prior rate with `published_date` set to the publication day and
  `days_since_published` equal to the gap.
- **Identity rows.** A single-currency profile gets a populated
  `net_worth_home` on every date with no NULL and no special case.
- **Fail-closed summary.** With one unpriced currency held,
  `reports.net_worth.net_worth` is NULL and `unpriced_currency_count` is 1 —
  and the priced subset is *not* returned in the measure.
- **Double conversion.** A converted column passed through `convert_records`
  must not be priced twice. Write this before any converted column exists; it
  fails silently today and passes every existing test.
- **Date-scoped archival.** An account archived on date D contributes to
  `balance_date <= D` and not after, on all three rungs. Archive the account
  through `AccountService.settings_update` rather than by writing the columns
  directly — writing them directly is what makes this test pass while the
  cascade is still in place and the behavior is still broken.
- **Grain integrity.** Each rung's declared grain is unique.
- **The naming rule has a guard.** For every runner in `ALL_REPORTS`, the name
  half of `spec.report_id` equals `spec.view.name`. Requirement 13 is a
  convention until a test enforces it, and the six mismatches this spec removes
  are what an unenforced convention looks like after a year.

### Tier 2 — Synthetic scenarios

`make test-scenarios` compares all three rungs against generator ground truth,
including the multi-currency persona, which already contains an account in a
currency outside the rate provider's published set — so the unpriced path is
reachable from a shipped fixture rather than a hand-built one.

### Tier 3 — Integration

- The privacy-class derivation must accept all three views and reject a stacked
  variant; the second half is a guard on Requirement 2, not a hypothetical.
- CLI and MCP parity on all three reports, before and after, over the same
  fixture.
- No old id or command survives: a search for `core:networth`,
  `core:cashflow`, `core:spending`, `core:recurring`, `core:merchants`, and
  their derived command names returns nothing outside prose describing the
  rename. Mechanical renames across 30 files are exactly where one gets
  missed.

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

## Prerequisites

Work that is not this spec's to build, but that this spec cannot ship correct
without. Both are sequenced ahead of it, for the same reason: each is a change
to a different subsystem, and folding it into a reports change would get it
approved as a footnote rather than reviewed on its own terms.

- **Retire the archive cascade** — blocks Requirement 9, and only that
  requirement. `AccountService.settings_update` stops forcing
  `include_in_net_worth=False` when `archived=True`; `archived_at` carries the
  exclusion instead, date-scoped, and the net-worth eligibility filter becomes
  `include_in_net_worth AND (archived_at IS NULL OR balance_date <=
  archived_at)`. Needs the `archived_at` column and its migration, the service
  change, and a backfill that reads `before_value.include_in_net_worth` from the
  `archived` FALSE→TRUE audit row to distinguish a cascade-written FALSE from
  one the user chose. `app.audit_log` is append-only, so that reconstruction
  does not decay while this waits. Rationale and the redundancy that makes the
  cascade removable: §`app.account_settings`.
- **The margin-loan defect** (Defect 1) — **closed** by #565, ahead of this
  spec, which is the sequencing this section describes working as intended. The
  guard its neighbouring docstring implied — a test that fails when a wire field
  the server sends is undeclared on the client model — is the remaining piece,
  tracked with the other undeclared wire fields rather than here.

## Out of Scope

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
