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
| 4 | Staleness is invisible on every net-worth surface. `fct_balances_daily` carries `is_observed`, `observation_source`, and `reconciliation_delta`; only `observation_source` reaches a report, rendered as a bare blank cell, and `reconciliation_delta` reaches none. No `system doctor` check covers balance staleness. | `src/moneybin/services/networth_service.py:111-118`, `src/moneybin/cli/commands/reports/networth.py:110` | **Closed here** — Requirement 8. The `system doctor` balance-staleness check moves to the beta increment in Defect 6. |
| 5 | The double-count invariant that Pillar D must uphold has no guard. Safe today only because no holding is wired into net worth. | `investments-overview.md` §Pillar D states the two tests in future tense | **Not this spec.** Belongs with Pillar D; named here so it is not lost. |
| 6 | An investment account with priced holdings and no balance observation contributes exactly zero to net worth — Requirement 9 of M2B.1 emits no rows without an anchor, and nothing detects the gap. | `investments-overview.md` §Open, `doctor_service.py` invariant list | **Closed for the public beta** — Requirement 14, delivered as work item `M2B.3` rather than as part of the work already in flight here. Full Pillar D integration stays post-release; the guard that keeps its absence honest does not. |

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
   §Prerequisites. **A single `archived_at` cutoff cannot represent an
   archive → unarchive → re-include round trip** — see §Out of Scope for the
   interval it reports wrongly and why the fix is deferred.

    **Inherited from the prerequisite: a set of accounts this requirement has
    to decide.** `V063` backfills `archived_at` but deliberately leaves
    `include_in_net_worth` exactly as stored, including where the retired
    cascade is what forced it to `FALSE`. It cannot do otherwise: the cascade
    ran ahead of `_resolve()`, so an account the user only archived and one the
    user archived *and* explicitly excluded — a pair both the CLI and
    `accounts_set` accept in a single call — leave byte-identical audit images,
    and `AccountSettingsRepo.set` records row snapshots rather than caller
    kwargs. Nothing moves while the blanket `NOT archived` filter stands,
    because those accounts are excluded by it regardless. The moment this
    requirement replaces that filter with the date-scoped one, they stop being
    excluded by `archived` and start being excluded by a flag some of their
    owners never set. Deciding them is part of this requirement, not a
    leftover of the migration, and closes with a named `system doctor`
    check rather than an inferred intent — see §Prerequisites for the
    check's predicate and §Implementation Plan for where it lands.
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
    (`src/moneybin/reports/_framework/contract.py:474-476`). Applies to every
    report, not only these three — see §Report allocation.
14. **An unanchored account is visible, never a silent zero.** An eligible
    account carrying **evidence of holding value** but **no balance
    observation at all** must not contribute zero in silence. `core.fct_balances`
    silently drops every one of these: priced holdings with no matching
    balance row (Defect 6's case), a tabular import with no balance column,
    a Plaid account whose `current_balance` or `account_type` never
    resolved, and a manual account with recorded postings and no balance
    assertion. It behaves the way an unpriced currency already does under
    Requirement 7: the profile total is NULL, with an unanchored-account
    count beside it, and it carries the `system doctor` balance-staleness
    check that Defect 4 deferred — see §"`moneybin system doctor`: balance
    staleness" for its full specification.

    **The guard applies at the aggregate and per-account grains, deliberately
    not at the per-currency one.** `reports.net_worth_accounts` reads the
    same `core.fct_balances_daily` spine as `reports.net_worth`, so an
    eligible unanchored account would otherwise produce no row there either —
    the identical silent-zero failure this requirement exists to close, one
    rung down, where it is arguably worse: that rung is the one that answers
    "which accounts do I have," so an account vanishing from it is a more
    direct breach of the accuracy guarantee than a wrong aggregate is. The
    account rung's own signal is a row, not a count — see
    §`reports.net_worth_accounts` for why the finer grain needs no count of
    its own. `reports.net_worth_currencies` carries the same failure in
    principle — see §Out of Scope for why closing it there is real design
    work rather than a copy of either arm above, and is deferred rather than
    folded into this pass.

    **The qualifier, and why one is needed.** "No balance observation" alone
    is not sufficient to flag an account. An account with no balance, no
    holdings, and no transaction of any kind is not demonstrably holding
    anything — it may be a freshly linked or newly created account nothing
    has synced to yet — and flagging it would NULL every profile that has
    one, for no evidence-backed reason, which is a worse failure than the
    silent zero this requirement exists to replace. The guard therefore
    requires **evidence**: priced holdings (`core.dim_holdings` /
    `core.dim_holdings_broker_reported`, below) OR at least one row in
    `core.fct_transactions` or `core.fct_investment_transactions` for the
    account, ever, regardless of amount or source — a dividend, fee, or
    other investment-ledger event counts exactly as a cash-ledger posting
    does, with **no exception for a broker's separately reported position
    status** (§Data Model states the exact predicate). That is a **narrower reading than "any account with no balance
    anchor,"** taken literally — an account with genuinely zero activity of
    any kind stays silently absent, unchanged from today. That residual gap
    is this requirement's own, stated plainly rather than absorbed: see
    §Data Model for the exact predicate and what it does and does not catch.

    **Subject to the same account eligibility as every rung.** Requirement 9's
    `include_in_net_worth` and date-scoped `archived_at` predicate applies to a
    candidate unanchored account exactly as it applies to a balance-backed one:
    an account the user has excluded or archived does not count toward
    `unanchored_account_count` and does not NULL a total it was never part of.
    This is not a second eligibility rule beside Requirement 9's — it is the
    same one, applied to a source Requirement 9 did not yet have to join, and
    at the same granularity: for every ordinary balance-driven row this is
    evaluated at *that row's own* `balance_date`, exactly like every other
    Requirement 9 check. Only the one row with no `balance_date` of its own —
    the synthesized row below — needs a stand-in, and that stand-in is the
    only place a requested range enters this guard at all.

    **A profile with no balance-spine row at all still gets exactly one.**
    `core.fct_balances_daily` returns zero rows, not a NULL-valued one, when
    `core.fct_balances` is empty for the whole profile. Reading that spine
    alone, a profile whose only accounts are unanchored would then publish no
    `reports.net_worth` row at all — the same silent failure this requirement
    exists to close, one layer further out. This is the named case, but not
    the only one that empties the balance-driven output a requested range
    sees: a range that predates or postdates every account's own recorded
    balance history empties it too, even on a profile with data elsewhere —
    see §Data Model for why the guard's trigger is the requested range's own
    emptiness, not a check scoped to the whole profile. When the balance-driven output is
    empty and the eligible unanchored count is greater than zero,
    `reports.net_worth` publishes exactly one synthesized row, with
    `net_worth` NULL and `unanchored_account_count` set to that count — see
    below and §Data Model for how that row is dated and how its eligibility
    is evaluated against a requested range rather than always today. When the
    balance-driven output is empty and the eligible unanchored count is zero
    too — no accounts connected, or none carrying evidence of holding value
    — no row is published either: a NULL total with a count of
    zero would misreport an empty profile as an incomplete one, which is a
    worse answer than an honest absence of rows. **The eligible unanchored
    count is what gates the synthesized row, not spine emptiness by itself.**

    **The one row with no `balance_date` of its own has to borrow the
    requested range instead — surviving the date filter is not enough on its
    own.** This is the synthesized row from a profile with no balance-spine
    row at all (above), and only that row: every ordinary row already
    carries its own `balance_date` to evaluate `archived_at` against, per
    the eligibility rule just stated. This requirement already counts an
    unanchored account across the whole requested range (below), so a
    caller asking about last quarter, on a profile that was unanchored and
    still live then but has since been archived, has to see the same NULL
    total and count a caller asking about that quarter *at the time* would
    have seen. Evaluating the synthesized row's eligibility at `CURRENT_DATE`
    regardless of what range was asked for would silently drop an account
    that was live and unanchored throughout the requested range but is not
    live today — the report would read as complete for a range it actually
    understated, which is the exact failure class this requirement exists to
    prevent. See §Data Model for how that one row's eligibility is evaluated
    against the requested range instead of today, and how it is dated
    without changing the spine.

    **Scoped to the account, not to the date, and deliberately so.**
    `core.dim_holdings` is a current snapshot with no date dimension, and the
    dated position spine that would answer "did this account hold priced value
    on that past date" is `core.fct_holdings_daily` (Pillar C.3), which
    §Out of Scope excludes. The question this guard can answer from the
    relations it reaches is "can this account be anchored at all", not "was it
    anchored on this date". An account acquired partway through a requested
    range is therefore counted unanchored across the whole range. That
    over-states incompleteness rather than under-stating it, which is the
    direction Requirement 7 already chose; the date-precise form waits for C.3,
    and is the same guard with a finer input rather than a second pattern
    beside it. That is a different axis from *which* range the guard is
    evaluated against for the one row that has no date of its own to
    correlate with: the holdings signal itself carries no date, but the
    synthesized row's archival cutoff still has to respect the requested
    `from_date`/`to_date` rather than always today — see §Data Model. Every
    ordinary row needs no such stand-in; it already has its own date.

    **Beta-gating, and its own work item — `M2B.3`.** It does not ride along
    with the rungs or the rate spine, because the number a first-time user sees
    has to be either right or visibly incomplete before anything is built on
    top of it, and because M2B.2 closing must not retire a release gate that
    outlives it. Full Pillar D net-worth integration stays post-release; this
    requirement only keeps its absence honest.

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

`M2B.3` adds a new `core.*` relation, and extends `reports.net_worth`'s own
`kind VIEW` query with it directly. The count and its NULL gate are a
property of the row, computed on the SQL surface every rung already is, not
hidden behind a runner a direct SQL reader never sees — and that includes
the wholly-unanchored profile: the view synthesizes its own `CURRENT_DATE`
row when the balance-driven output is empty but an eligible candidate
exists, so "what am I worth right now" needs no runner even in that case.
Only a specific *historical range* with no balance-spine rows in it —
something a view with no per-query parameters genuinely cannot date — still
needs the runner, and even that reads no `prep.*` and touches no spine.

**Why the candidate set cannot be read from `prep.*` directly, in the view or
anywhere else in `reports.*`.** `report_class_derivation.py` parses every
`src/moneybin/sqlmesh/models/reports/*.sql` file and calls `assert_acyclic`
(`src/moneybin/privacy/report_materialization.py:106-128`) against it: a
`reports.*` model may read only `core.*`/`app.*`, because `CLASSIFICATION` is
the independently-authored ground truth CI derives against, and a schema
outside that set — `prep.*` included — has none to derive from, so a read of
it fails the build rather than resolving to a floor. `prep.stg_plaid__investment_holdings`
is such a schema, so `reports.net_worth`'s model file cannot read it
directly, scoped or not, whether that read sits in the view's own query or
in a per-row correlated subquery. The fix is not to work around that check;
it is to put the read behind one new relation the check already permits, and
have the view read *that*.

**`core.dim_holdings_broker_reported`** — a new `kind VIEW` in `core`,
sitting beside `dim_holdings.sql` in `src/moneybin/sqlmesh/models/core/`,
grain `account_id`. Its account universe is direction-split, per the
invariant above: every `account_id` `prep.stg_plaid__accounts` reports
**whose own `account_type` column resolves to `investment`** (the
seed-normalized value `seeds.account_type_map` writes there; Plaid's
`INVESTMENT` and `BROKERAGE` aliases both resolve to it —
`src/moneybin/sqlmesh/models/seeds/account_type_map.csv`) is eligible for
both a `TRUE` and a `FALSE`/*absent* reading, below — **plus** every
`account_id`, of any `account_type` including NULL, that carries a
nonzero holdings row of its own in a mapped origin's newest snapshot,
which is eligible for `TRUE` only. The second clause is what keeps this
relation from reintroducing the defect the first clause was itself
introduced to fix:
`prep.stg_plaid__accounts:16-18` documents that `account_type` can stay
NULL for an unmapped Plaid subtype, and an account in that state can
still report a real, nonzero position — a positive observation the
invariant says must count regardless of what the type column resolved
to.

**One canonical `account_id` can carry more than one Plaid `source_origin`
— a relink is the common cause — so the universe is `account_id`s, never
`(account_id, source_origin)` pairs, and the view reduces to that grain
before it is exposed rather than emitting one row per pair.** Emitting per-pair would let one account carry
several rows at a declared `account_id` grain, and would let an old
item's definitive-zero receipt stand unreduced beside a newer item's
position or missing receipt. For a given `account_id`, its **mapped
origins** are every distinct `source_origin` `prep.stg_plaid__accounts`
reports for it. Each mapped origin is evaluated exactly as a
single-origin account would be — scoped to that origin's own row in
`dim_holdings.sql`'s `newest_snapshot` CTE, per the `has_position`/`as_of`
derivation below — and the per-origin results reduce to one row per
`account_id`:

**The reduction is evaluated in this explicit order — TRUE, then Absent,
then FALSE, then NULL — each rule's condition read only once every
earlier rule has failed to match, so a relinked account whose mapped
origins could satisfy two rules at once never leaves it to a reader to
guess which one wins** — the invariant applied: a confirmed position is a
positive observation and always outranks a sibling's silence, so it is
checked, and wins, first:

1. **`has_position = TRUE`** if **any** mapped origin resolves `TRUE`
   (nonzero position or value evidence, below) — **type-agnostic**: an
   origin's own `account_type` never gates this rule, because a positive
   reading needs no license from the invariant, only an observation. One item's confirmed
   position outranks a sibling item's silence, zero, *or absence*; a
   relink does not erase a position the surviving item still reports,
   whichever of those three states the sibling is in.
2. **Absent — not published at all** — else, if **any** mapped origin
   *eligible for a negative reading* (its own `account_type` resolves to
   `investment`) has no successful newest pull, i.e. is missing from
   `newest_snapshot` entirely. Reached only once rule 1 has failed to
   match. This is the same absence described below for a single-origin
   account (state 3), extended across an account's mapped origins rather
   than invented as a second concept: an inconclusive-by-absence origin
   must never license an override on another origin's say-so, so the
   whole account falls out of this relation and is read by the outer join
   downstream instead of by a value. A mapped origin whose `account_type`
   is not `investment`, and which did not resolve `TRUE` under rule 1,
   contributes to neither this rule nor rule 3 — the invariant permits no
   negative inference from a source not competent to make one (§"The
   account-type filter," below), so that origin is simply not evaluated
   past rule 1.
3. **`has_position = FALSE`** — else, when **every** mapped origin
   eligible for a negative reading is present in `newest_snapshot` and
   resolves `FALSE` — an account is a definitive zero only when every
   item competent to speak for it says so, and only once rules 1 and 2
   have both failed to match.
4. **`has_position = NULL`** — otherwise: at least one origin eligible
   for a negative reading is present in `newest_snapshot`, none resolved
   `TRUE`, and at least one resolves `NULL`.

**`as_of` is the MIN, not the MAX, of the contributing origins' own
`as_of`.** Each receipt is definitive only for its own item as of its own
observation; a newer receipt for item B says nothing about item A still
being empty as of that later date, so taking the MAX would backdate an
older item's zero to an observation it never made and misdate this
column for any caller reading it directly.

**The universe's negative branch is scoped to `account_type = 'investment'`,
not to every account sharing the item's `source_origin`.**
`_load_holdings_snapshots` writes one receipt per Plaid *item*, not per
account — its own docstring calls it "Record that each item's holdings were
fetched" (`src/moneybin/extractors/plaid/extractor.py:805-840`) — and one
item can carry a brokerage account beside a depository or credit account
under a single connection, because `prep.stg_plaid__accounts` carries every
normalized `account_type` the source reports, not only `investment`.
Enumerating from `source_origin` alone therefore let a checking account
sharing that item inherit the brokerage's receipt, resolving to
`has_position = FALSE` with no holdings rows of its own — a wrong reading
on a published `core.*` column, misrepresenting an account the holdings
product never covers as a confirmed zero investment position. Restricting
the universe to `account_type = 'investment'` is
what keeps a non-investment account off the *negative* branch of this
relation: it can never resolve `FALSE` or *absent* through a sibling's
receipt, because those readings require the invariant's license and a
non-investment account's own receipt status says nothing about whether
the holdings product describes it at all.

**The universe's positive branch must stay type-agnostic even though the
negative branch is `account_type = 'investment'`-gated — the
negative-inference half above is exactly what makes that split safe.** A
type-symmetric filter — excluding a non-investment account from this
relation *entirely*, in both directions at once — would be a stronger
reading of the invariant than the invariant requires. A checking account
with no holdings evidence of its own still correctly stays off this
relation in the ordinary sense — it never resolves to any `has_position`
value at all, and stays subject to the ordinary candidate-evidence rule
(Requirement 14) like any other account the holdings product does not
describe; nothing above changes that outcome, and §"The unanchored-account
guard judges a depository account on its own evidence" (Tier 3) still
pins it. But `prep.stg_plaid__accounts:16-18` documents that
`account_type` can stay NULL for an unmapped Plaid subtype, and
Requirement 14's own text names "a Plaid account whose `current_balance`
or `account_type` never resolved" as a case the guard exists to close
(§Requirements, above). An account in exactly that state — unresolved type, a genuine
nonzero holdings snapshot of its own, no `core.dim_holdings` lot because
the security is unbound, and no transaction posted yet — would be
invisible to every source under a type-symmetric filter: excluded here,
unreachable via `core.dim_holdings`, and absent from both ledgers,
contradicting Requirement 14. The universe's positive branch is therefore
type-agnostic (rule 1, above), so a
real position surfaces regardless of what the type column resolved to,
while the negative branch stays `account_type = 'investment'`-gated
— an empty receipt from a source
the holdings product may not even describe is not a positive observation
of emptiness, so it earns no license under the invariant to declare
`FALSE` or *absent*.

For every mapped origin of a published account, the view LEFT JOINs
`prep.stg_plaid__investment_holdings`, scoped to that origin's newest
snapshot (`source_origin` and `source_file` both), and derives one
nullable per-origin `has_position` value over the joined rows' own
`quantity` and `institution_value` (never `cost_basis` — the raw table's
own column comment marks it "reconciliation reference ONLY — never
overwrites ledger-derived basis",
`src/moneybin/extractors/plaid/schema/raw_plaid_investment_holdings.sql:12`),
plus one non-nullable per-origin `as_of` — the receipt's own
`extracted_at::DATE`, carried straight from `newest_snapshot` (the same
CTE `dim_holdings.sql:75-105` already computes for this identical
receipt-scoped join, `extracted_at` itself sourced there at `:95`). The
reduction above (grain `account_id`, above) folds these per-origin values
into the view's own published `has_position`/`as_of` columns.
`has_position` says what the receipt observed; `as_of` says when.

- **`TRUE` — nonzero position or value evidence.** At least one row for the
  account in that origin's newest snapshot satisfies `quantity <> 0 OR
  institution_value <> 0`. Row PRESENCE in the newest snapshot is not by
  itself evidence of a position: the raw/staging layer legitimately carries a
  snapshot row reporting `quantity = 0, cost_basis = 0` — exercised today by
  `tests/moneybin/test_stg_plaid_investments.py:1903-1904` — so the predicate
  reads the row's own figures, not merely that a row exists.
- **`FALSE` — a definitive zero.** That origin did successfully pull, and
  the account holds nothing per that pull — either because the account
  has **no rows at all** in that origin's newest snapshot (the no-row form:
  a receipt exists for the item, but this account's own holdings are absent
  from it), or because every row it does have is *decisive* on `quantity` or
  `institution_value` (not NULL on both) and none is nonzero.
- **`NULL` — inconclusive.** A receipt exists and the account has rows in
  it, but every one is NULL on both `quantity` and `institution_value`: no
  decisive evidence either way.

**NULL handling is what SQL's three-valued logic already does here; this
states it rather than leaving it implicit.** A NULL `quantity` or a NULL
`institution_value` makes its own `<> 0` comparison UNKNOWN — never FALSE,
never TRUE. A row NULL on both columns therefore resolves that row's `OR` to
UNKNOWN; if no other row for the account is nonzero either, that row carries
no evidence toward `TRUE`. It also does not by itself force `FALSE` — a row
that is NULL on both is not *decisive* zero evidence, so an account whose
only rows are NULL-on-both resolves to `has_position = NULL`, not `FALSE`. A
row nonzero on either column, with the other NULL, still satisfies the `OR`
and counts toward `TRUE` — one broker-confirmed figure is evidence regardless
of what the other column says.

**Publishing only `TRUE` accounts collapses a real three-state signal into
a lossy boolean.** Restricting
the relation to accounts with nonzero evidence, enumerated straight from the
holdings rows rather than from the account universe below, would be enough to
correctly hold a liquidated account out of the *positive*-evidence reading
Requirement 14 needs — both the item-level and account-level no-row shapes
below leave such an account with no holdings row to enumerate it by, so it
would be silently and correctly never `TRUE`. But that same shortcut would
erase the
`FALSE`/*absent* distinction this published column exists to carry: an
account absent from a `TRUE`-only relation is indistinguishable from an
account whose item never reported at all, so a caller reading this column
directly could no longer tell a confirmed empty position from one never
observed. Publish the full three-state column, enumerated from
the account universe, instead of narrowing back to a boolean presence check
— narrowing to nonzero evidence alone would remove exactly the
absence/zero distinction Requirement 14 needs.

**This closes three liquidation shapes for `has_position`; each closes a
distinct failure, and none of the three subsumes another.**

- **No-row liquidation, item-level.** Retained rows survive across
  snapshots, so reading the raw table without scoping to the receipt would
  let a liquidated item's newest pull — which writes ZERO holdings rows, per
  `dim_holdings.sql`'s own `newest_snapshot` comment — leave its last
  non-empty snapshot's rows in place, permanently reading a correctly-empty
  account as `TRUE`. Scoping to the newest snapshot receipt is what keeps
  that pull from producing a false `TRUE`.
- **No-row liquidation, account-level — the empty-receipt shape.** A
  multi-account item's newest pull can legitimately write rows for some of
  its accounts and none for others: one account fully liquidated while a
  sibling account under the same item still holds positions. That account's
  own rows are absent from the newest snapshot exactly as they would be if
  the *whole item* had reported empty. Enumerating the account universe from
  `prep.stg_plaid__accounts` — not from the holdings rows themselves — and
  LEFT JOINing the holdings is what lets that absence resolve to `FALSE`
  rather than to the account being unpublished (and therefore
  indistinguishable from "no receipt") the way an INNER-JOIN-only shape
  would read it.
- **Zero-row liquidation — the row exists and reports zero.** The newest
  pull DOES write a holdings row for the account, but the row itself reports
  a zero quantity and a zero-or-absent value. Receipt-scoping alone does not
  catch this one: the row sits in the correctly newest snapshot, so a
  row-presence predicate would still read it as `TRUE`. The nonzero-evidence
  predicate — reading the row's own figures — is what resolves this account
  to `FALSE` instead.

This is the established pattern for the receipt-scoped read, not a new one:
`dim_holdings.sql` already reads `prep.stg_plaid__investment_holdings_snapshots`
this way over the sibling holdings table; the new relation adds the
account-universe LEFT JOIN the second shape requires and the nonzero-evidence
filter that `dim_holdings.sql`'s own `positions` CTE never needs, because it
sums open LOTS, which a closed position simply has none of.

**It carries its own `CLASSIFICATION` entry in
`src/moneybin/privacy/taxonomy.py`, one line per published column — all
three of them, not `account_id` alone:**
`tests/moneybin/test_privacy/test_classification_registry_coverage.py`
reads the live catalog and requires a class for every `core.*` column, so
an entry naming only one of the view's three columns fails that gate on
implementation.

- `account_id` → `DataClass.RECORD_ID`, matching `("core",
  "dim_holdings")`'s own entry (`taxonomy.py:816`).
- `has_position` → `DataClass.TXN_TYPE` — every boolean flag in the
  taxonomy takes this class; the closest analogue is `fct_investment_lots.is_open`
  (`taxonomy.py:960`).
- `as_of` → `DataClass.TIMESTAMP_OBSERVABILITY`, matching
  `dim_holdings.provider_reported_as_of` in the same entry
  (`taxonomy.py:849`) and `dim_holdings.price_date`'s identical reasoning
  just above it (`taxonomy.py:830-832`).

So the read has ground truth to derive against instead of needing an
exception.

**The invariant every rule in this section is an application of, stated
once so each rule below can cite it instead of re-deriving it:**

> **Absence of evidence is never evidence of absence. Only a positive
> observation of emptiness — a receipt that actually reports, and
> reports nothing, from a source competent to report it for this
> account — may remove an account from candidacy. Anything
> inconclusive — no receipt at all, a receipt from a source that does
> not describe this account, a sum whose zero cannot be told apart
> from a canceled synthetic entry, two rules that could both fire with
> no stated order between them — leaves the account a candidate.**

Three consequences follow, each cited by name where it applies elsewhere in
this section, rather than re-argued from scratch: `core.dim_holdings_broker_reported`'s
account-type filter, just above, licenses a negative inference
(`FALSE`/*absent*) only where the source is competent to make one, while a
positive reading (`TRUE`) needs no such license and stays type-agnostic
(§"The account-type filter," above); a relinked account's per-origin
reduction, also just above, resolves in an explicit order — a confirmed
position first, before any rule that turns on a sibling's silence — rather
than as independently stated bullets a reader must sequence themselves
(§above); and a zero net cash effect on `core.fct_investment_transactions`
is never decisive at all, regardless of what produced it, so that ledger
is read existentially instead (§"Cash held is never decisive at zero,"
below).

**Evidence of holding value has four sources, and `reports.net_worth`'s
`kind VIEW` reads all four directly — no runner involved.**
`core.dim_holdings` sums open lots, so it emits no row at all for a
broker-reported position with no matching lot (an unbound security, a
declined bootstrap, or a holdings snapshot that landed before its
transactions); `core.dim_holdings_broker_reported` (a row with `has_position
= TRUE`) is exactly the source `dim_holdings.sql`'s own comment names for
that direction; and `core.fct_transactions` and
`core.fct_investment_transactions` — both already `core.*`, so neither needs
a relation of its own — together supply the non-investment and
investment-ledger cases Requirement 14's qualifier adds: any account with at
least one recorded transaction on either ledger, regardless of source or
amount. The two ledgers are genuinely separate models —
`core.fct_investment_transactions` is never unioned into
`core.fct_transactions` — so an account whose only activity is investment
events (a dividend, a fee, a fully-disposed position) needs its own arm; the
cash-ledger table cannot see it. All four are `core.*`, so the view joining
them keeps `assert_acyclic` satisfied on its own terms, not through a runner
workaround of the check.

**Cash held is never decisive at zero — no zero sum
`core.fct_investment_transactions` can produce is a positive observation
of zero cash, so the cash arm is never cancelable by anything
downstream.** A zero `SUM(amount)` only says the *recorded* rows net to
zero — it says nothing about cash the account held before its first
recorded row, so it can never stand in for a real balance observation. Two
shapes reach the identical zero: a synthetic opening-lot bootstrap row
(`subtype = 'opening_bootstrap'`, `prep.stg_plaid__opening_lots.sql`)
synthesizes its `amount` from cost basis rather than observing one —
"these rows carry no Plaid amount" per that model's own comment
(`stg_plaid__opening_lots.sql:27-30`) — so a real in-window sale of a
bootstrapped position, credited at exactly the bootstrap's synthesized
cost, nets `SUM(amount)` to zero while the account's real sale proceeds
go unobserved by any balance; and, with no bootstrap row involved at all,
an ordinary buy funded from cash this ledger never recorded a deposit
for, later sold at exactly that cost, reaches the same `SUM(amount) = 0`
while the same unobserved proceeds sit unrecorded. Both are cumulative
ledger *movement* canceling to zero, never a positive observation that
cash on hand is zero, and the invariant draws no line between them:
"a sum whose zero cannot be told apart from a canceled synthetic entry ...
leaves the account a candidate" says nothing about *why* the sum reached
zero. So the cash arm reads existentially instead — any row on
`core.fct_investment_transactions` for the account, cash-only or
security-linked, is candidate evidence, full stop:

```
candidate_via_investment_ledger := EXISTS (a row WHERE account_id = ...)
```

This is a single, deliberate arm over `core.fct_investment_transactions`,
not a narrowed-down leftover of one. It is read exactly the way
`core.fct_transactions` already is (§"Evidence of holding value has four
sources," above): any recorded row, ever, regardless of amount, source,
or whether it carries a `quantity`. A row's standing as *security*-position
evidence specifically is not a second question this predicate answers —
a caller who wants to know what the broker's newest snapshot reported,
and when, already has that directly on `core.dim_holdings_broker_reported`'s
own `has_position`/`as_of` columns (above), which this predicate does
not need to re-derive or gate candidacy on. It never cancels any
`core.fct_transactions` row either: that ledger is read the same way,
existentially, closed only through Requirement 9's `archived_at`
eligibility once the account is actually closed, never through a broker
snapshot a cash ledger has no equivalent of.

**This closes the cash arm permanently, the same way `core.fct_transactions`
already closes: an investment account with any recorded ledger activity —
however old, and regardless of how its position was eventually settled —
stays a net-worth candidate until it is archived or a real balance
observation clears it.** An unclearable `fail` on a correctly-liquidated,
genuinely-zero-cash account is the accepted cost of never silently
dropping one that still holds unobserved cash.

This corrects a further defect beyond the three below: reading the cash
arm's decisive-zero test as bootstrap-specific was itself only a partial
fix, narrower than the invariant it was meant to apply. A narrower version
of this predicate excludes a bootstrap-tainted zero from counting as
decisive but still treats an ordinary, fully-recorded zero as decisive
proof of no cash — exactly the gap the concrete counter-example above
closes.

Three narrower predicates for this test are rejected below, each a
restriction patched to fix one case rather than derived from the invariant
above. **No restriction at all** — any zero sum, on either ledger, read as
decisive — is too broad, since it reads an unbound buy or sell's own
NULL-shaped cash evidence as decisive. **Keying the restriction on
`security_id`** restricts that reading correctly in principle but keys it
on the wrong column: `security_id` carries two NULL
cases, not one — "NULL for cash-only events (deposit, withdrawal, account
fee, cash interest) and for a synced security with no accepted binding"
(`src/moneybin/sqlmesh/models/core/fct_investment_transactions.sql:101`) —
so a predicate keyed on its nullability misreads an unbound buy or sell as
cash evidence. `quantity` has no such overlap:
`prep.stg_plaid__investment_transactions` sets `ledger_quantity` NULL only
for the closed, non-security set of mapped types (dividend, interest,
capital_gain_distribution, deposit, withdrawal, fee, return_of_capital,
other; `stg_plaid__investment_transactions.sql:250-263`) and otherwise
carries the row's raw signed share count regardless of whether
`security_id` resolves through an accepted binding — an unbound buy or sell
keeps its `quantity`; only its `security_id` goes NULL. The canonical
column's own comment states the same contract independent of staging
branch: "Signed units: + acquire, − dispose, NULL cash-only"
(`fct_investment_transactions.sql:108`). **Keying the restriction on row
*selection*** — preserve `quantity IS NULL` rows, drop `quantity IS NOT
NULL` rows from consideration entirely — cancels a sell row's own
cash credit along with its position evidence whenever the sale's proceeds
are the account's only cash evidence: `core.fct_investment_transactions.amount`
is the signed cash effect on *every* row regardless of `quantity`
(`fct_investment_transactions.sql:110`, "Signed cash effect: − out (buy),
+ in (sell/dividend)"), so a sell that liquidates the account's last
security still credits cash on that very row. An investment account that
sells its last security but retains the sale proceeds, or later receives
a cash-only dividend or deposit, still carries a row on this ledger, so
it still holds cash: `docs/specs/investments-overview.md:283`
independently states that an investment account may hold uninvested cash
with no security row. Reading the cash arm existentially, rather than as
a filtered subset of rows, is what avoids reintroducing any of these three
failure shapes on the next liquidation case.

**What the cash arm catches, and what it still misses.** Both
`core.fct_transactions` and `core.fct_investment_transactions` carry their
own transaction date, but the guard reads each existentially — *has this
account ever posted, on either ledger* — the same account-not-date
scoping Requirement 14 already applies to the holdings signal, not a
second pattern beside it. This is what makes a tabular import with no
balance column, a Plaid account whose `current_balance` or `account_type`
never resolved, a manual account with postings and no assertion, and an
investment account with a dividend, a fee, or retained proceeds after its
last security sale (whether the credit lands on a separate cash-only row
or on the sell row itself) all surface as unanchored rather than silently
absent — every one of them has a row on one ledger or the other even
though `core.fct_balances` has none. A fully-disposed position whose
newest broker snapshot reports a definitive zero, with a net cash effect
of exactly zero, surfaces too, through the same cash arm, for the same
reason every other net-zero investment-ledger account does. This is the
accepted tradeoff named above: an `archived_at` transition, or a genuine
balance observation, is what clears such an account, never a broker's
separately reported zero position.

It does not reach an account with no transaction on either ledger, no
holding, and no balance of any kind: nothing in `core.*` distinguishes
"genuinely never funded" from "funded but nothing observed yet" for an
account with zero rows anywhere, and guessing would reintroduce the
false-positive risk Requirement 14's qualifier exists to avoid. That account
stays silently absent, exactly as it does today — the residual gap
Requirement 14 states rather than absorbs, narrower now than it was with
three sources but not closed.

**The count and the NULL gate are per row, computed by the view, at that
row's own `balance_date` — the same shape Requirement 9's eligibility
predicate already uses.** For every balance-driven row `reports.net_worth`
emits, `unanchored_account_count` is the number of candidate accounts (the
four sources above) that have no row in `core.fct_balances` at all and pass
`include_in_net_worth AND (NOT archived OR (archived_at IS NOT NULL AND
balance_date <= archived_at))` — correlated to that row's own `balance_date`, exactly as
Requirement 9 already evaluates eligibility for the balance-backed measures
on the same row. `total_assets`, `total_liabilities`, and `net_worth` NULL
when that count is greater than zero — the same NULL gate
`unpriced_currency_count` already drives. Because the count travels with the
row instead of living in a runner, a caller reading `reports.net_worth`
through `sql_query`, `moneybin sql query`, or DuckDB directly — no runner
involved at all — sees the same NULLed total and the same count any other
caller does: `SELECT * FROM reports.net_worth ORDER BY balance_date DESC
LIMIT 12` (§CLI Interface) is correct on this dimension with no
special-casing, because the guard is a property of the row, not of how it
was fetched. This is what resolves the SQL-surface gap directly: a mixed
anchored/unanchored profile no longer depends on the runner to expose either
the incompleteness or the NULL.

**`core.fct_balances_daily` is unchanged, deliberately.** Its early return on
an empty profile (`if obs.empty: yield from (); return`) stays exactly as
shipped — this requirement does not make the spine read any of the three
sources above or synthesize a row itself. That model is general-purpose and
holdings-agnostic, read by all three rungs and by consumers outside this spec
that have no concept of an unanchored account; coupling it to this guard
would leak a scaffold row to every one of them, not only to
`reports.net_worth`.

**The view carries a second `UNION ALL` arm for the wholly-unanchored
profile — no runner needed for that case either.** A view with no
per-query parameters cannot represent "the range the caller asked for," but
it can represent "today," the same way `CURRENT_DATE` already does inside
its per-row predicates. `reports.net_worth`'s query therefore gains one
more unconditional arm, evaluated fresh on every read like the rest of the
view: when the balance-driven output (the same three-rung read every other
arm shares) is empty and the eligible-candidate count — the four sources,
joined to `core.dim_accounts`, evaluated at `CURRENT_DATE` exactly as the
per-row predicate evaluates each real row at its own `balance_date` — is
greater than zero, this arm emits exactly one row: `balance_date =
CURRENT_DATE`, every measure NULL, `account_count = 0`,
`unanchored_account_count` set to that count. A bare `SELECT * FROM
reports.net_worth` on a profile whose only accounts are eligible and
unanchored now returns that one row with no runner, no `sql_query`
wrapper, and no bound parameters — which is the "now" answer a direct SQL
reader actually wants, and exactly what AGENTS.md's `sql_query`/`moneybin
sql query` inspection path reads.

**That still leaves one case a view cannot express: a specific historical
range with no balance-spine rows in it.** The view's own arm is always
dated `CURRENT_DATE`; a caller who explicitly asks for a range that
excludes today gets nothing from the bare view for a profile whose only
data is that one arm's row — the same gap as before, just narrower now
that "today" is covered. This is not hypothetical:
`fct_balances_daily.py`'s per-account spine runs from that account's own
first observation through the global last one
(`fct_balances_daily.py:186-197`) and no further, so a historical query for
a range that predates every account's first balance observation returns
zero real rows and excludes the view's `CURRENT_DATE` arm too, even when an
eligible unanchored account was live throughout that range. This is the one
remaining place `reports.net_worth`'s runner does work the view genuinely
cannot: it holds `from_date`/`to_date` as bound parameters, the way
`src/moneybin/reports/definitions/cash_flow.py:172-181` already holds
`from_month`/`to_month` for `core:cash_flow` — there a `WHERE 1=1`
conditionally extended with `>=`/`<=` comparisons on a derived string
column, here a `BETWEEN` on a date column; the predicate shapes differ, the
architectural move (a Python-built predicate over bound parameters the view
itself cannot see) is the same.

The runner triggers this fallback only when the caller supplied an explicit
`from_date` or `to_date` **and** its own filtered result is empty — not a
bare emptiness check on its own, and not a check for "which arm produced
which row," since the runner reads the view's output the same way any
caller does. An unranged call never reaches this fallback at all: it
answers straight from the ordinary `MAX(balance_date)` default below, and
that default already subsumes the wholly-empty-profile case correctly —
when a genuinely eligible (un-archived) candidate exists, the view's own
`CURRENT_DATE` arm supplies that one row and the default is non-empty; when
the profile's only unanchored candidate has since been archived, that arm
correctly excludes it, the default finds nothing, and the runner returns
that emptiness rather than falling through to synthesis. A closed account's
old incompleteness is not the answer to an unranged "what is my net worth
now," and this fallback exists to date a row inside a range the caller
actually asked for, never to resurrect a candidate the caller never scoped
a query to. Requiring an explicit range is also what keeps the
eligible-candidate predicate's `effective_from IS NULL` arm, below, honest:
reached only this way, it always means "an explicit range with no lower
bound," never "no range was given at all" — the reading that let a stale
`archived_at`-dated row leak into an unranged read, fixed the same way for
`reports.net_worth_accounts`'s own anti-join below (§Tier 2, "A ninth case
pins the anti-join's unranged eligibility date"). The fallback fires only
when it is still needed: an explicit range that excludes both real data and
the view's own `CURRENT_DATE` row.

**Before computing `effective_from`/`effective_to` or issuing any query,
the runner validates the range — an inverted one is rejected, not silently
reinterpreted.** `service_reports.py`'s
`_validate_networth_history_parameters` — the validation this spec retires
along with the rest of `service_reports.py` — already rejects `from_date >
to_date` with `UserError(code=error_codes.REPORT_PARAMETER_INVALID_RANGE)`
before running any query. Without that check preserved, an inverted range
would flow straight into the `effective_from`/`effective_to` computation
below, which does not itself validate order: `effective_to` would resolve
to `to_date` and `effective_from` to `from_date`, with `effective_from >
effective_to`. The ordinary `BETWEEN` filter over an inverted pair matches
no real row (DuckDB's `BETWEEN` is `low AND high`, never reordered), so it
correctly returns nothing — but the empty-result fallback would then read
that as "no data in this range," and an eligible unanchored candidate would
make it synthesize a row dated `effective_to`, a date that lies *before*
`effective_from`. That row is well-formed and NULL exactly like any other
synthesized row — nothing in its shape marks it as answering a nonsensical
request — which is a silently plausible wrong answer standing in for the
input the retired service rejected outright, the worse failure of the two.
`reports.net_worth`'s runner therefore runs the identical check —
parse `from_date`/`to_date`, and if both are given and `from_date >
to_date`, raise the same `UserError`/`REPORT_PARAMETER_INVALID_RANGE` —
before computing `effective_from`/`effective_to` or issuing any query.

**A one-sided range stays open on the side that wasn't given — it does not
collapse to a single day.** `cash_flow.py`'s own precedent, already cited
above for the parameter-binding shape, is explicit about this:
`if from_month: ... AND year_month >= ?` and `if to_month: ... AND
year_month <= ?` are two independent, separately-conditional clauses
(`cash_flow.py:176-181`) — supplying only `to_month` leaves the lower bound
un-appended entirely, not defaulted to `to_month` itself. `effective_from`
must follow the same shape: `from_date` when supplied, else **unbounded**
— never `effective_to`. Defaulting the missing lower bound to the upper
bound is the bug this fixes: it collapsed "net worth through March" into
"net worth on March 31st alone," and if no balance row happens to land on
that exact day, the runner would misread a normal profile's real,
differently-dated history as an empty result and could synthesize a
fabricated incompleteness row in its place — the same silent-wrong-answer
class the inverted-range fix above exists to prevent, from the opposite
direction. `effective_to` keeps its existing rule: `to_date` when supplied,
else `CURRENT_DATE`.

The runner's ordinary filter mirrors `cash_flow.py`'s conditional-append
shape rather than a fixed `BETWEEN`, because `BETWEEN` needs both sides:
`from_date` given appends `AND balance_date >= ?`; `to_date` given appends
`AND balance_date <= ?`; both given is the conjunction of the two (the
`BETWEEN` shape falls out of that, not the other way around); neither given
resolves to `WHERE balance_date = (SELECT MAX(balance_date) FROM
reports.net_worth)` — the same "latest row" default `NetworthService.current()`
already runs (`WITH latest AS (SELECT MAX(balance_date) FROM
reports.net_worth ...) ... INNER JOIN latest`,
`src/moneybin/services/networth_service.py:54-62`), because
`core.fct_balances_daily`'s spine ends at the newest observation across
*all* accounts (`fct_balances_daily.py:186`, `global_last_date =
obs["balance_date"].max()`), not at `CURRENT_DATE` — the ordinary case for a
statement-backed account is that its freshest observation is a few days
old, not today. For a wholly-unanchored profile this "no range" default
resolves to the view's own `CURRENT_DATE` arm — the only row in the view —
composing with zero extra logic: `MAX(balance_date)` finds it, the equality
filter keeps it, and the fallback below never even evaluates its condition,
because the filtered result it would check is already non-empty.

When the runner's own filtered query returns zero rows and the count of
eligible candidates — `include_in_net_worth AND (archived_at IS NULL OR
effective_from IS NULL OR archived_at >= effective_from)`, over-stating
rather than under-stating exactly as the per-row predicate would if it had
a date to correlate against — is greater than zero, the runner does not
yet know it should synthesize a row. It still has to answer one more
question first: is there a date inside the requested window the fallback
can honestly date the row at.

**The general rule governing every variant of this fallback: a synthesized
row's date must lie in the intersection of three windows, and the row is
emitted only when that intersection is non-empty.** The three windows are the
caller's own requested range (`effective_from`/`effective_to`), present-or-past
knowledge (the guard reports what is known now or was known on a past date —
never a forecast, so no later than `CURRENT_DATE`), and the eligible window of
every candidate the row is about to count — which Requirement 9 already bounds
above by that candidate's own `archived_at`. A row that counts several
candidates has to sit inside all of their windows at once, not just the first
two; that is the same per-account eligibility rule Requirement 9 already
applies to every ordinary row, extended to the one row that has no
`balance_date` of its own to apply it at.

Let `archived_at_floor` be the smallest non-NULL `archived_at` among the
accounts satisfying the eligible-candidate predicate stated once, above, at
`:767-768` — not restated here, so the two cannot drift apart — NULL, and
therefore unbounded, when
none of them carry an `archived_at` at all. Then:

```
synthesis_date = LEAST(effective_to, CURRENT_DATE, archived_at_floor)
```

taking the least of only the bounds that are actually present — a NULL
`archived_at_floor` drops out of the `LEAST`, exactly as a NULL `effective_from`
already drops out of the lower-edge test below. The row is emitted, dated
`balance_date = synthesis_date`, every measure NULL, `account_count = 0`,
`unanchored_account_count` set to the eligible-candidate count, **only when
`synthesis_date` also clears the window's lower edge**: `effective_from IS
NULL OR effective_from <= synthesis_date`. When it does not — the entire
requested window lies strictly after `CURRENT_DATE`, e.g. an explicit
`from_date` next month with no `to_date`, or a `from_date`/`to_date` pair that
are both future dates, or (new here) every eligible candidate's own window
closes before the requested range even opens — no date inside the window
qualifies, and the runner returns no row at all: the same honest emptiness a
real historical range with no eligible candidate already returns, never a row
dated outside the bounds the caller actually asked for.

Because `archived_at_floor` is computed from exactly the accounts the count
sums, lowering the synthesis date to it never drops an account out of the
count it dates: every account passing the eligible-candidate predicate has
`archived_at IS NULL OR archived_at >= archived_at_floor >= synthesis_date`,
which is Requirement 9's own per-row eligibility test (`balance_date <=
archived_at`) evaluated at this row's own `balance_date`. The count and the
date agree by construction, the same way every ordinary row's count and date
already agree — not by a second rule bolted on beside the first.

This single three-window test — `synthesis_date = LEAST(effective_to,
CURRENT_DATE, archived_at_floor)`, emit only if `effective_from IS NULL OR
effective_from <= synthesis_date` — is what the fallback checks in every case,
not one rule per shape of range:

- **An open lower edge, bounded or unbounded upper edge, both not in the
  future, no eligible candidate carrying an `archived_at`** (the ordinary
  case this fallback exists for, including a historical range):
  `archived_at_floor` is NULL, so `synthesis_date = effective_to`;
  `effective_from` unset or no later than it, so the row is emitted, dated at
  `effective_to`, exactly as before this rule was stated.
- **A future-only lower bound with no upper bound:** `to_date`
  unset resolves `effective_to` to `CURRENT_DATE`, so
  `synthesis_date = CURRENT_DATE`; the supplied `from_date` is later than
  `CURRENT_DATE`, so the lower-edge test fails and no row is emitted — no
  out-of-window row synthesizes.
- **A single-day window that is itself in the future**
  (`from_date == to_date`, both after today — a harder variant than the
  reported one, since even the emitted date can no longer coincide with
  either bound): `effective_to` is that future day, so
  `synthesis_date = CURRENT_DATE` (today, earlier than the window); the
  window's lower edge is that same future day, later than
  `synthesis_date`, so the test fails identically and no row is emitted —
  the rule generalizes past the single reported shape without adding a
  second case for it.
- **An inverted range** (`from_date > to_date`, both given) never reaches
  this fallback at all — the validation above rejects it before
  `effective_from`/`effective_to` are computed.
- **An eligible candidate archived partway through the window**: a no-spine range that starts before the candidate's
  `archived_at` and ends after it. The candidate still satisfies the
  eligible-candidate predicate (`archived_at >= effective_from`), so
  `archived_at_floor` equals that `archived_at` — earlier than `effective_to`
  whenever the range extends past it — and `synthesis_date` drops to the
  floor instead of staying at `effective_to`. The row is emitted dated at the
  candidate's own `archived_at`, the last date inside its eligible window,
  never at a later date Requirement 9 already says it stopped counting on.

Dating the row at `synthesis_date` rather than unconditionally at
`effective_to` changes nothing for a window that includes `CURRENT_DATE` or
lies wholly in the past and carries no eligible candidate with an
`archived_at` earlier than that bound — `synthesis_date` and `effective_to`
are the same date there — and the `effective_from IS NULL` arm inside the
eligible-candidate predicate above is untouched: it still governs which
*accounts* count as eligible against an unbounded lower edge, a separate
question from whether the window contains a legitimate synthesis date at
all. The behavioral change is that a window lying wholly in the future no
longer synthesizes a row dated outside it, and a synthesized row can no
longer be dated past the point Requirement 9 already excludes one of its own
counted candidates. An out-of-range query against a profile with *no*
eligible candidate still correctly returns zero rows, exactly like any other
`@report` — the count is zero, so neither the view's arm nor the runner's
fallback fires.

**The runner's conditional-append filter inherits the same lower/upper
asymmetry as `cash_flow.py`'s, and that is intentional, not a gap this
rule papers over.** Per §"A one-sided range stays open on the side that
wasn't given" above, supplying only `to_date` leaves `effective_from`
unbounded rather than snapping it to `to_date`, exactly as
`cash_flow.py:172-181`'s two independent conditionals leave
`from_month`/`to_month` unbounded on whichever side is omitted. This rule
does not change that: `effective_from` still defaults to unbounded, not to
`effective_to`, when `from_date` is omitted — the fallback's lower-edge
test reads that `NULL` as "clears any `synthesis_date`," which is what
makes the past-and-present cases above fall through unchanged.

**Which layer owns which case, stated once.** The view owns every row that
can be dated without knowing the request: every real balance-driven row
(per-row count and NULL gate, correct in any requested range), plus the one
`CURRENT_DATE` row for a profile with no balance data at all but an
eligible candidate. No runner involvement in either — an unranged read
never reaches the runner's own synthesis logic, whether or not a currently
eligible candidate exists. The runner owns exactly one thing beyond
applying the ordinary range filter: deciding what to do when an *explicit*
range's own filtered result is empty despite the view's best effort — check
for a range-eligible candidate, and if one exists, synthesize the one row
the view had no way to date for a range it never saw. The two never both
fire for the same query: the view's arm answers "now" — correctly
publishing nothing when no candidate is currently eligible, never falling
through to the runner for that — and the runner's fallback answers only "an
explicit range with nothing in it." A query is either unranged (the view's
arm alone answers it, whatever that answer is) or ranged (the runner's
ordinary filter runs, and only misses when the view's arm falls outside
that specific range).

**What a direct SQL reader still cannot get from the bare view.** Only an
explicit historical range that excludes both real data and today, on a
profile with an eligible unanchored candidate, returns nothing to a plain
`SELECT * FROM reports.net_worth ... WHERE balance_date BETWEEN ? AND ?` —
because dating that one row correctly requires knowing the range, which
only the runner sees. Every other case — the ordinary "what am I worth"
read with no range at all, including a wholly-unanchored profile, and every
mixed anchored/unanchored profile within its own data — is correct from
the bare view with no runner involved. Stated plainly rather than absorbed:
the split is clean except for this one narrow case, which is inherent to
what a `kind VIEW` with no per-query parameters can express, not a gap in
this design.

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
is_observed           BOOLEAN        -- FALSE means carried forward
observation_source    VARCHAR        -- ofx / tabular / assertion / plaid; NULL when interpolated
rate_source           VARCHAR        -- override / provider / identity; NULL when unpriced
balance_date          DATE           -- Grain
rate_published_date   DATE           -- The day the rate applied here was actually published
days_since_observed   INTEGER        -- 0 on an observed day
reconciliation_delta  DECIMAL(18,2)  -- Observed minus transaction-derived; NULL on interpolated days
account_balance       DECIMAL(18,2)  -- In currency_code
account_balance_home  DECIMAL(18,2)  -- In home_currency_code; NULL when the pair is unpriced
```

`is_observed`, `observation_source`, and `rate_source` are `DataClass.TXN_TYPE`
dimensions (`src/moneybin/privacy/taxonomy.py`), so Rule B
(`.claude/rules/column-ordering.md`) places them before the date block, not
after it.

`days_since_observed` reuses the name and meaning already established by
[`asset-tracking.md`](asset-tracking.md) and implemented by
[`investments-price-feeds.md`](investments-price-feeds.md); it is the same
concept and must not acquire a second spelling.

**An eligible unanchored account is `M2B.3`'s addition to this rung too, and
it needs no count column of its own.** `reports.net_worth`'s guard needs
`unanchored_account_count` because one row there aggregates every account on
a date; this rung's grain is already `(account_id, balance_date)`, so the
account IS the row — the guard's signal is the row's own presence with its
balance-derived columns NULL, not a number carried beside it. The view gains
a second `UNION ALL` arm reading the same four evidence sources joined to
`core.dim_accounts` (§Data Model): one row per eligible unanchored account,
dated `balance_date = COALESCE((SELECT MAX(balance_date) FROM
core.fct_balances_daily), CURRENT_DATE)` — see below for why this date, not
`CURRENT_DATE` unconditionally — with `account_balance`,
`account_balance_home`, `rate_published_date`, `rate_source`,
`observation_source`, `days_since_observed`, and `reconciliation_delta` all
NULL, and `is_observed = FALSE`. `currency_code` populates from
`core.dim_accounts.currency_code` rather than going NULL: that column is the
account's own denomination, per this rung's own column comment at `:1069`,
independent of whether a balance was ever observed, so a Plaid account with
a populated `iso_currency_code` but no balance, or a manual/tabular account
with a configured or source-derived currency, still carries its known
currency on this row. NULL stays reserved for an account whose own
denomination is genuinely unknown — a fact about the account, not about
whether it has been observed. `account_id`, `account_name`, `account_type`,
`currency_code`, and `home_currency_code` are not balance-derived, so they
still populate from `core.dim_accounts` and `app.profile_settings` on this
synthesized row, the same as on every ordinary one.

**Unconditional per candidate, unlike `reports.net_worth`'s own arm — and
dated at the spine's own maximum, not at `CURRENT_DATE`.**
`reports.net_worth`'s `CURRENT_DATE` arm fires only when the whole
balance-driven result is empty, because an ordinary row there already
carries the correlated count for a mixed profile — the arm exists only to
cover the wholly-empty case a correlated subquery has no row to attach to.
This rung has no such correlated column to fall back on: an eligible
unanchored account produces zero rows of its own whether or not every other
account in the profile is anchored and observed today. The arm therefore
emits its one row per eligible unanchored candidate regardless of what the
rest of the profile's accounts are doing, so a single unanchored account
inside an otherwise ordinary, fully-anchored profile still appears here —
not only in the wholly-unanchored case `reports.net_worth`'s own arm is
scoped to.

**The synthesized row must not be dated at `CURRENT_DATE`
unconditionally.** This
rung's own unranged default is the same one every rung shares —
`balance_date = MAX(balance_date)` (§Data Model, mirroring
`NetworthService.current()`, `src/moneybin/services/networth_service.py:54-62`)
— and `core.fct_balances_daily` carries every anchored account forward to one
shared date, not its own last observation: `global_last_date =
obs["balance_date"].max()` (`fct_balances_daily.py:186`), then `last_date =
global_last_date` becomes every account's own spine end
(`fct_balances_daily.py:193`), precisely so that, per that assignment's own
comment, "no account drops out of a cross-account aggregate just because
another account has a fresher statement" (`fct_balances_daily.py:183-185`) —
the same principle the module's docstring states at greater length
(`fct_balances_daily.py:8-15`). `global_last_date` is ordinarily older than
`CURRENT_DATE` — the newest statement is usually a few days stale, not dated
today. An unconditional `CURRENT_DATE` arm therefore makes `CURRENT_DATE` the
new `MAX(balance_date)` over this rung the moment any eligible unanchored
candidate exists, and the unranged default then filters to `CURRENT_DATE`
alone — discarding every anchored account's row on `global_last_date` and
returning only the synthesized ones. The synthesized row must instead be
dated at the same date `core.fct_balances_daily` already carries every
anchored account to — `COALESCE((SELECT MAX(balance_date) FROM
core.fct_balances_daily), CURRENT_DATE)` — falling back to `CURRENT_DATE`
only when the spine itself is empty, the wholly-unanchored profile that is
exactly the one case `reports.net_worth`'s own arm is already scoped to. The
synthesized row is simply one more account carried forward to the date every
other account already occupies, not an account entitled to define a new one.

**The day-grain rung does not share this defect — confirmed, not assumed.**
`reports.net_worth`'s own `CURRENT_DATE` arm fires only when its whole
balance-driven result is already empty (§"The view carries a second `UNION
ALL` arm for the wholly-unanchored profile," above), so it can only ever
supply the one row `MAX(balance_date)` finds when there is no anchored row to
compete with — the empty-result guard means it never coexists with a real
anchored row inside the same unranged read, so it never pushes
`MAX(balance_date)` past one. That guard is exactly what this rung's own arm
lacks, which is why this rung needed the fix above and the day-grain rung did
not.

**One synthesis rule, reached by a per-candidate anti-join — not two rules
split by ranged versus unranged.** It is tempting to claim
the view's own arm "only ever answers an unranged 'now' read," the runner's
fallback "only ever answers a bounded range the view cannot see," and
conclude the two "never fire on the same query." That claim is false,
and it contradicts this section's own "unconditional per candidate" text
above: a `kind VIEW` has no notion of "ranged" versus "unranged" at all, so
the view's arm's row — dated by the spine-maximum rule stated above, present
in the view's raw output on every read — survives the runner's ordinary date
filter exactly like any other row whenever the requested range happens to
include that date. Running the old per-candidate fallback unconditionally
on top of that would double-emit the same `(account_id, balance_date)`;
gating the fallback on the *whole* filtered result being empty — the
day-grain rung's own, correct trigger, confirmed above — would instead drop
the candidate from a range that contains other accounts' anchored rows but
excludes the spine's own maximum.

The runner instead runs a **per-candidate anti-join against its own
range-filtered output**: for each eligible unanchored candidate, check
whether that output already contains a row for the candidate's `account_id`.
Skip a candidate already present — the view's own row already answered it
inside the requested range, and Requirement 14's own definition of an
eligible unanchored candidate (above) means that row is the *only* row this
account can ever produce, since the account carries no balance observation
of any kind for a real row to compete with it. Synthesize one otherwise,
dated independently by `reports.net_worth`'s own *runner* fallback rule
(§Data Model's `synthesis_date` formula), with `archived_at_floor` computed
over that one candidate alone — so it reduces to that account's own
`archived_at` — unlike the aggregate rung, no `archived_at_floor` across
candidates is needed here, because there is no shared row whose single date
has to stay honest for more than one account at a time. The anti-join, not a
shared trigger condition, is what keeps both directions correct: no
duplicate, because a candidate the view already answered inside the range is
excluded; no omission, because every remaining candidate is still checked on
its own, regardless of what the rest of the profile's accounts are doing in
that same range.

**Membership in this anti-join is itself evaluated at the date the
candidate's row would carry, not at the aggregate rung's own
existence-only predicate — filtering on the real condition rather than a
proxy for it, the same discipline this section's "unconditional per
candidate" text above already follows.** For a ranged read, that date is
exactly the per-candidate `synthesis_date` this section already computes
above, so ranged behavior is unchanged. For an unranged read — no
`from_date` and no `to_date` supplied — every row this rung's read can
produce, synthesized or not, is dated at exactly the one date this rung's
own dating rule already names, stated once above at `:1096`:
`COALESCE((SELECT MAX(balance_date) FROM core.fct_balances_daily),
CURRENT_DATE)` — never at `effective_to`'s own general default of
`CURRENT_DATE` (§Data Model), which is usually a few days ahead of it. An
eligible-by-Requirement-14 candidate whose `archived_at` predates that date
fails Requirement 9's own eligibility test at the one date an unranged read
actually returns — the same test the view's own arm above already applies
to it at that same date, and the reason that arm already omits its row for
exactly this candidate. The candidate therefore never reaches the anti-join
step at all on an unranged read: it is not "missing" from the
range-filtered output, it is correctly absent, and nothing is synthesized
for it at any date, including its own `archived_at`. Gating this on
`effective_from` instead — the aggregate rung's own predicate, which
resolves `effective_from IS NULL` to "unbounded, admit everything" because
that is the right reading for a genuinely open-below *range* — reads an
unranged read's absent lower bound as unbounded history rather than "no
range at all," and readmits exactly the candidate this fix excludes, dated
at its own `archived_at` in place of the single date the unranged contract
(`:2236`, below) actually owes the read. That is the defect the anti-join
above corrects.

**This is also the row `moneybin system doctor`'s `net_worth_unanchored_accounts`
invariant reads** — see §"`moneybin system doctor`: unanchored accounts" —
because it is the one relation in this spec that names the affected accounts
by id rather than only a count.

#### `reports.net_worth_currencies`

```
currency_code            VARCHAR        -- Grain. NULL is the unknown-currency segment
home_currency_code       VARCHAR
rate_source              VARCHAR
balance_date             DATE           -- Grain
rate_published_date      DATE
account_count            INTEGER        -- Accounts contributing on this date in this currency
carried_forward_count    INTEGER        -- How many of them are carried forward, not observed
total_assets             DECIMAL(18,2)
total_liabilities        DECIMAL(18,2)  -- Kept negative
net_worth                DECIMAL(18,2)  -- This currency's segment, in its own unit
total_assets_home        DECIMAL(18,2)
total_liabilities_home   DECIMAL(18,2)
net_worth_home           DECIMAL(18,2)  -- NULL when this currency is unpriced on this date
```

`rate_source` is a `DataClass.TXN_TYPE` dimension
(`src/moneybin/privacy/taxonomy.py`), so Rule B
(`.claude/rules/column-ordering.md`) places it before the date block, same as
`reports.net_worth_accounts` above.

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
unanchored_account_count INTEGER        -- M2B.3 (Requirement 14). Priced holdings or transaction activity, no balance row; 0 means none
total_assets             DECIMAL(18,2)  -- NULL when unpriced_currency_count > 0 or unanchored_account_count > 0
total_liabilities        DECIMAL(18,2)  -- NULL when unpriced_currency_count > 0 or unanchored_account_count > 0
net_worth                DECIMAL(18,2)  -- NULL when unpriced_currency_count > 0 or unanchored_account_count > 0
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

`unanchored_account_count` is `M2B.3`'s column on this same rung, not M2B.2's:
Requirement 14 delivers the guard as its own work item precisely so the
release gate on this row outlives M2B.2 closing. The view itself counts it,
per row, from the union of `core.dim_holdings`, `core.dim_holdings_broker_reported`,
`core.fct_transactions`, and `core.fct_investment_transactions` (§Data Model
above) — sources none of the other two rungs reads — correlated against
`core.fct_balances` and `core.dim_accounts`
at that row's own `balance_date`, to count an eligible account carrying
evidence of holding value with no balance row at all, and drives `net_worth`
NULL the same way `unpriced_currency_count` already does. The view also
carries its own unconditional `CURRENT_DATE`-dated row for a profile with
no balance data at all but an eligible candidate, so a bare read gets the
right "now" answer with no runner. Only the report's runner, and only for
a specific historical range with no balance-spine rows in it, computes
this column outside the view — §Data Model states why that one case
cannot be expressed without knowing the requested range.
Requirement 14 states why the join needs no per-date `balance_date`
predicate beyond the row's own and why `core.fct_holdings_daily` (Pillar
C.3) is deliberately not one of these sources.

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
rate_source           VARCHAR        -- provider / identity — never override here; only core.fct_exchange_rates_effective adds override
rate_vendor           VARCHAR        -- The named feed behind a provider row (e.g. 'frankfurter'); NULL when rate_source is identity or override
rate                  DECIMAL(18,8)  -- Multiply a from_currency amount by this
days_since_published  INTEGER        -- effective_date - published_date; 0 on a publication day
effective_date        DATE           -- Grain. The calendar day this rate is applied ON
published_date        DATE           -- The day the provider priced it (= fct_exchange_rates.rate_date)
```

Rule A (`.claude/rules/column-ordering.md`) orders `core` and `prep` columns
ids → strings → numerics → booleans → dates → timestamps, so the string
`rate_source` and the numeric columns precede the date pair rather than
following them.

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

**On the provider-rate refresh path.** `CurrencyService._store()` restates
this model in the same `TransformService.restate_models` call as
`core.bridge_currency_conversions` whenever `moneybin fx rate` caches a newly
fetched quote (`committed_change="exchange rate"`). A pair/date fetched by that
path reaches this model — and `core.fct_exchange_rates_effective`, which reads
it — immediately, with no separate `sqlmesh run`. A user override still applies
only at read time in `core.fct_exchange_rates_effective` and triggers no
restatement here.

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

Four rules, in this precedence, reproduce `_stored_rate`:

1. **An override on the `effective_date` itself wins** — `_stored_rate`'s
   exact-day check. `published_date` becomes that day and
   `days_since_published` is 0: the user priced the day itself.
2. **Otherwise, on a Saturday or Sunday row that is itself CARRIED from an
   earlier publication (`published_date <> effective_date`) — never on a
   weekend row that is a genuine same-day observation — an override on the
   calendar Friday immediately before it wins.** This is
   `_last_publication_day`'s weekend hop, a function of the calendar date
   asked about rather than of whatever `published_date` the daily spine
   happens to record for that row. `_stored_rate(Friday)` checks the
   override table before ever touching the daily spine's own carry, so a
   Friday override reaches the weekend it hops to even when Friday itself
   was never a provider publication day — a gap the daily spine carries
   straight through from the prior observation. `published_date` becomes
   that Friday and `days_since_published` counts from it (1 for Saturday, 2
   for Sunday). The carried-row restriction exists because `_stored_rate`
   checks the exact requested day first: a vendor observation dated
   precisely on a Saturday or Sunday already answers `resolve_rate` on its
   own, and a Friday correction must not outrank it — no shipped adapter
   writes such a row today, but the view must not manufacture the wrong
   answer for it if that changes.
3. **Otherwise an override on the row's `published_date` wins** — the same
   exact-day check reached through the ordinary carry-forward. A corrected
   publication prices every day carrying from it, weekend or interior
   weekday alike. `published_date` and `days_since_published` keep the hop
   they already recorded — only the rate is replaced. An override filed
   directly on an interior non-publication day that is not itself the
   calendar Friday of a weekend it precedes does **not** gain this cascade —
   it wins only under rule 1, on its own day. Extending a same-pair,
   non-publication correction past the single day it was filed under would
   assume a claim about neighboring days the user never made; Requirement 5
   governs the ambiguity the same way rule 4 states it for an uncovered
   override.
4. **An override on a pair and date the spine does not cover contributes its
   own row** — `_stored_rate` answers from the override table whether or not
   a provider ever priced that day, so a correction is never invisible
   because the provider was silent. Such a row gets the same bounded weekend
   hop a provider observation would (Friday carries to Saturday/Sunday,
   nothing further). It does **not** interior-fill between two disconnected
   uncovered override dates for the same pair; Requirement 5 (never
   manufacture a rate) is the controlling invariant when that is ambiguous,
   so an uncovered gap between two standalone overrides stays unpriced
   rather than guessed.

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

**The column alone would have preserved nothing.** `AccountService.settings_update`
used to force `include_in_net_worth=False` in the same write as `archived=True`
(`src/moneybin/services/account_service.py:714-718` as of commit `6cf32bba`,
the revision immediately before the cascade was retired in `dc158055`; the
citation is pinned because the line range now resolves to unrelated code),
and `archived=False` deliberately did not restore it. That flag carried no
date, so every historical
row of an archived account still failed the `include_in_net_worth` half of the
eligibility filter, and the history this column exists to preserve was excluded
anyway. Adding the date predicate on top of that cascade would have been inert.

The cascade is also redundant with the filter it defends. `reports.net_worth`
already reads `a.include_in_net_worth AND NOT a.archived`
(`src/moneybin/sqlmesh/models/reports/net_worth.sql:21`); the `NOT a.archived`
half enforces "an archived account never contributes" on its own, at read time.
The write buys exactly one behavior the predicate does not — an account stays
excluded after archive-then-unarchive — and pays for it by overwriting a
user-authored preference with a derived one.

So the fix is to retire the cascade and let `archived_at` carry the exclusion,
date-scoped: `include_in_net_worth AND (NOT archived OR (archived_at IS NOT
NULL AND balance_date <= archived_at))`. `archived` does not drop out of the
predicate — it is what a NULL `archived_at` falls back to. An archived account
with no `archived_at` (no audit evidence for when the FALSE→TRUE transition
happened — see §Prerequisites) has no date to scope by, so it is excluded at
every `balance_date` rather than every date after some inferred cutoff: the
same blanket exclusion `NOT archived` already applies today, preserved rather
than narrowed. Only an archived account that *does* carry an `archived_at`
gets the date-scoped exclusion this requirement exists to add. That is a
change to a service write path, an `app.*` column semantic, and a backfill
that stamps `archived_at` from the `archived` FALSE→TRUE audit row —
`include_in_net_worth` is left exactly as stored, never reconstructed: a
cascade-written `FALSE` and a caller's own explicit
`archived=True, include_in_net_worth=False` produce the same audit image, so
there is no way to tell them apart from history alone (see §Prerequisites).
`.claude/rules/design-principles.md` puts `app.*` schema semantics on the
one-way-door trigger list, and a change of that shape earns its own review
rather than approval alongside three report views. It is therefore a
**prerequisite**, sequenced ahead of this spec exactly as the margin-loan defect
was — see §Prerequisites.

Nothing about the audit evidence this backfill reads decays while any future
decision about the ambiguous accounts it leaves untouched waits: `app.audit_log`
is append-only, with no prune, retention, or delete path, so each archive write
keeps its full prior row state indefinitely.

### `moneybin system doctor`: balance staleness — `M2B.3`

Requirement 14 defers this check to the same work item as the unanchored-account
guard rather than stating its shape there. This is that shape, specified at the
same level of detail as Requirement 5's rate-window bound and Requirement 7's
NULL-total behavior.

`net_worth_stale_balance` reads each eligible account's own latest
observation from `reports.net_worth_accounts` — never a `balance_date` the
check assumes to be today's. `core.fct_balances_daily`'s spine ends at the
newest observation across *all* accounts — the model's own docstring calls
this "the global last date," deliberately not each account's own — which is
not necessarily `CURRENT_DATE`, so a profile where every account has gone
stale together has no row dated today at all; filtering to `balance_date =
CURRENT_DATE` would find nothing and pass silently in precisely the case this
check exists to catch. **"Eligible" here means the account's current state,
not the date-scoped predicate Requirement 9 applies inside
`reports.net_worth_accounts` itself.** That view keeps an archived account's
rows for the dates before `archived_at` — preserving that history is the
whole point of Requirement 9 — so reading `MAX(balance_date)` straight off it
would still find a deliberately closed account's last pre-archival balance
and flag it stale. The check instead joins against `core.dim_accounts` and
applies `include_in_net_worth AND NOT archived` evaluated against the
account's present row — current-state, not date-scoped — before taking, per
account that passes it, that account's own most recent *observed* row from
`reports.net_worth_accounts` (`is_observed = TRUE`, `MAX(balance_date)`). That
explicit current-state join is what makes a deliberately excluded or closed
account never produce a warning nobody can act on. The check then compares
`CURRENT_DATE - balance_date` — not the row's own `days_since_observed`, which
is relative to the spine's last date rather than to today — against
`DoctorSettings.balance_staleness_threshold_days`
(default 30 — long enough to absorb an ordinary monthly statement cycle
without firing on routine use, short enough to still catch an account nobody
has refreshed in over a month). Reading each account's own latest row rather
than a shared `balance_date` filter is what makes the check correct whether
or not the spine happens to reach today.

Severity is `warn`, not `fail`. The balance the check flags is still present
and still contributes to the total; only Requirement 14's own guard — an
account with **no** balance row at all — drives the total to NULL.
`DoctorReport.failing` counts only `fail` toward `moneybin system doctor`'s
release-gating exit code (`doctor_service.py:553-556`, read at
`cli/commands/system/doctor.py:68`), so a stale-but-present
balance can surface without turning a release artifact red. That is the same
trade the shipped `investment_stale_prices` check already makes for a
carried-forward security close, and for the same reason: an aging number is a
prompt to refresh it, not proof it is wrong.

Implemented as one more invariant in `DoctorService` (`doctor_service.py`)
alongside `investment_stale_prices`, with `balance_staleness_threshold_days`
added to `DoctorSettings` (`src/moneybin/config.py`) and the check's row and
threshold documented in `docs/specs/moneybin-doctor.md`'s invariant table.

### `moneybin system doctor`: unanchored accounts — `M2B.3`

Requirement 14's own text says the unanchored-account guard makes a release
artifact fail; nothing above does that, because `net_worth_stale_balance` is
`warn` by design and no other invariant reads this guard's signal. This is
the check that closes that gap — the one Requirement 14 was actually
describing when it said "an eligible account ... must not contribute zero in
silence," stated at the same level of detail as the staleness check above.

`net_worth_unanchored_accounts` first joins `core.dim_accounts` and applies
`include_in_net_worth AND NOT archived` — the same current-state
eligibility `net_worth_stale_balance` applies just above, evaluated
against the account's present row rather than date-scoped (`archived`,
`dim_accounts.sql:362`; `include_in_net_worth`, `dim_accounts.sql:363`;
both COALESCEd there from `app.account_settings`'s current row, not
history). **The two invariants share one eligibility definition**, so a
future change to what "eligible" means moves both checks together. That
join replaces two proxies for it that each fail differently: a
`balance_date = CURRENT_DATE` filter produces a false
negative (below); dropping the date filter entirely cures that but
produces the opposite false positive (also below). Neither names the real
condition, which was never a date — it is the account's current inclusion
state.

Only an account that passes the join is then scanned in
`reports.net_worth_accounts` for `account_balance IS NULL`, still with
**no `balance_date` filter** — never a NULL-total read off
`reports.net_worth`'s own aggregate rung, and never scoped to
`CURRENT_DATE`. Every ordinary row populates `account_balance` (the
column's own comment at `:1079`, "In currency_code," carries no NULL case);
the synthesized-row arm is the only source of a NULL there, so the bare
predicate identifies it regardless of what date §`reports.net_worth_accounts`
dates that row at — that date is stated once, where the row is produced,
and is not restated here. A mixed profile whose newest anchored balance
predates today dates its synthesized row at the balance spine's own
maximum, not at `CURRENT_DATE` (§`reports.net_worth_accounts`'s dating
rule); filtering this check on `balance_date = CURRENT_DATE` in addition
to `account_balance IS NULL` would silently exclude exactly that mixed
profile — a false negative,
because the spine can end before today with no row dated today at all.
Omitting the date filter without the eligibility join produced the other
failure: `reports.net_worth_accounts` intentionally retains an archived
account's synthesized pre-archive row — preserving it for the range in
which the account really was eligible and unanchored is the whole point
of Requirement 9's date-scoped history — so an unrestricted scan kept
that row at `fail` long after the account's current `archived = TRUE`
excludes it from Requirement 14's guard. The eligibility join closes that
false positive without bringing a date filter back. The account rung is
the one relation in this spec that names the affected accounts by id;
`reports.net_worth`'s `unanchored_account_count` is a number with nothing
to attach `affected_ids` to, while several existing `fail` invariants in
this file already return the specific rows they flag rather than only a
count — `investment_source_overlap`, `orphan_app_state`,
`app_audit_coverage_*`, `currency_integrity` all do (`dedup_reconciliation`
is the exception, and only because a global count mismatch genuinely has
no individual row to name). The affected accounts here are individually
addressable, so this check follows the row-naming pattern rather than
`dedup_reconciliation`'s. A row surviving both the eligibility join and
the `account_balance IS NULL` filter means `core.dim_holdings`,
`core.dim_holdings_broker_reported`, `core.fct_transactions`, or
`core.fct_investment_transactions` carries evidence for a presently
eligible account `core.fct_balances` has no anchor for at all —
Requirement 14's own predicate, already computed once by the view this
check re-reads rather than duplicating.

Severity is `fail`, not `warn` — the opposite of the staleness check just
above, and deliberately so: the balance `net_worth_stale_balance` flags is
still present and still contributes to the total, but the account this check
flags contributes nothing, and `reports.net_worth.net_worth` is already NULL
for exactly this profile per Requirement 14. `DoctorReport.failing` counts
`fail` — and only `fail` — toward `moneybin system doctor`'s release-gating
exit code (`doctor_service.py:553-556`, read at `cli/commands/system/doctor.py:68`
and turned into `raise typer.Exit(1)` at both the JSON
(`cli/commands/system/doctor.py:116`) and default-text
(`cli/commands/system/doctor.py:190`) output paths), so a profile whose only
accounts are eligible and unanchored now exits non-zero, closing the
self-contradiction between this spec's own release-bar claim and its M2B.3
implementation plan.

Implemented as one more invariant in `DoctorService` (`doctor_service.py`)
beside `net_worth_stale_balance`, needing no new `DoctorSettings` field —
the check is a boolean presence test, not a threshold — and documented
alongside it in `docs/specs/moneybin-doctor.md`'s invariant table.

## Report allocation

### One name per report

A report's `report_id` is a hand-declared `namespace:name` string
(`src/moneybin/reports/_framework/contract.py:367`) with nothing binding its
name half to `spec.view`. Across the eight shipped reports, two match and six do
not. That was drift, not a convention, and this spec ends it:

> **The name half of `report_id` is the view's name.**

`ReportSpec.cli_name` already derives the Typer command from that same name by
swapping underscores for hyphens (`contract.py:474-476`), so one name fixes all
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

Each takes the same optional `from_date` / `to_date`. `interval` is
`core:net_worth`'s alone — it is not a parameter of the other two. **The
snapshot-versus-history split does not survive.** It was an artifact of how the
two reports were built, not a real distinction: every rung is a daily series,
and "now" is `WHERE balance_date = MAX(balance_date)`. `core:networth_history`
is therefore retired — it is `core:net_worth` with a range — and
`change_abs` / `change_pct` become runner-computed columns on `core:net_worth`
when `interval` is given, bucketing that single day-grain series
(week-over-week, month-over-month) the same way `networth_history` did.
`core:net_worth_accounts` and `core:net_worth_currencies` stay daily at every
grain they carry (`account_id`/`currency_code` × `balance_date`); nothing in
this spec defines what "weekly" or "monthly" would mean for an
account-or-currency-level series, so `interval` is out of scope for them
rather than an implicit no-op or an inconsistent bucketing an implementer
would otherwise have to invent.

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
- `src/moneybin/sql/migrations/V063__add_account_settings_archived_at.py`

Tests: unit tests for each new model's shape and null behavior, a scenario test
comparing the three rungs against generator ground truth, the two guard
tests named in §Testing Strategy, and thirteen acceptance tests for
`account_archive_intent_ambiguous`: an account backfilled by V063 into the
ambiguous state warns; the same account after `unarchive()` — `archived`
back to `FALSE`, `include_in_net_worth` still the cascade-written `FALSE`
per that method's own contract — still warns, pinning that the check is not
scoped to *current* `archived = TRUE`; the warning clears once `accounts set
--include` or `--exclude` writes the `confirms_include_in_net_worth` marker
(whichever value is passed, including the idempotent `--exclude` that
leaves `include_in_net_worth` unchanged); as a negative, an unrelated
`accounts set` write on the same account — a rename or a currency change,
`include_in_net_worth` untouched — leaves it warning, pinning that a
generic settings write is not what clears it; as a second negative, an
account whose `include_in_net_worth = FALSE` was set directly via `--exclude`
with no `archived = TRUE` audit row ever written for it never warns at all —
pinning that a legitimately, deliberately excluded account that predates this
feature (or simply never went through the archive cascade) is not what this
invariant exists to flag; as a third negative, an account whose
`account_settings.set` history contains a pre-marker row that turns
`include_in_net_worth` from `TRUE` to `FALSE` while that same row's
`archived` stays `FALSE`, followed later by a separate row recording
`archived = TRUE` — never warns, pinning that an archive which found the flag
already `FALSE` is no cascade evidence; and, as a fourth negative,
an account whose very first `account_settings.set` row is that same
standalone `--exclude` — `before_value IS NULL` (the INSERT path, no prior
row to snapshot), `after_value.include_in_net_worth = FALSE`,
`after_value.archived = FALSE` — followed later by a separate row recording
`archived = TRUE`: never warns either, for the same reason; as a fifth
negative, an account whose `include_in_net_worth =
FALSE` predates the audit log, archived today through `AccountService.archive()`
with no other audit history, never warns — pinning that only a pre-V063 image
(no `archived_at` key) is cascade evidence; a backfilled account settled by
`accounts set --include` and then returned to `FALSE` by undoing that write
warns again, pinning that an undone decision does not settle; and a backfilled
account whose history also holds a hand-built `account_settings.set.undo` row
with the standalone-exclusion shape still warns, pinning that an undo row never
settles; a pre-marker exclude, then an include, then a pre-V063 cascade archive
warns, pinning that a settling row must postdate the latest evidence row; a
backfilled cascade followed by an unarchive-and-include and then a pre-marker
standalone exclude never warns, pinning that the row-shape evidence settles the
account exactly as the marker would; and a lone pre-V063 archive whose before
image already reads `include_in_net_worth = FALSE` never warns, pinning that
only a write that flipped the flag is cascade evidence.

### Files to Modify

- `src/moneybin/sqlmesh/models/reports/net_worth.sql` — becomes the day-grain
  rung. Its current per-currency body moves to `net_worth_currencies.sql` and
  gains the converted measures, the rate provenance columns, and
  `carried_forward_count`; both switch the eligibility filter to the
  date-scoped form. **`M2B.3`** later adds the `unanchored_account_count`
  column and its NULL gate to this file's own query, in its own change —
  joining the new `core.*` model (below), `core.fct_transactions`, and
  `core.fct_investment_transactions`, correlated per row against each row's
  own `balance_date`, plus a second `UNION ALL` arm dated `CURRENT_DATE` for
  a profile with no balance-driven output at all but an eligible candidate.
  Only a historical range with no balance-spine rows in it still needs the
  runner (`net_worth.py`); see §Data Model.
- `src/moneybin/sqlmesh/models/reports/net_worth_accounts.sql` — **`M2B.3`**
  adds the same shape of second `UNION ALL` arm, one row per eligible
  unanchored account rather than one aggregate row and dated by this rung's
  own rule rather than `net_worth.sql`'s, per §`reports.net_worth_accounts`.
- The four report definitions being renamed — `cash_flow`, `spending_trend`,
  `recurring_subscriptions`, `merchant_activity` — plus every test, guide, and
  fixture naming an old id or command. Mechanical, but repo-wide; see
  §Report allocation → Scope.
- `src/moneybin/sql/schema/app_account_settings.sql` — declare `archived_at`.
- `src/moneybin/sqlmesh/models/core/dim_accounts.sql` — resolve `archived_at`
  alongside `archived`. Two column comments there still name the retired
  `agg_net_worth` model (`:362-363`); correct them while in the file.
- `src/moneybin/privacy/taxonomy.py` — a `CLASSIFICATION` entry for
  `archived_at` in both `("app", "account_settings")` and
  `("core", "dim_accounts")` — the two bullets directly above add a live
  column to each. Without both entries, classification-completeness tests
  reject the new columns and strict report-class derivation cannot resolve
  the `archived_at` predicates Requirement 9's eligibility filter adds.
- `src/moneybin/services/doctor_service.py` — the
  `account_archive_intent_ambiguous` invariant (§Prerequisites), `warn`
  severity, flagging an account the V063 backfill left ambiguous with no
  audit row proving a deliberate decision — the
  `confirms_include_in_net_worth` marker, or the pre-marker row-shape
  evidence §Prerequisites defines.
- `src/moneybin/repositories/account_settings_repo.py` — `set()` gains a
  `context: dict[str, Any] | None = None` parameter, forwarded to the
  existing `_emit_audit(context=...)` (`repositories/base.py:146`) it
  already accepts but this repo never passes.
- `src/moneybin/services/account_service.py` — `settings_update()` passes
  `context={"confirms_include_in_net_worth": True}` when its own
  `include_in_net_worth: bool | None` parameter is not `None` — the one
  point that already knows the caller touched the flag, before that
  distinction disappears into a full-row snapshot.
- `docs/specs/moneybin-doctor.md` — that invariant's table entry, separate
  from `net_worth_stale_balance`'s (`M2B.3`, its own change).
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

**`M2B.3`** — not this pass, its own change:
- `src/moneybin/sqlmesh/models/core/dim_holdings_broker_reported.sql` — the
  classified `core.*` relation the guard reads instead of `prep.*` directly;
  see §Data Model.
- `src/moneybin/privacy/taxonomy.py` — its `CLASSIFICATION` entry.
- `src/moneybin/reports/definitions/net_worth.py` — the inverted-range
  validation carried over from `service_reports.py`'s retired
  `_validate_networth_history_parameters`, the ordinary range filter (or
  `MAX(balance_date)` when unranged), and the range-scoped synthesized row
  for a historical range with no balance-spine rows in it. The guard's
  per-row count, NULL gate, and `CURRENT_DATE` `UNION ALL` arm live in
  `net_worth.sql` itself, not here.
- `src/moneybin/reports/definitions/net_worth_accounts.py` — the same
  range-filter and inverted-range-rejection shape as `net_worth.py`, plus the
  per-candidate anti-join fallback described in §`reports.net_worth_accounts`:
  for each eligible unanchored candidate missing from the runner's own
  range-filtered output, one row synthesized independently — never gated on
  the whole filtered result being empty, unlike `net_worth.py`'s own
  fallback above. The guard's own NULL-column arm and its synthesized-row
  `UNION ALL` arm — dated per §`reports.net_worth_accounts`'s own rule, not
  unconditionally at `CURRENT_DATE` the way `net_worth.sql`'s arm above is —
  live in `net_worth_accounts.sql` itself, not here.
- `src/moneybin/config.py` — `DoctorSettings.balance_staleness_threshold_days`.
- `src/moneybin/services/doctor_service.py` — the `net_worth_stale_balance`
  (`warn`) and `net_worth_unanchored_accounts` (`fail`) invariants; see
  §"`moneybin system doctor`: unanchored accounts" for why the second one is
  `fail` and the first stays `warn`.
- `docs/specs/moneybin-doctor.md` — both invariants' table entries.

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
error. `--interval` stays a `net-worth`-only flag, per §"The three net-worth
reports" above — `net-worth-currencies` and `net-worth-accounts` do not
declare it.

## MCP Interface

Three reports, listed in §Report allocation, each reached as
`reports(report_id=..., parameters=...)` with `from_date` and `to_date`
optional on all three; `interval` is optional on `core:net_worth` only, per
§"The three net-worth reports" above. Changes an existing caller sees:

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
- **Inverted range is rejected, not reinterpreted.** `core:net_worth` with
  `from_date > to_date` raises `UserError`/`REPORT_PARAMETER_INVALID_RANGE`
  before issuing any query — the same error the retired
  `_validate_networth_history_parameters` raised. Assert no row comes back,
  populated or synthesized, on a profile that has an eligible unanchored
  candidate: that combination is exactly the one an inverted range could
  otherwise turn into a silently plausible wrong answer.
- **`to_date` alone stays open below.** A persona with real balance history
  starting well before the requested `to_date`, queried with `to_date` only,
  returns every row on or before it — not a single row on `to_date` itself.
  Pick a `to_date` that lands on a day with no balance observation of its
  own, so the assertion fails under the collapsed-to-a-single-day rule and
  passes only under the open-below one.
- **A future-only lower bound synthesizes nothing.** A persona with an
  eligible unanchored candidate, queried with `from_date` set to a day after
  `CURRENT_DATE` and no `to_date`, returns zero rows — not a row dated
  before the requested lower bound. This pins the general `synthesis_date`
  rule in §Data Model: it must also clear `effective_from` before the runner
  emits anything, and the case that surfaced the omission has to stay
  failing.
- **A synthesized row respects an archived candidate's own window.** A
  no-spine historical range that starts before an eligible candidate's
  `archived_at` and ends after it: the candidate still satisfies the
  eligible-candidate predicate, but the runner dates the synthesized row at
  the candidate's own `archived_at` (`archived_at_floor`), never at
  `effective_to`. This pins `archived_at_floor` in the general
  `synthesis_date` rule (§Data Model): dating the row past a counted
  candidate's own eligible window would
  report it unanchored on a date Requirement 9 already excludes it from.
- **Staleness invariant: entirely stale profile.** A persona whose every
  eligible account's most recent *observed* balance
  (`reports.net_worth_accounts.is_observed = TRUE`) is 45 days before
  `CURRENT_DATE` — old enough that `core.fct_balances_daily`'s spine ends
  more than a threshold-length ago and no row is dated `CURRENT_DATE` at
  all — asserts `net_worth_stale_balance` returns one `warn` entry per
  stale account, `affected_ids` naming every one. This is the regression
  §"`moneybin system doctor`: balance staleness" names directly: a check
  written against a shared `balance_date = CURRENT_DATE` filter instead of
  each account's own latest row finds nothing and passes silently on this
  exact fixture.
- **Staleness invariant: current-state exclusion.** Two accounts share the
  same 46-day-stale last observation; one has `include_in_net_worth =
  FALSE` (a second fixture repeats this with `archived = TRUE` instead),
  the other is ordinary and eligible. Asserts exactly one `warn` entry,
  naming only the ordinary account. This is the regression guard for
  joining eligibility off `core.dim_accounts`'s present-state row rather
  than off `reports.net_worth_accounts`'s date-scoped `archived_at`
  predicate, which would still surface a deliberately excluded or closed
  account's last pre-archival balance as a live warning nobody can act on.
- **Staleness invariant: threshold boundary.** One account's most recent
  observed balance is dated exactly `balance_staleness_threshold_days`
  (default 30) before `CURRENT_DATE`; a second is dated
  `balance_staleness_threshold_days + 1` before it. Assert both in the same
  test: the first produces no `warn` entry, the second does. This pins
  `CURRENT_DATE - balance_date` as the compared quantity — not the row's
  own `days_since_observed`, which is relative to the spine's last date
  rather than to today — so an off-by-one cannot silently narrow or widen
  the 30-day default this spec chose to absorb a monthly statement cycle
  without over-firing.
- **The naming rule has a guard.** For every runner in `ALL_REPORTS`, the name
  half of `spec.report_id`, `spec.view.name`, and `spec.name` are all equal —
  three-way, not a pair. `spec.name` is the third because it is independent of
  the other two and `ReportSpec.cli_name` derives the Typer command from it
  (`src/moneybin/reports/_framework/contract.py:474-476`), so a pairwise guard
  passes while a definition carries its old CLI command through a rename — the
  one-name rule broken on the one surface a user actually types. Requirement 13
  is a convention until a test enforces it, and the six mismatches this spec
  removes are what an unenforced convention looks like after a year.

### Tier 2 — Synthetic scenarios

`make test-scenarios` compares all three rungs against generator ground truth,
including the multi-currency persona, which already contains an account in a
currency outside the rate provider's published set — so the unpriced path is
reachable from a shipped fixture rather than a hand-built one.

`M2B.3` adds four scenarios beside it, shaped like the unpriced-currency case
they follow. The first: a persona account holding priced securities and
carrying no balance observation, alongside other balance-backed accounts,
asserted to drive `net_worth` to NULL with an unanchored-account count of
exactly one — never to a smaller populated total. The second: a persona
account with recorded transactions and no priced holding and no balance
observation — a tabular-import-with-no-balance-column shape — asserted to
drive the same NULL-plus-count result through the transaction-activity arm
of the qualifier, never through the holdings arm. The third: a persona
whose accounts are *all* unanchored, so `core.fct_balances_daily` has no row
for the profile at all — asserted twice, first against the bare view
(`SELECT * FROM reports.net_worth` with no runner involved, pinning that
the view's own `CURRENT_DATE` arm needs no `sql_query` wrapper) and again
through the runner's unranged default, both producing exactly one row dated
`CURRENT_DATE`, `net_worth` NULL, unanchored-account count equal to the
number of qualifying accounts — never zero rows from either path. The
fourth: the same wholly-unanchored persona with one of its
accounts archived after the fact, queried over a historical range that
predates the archival — asserted to still publish the synthesized row for
that range, because the account was live and unanchored throughout it, even
though a query with no range (evaluated as of today) would now find it
ineligible. This is the regression guard for evaluating the guard's
eligibility against the requested range rather than always against today.

A fifth case is asserted as a **negative**: a persona account with no
balance, no holding, and no transaction of any kind does *not* raise
`unanchored_account_count` and does *not* NULL the total — the qualifier's
residual gap, pinned so a future author does not "fix" it by widening the
candidate set back to every balance-less account.

A sixth case pins the empty-range fix directly: a persona with ordinary
balance-backed accounts and one eligible unanchored account, queried over a
historical range that predates every account's own first balance
observation — asserted to still publish the synthesized row for that
range, because `core.fct_balances` is not globally empty (the ordinary
persona's rows exist elsewhere) but the range-filtered result is. This is
the case that distinguishes "filtered result empty" from "globally empty"
as the trigger — a guard keyed on the wrong one of the two would pass the
third and fourth scenarios above while still failing this one. A seventh
case covers
the fourth evidence arm: a persona investment account whose only activity
is a dividend or fee in `core.fct_investment_transactions` — no
`core.fct_transactions` row, no holding, no balance — asserted to drive the
guard through that arm alone.

The first, second, third, and seventh scenarios above also assert
`reports.net_worth_accounts`'s own signal for the same fixture: the
eligible unanchored account appears as a row (never absent), with
`account_balance` and `account_balance_home` NULL and `is_observed = FALSE`
— never zero rows for that account on that rung, which is the failure
§`reports.net_worth_accounts` states this addition closes. Its
`balance_date` follows that rung's own dating rule stated once there, not
restated here: the balance spine's own maximum for the first, second, and
seventh scenarios' mixed anchored/unanchored profiles, and `CURRENT_DATE`
only for the third scenario's wholly-unanchored persona, where the spine is
empty and the rule's fallback applies.

**An eighth case pins the account-rung anti-join directly** — the regression
guard against double-emitting or dropping a candidate the view's own arm
already answered. The first
scenario's mixed persona, queried over a
historical range that includes `core.fct_balances_daily`'s own spine maximum: the
eligible unanchored candidate's view-arm row (dated at that maximum,
§`reports.net_worth_accounts`) already satisfies the range, so the account
appears exactly once in the result, never twice — pinning that the runner's
per-candidate anti-join skips a candidate its own range-filtered output
already contains. The same persona queried again over a range that excludes
the spine maximum but still contains other accounts' anchored rows: the
candidate still appears, now synthesized independently and dated by the
runner's own `synthesis_date` rule (§Data Model) — pinning that the fallback
is evaluated per candidate, never gated on the whole filtered result being
empty, which a range containing other accounts' rows never is.

**A ninth case pins the anti-join's unranged eligibility date** — the
regression guard for a defect found in the per-candidate anti-join above.
The same first-scenario mixed persona,
with its eligible unanchored candidate additionally archived before
`core.fct_balances_daily`'s own spine maximum, queried with no range at
all: the candidate does not appear in the result at any date — never at the
spine maximum (Requirement 9 already excludes it there, the same as an
ordinary row), and never synthesized at its own `archived_at` either. This
is the regression guard for evaluating the anti-join's candidate set at the
date an unranged read actually returns rather than at the aggregate rung's
`effective_from`-based existence check, which would otherwise readmit the
candidate and synthesize a historical row inside
what the unranged contract promises is a `MAX(balance_date)`-only result.
The same persona queried again over an explicit historical range spanning
the candidate's `archived_at` still synthesizes its row there, dated at
`archived_at_floor` exactly as the eighth case already pins — proving the
fix is scoped to the unranged path and does not regress the ranged one.

**A tenth case pins the cash arm's general zero-decisiveness rule
directly** — the regression guard for reading any net-investment-ledger
zero as decisive, bootstrap-tainted or not. A persona investment
account whose only priced position is a pre-window opening-lot bootstrap
(`subtype = 'opening_bootstrap'`), sold in full, in-window, at exactly
its synthesized cost basis, so the ledger's `SUM(amount)` nets to zero
even though the sale's real proceeds were never observed on any balance —
paired with a definitive-zero newest holdings snapshot in either
liquidation shape and no other ledger row for the account: asserted to
drive `net_worth` to NULL with an unanchored-account count that includes
the account, never to a decisive-zero reading that drops it. Paired
against the same-shaped fixture with an ordinary, fully-recorded
buy-then-sell pair instead of a bootstrap row (§Tier 3, "still fails for
a broker-reported definitive zero when the investment ledger nets to
zero"), which fails identically, so the two together prove the predicate
never discriminates on how the zero was reached, only on whether the
ledger carries any row at all.

**An eleventh case pins `reports.net_worth`'s own unranged eligibility
date** — the regression guard for the same class of defect the ninth case
fixed in `reports.net_worth_accounts`'s anti-join, found separately in the
aggregate rung's own fallback. The fourth scenario's fixture — the wholly-
unanchored persona with its one account later archived — queried with no
range at all: the result is empty, no row at any date, because the account
fails Requirement 9's eligibility test at the date an unranged read
actually answers rather than being readmitted through the
`effective_from IS NULL` arm of the eligible-candidate predicate (§Data
Model). This is the assertion the fourth scenario's own prose already
promised ("a query with no range ... would now find it ineligible") but did
not yet pin. The same fixture queried again over the historical range that
predates the archival still publishes the synthesized row there, exactly as
the fourth scenario already asserts — proving this fix is scoped to the
unranged path and does not regress the ranged one, the same proof the ninth
case already gives for the account rung.

### Tier 3 — Integration

- The privacy-class derivation must accept all three views and reject a stacked
  variant; the second half is a guard on Requirement 2, not a hypothetical.
- CLI and MCP parity on all three reports, before and after, over the same
  fixture.
- **Staleness warns, it never fails the release gate.** `moneybin system
  doctor` against a profile carrying only the entirely-stale-profile fixture
  above exits `0`. `DoctorReport.failing` (`doctor_service.py:553-556`), which
  the CLI's exit code (`cli/commands/system/doctor.py:68`) reads, does not
  count a `warn` entry. This is the parity guard for keeping
  `net_worth_stale_balance` at `warn` severity: only Requirement 14's own
  guard (a NULL total from a wholly unanchored account) may turn the
  release-gating exit code red, and this must stay true even as
  `net_worth_stale_balance` gains the affected-account cases above.
- **The unanchored-account guard does fail the release gate.** `moneybin
  system doctor` against a profile carrying only an eligible unanchored
  account (the wholly-unanchored fixture from Tier 2's third scenario)
  exits `1`, with `net_worth_unanchored_accounts` at `fail` and
  `affected_ids` naming that account. This is the release-blocking claim
  Requirement 14 makes about itself, verified end to end rather than left
  as a stated intention — the self-contradiction closed by
  §"`moneybin system doctor`: unanchored accounts."
- **The unanchored-account guard does not fail for an account Requirement
  14 excludes.** `moneybin system doctor` against the fourth Tier 2
  scenario's fixture — the wholly-unanchored persona with one account
  archived after the fact — exits `0` with no `fail` entry naming that
  account, even though `reports.net_worth_accounts` still carries its
  synthesized pre-archive row for the historical range in which it really
  was eligible and unanchored. This is the regression guard for the
  `core.dim_accounts` eligibility join `net_worth_unanchored_accounts`
  shares with `net_worth_stale_balance` — closing the false positive an
  unrestricted `account_balance IS NULL` scan produces once the
  `balance_date = CURRENT_DATE` filter that caused the opposite false
  negative was dropped.
- **The unanchored-account guard still fails for a broker-reported
  definitive zero when the investment ledger nets to zero — the cash
  arm's general rule (§Data Model, "Cash held is never decisive at
  zero").** `moneybin system doctor` against a persona whose
  only account is a liquidated investment account, its broker connection
  staying live and — realistically, not by omission —
  `core.fct_investment_transactions` carrying the account's real
  buy-then-sell history ending in that disposal, **sold at cost so the
  pair's net cash effect is exactly zero**, tested twice against the same
  persona shape: **zero-quantity row**, where the newest snapshot still
  carries a holdings row for the account reporting `quantity = 0` and no
  institution value; and **empty receipt**, where the newest snapshot's
  receipt exists but carries no holdings row for the account at all — the
  no-row form §Data Model's liquidation shapes already address. The
  account has no balance observation of any kind in either case. Both
  exit `1`, with `net_worth_unanchored_accounts` at `fail` and
  `affected_ids` naming the account. A weaker version of this fixture
  would instead read this same net-zero, fully-recorded sum as decisive proof of
  zero cash and exit `0` in both shapes — the exact failure this fixture
  guards against: the buy itself proves the account held real cash at
  some point that no balance observation ever confirmed was later zero,
  so a fully-recorded round trip is no more decisive than a
  bootstrap-tainted one. This is the regression guard for the cash arm's
  boundary in both liquidation shapes together — a row-presence reading of
  the holdings snapshot in either shape reports `has_position = FALSE`
  either way, but this arm's own existential test names the account
  regardless of that reading. Pair both with the existing "does fail the
  release gate" case above using the *same* fixture shape but a nonzero
  reported quantity, so the three together prove the predicate never
  discriminates on the account's own zero-versus-nonzero sum, only on
  whether the ledger carries any row at all.
- **The unanchored-account guard judges a depository account on its own
  evidence, never a sibling brokerage's snapshot — the receipt-scope
  correction (§Data Model, `core.dim_holdings_broker_reported`).**
  `moneybin system doctor` against a persona with one Plaid item carrying two
  accounts: an investment account with a definitive-zero newest snapshot (in
  either shape above) and a depository (checking) account sharing that
  item's `source_origin`, carrying its own `core.fct_transactions` activity
  and no balance observation of any kind. Exits `1`, with
  `net_worth_unanchored_accounts` at `fail` and `affected_ids` naming the
  depository account. This is the regression guard for scoping
  `core.dim_holdings_broker_reported`'s account universe to `account_type =
  'investment'`: before the fix, the depository account inherited the
  item's receipt purely by sharing `source_origin`, incorrectly resolving
  `has_position = FALSE` with no holdings rows of its own — a wrong
  reading on a published `core.*` column for an account the holdings
  product never covers.
- **The unanchored-account guard still fails when the sale that liquidates
  the position is itself the account's only cash evidence — the cash arm's
  aggregate boundary (§Data Model).** `moneybin system doctor` against a
  persona whose only account is the same liquidated investment account as
  above — live broker connection, a definitive-zero newest snapshot in
  either shape — but with `core.fct_investment_transactions` carrying only
  the buy-then-sell pair itself, sold at a gain so the pair's amounts do
  not net to zero, and no separate cash-only row of any kind. The account
  still has no balance observation of any kind. Exits `1`, with
  `net_worth_unanchored_accounts` at `fail` and `affected_ids` naming the
  account. This is the regression guard for computing the cash arm
  existentially over every row rather than as a filtered subset of
  `quantity IS NULL` rows: a predicate that drops every `quantity IS NOT
  NULL` row from consideration discards the sell row's own credit along
  with the position it also carries, and this fixture has no other row to
  fall back on, so the earlier predicate silently contributed zero for it.
  Pair with the "still fails for a broker-reported definitive zero when the
  investment ledger nets to zero" case above, whose buy-then-sell pair is
  sold at cost and nets to exactly zero — the two together prove the
  predicate reads the ledger's rows, never their arithmetic or their shape.
- **The unanchored-account guard still fails when a synthetic bootstrap
  row is what makes the liquidating sale net to zero — one instance of the
  same general rule (§Data Model).** `moneybin system doctor` against a
  persona whose only account is a liquidated investment account — live
  broker connection, a definitive-zero newest snapshot in either shape —
  whose entire `core.fct_investment_transactions` history is a synthetic
  opening-lot bootstrap row (`subtype = 'opening_bootstrap'`) opening the
  position at cost, paired with one real in-window sell crediting exactly
  that cost, so the pair's net cash effect is zero. The account has no
  balance observation of any kind. Exits `1`, with
  `net_worth_unanchored_accounts` at `fail` and `affected_ids` naming the
  account. This is the regression guard for the cash arm's rule applying
  uniformly regardless of what produced the zero: the bootstrap row's
  `amount` is synthesized from cost basis, never observed
  (`prep.stg_plaid__opening_lots.sql:27-30`), but an ordinary,
  fully-recorded zero — proven by the "still fails for a broker-reported
  definitive zero when the investment ledger nets to zero" case above —
  fails the exact same way for the exact same reason, so together the two
  prove the predicate never discriminates on how the zero was reached, only
  on whether the ledger carries any row at all.
- No old id or command survives: a search for `core:networth`,
  `core:cashflow`, `core:spending`, `core:recurring`, `core:merchants`, and
  their derived command names returns nothing outside prose describing the
  rename. Mechanical renames across 30 files are exactly where one gets
  missed.

## Synthetic Data Requirements

The `international` persona already supplies the shapes needed: several
currencies, one of them unpriced. Two additions for Requirement 9 and
multi-currency, and thirteen for `M2B.3`:

- A persona account archived partway through its history, so the date-scoped
  exclusion is exercised end to end rather than only in unit tests.
- A rate-observation gap of more than one non-publication day inside a pair's
  window, so `days_since_published` takes a value greater than 1.

- For `M2B.3`: a persona account holding priced securities with no balance
  observation of any kind, added to an existing balance-backed persona, so
  the unanchored guard's holdings arm is exercised against a shipped fixture
  rather than a hand-built one.
- For `M2B.3`'s transaction-activity arm: a persona account with recorded
  transactions, no priced holding, and no balance observation, added to the
  same persona — the tabular-import-without-a-balance-column shape.
- For `M2B.3`'s residual gap: a persona account with no transaction, no
  holding, and no balance of any kind, added to the same persona — asserted
  NOT to raise the unanchored count, pinning the qualifier's negative case.
- For `M2B.3`'s synthesized-row path: a wholly-unanchored persona — every
  account holding priced securities and no balance observation, so
  `core.fct_balances_daily` has no row for the profile at all.
- For `M2B.3`'s range-evaluated eligibility: the same wholly-unanchored
  persona with one account later archived, so a query for a historical
  range predating the archival and a query with no range give different
  answers — the fixture the fourth and eleventh Tier 2 scenarios above
  read.
- For `M2B.3`'s empty-range trigger: the existing balance-backed persona,
  queried over a historical range that predates its own earliest balance
  observation, with the persona account from the first bullet above still
  present and eligible — the fixture the sixth Tier 2 scenario reads.
- For `M2B.3`'s fourth evidence arm: a persona investment account with a
  dividend or fee recorded only in `core.fct_investment_transactions` — no
  cash-ledger transaction, no holding, no balance — the fixture the
  seventh Tier 2 scenario reads.
- For `M2B.3`'s unranged anti-join eligibility: the first bullet's mixed
  persona, with its unanchored account additionally archived before the
  persona's other accounts' latest balance date, queried both unranged and
  over an explicit range spanning the archival — the fixture the ninth Tier
  2 scenario reads.
- For `M2B.3`'s zero-quantity liquidation shape: a persona investment account
  whose broker connection stays live — its newest snapshot still carries a
  holdings row for the account — but whose position is fully liquidated:
  the row reports `quantity = 0` and no institution value, the account
  carries no balance observation of any kind, and
  `core.fct_investment_transactions` carries its real buy-then-sell history
  ending in that disposal, **sold at cost so the pair's net cash effect is
  exactly zero** — not an empty ledger, which would prove nothing about
  the zero-decisiveness rule this fixture exists to exercise, and not a
  nonzero net cash effect, which the cash-arm-aggregate fixture below
  distinguishes it from.
  Added to the same persona; the fixture the Tier 3 "still fails for a
  broker-reported definitive zero when the investment ledger nets to zero"
  case reads for its zero-quantity-row half.
- For `M2B.3`'s empty-receipt liquidation shape — the no-row twin of the
  fixture above, and the common case in practice: the same fully-liquidated
  persona account, broker connection live, but with its newest snapshot
  receipt carrying **zero holdings rows for the account** rather than one
  reporting `quantity = 0`. The account still carries no balance observation
  of any kind, and `core.fct_investment_transactions` still carries its real
  buy-then-sell history ending in the disposal, sold at cost for the same
  reason as above. Added to the same persona; the fixture the Tier 3 "still
  fails for a broker-reported definitive zero when the investment ledger
  nets to zero" case reads for its empty-receipt half.
- For `M2B.3`'s receipt-scope correction: a two-account Plaid item — an
  investment account with a definitive-zero newest snapshot (either shape
  above) and a depository (checking) account sharing that item's
  `source_origin`, carrying its own recorded `core.fct_transactions`
  activity and no balance observation of any kind. Added as its own
  persona; the fixture the Tier 3 receipt-scope case reads.
- For `M2B.3`'s cash-arm aggregate boundary — the sell-row-is-the-credit
  shape: the same fully-liquidated persona account — live broker
  connection, a definitive-zero newest snapshot in either shape above —
  but with `core.fct_investment_transactions` carrying only the
  buy-then-sell pair itself, sold at a gain so the pair's amounts do not
  net to zero, and no separate cash-only row of any kind. The account
  still carries no balance observation of any kind. Added to the same
  persona; the fixture the Tier 3 cash-arm-aggregate case reads.
- For `M2B.3`'s cash-arm bootstrap boundary: the same fully-liquidated
  persona account — live broker connection, a definitive-zero newest
  snapshot in either shape above — but with
  `core.fct_investment_transactions` history replaced by a synthetic
  opening-lot bootstrap row (`subtype = 'opening_bootstrap'`) opening the
  position at cost, paired with one real in-window sell crediting exactly
  that cost, so the pair's net cash effect is zero. The account still
  carries no balance observation of any kind. Added to the same persona;
  the fixture the Tier 3 cash-arm bootstrap case reads.

Ground truth needs expected net worth per day in the home currency, the
expected NULL dates for the unpriced currency, and — for `M2B.3` — the
expected unanchored-account count and, for the wholly-unanchored persona,
the synthesized row's `balance_date` — computed as `synthesis_date` per the
general rule in §Data Model, never hard-coded to `to_date` or `CURRENT_DATE`:
a fixture whose range straddles an eligible candidate's `archived_at` must
derive the expected date from that same rule (which can instead resolve to
`archived_at_floor`), matching the acceptance case above.

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

- **Retire the archive cascade** — **closed**, ahead of this spec, the same
  sequencing as the margin-loan defect below. `AccountService.settings_update`
  no longer forces `include_in_net_worth=False` when `archived=True`;
  `archived_at DATE` (migration V063) carries the exclusion instead,
  date-scoped, stamped with today's date on the archived FALSE→TRUE transition
  and cleared on unarchive. V063 backfilled every already-archived account's
  `archived_at` from the most recent `archived` FALSE→TRUE audit row (direct
  or via an undo of a prior unarchive). `include_in_net_worth` is left exactly
  as stored throughout — never restored, even when
  `before_value.include_in_net_worth` reads `true` (the retired cascade's own
  signature): that signature is not unique to the cascade, since a caller who
  explicitly passed `archived=True` *and* `include_in_net_worth=False` in one
  call produces a byte-identical audit image, and the repo records full row
  snapshots, not the kwargs a caller passed — there is no way to tell the two
  apart from history alone, so V063 does not guess. An archived account with
  no audit evidence for the transition was left untouched rather than guessed
  at either — `archived_at` stays NULL, preserving today's behavior for that
  account: still excluded at every date, because `archived` alone (with no
  date to scope by) is what today's blanket `NOT archived` filter already
  keys on. `core.dim_accounts` now resolves `archived_at` alongside
  `archived`.

  What remains **for this spec**: Requirement 9's own eligibility filter —
  `include_in_net_worth AND (NOT archived OR (archived_at IS NOT NULL AND
  balance_date <= archived_at))` — on the three net-worth rungs themselves;
  this prerequisite only made that filter possible. `archived` does not drop
  out once `archived_at` exists: a NULL `archived_at` on an archived row falls
  back to it, so the row stays excluded at every date rather than reading as
  active.

  Also for this spec: deciding *which* already-`FALSE` accounts were
  cascade-written versus deliberately excluded — V063's backfill leaves
  `include_in_net_worth` exactly as stored precisely because it cannot tell
  the two apart (above), so `AccountSettingsRepo.set` records row snapshots
  rather than caller kwargs and cannot tell a cascade-written `FALSE` from one
  the user chose (Requirement 9's "a set of accounts this requirement has to
  decide" is the same fact, stated once). Deciding that set is this
  requirement's own job, not the backfill's, and it closes with a named
  mechanism rather than a restated intention: a new `system doctor` invariant,
  `account_archive_intent_ambiguous` (`warn` severity, alongside
  `net_worth_stale_balance`), flags every account where
  `include_in_net_worth = FALSE`, no `app.audit_log` row for it proves a
  deliberate decision (the marker, or the pre-marker row-shape evidence,
  both defined below), **and at least one `app.audit_log` row for it is
  evidence of a historical archive/cascade action** — an
  `account_settings.set` row whose full-row snapshot (`after_value`) records
  `archived = TRUE` at that point in time, regardless of the account's
  *current* `archived` value, in a pre-V063 image (one whose `after_value`
  carries no `archived_at` key). A post-V063 archive cannot be the retired
  cascade, and every post-V063 capture carries the key, present even when
  `NULL`. The row must also be the write that flipped the flag:
  `before_value.include_in_net_worth` `TRUE` (or `before_value IS NULL`) and
  `after_value.include_in_net_worth = FALSE`. A pre-V063 archive of an
  account already excluded did not write that `FALSE`, so it is no evidence.
  That third clause is the scope fix: a legacy
  account whose `include_in_net_worth = FALSE` never passed through the
  cascade — set directly via `--exclude`, or predating this feature and its
  audit trail entirely — carries no `archived = TRUE` audit row at all, so it
  has no cascade write to disambiguate from a deliberate one and the
  invariant leaves it alone. Genuine cascade candidates keep exactly the
  coverage described below: the audit row proving the cascade fired once is
  permanent (`app.audit_log` is append-only), so it still satisfies this
  clause after `unarchive()` clears the *present-tense* `archived` flag —
  never merely "some later settings write exists," and never scoped to
  *current* `archived = TRUE`. `AccountService.unarchive()`
  sets `archived = FALSE` and, by contract, does **not** restore
  `include_in_net_worth` — its own docstring states the flag "is untouched
  -- it was never changed by archiving" (`account_service.py:713-719`). A
  reopened
  account therefore keeps a cascade-written `FALSE` while no longer being
  `archived` at all — scoping the check to `archived = TRUE` would lose
  exactly that account the moment it comes back into use, which is the
  louder failure: a dormant excluded account surprises nobody, but a live
  account silently missing from every net-worth total is the failure this
  guard exists to catch. `AccountSettingsRepo.set` writes one `account_settings.set` audit
  row for every field touched, alike, so a later row alone cannot separate a
  genuine confirmation from an unrelated rename or currency edit; and
  because that row is a full-row snapshot, an idempotent `--exclude` (the
  flag re-asserting the same `FALSE` the cascade already wrote) is
  indistinguishable from an edit that merely leaves the flag untouched at
  its cascade-written value. Inferring intent from either would be the
  cascade's own mistake a third time, on the check built to stop it.
  `AccountService.settings_update` already knows the difference the audit
  row cannot carry on its own: `include_in_net_worth` arrives as `bool |
  None`, and `None` means "the caller never touched this flag" — exactly
  the caller-intent signal a snapshot diff can't reconstruct after the
  fact. When it is not `None`, the write's audit event carries
  `context_json: {"confirms_include_in_net_worth": true}`; when it is
  `None`, no marker is written, regardless of what the flag's stored value
  ends up being. The check's `NOT EXISTS` therefore looks for that marker,
  not for a merely later write — any account with a marked row is settled
  (every marked row postdates all pre-V063 evidence), unless that write was
  later undone, and a rename or an omitted flag never produces one.
  A settling row, marked or matching the row shape below, counts only while
  no audit row carries its `operation_id` as `undoes_operation_id`, and an
  undo row (`account_settings.set.undo`) never settles on its own. A redo
  carries no marker, so a decision undone and then redone warns again until
  `accounts set` restates it: the accepted cost of reading intent only from
  the write that named the flag.

  That marker only exists on writes made after this feature ships, though
  — a `--exclude` predating it leaves no `context_json` at all. Such a
  write can still prove the same fact directly from its own before/after
  images, without the marker: the retired pre-V063 cascade forced
  `include_in_net_worth = FALSE` only when the caller's own `archived`
  argument was `True` in that exact call
  (`src/moneybin/sql/migrations/V063__add_account_settings_archived_at.py:5-9`),
  so any `account_settings.set` row where `before_value.include_in_net_worth`
  reads `TRUE`, `after_value.include_in_net_worth = FALSE`, and
  `after_value.archived = FALSE` could not have been the cascade — a
  standalone write named `include_in_net_worth` directly, independent of
  `archived` becoming `TRUE` in that same row. `before_value.include_in_net_worth`
  reading `TRUE` includes a `before_value IS NULL` row, not only a stored
  `TRUE`: `AccountSettingsRepo.set` captures `before = None` on an INSERT —
  the account's first-ever settings row — and `_serialize_for_audit` maps
  that `None` straight to a JSON `NULL` before value
  (`repositories/base.py:262-263`), so an account excluded on its very
  first `accounts set --exclude` call carries no `before_value` at all, not
  a `before_value` recording the flag's prior stored state. `AccountSettings`'s
  own default (`account_service.py:239`, `include_in_net_worth: bool =
  True`) is what `settings_update` treats as that unwritten row's implicit
  prior value, so a `NULL` `before_value.include_in_net_worth` reads the
  same as a stored `TRUE` for this test — never as "unknown, so exclude
  it" — and the `after_value.archived = FALSE` clause still does the same
  work it does for a stored-`TRUE` row: a first-ever write that also
  archives is the cascade, not a standalone exclusion, and its
  `after_value.archived = TRUE` already fails this shape without a
  separate case for it. That is the same fact the marker states for every
  future write, recovered from a row that predates the marker entirely.
  The check's `NOT EXISTS` is therefore one settled-decision test with two
  ways to satisfy it — the marker or this row shape — not two exemptions
  to keep in sync as a third shape surfaces: a legacy account excluded
  standalone (on its first settings write or a later one alike) and
  archived only later never warns: its archive row found the flag already
  `FALSE`, so it is no cascade evidence at all.
  Either kind of settling row counts only when it postdates the account's
  latest evidence row, ordered by `(occurred_at, rowid)` exactly as V063
  orders audit rows. A pre-marker row-shape exclusion can predate the
  evidence, and an exclude, then an include, then a pre-V063 archive leaves a
  `FALSE` the cascade wrote: the later include→archive cascade overwrote the
  earlier choice, so that exclusion no longer settles the account.
  Rationale and the redundancy that makes the cascade removable:
  §`app.account_settings`; the check's file and acceptance test:
  §Implementation Plan.
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
  position value. Excluding C.3 is also what narrows Requirement 14's guard to
  the account rather than the date — see that requirement for the consequence.
- **An account with zero evidence of any kind** — no balance row, no priced
  holding, and no transaction, ever. Requirement 14's qualifier deliberately
  does not flag it: nothing in `core.*` distinguishes "genuinely never
  funded" from "funded but nothing observed yet" for a zero-row account, and
  guessing risks NULLing every profile that has one for no evidence-backed
  reason. It goes silently absent, unchanged from today — the residual gap
  stated at Requirement 14's qualifier rather than a case this guard closes.
- **Reactivation after an archive is not represented — one cutoff cannot hold
  two transitions.** An account that is archived, later unarchived, and then
  explicitly re-included (`unarchive()` does not restore
  `include_in_net_worth` on its own — see §Prerequisites) passes through
  three lifecycle states: active, archived, active again. Requirement 9
  represents eligibility with a single `archived_at DATE`, cleared back to
  `NULL` on unarchive, and its predicate (`archived_at IS NULL OR
  balance_date <= archived_at`) reads a cleared cutoff as "never archived."
  So once the round trip completes, every balance dated during the interval
  the account was archived is retroactively restored into net worth — this
  ships with history overstated for exactly that interval, not the interval
  before or after it. The alternative — leaving `archived_at` stamped through
  `unarchive()` — trades that failure for the opposite one, excluding every
  balance dated after reactivation instead; neither single cutoff represents
  both transitions correctly. The durable fix is dated eligibility intervals,
  or an equivalent archive/unarchive lifecycle history, replacing the single
  cutoff. That rework is deferred to before the launch trigger
  (`.claude/rules/design-principles.md`'s pre-launch posture: M3E hosted
  launch, or the first tagged release adopted by a non-author user), not to
  `M2B.3` — an archive→unarchive round trip is not this beta's critical
  path, and the `app.*` semantic it would change is not yet locked. Until the
  rework lands, a user who round-trips an archive gets a wrong net-worth
  history for the archived interval.
- **A reversible-setting change is not reflected until the next `moneybin
  transform`.** `core.dim_accounts` is `kind FULL`
  (`src/moneybin/sqlmesh/models/core/dim_accounts.sql`) — refreshed only by an
  explicit transform run, not on every `app.account_settings` write.
  `accounts set --exclude`, archive, and unarchive all write `app.*` directly
  with no call that rebuilds `dim_accounts`, so both `net_worth_unanchored_accounts`
  (`fail`) and `net_worth_stale_balance` (`warn`) read the account's
  pre-change eligibility until the next transform — inherited from the
  already-shipped `reports.net_worth` filter this spec did not introduce, but
  now wired to a release-gating exit code for the first time. Closing it
  means reading eligibility from a live relation instead of a materialized
  one, which is a bigger change than this beta's scope; until then, a
  settings change that should flip an account's eligibility needs a
  `moneybin transform` before `moneybin system doctor` reflects it. These are transaction-replay problems,
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
- **Balance forecasting** — unchanged from M2B.1.
- **Arbitrary display-currency conversion in SQL** — Key Decision 7.
- **The unanchored-account guard on `reports.net_worth_currencies`** —
  Requirement 14 closes the aggregate and per-account grains, not this one.
  The gap is real, not hypothetical: an eligible unanchored account whose own
  `currency_code` has no other anchored account in the whole profile
  contributes nothing to any row of this rung, with no count or NULL to say
  so — the same silent-zero failure the other two grains now close, and,
  unlike the zero-evidence-account gap above, one with a known cause rather
  than an unknowable one. Closing it is not a copy of either existing arm:
  this rung's rows already aggregate several accounts per currency, the way
  `reports.net_worth`'s day-grain rows aggregate every account, so a
  currency-day row could carry a correlated `unanchored_account_count`
  grouped by `core.dim_accounts.currency_code` (an account whose own
  currency is itself unresolved falling into this rung's existing
  NULL-is-unknown segment, per its own grain comment) the way
  `reports.net_worth`'s per-row count already does. But a currency held by
  no anchored account at all still needs its own synthesized row, and that
  row would collapse `MAX(balance_date)` across the *whole* rung exactly the
  way `reports.net_worth_accounts`'s did before its own fix
  (§`reports.net_worth_accounts`), because this rung's rows are one per
  currency rather than the single global row `reports.net_worth`'s own arm
  produces. A correct extension therefore reuses
  `reports.net_worth_accounts`'s synthesis-date rule at a third grouping key
  — real design and test work, not a mechanical repeat — and is deferred to
  its own change rather than folded into this pass silently.
