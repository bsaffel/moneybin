# Column ordering — what is guarded, and what review must catch

Cited from [`.claude/rules/column-ordering.md`](../rules/column-ordering.md).
Read this when changing the ordering guard itself, or when you need to know
whether a rule is machine-checked or review-enforced before relying on a green
test run.

Honest about what is and is not caught.

**Guarded, unit tier — the report specs.** `DataClass` and `money_kind` supply
the categories Rule B needs, and both live on `OutputColumn`, so a spec can be
checked with no database. Two assertions:

1. `ReportSpec.columns` is non-decreasing under Rule B.
2. `default_columns` is non-decreasing under Rule B.

There used to be a third: a tripwire pairing `_SNAPSHOT_COLUMN_TYPES`'s three
entries most likely to drift (`balance_date`, `net_worth`, `account_count`)
against `_SNAPSHOT_COLUMNS`, because the two were parallel by position alone.
Both `core:networth` and `core:networth_history` now derive `column_types`
from a name-keyed dict projected through their own columns tuple, so neither
has a second list to keep in step, and neither needs an assertion — the
tripwire retired along with the hazard it guarded.

**The SQL-backed mirror is checked per report, not globally.** A runner builds
its own `SELECT` over the view rather than inheriting the model's projection, so
no static read of the model gives the runner's order. Each report's own
execution test asserts that the columns its result carries appear in the same
relative order as `ReportSpec.columns`, using the fixture that test already
has. That is a stronger check than a static one — it reads what the report
actually returned.

**The guard checks labels and dimensions as one block.** Which of the two a
name column is depends on whether it names the report's own grain, and no
declaration carries the grain: `merchant_normalized` is the label on
`core:merchant_activity` and a dimension on `core:large_transactions`, which is grained
by transaction. Separating them would enforce a distinction the guard cannot
see, so it checks only that both precede the dates. Their relative order is
review's call, against the sentence above.

The guard skips `AGGREGATE` columns entirely, for the reason given in the rule
file. It also cannot notice a **mis-declared** class —
`balance_drift.days_since_assertion` is declared `TXN_DATE` and holds an integer
day count, so the guard would place it among the dates and be satisfied.
Ordering rules assume the classes are right; fixing a wrong one is its own
change.

**Not guarded — the `reports/*.sql` model projections.** These are a separate
surface from the specs above: a SQL-backed runner names its own columns, so a
model's projection order does not reach a report's consumers — but it *is* what
a `SELECT *` through `sql_query` or `moneybin sql query` returns, so it is
observable and Rule B governs it. All seven are swept, each mirroring the order
its report declares.

They stay **review-enforced**, and not for Rule A's reason. The column *names*
do resolve offline — `derive_report_classes()` parses every model
connectionlessly and returns them in projection order — but their Rule B
*category* does not, because that same derivation deliberately over-classifies
computed columns (`reports.md`). `balance_drift.status` is a `CASE` over two
balances and derives as `BALANCE`; the percentage columns derive as
`TXN_AMOUNT`. A guard ranking off derived classes would therefore fail a
correctly ordered file for putting a "measure" among the dimensions. Reading
declared classes instead is no way out: a runner-less view has none. So the
resolvable part is the half that was never in doubt.

**Not guarded — `prep` and `core`.** Rule A is a data-type rule, and SQLMesh
resolves every `core` column as `UNKNOWN` offline: the type chain bottoms out at
`raw`, which Python loads and SQLMesh never sees. Checking real types needs a
built database, which puts the guard at integration tier where `make test` would
skip it. Rule A is therefore maintained by **review**, deliberately. A reviewer
touching a `prep` or `core` projection is the enforcement point.

**Not guarded anywhere — Rule C.** Nothing declares that `total_assets` and
`total_liabilities` compose `net_worth`, that `prev_month_spend` is
`total_spend`'s comparative, or which measure answers the report. All three are
semantic relationships between columns, and `OutputColumn` carries no field for
them. The guard places the measure block; ordering inside it is review.

That is a deliberate stopping point rather than an oversight. A `composes` or
`compares_to` field on `OutputColumn` would make Rule C checkable, and it is the
obvious extension if these keep drifting — but adding declaration surface to the
report contract to guard four columns in two reports is not yet worth it. Revisit
if a third report inverts.
