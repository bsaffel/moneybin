---
description: "Column order across every dataset surface: dbt data-type order for prep/core, grain-first for reports, statement order inside the measure block, and what is guarded"
paths: ["src/moneybin/sqlmesh/models/**", "src/moneybin/reports/**", "src/moneybin/exports/**"]
---

# Column Ordering

## There is one source of column order, and it is the SQL projection

Every machine-facing surface reads the order the model's `SELECT` writes. This
was measured against the tree, not assumed:

| Surface | Order comes from |
|---|---|
| `--output json`, every MCP caller | SQL projection — `execute.py` builds records as `dict(zip(columns, row))` |
| `reports … --wide` | SQL projection — `visible_columns` returns `tuple(result_columns)` |
| Exports (CSV, XLSX, Parquet, Sheets) | SQL projection — `exports/snapshot.py`, `exports/local.py` |
| `moneybin sql query`, MCP `sql_query` | SQL projection — `cli/commands/sql.py` |
| The narrow `reports` text table | The `default_columns` declaration |
| MCP `reports` catalog, `moneybin reports list` / `describe` | The `ReportSpec.columns` declaration — `_catalog_entry_to_payload` builds the published `columns` array straight from it |

**For a SQL-backed report, `ReportSpec.columns` orders no result, but it does
order the *description*.** No row a caller reads is ordered by it, and no
in-tree report falls back to it for the text table, because every one declares
`default_columns`. The catalog row above is the surface it does reach:
`catalog_to_payload` serves both `mcp/tools/reports.py` and
`cli/commands/reports/user_reports.py`, so the declared order is what an agent
is told a report returns. An agent that reads the description and then the
result sees two different orders unless the mirror rule below holds them
together. Reordering the tuple is a visible change to the catalog, not a no-op.

**A service-backed report is the exception: there, the tuple *is* the
projection.** `service_reports.py` passes `columns=[column.name for column in
_SNAPSHOT_COLUMNS]` directly into its execution, so for `core:networth` and
`core:networth_history` the declared order is what JSON, MCP, `--wide`, and every
export emit. Reordering that tuple is a user-visible change, not a cosmetic one.

A service report in fact carries **three** parallel orderings, and reordering one
means reordering all three:

1. The `columns` tuple — sets `result.columns`, drives `--wide`.
2. A positional `column_types` list in the same call, parallel **by index alone**.
   Reorder one without the other and every column is handed the type of whichever
   column now occupies its slot — a silent mis-typing with no test between it and
   a caller. `networth_history`'s entries read `_decimal_column_type(rows,
   "net_worth", …)`, naming their column in the argument while position is what
   binds, so they look order-independent and are not.
3. The **record dict literals** built in the row comprehensions. The envelope
   carries `data=records` and Python dicts preserve insertion order, so those
   keys — not the `columns` tuple — are what `--output json` and every MCP caller
   serialize. Miss this one and a single response reports a column order its own
   JSON body disagrees with.

Reordering a shipped `core` or `reports` column is a public-contract change
under `design-principles.md`'s trigger list. Pre-launch posture permits it;
disclose it in the CHANGELOG rather than shipping it silently.

## Rule A — `prep` and `core`: dbt data-type order

**ids → strings → numerics → booleans → dates → timestamps.**

Verbatim from dbt's published style guide: *"We prefer to use the following
order: ids, strings, numerics, booleans, dates, and timestamps."* Staging is
covered by the same guide — *"identifiers are first and date/time fields are at
the end"* — so `prep` is **not** exempt. MoneyBin's staging models transform
rather than mirror their source (they join links, coalesce, and rank), so there
is no source-fidelity argument for carving them out.

Within a category, keep related columns adjacent. A column and the column that
qualifies it belong together: `amount` beside `currency_code`, `last_four`
beside `institution_name`.

## Rule B — `reports`: grain-first

**grain keys → identifying labels → dimensions → dates → provenance → measures.**

**Both ends are load-bearing; the middle is the low-value zone.** Two separate
mechanisms make that true, and only one of them is the terminal fitter — do not
justify an in-tree order by the fitter alone.

`_fit_columns` keeps the first and last columns and drops one contiguous run
from the middle, so importance decreases *inward from both edges* rather than
left to right. That path is real but narrow: `column_view` sets
`fit=spec.default_columns is None and not wide`, and every in-tree report
declares `default_columns`, so the fitter runs for **no** report in this
repository — narrow or `--wide`. It governs extension reports that omit the
field.

For every report here the constraint is the reader rather than the renderer. A
`--wide` table is read left to right and cut off by the terminal's own width, and
a JSON record, an export header, and the catalog description are all read in
order; each puts the last column where a scanner stops. Metadata therefore sits
between the dates and the measures and never at the tail, because ending a row
on provenance anchors it on the column that answers nothing.

The `reports` schema is read by humans and by agents answering a human's
question, so it orders by *importance*, not by storage type. Nielsen Norman
Group, on data tables: *"The default order of the columns should reflect the
importance of the data to the user and related columns should be adjacent,"* and
*"the (default) first column should be a human-readable record identifier
instead of a 'mystery meat' automatically generated ID."*

The categories, and what declares each:

| Category | Declared by | Examples |
|---|---|---|
| Grain key | `DataClass.RECORD_ID` | `account_id`, `merchant_id`, `transaction_id` |
| Identifying label | `USER_NOTE`, `MERCHANT_NAME`, `INSTITUTION`, `DESCRIPTION` | `account_name`, `merchant_normalized`, `description` |
| Dimension | `CATEGORY`, `CURRENCY`, `TXN_TYPE` | `category`, `currency_code`, `status`, `cadence` |
| Date | `TXN_DATE` | `year_month`, `txn_date`, `first_seen`, `last_seen` |
| Provenance | `TIMESTAMP_OBSERVABILITY` | `extracted_at`, `loaded_at` |
| Measure | `money_kind` set | `net`, `total_spend`, `net_worth`, `drift` |

**The label is the one that names the grain.** A name column that is *not* the
grain's own name is a dimension: `institution_name` on a report grained by
account is something you slice by, not what identifies the row.

**`AGGREGATE` is two things and the declaration does not say which.** It carries
both counts of the grain (`txn_count`, `account_count`, `occurrence_count`) and
derived scores about the row (`drift_pct`, `amount_zscore_account`,
`confidence`, `is_top_100`). Counts are measures; scores are provenance. Nothing
in `DataClass` separates them, so **the guard does not check the position of an
`AGGREGATE` column at all** — placing one is a review judgement against the two
sentences above, not a checked rule.

**The headline measure goes last.** Among a report's measures, the one that
answers the report's question is the final column. `core:cashflow` ends on
`net`, not on `txn_count`. This is the other half of Rule B a guard cannot
check — see Enforcement.

**A per-unit price is not a measure, and must not be made one to satisfy this
rule.** The guard ranks a column as a measure by `money_kind`, so an author who
wants a price ranked will be tempted to declare one. Do not: `money_kind` routes
a column through `format_money`, which rounds to two places, and the per-unit
columns are `DECIMAL(28, 10)` — a sub-cent close renders `0.00`, destroying the
value rather than abbreviating it. `.claude/rules/cli.md` carries the test: if
the column answers *how much is this worth*, format it; if it answers *what does
one unit cost*, do not. Such a column stays undeclared, is therefore unranked,
and is placed by the same judgement as an `AGGREGATE`.

Per NN/g's second sentence, a grain key at the front does not license replacing
the human label with it. Both belong, key first; where a report cannot afford
both columns, the label is the one that stays and the grain question is settled
in `dim_accounts` / `dim_merchants` rather than by widening the table.

## Rule C — inside the measure block: statement order

Rules A and B place the measure block. This one orders it, and it is the rule
that decides what a reader's eye lands on.

**Components come before the figure they compose.** A financial statement lists
its line items and then the subtotal that sums them; it does not lead with the
bottom line and explain afterwards. IAS 1 ¶55 treats subtotals as presented
*additionally to* the line items they aggregate, and ¶52 puts the identifying
information in headings rather than in the body.

**A comparative sits beside its base.** `total_spend`, `prev_month_spend`,
`mom_delta`, `mom_pct` — the base, then what it is being compared against, then
the difference. A delta separated from the quantity it measures is a number
with no referent.

**The headline measure is last.** Among the measures, the one the report exists
to answer ends the row: it is where a reader scanning left to right stops, and
where the fitter — for an extension report that reaches it — is guaranteed to
keep it.

**Where the base and the headline are the same column, the base wins.** The
first clause is about *composition* — line items and the subtotal that sums
them. The third is about where a reader stops. They usually name different
columns and never meet, but when a report's headline measure is also the base
its comparatives are measured against, they demand opposite ends of the block.
The base leads. `core:spending` is that case: `total_spend` is what
`prev_month_spend` is compared against *and* the figure the report exists to
report, so it opens the measure block rather than closing it. Printing
`mom_delta` before the quantity it is a delta of is the more expensive of the
two mistakes, and current-then-prior-then-change is the layout every variance
report already uses. Composition is untouched — nothing composes `total_spend`,
so the subtotal-last reading never reached it.

Two reports already satisfy this and are the models to copy:

- `core:cashflow` — `inflow`, `outflow`, `net`. Components, then the figure.
- `core:balance_drift` — `asserted_balance`, `computed_balance`, `drift`. The
  two positions being reconciled, then the discrepancy.

Two inverted it before this rule, and are why it is written down:

- `core:networth` — `net_worth`, `total_assets`, `total_liabilities`. The bottom
  line leads and its components trail, so the row reads backwards and the
  fitter's kept tail is a component rather than the answer.
- `core:merchants` — `total_spend` precedes `total_inflow` and `total_outflow`.

**A runtime-attached column obeys Rule B too.** A display-currency conversion
adds `original_currency_code` to a result no report declares it in. It is
`DataClass.CURRENCY`, the same as the `currency_code` it records, so it goes
beside that column rather than on the end — otherwise a converted read is the
one shape that ends on provenance instead of the headline measure, and the
sweep would be true of every projection except the ones a caller actually asked
to convert. `execute.py::_original_currency_position` places it for the
machine-readable projection and `cli_register.py::visible_columns` for the
narrow table; both anchor on `ReportSemantics.currency` rather than assuming the
name, and both fall back to appending when the result carries no currency
column.

This rule governs order only. Alignment, sign glyphs, and colour are settled by
`money_kind` in `cli-output-coherence.md`, not here.

Rule C applies inside Rule A's **numerics** block too, wherever the relationship
exists. It rarely does: a `core` fact table holds atomic amounts, not subtotals
over them. Where it does, the same order holds — a `core` model is not exempt
from reading in the direction its numbers compose.

## Where A and B conflict

They agree everywhere except one adjacency. **dbt places numerics before dates;
Rule B places measures after dates, last.**

Rule B wins in `reports`. Rule A wins in `prep` and `core`. The boundary is the
promotion seam: a measure moving from a `core` model into a `reports` model
moves from mid-table to the end, and that is expected rather than a mistake.

Everything else — ids first, related columns adjacent — is common to both, so
the seam costs one move, not a re-plan.

## The narrow text table

`default_columns` orders the narrow `reports` table, and it is **deliberately
decoupled** from the projection: an author may order the visible table without
reordering the SQL that `--wide`, JSON, and MCP read. That decoupling is
intentional and stays.

Decoupled does not mean unruled. **Rule B applies to `default_columns` in its own
right** — grain-first, headline measure last — it simply is not required to be a
subsequence of the projection.

## `ReportSpec.columns` mirrors the projection

A **SQL-backed** report's declared `columns` tuple must be in the same order as
the SQL its runner projects. The declaration is metadata there, not a
projection: making it mirror costs nothing and removes the trap where an author
reorders it expecting an effect and gets none.

A **service-backed** report has nothing to mirror against — its tuple is already
the projection, per the exception above. Rules B and C govern it directly, and
the `column_types` list beside it moves with it.

## Enforcement

Honest about what is and is not caught.

**Guarded, unit tier — the report specs.** `DataClass` and `money_kind` supply
the categories Rule B needs, and both live on `OutputColumn`, so a spec can be
checked with no database. Three assertions:

1. `ReportSpec.columns` is non-decreasing under Rule B.
2. `default_columns` is non-decreasing under Rule B.
3. `_SNAPSHOT_COLUMN_TYPES` is the same length as `_SNAPSHOT_COLUMNS`, and the
   three entries most likely to drift (`balance_date`, `net_worth`,
   `account_count`) still pair with the column they name.

Read the third one narrowly. It is a tripwire on `core:networth`, not a
derivation: it does not check every column against its declared class, and it
does not cover `core:networth_history` at all — whose `column_types` is the
subtler instance of the same hazard, since its two leading `"VARCHAR"` literals
name nothing a reader could check. A `_HISTORY_COLUMNS` reorder that leaves
that list alone fails only `tests/moneybin/test_exports/test_report_snapshot.py`,
incidentally, and would pass if that test were narrowed. Closing that is
outstanding work, not a property this rule can claim.

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
`core:merchants` and a dimension on `core:large_transactions`, which is grained
by transaction. Separating them would enforce a distinction the guard cannot
see, so it checks only that both precede the dates. Their relative order is
review's call, against the sentence above.

The guard skips `AGGREGATE` columns entirely, for the reason given above. It
also cannot notice a **mis-declared** class — `balance_drift.days_since_assertion`
is declared `TXN_DATE` and holds an integer day count, so the guard would place
it among the dates and be satisfied. Ordering rules assume the classes are
right; fixing a wrong one is its own change.

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

## Exemptions

- `seeds` — static reference CSVs; their column order is the file's.
- `meta` — internal SQLMesh bookkeeping, not a dataset surface.
- `app.*` repositories — mutable state addressed by key, not projected as a
  dataset. If a repository ever projects to a user-visible table, Rule B applies
  to that projection.
