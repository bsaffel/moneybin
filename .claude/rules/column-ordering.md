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

**`ReportSpec.columns` order reaches no surface.** It is read only when a report
declares no `default_columns`, which in-tree reports never do. Reordering that
tuple changes nothing a user or agent sees — which is why the mirror rule below
exists.

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

**Both ends are load-bearing; the middle is the low-value zone.** `_fit_columns`
keeps the first and last columns and drops one contiguous run from the middle,
so importance decreases *inward from both edges* rather than left to right.
Metadata therefore sits between the dates and the measures — the first thing a
narrowing terminal eats — and never at the tail. Putting provenance last would
anchor a squeezed table on the column that answers nothing.

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
| Identifying label | `USER_NOTE`, `MERCHANT_NAME`, `INSTITUTION` | `account_name`, `merchant_normalized` |
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
to answer ends the row, because it is the column the terminal fitter is
guaranteed to keep.

Two reports already satisfy this and are the models to copy:

- `core:cashflow` — `inflow`, `outflow`, `net`. Components, then the figure.
- `core:balance_drift` — `asserted_balance`, `computed_balance`, `drift`. The
  two positions being reconciled, then the discrepancy.

Two invert it and are the reason this rule is written down:

- `core:networth` — `net_worth`, `total_assets`, `total_liabilities`. The bottom
  line leads and its components trail, so the row reads backwards and the
  fitter's kept tail is a component rather than the answer.
- `core:merchants` — `total_spend` precedes `total_inflow` and `total_outflow`.

This rule governs order only. Alignment, sign glyphs, and colour are settled by
`money_kind` in `cli-output-coherence.md`, not here.

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

A report's declared `columns` tuple must be in the **same order** as the model's
SQL projection. The declaration is metadata, not a projection: making it mirror
costs nothing and removes the trap where an author reorders it expecting an
effect and gets none.

## Enforcement

Honest about what is and is not caught.

**Guarded, unit tier — `reports` and the report specs.** SQLMesh resolves a
`reports` model's ordered columns offline (`Context(paths=…)`,
`model.columns_to_types`), and `DataClass` / `money_kind` supply the categories
Rule B needs. Three assertions: the projection's category sequence is
non-decreasing under Rule B, `ReportSpec.columns` mirrors the projection, and
`default_columns` is non-decreasing under Rule B.

The guard skips `AGGREGATE` columns entirely, for the reason given above. It
also cannot notice a **mis-declared** class — `balance_drift.days_since_assertion`
is declared `TXN_DATE` and holds an integer day count, so the guard would place
it among the dates and be satisfied. Ordering rules assume the classes are
right; fixing a wrong one is its own change.

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
