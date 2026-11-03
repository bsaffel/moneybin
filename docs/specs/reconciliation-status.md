# Reconciliation Status

> Last updated: 2026-11-03
> Status: implemented
> Address: M1J.9 (Investments — reconciliation status on derived investment figures)
> Type: Feature
> Owns: the `reconciliation_status` column contract shared by `core` models, its
> investment predicate, and how unreconciled investment figures are disclosed
> Refines: [`investments-data-model.md`](investments-data-model.md) and
> [`investment-source-choice.md`](investment-source-choice.md)
> Constrained by:
> [`investment-event-matching.md`](investment-event-matching.md) §"Visible-collision guard and promotion",
> [`core-updated-at-convention.md`](core-updated-at-convention.md),
> [`materialization-freshness-contract.md`](materialization-freshness-contract.md), and
> [`observability.md`](observability.md)
> Closes: GitHub issue #556

## One-line goal

Every figure derived from an investment account whose history arrives from two
sources carries, in the data itself, the fact that it is unreconciled, and
every surface that shows it says why and how to fix it.

## Why this exists

An account can carry investment history from manual recording and from sync at
once. Until investment matching (M1J.7) can accept a Match, nothing collapses
those histories, so every event in such an account exists twice. Lots
double-count, cost basis mixes two accountings, and realized gains, which feed
tax filings, are wrong.

Measured on `main` at `fd81f46e3`:

| Surface | Today for an overlapping account |
|---|---|
| `core.dim_holdings` | Blanks five valuation figures under `valuation_status = 'source_overlap'` (#541); `quantity` and `cost_basis` still publish the doubled figures |
| `core.fct_investment_lots`, `core.fct_realized_gains` | Every figure published, no marker |
| `core.fct_currency_lots`, `core.fct_realized_fx_gains` | A duplicated foreign-currency security sale opens a duplicate currency lot (`currency_lots/sqlmesh_loader.py:1144`), so FX gains double too, with no marker |
| CLI and MCP holdings, lots and gains reads | `degraded_reason` warning with the remedy (`investment_service.py:1829`) |
| Portfolio total (`total_market_value`) | Sums only valued positions, so an overlapping account's blanked positions silently drop out (`investment_service.py:2083`) |
| Bundle export (`exports/catalog.py:59-62`) and `sql_query` | No marker and no warning |

The data reaching a spreadsheet or another tool carries nothing that says it is
doubled. The one place that withholds, holdings, does so inconsistently: it
blanks the valuation but publishes the doubled share count, and the blanks
quietly shrink the portfolio total.

### Disclosure, not withholding

The product decision for this spec is to publish the figures MoneyBin has and
annotate them. A number that looks wrong, arrives with a warning, and names its
fix is more useful than a blank: people read partial numbers as information,
and withholding has produced more frustration than confidence. MB-97's
containment criterion is satisfied by disclosure. An unreconciled figure never
enters a result looking trustworthy, because the marker travels with the row
into every surface and every warning names the remedy.

The investment-event-matching spec keeps its guard after acceptance ships,
because one accepted Match does not make the remaining rows safe. The
reconciled/unreconciled state therefore outlives this release and needs a
durable home in the data rather than in each reader.

## The concept

**Reconciliation status** (glossary: [`CONTEXT.md`](../../CONTEXT.md)) is whether
a published figure rests only on records whose cross-source correspondence is
settled. It is a consequence, never an input:

| | `app.match_decisions.match_status` | `reconciliation_status` |
|---|---|---|
| Describes | A Decision about one proposed pair | A published figure |
| Lives in | `app` (authored) | `core` (derived) |
| Values | `pending` / `accepted` / `rejected` | `reconciled` / `unreconciled` |
| Covers | Proposed pairs only | Every row of a model that carries it |

No writer, CLI command, MCP tool or migration sets `reconciliation_status`. It
is recomputed from Decisions, source choices and the data on every build.

## Goals

1. One column, `reconciliation_status VARCHAR NOT NULL`, with exactly two values,
   `reconciled` and `unreconciled`, on every `core` model whose figures derive
   from the investment ledger.
2. Every figure stays published. An unreconciled row carries the marker; it is
   not blanked, removed or zeroed.
3. Every surface that shows an unreconciled figure, or a total built from one,
   says so: CLI and MCP through the envelope warning and `degraded_reason`, the
   bundle export through its manifest, and `sql_query` and other readers through
   the column.
4. One predicate, computed once, read by every model and service.
5. The existing remedy clears it: choosing an `investment_source_type` for the
   account makes every row `reconciled` on the next refresh.
6. The column is shaped so `core.fct_transactions` can adopt it under MB-89
   with the same name and values.

## Non-goals

- Investment-event matching, acceptance or Golden materialization (M1J.7,
  #529).
- Adopting the column on `core.fct_transactions`. MB-89 owns that, including
  its predicate (membership in a `pending` dedup Match).
- Changing `system doctor`'s `investment_source_overlap` detection, which stays
  `fail` and reads Raw (`investments/source_overlap.py`).
- Changing the `withheld` or `unpriced` valuation states. See "Known
  interaction" below.
- Deleting, merging or rewriting any source row.
- Choosing a source on the user's behalf.

## Anti-goals

This fails even if it technically works when:

- An unreconciled figure reaches any surface without its marker or warning.
- A total that includes an unreconciled figure is presented without saying how
  many accounts in it are unreconciled.
- Unreconciled rows disappear, or their figures are replaced with NULL or zero.
- Any reader recomputes the predicate instead of reading the column.
- A path exists to set the column directly.
- An account with one source changes in any published value.

## Decisions

### D1. The predicate lives on `core.fct_investment_transactions`

`core.fct_investment_transactions` gains `reconciliation_status`, computed per
account in that model. It is `unreconciled` when the account's ledger rows,
excluding `subtype = 'opening_bootstrap'`, carry more than one distinct
`source_type`. That is the predicate `dim_holdings.sql`'s
`source_overlap_accounts` CTE and `InvestmentService._source_overlap_accounts`
use today, moved to one place. Because the model already applies the
`investment_source_type` filter (`fct_investment_transactions.sql:133`), an
account with a source chosen is `reconciled` without further logic.

The flag is account-scoped on purpose. One duplicated buy changes which lots
every later sale of that security consumes, so even a lot opened by a
single-copy event in an overlapping account is wrong. The
investment-event-matching spec keeps its guard account-level for the same
reason.

As shipped, one `account_reconciliation` CTE over the filtered ledger computes
the status and the account's newest row timestamp (D6) once per account, and
every row joins it with `IS NOT DISTINCT FROM`, so a ledger row whose
`account_id` is NULL keeps a status rather than dropping out. The two existing
copies of the predicate are deleted and replaced by reads of the column. The doctor's Raw-level detection answers a different question, since
it must fire before any transform, and stays separate.

### D2. Derived models inherit the marker and publish their figures

`core.fct_investment_lots`, `core.fct_realized_gains`, `core.dim_holdings`,
`core.fct_currency_lots` and `core.fct_realized_fx_gains` each gain
`reconciliation_status` and compute every figure as they do today.

In `prep` and `core` the column takes Rule A's string block
(`.claude/rules/column-ordering.md`), placed immediately before
`currency_code`, with no other column moving. `core.dim_holdings` reads it from
the position's open lots: `unreconciled` when any of them is.

### D3. #541's holdings blanking is removed

`core.dim_holdings` stops blanking `market_value`, `unrealized_gain`,
`price_date`, `price_source` and `days_since_observed` for an overlapping
account. `valuation_status` drops the `source_overlap` value and goes back to
describing price and share-count state only (`valued`, `carried_forward`,
`unpriced`, `withheld`). The account-level fault is carried by
`reconciliation_status`.

This is a pre-launch breaking change to a `core` value domain and to shipped
behavior. It is disclosed in the CHANGELOG, and every mention of
`valuation_status = 'source_overlap'` and of the holdings withholding is
updated: `investments-data-model.md`, `investment-source-choice.md`,
`investment-event-matching.md` §"Visible-collision guard and promotion", the
roadmap's M1J row, the `dim_holdings.sql` header and withhold CTE comments, the
MCP and CLI help text (`mcp/tools/investments.py:174,1547`,
`cli/commands/investments/__init__.py:364`), the `PRICE_RESOLUTION_STATUS_TOTAL`
label set, and the existing #541 tests that assert NULL figures.

**Known interaction.** The `withheld` state still blanks valuation when a broker
snapshot contradicts the ledger's share count. A doubled ledger in a
Plaid-synced account usually contradicts the broker's snapshot, so many of an
overlapping account's synced positions will still read `withheld`, with NULL
market value, through that pre-existing clause. That is a separate state with
its own remedy. Whether `withheld` should also move to disclosure is a separate
decision, recorded here and not taken.

### D4. Service reads warn, and totals say what they contain

`holdings`, `lots` and `gains` keep `summary.degraded_reason` with the existing
`investment_source_overlap:` code and remedy text (`investment_service.py:1829`),
now derived from `reconciliation_status` on the returned rows rather than a second
ledger scan. Row types expose `reconciliation_status`.

Totals publish over every row with a figure, unreconciled included. When any
contributing row is unreconciled, the result's warning names how many accounts in
the total are unreconciled and the remedy, and `degraded_reason` is set. This
applies to `total_market_value` and `market_value_by_currency`, and to any
summary the CLI or MCP computes over lots or gains.

The CLI text table marks an unreconciled row visibly and prints the warning
beneath. As shipped, `investments holdings`, `investments lots` and
`investments gains` add a `reconciliation` column to the default column set
only when at least one rendered row is `unreconciled`; that column reads
`unreconciled` on such rows and `-` on the rest, so a clean table carries no
column of dashes. `--output json` always carries `reconciliation_status` on
every row, whatever the table shows.

### D5. Lot selection warns

`investments lots select` and `investments_lots_select` keep working for a
disposal in an unreconciled account. The response carries the same warning and
remedy, because a selected lot may be one of a duplicated pair. The selection
is still written. `InvestmentService.select_lots` returns a
`LotSelectionResult` (`warnings`, `degraded_reason`) that both surfaces
render.

### D6. Freshness

A status flip is an input change. `core.fct_investment_transactions.updated_at`
folds `investment_source_type_changed_at` on every row, and on an `unreconciled`
row also the account's newest row timestamp (`MAX(row_updated_at)` over the
account's filtered ledger rows), so a second source's event advances every row
it flags. The derived models inherit those timestamps from the ledger rows they
read: lots and gains through their per-position `MAX(updated_at)`, currency
lots and FX gains through the sale event's timestamp, and `core.dim_holdings`
through its open lots plus its own `investment_source_type_changed_at` term.
Choosing or clearing a source therefore never reports an unchanged watermark.

**Known limitation: row removal can rewind.** The account-level fold applies
only while the account is `unreconciled`. When the second source's rows are
deleted rather than filtered — `import_revert` of the redundant batch, or a
sync disconnect purge — the account flips to `reconciled`, the fold stops, and
`updated_at` on its surviving rows, and on the lots, gains, currency lots, FX
gains and holdings that inherit from them, can fall back to an earlier value.
The deleted rows leave nothing to fold. Closing it needs a persisted,
account-scoped deletion time, which those paths do not record today.
[`core-updated-at-convention.md`](core-updated-at-convention.md) §"Known
limitation: the investment ledger can rewind when a second source's rows are
removed" carries the full statement. No current consumer polls these columns as
an incremental cursor.

### D7. Bundle export discloses

The bundle inherits the column from `core`. Its manifest also reports, per table
that carries the column, how many exported rows are `unreconciled`, so a consumer
reading only the manifest learns that the bundle contains doubled history.

As shipped, each manifest table entry whose columns include
`reconciliation_status` carries `unreconciled_row_count`, counted over the
exported rows: `0` for a table with no rows or only reconciled rows, absent on a
table without the column. Today that is `investment_transactions`,
`investment_lots`, `realized_gains` and `holdings`; the currency-lot and FX-gain
models are not in the bundle catalog. The field is additive, so
`ARTIFACT_VERSION` stays `2`.

### D8. Privacy and catalog

`reconciliation_status` is classified in `privacy/taxonomy.py` for every model
that carries it, at the class used for closed-vocabulary status columns such as
`valuation_status`. Column comments follow `.claude/rules/database.md`.

### D9. Observability

One gauge, `moneybin_reconciliation_unreconciled_accounts`, labelled by `ledger`
(`investment` now, `cash` reserved for MB-89). It is refreshed when the
`investment_source_overlap` doctor check runs, beside
`INVESTMENT_SOURCE_CHOICE_ACCOUNTS`, from
`unreconciled_investment_accounts(db)` in `investments/source_overlap.py`, which
counts distinct accounts with `unreconciled` ledger rows. A failure to refresh
it is logged and does not change the check's result. `PRICE_RESOLUTION_STATUS_TOTAL` drops its
`source_overlap` label (D3).

### D10. The cash contract, recorded now, built under MB-89

When `core.fct_transactions` adopts the column it uses the same name, the same
two values and the same never-authored rule. A transaction is `unreconciled`
while it belongs to a `pending` dedup Match. It follows the same disclosure
model: the figure stays published and the row is marked.

### D11. Scope and inheritance details

- **Rows with no investment parent.** A currency lot opened by a cash conversion
  or a Transfer, and an FX gain consuming only such lots, is `reconciled`. A
  currency lot's status is that of the investment
  event that opened it. A lot a Transfer moved into another account is
  `reconciled` even when it descends from an unreconciled sale: the transferred
  lot is opened by the Transfer, not by the sale. An FX gain is `unreconciled`
  when the lot it consumes is; an FX gain with no consumed lot (an incomplete
  disposal) is `reconciled`.
- **FX `coverage_status`.** `fct_realized_fx_gains.coverage_status`
  (`complete` / `incomplete`) describes missing records. `reconciliation_status`
  describes doubled records. Both columns are kept, because the remedies differ.
- **Where the derived models get the status.** `fct_investment_lots` and
  `fct_realized_gains` already declare `core.fct_investment_transactions` in
  `depends_on`, so no dependency edge is added. They read the column from the
  ledger rows they already load, and the cost-basis engine passes it through
  untouched. The overlap timestamp D6 folds is `MAX(updated_at)` over the
  account's ledger rows. That is a freshness input, not a second copy of the
  predicate.
- **Bootstrap-only accounts.** An account whose only rows are
  `opening_bootstrap` rows, or bootstrap rows plus one other source type, is
  `reconciled`.

## Interfaces

| Surface | Change |
|---|---|
| `core.fct_investment_transactions`, `core.fct_investment_lots`, `core.fct_realized_gains`, `core.dim_holdings`, `core.fct_currency_lots`, `core.fct_realized_fx_gains` | New `reconciliation_status` column |
| `core.dim_holdings` | Figures no longer blanked for overlap; `valuation_status` loses `source_overlap` |
| CLI `investments holdings / lots / gains`, MCP `investments` | Rows carry the status; unreconciled rows and totals are marked and warned |
| CLI `investments lots select`, MCP `investments_lots_select` | Warns for unreconciled accounts |
| Bundle export | Manifest reports unreconciled row counts per table |
| `system doctor` | Unchanged |
| Web | No web surface exists today; none added |

No MCP tool or CLI command is added.

## Resolution path

The user chooses which source feeds the account:
`moneybin accounts set <account> --investment-source-type manual|plaid`, or
`accounts_set` over MCP. The next refresh rebuilds the ledger from one source and
every derived row becomes `reconciled`. Clearing the setting brings the
overlap back. Once M1J.7 can accept Matches, accepting them becomes a second
path, under the promotion rules in that spec.

## Verification

Unit and model tests:

- Each model in D2 carries `unreconciled` on every row of an overlapping account
  and `reconciled` elsewhere, and its figures equal what it computes without the
  marker.
- `dim_holdings` publishes all valuation figures for an overlapping account that
  has no share-count conflict, and `valuation_status` never reads
  `source_overlap`.
- `opening_bootstrap` alone never makes an account unreconciled. This is the
  #541 regression test, moved to the new column.
- Setting `investment_source_type` flips every row to `reconciled`; clearing it
  flips them back; `updated_at` advances both ways.
- A single-source account's rows in every model are value-identical to `main`,
  apart from the new column.
- Holdings, lots and gains reads, their totals and lot selection set
  `degraded_reason` and name the unreconciled account count whenever an
  unreconciled row contributes, and never otherwise.
- The bundle manifest's unreconciled counts match the rows exported.

Scenario (`tests/scenarios`): a persona with one overlapping and one clean
investment account, including a foreign-currency sale, proves that:

- every unreconciled figure in every `core` model, report, export and service
  read carries the marker or the warning;
- choosing a source clears both;
- the clean account's figures are unchanged;
- Raw row counts are identical before and after.

Shipped as `tests/scenarios/test_investments_reconciliation_status.py`
(scenario `investments-reconciliation-status`). It also pins that every bundle
carrier table still has rows after the choice and reports
`unreconciled_row_count == 0`, so reconciled rows are proven excluded from the
count.

Gates: `make check`, `make format-sql`, the touched tests, the MCP surface
snapshot regeneration for changed descriptions, and
`tests/test_documentation_policy.py`.

## Left to the builder (resolved)

- The account-scoped flag is one CTE in `fct_investment_transactions` (D1).
- The CLI text-table marker is a conditional `reconciliation` column (D4).
- The manifest field is `unreconciled_row_count` (D7).
- The scenario persona is one overlapping account (a duplicated USD buy, a EUR
  security sale and its conversion back to USD) beside one clean manual
  account.
