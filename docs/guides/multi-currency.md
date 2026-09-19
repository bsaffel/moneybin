<!-- Last reviewed: 2026-09-12 -->
# Multi-currency

Every transaction, balance, and investment event keeps the currency it arrived in, and no report adds two currencies into one figure without a stored rate behind it. Declare a home currency and the three reports whose rows are single dated events price themselves into it at read time; the five that aggregate keep one sub-total per currency. Rates come from Frankfurter's ECB reference series, your own corrections outrank them, and a report's own read-time conversion is never written to disk — every source amount stays exactly as recorded. The one exception is the derived FX-accounting tables a set home currency persists; see [Choose a home currency](#choose-a-home-currency).

Every transcript below is real output from the international demo persona, trimmed only by whole lines:

```bash
uv run moneybin demo --persona international
```

That build ends with `✅ Demo profile 'demo' ready (5 accounts, 1468 transactions, 1324 categorized).` — two calendar years ending on the last complete one, one checking account each in EUR, GBP, CAD, AED, and USD, seed 42. One of the five, AED, is a currency the rate provider does not publish, so the unpriced path is reachable on purpose.

## Where a currency comes from

Nothing is ever assumed to be USD. Each row resolves its currency from its own source, then from its account, and otherwise stays unknown:

| Source | Currency read from |
|---|---|
| OFX / QFX / QBO | the file's `CURDEF` |
| Plaid sync — transactions | `iso_currency_code` alone; Plaid's transaction object carries no unofficial-code fallback |
| Plaid sync — balances, investment holdings/transactions, securities | `iso_currency_code`, or the unofficial code for a currency ISO 4217 does not cover |
| CSV, Excel, Parquet | a `currency` column when the file has one, else the account's |
| `transactions create` | `--currency`, else the account's |
| `investments add` | `--currency`, else the account's |
| The account itself | `accounts set <id> --currency`, else what its source reported |

An account with no currency anywhere in that chain is unknown, and `moneybin system doctor` fails on it rather than guessing, because two unknowns cannot be told apart and would sum into a figure in no unit. The fix is `accounts set <id> --currency EUR` followed by `moneybin transform apply` — the exact remediation `system doctor` prints — since the canonical account table is rebuilt rather than read live; a bare `moneybin refresh` also satisfies it, because its cascade includes that transform step. The [data model reference](../reference/data-model.md#currency-handling) records the resolution order per table.

## What a mixed profile reads like

With no home currency set, every money report sub-totals per currency. `reports net-worth-currencies` prints one row per currency and `reports net-worth-accounts` one row per account:

```console
$ uv run moneybin reports net-worth-currencies
Using profile: demo
┏━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━━━━━━━━━┓
┃ currency_code ┃ balance_date ┃ net_worth ┃ net_worth_home ┃
┡━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━━━━━━━━━┩
│ AED           │ 2025-12-27   │ 40,748.33 │              - │
│ CAD           │ 2025-12-27   │ 14,035.34 │              - │
│ EUR           │ 2025-12-27   │ 61,072.11 │              - │
│ GBP           │ 2025-12-27   │  8,786.52 │              - │
│ USD           │ 2025-12-27   │  6,294.20 │              - │
└───────────────┴──────────────┴───────────┴────────────────┘
4 of 13 columns shown — --wide for all
$ uv run moneybin reports net-worth-accounts
Using profile: demo
┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━┓
┃ account_name                  ┃ currency_code ┃ account_balance ┃ account_balance_home ┃
┡━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━┩
│ Barclays checking             │ GBP           │        8,786.52 │                    - │
│ Chase Bank checking …0005     │ USD           │        6,294.20 │                    - │
│ Emirates NBD checking         │ AED           │       40,748.33 │                    - │
│ ING checking …0001            │ EUR           │       61,072.11 │                    - │
│ RBC Royal Bank checking …0003 │ CAD           │       14,035.34 │                    - │
└───────────────────────────────┴───────────────┴─────────────────┴──────────────────────┘
4 of 14 columns shown — --wide for all
```

In JSON the headline row per currency carries `net_worth` and the account rows carry `account_balance`; `summary.display_currency` is `null`, which says no conversion happened. The doctor names the consequence once, as a warning rather than a failure, because every figure is correct in its own unit:

```console
$ uv run moneybin system doctor
Using profile: demo

65 invariants checked across 1,468 transactions — 64 passing, 1 warn, 0 skipped
```

The `currency_integrity` warning line is trimmed from the transcript above. It says the profile holds 5 currencies (AED, CAD, EUR, GBP, USD), that reports sub-total each currency separately and withhold any combined figure, that a transaction denominated differently from its account is left out of that account's carried daily balance and shows up as that account's reconciliation drift in `reports balance-drift`, and that setting a home currency and running `moneybin refresh` reads the converting reports in one currency — naming `moneybin fx set <from> <to> <date> <rate>` as the only way to fill a pair the provider does not publish, which on this profile is exactly AED's case.

The second sentence is the one rule that reaches below the reports: a daily balance carries forward only the transactions in the currency it is carrying. A EUR charge on a USD account is not added to the USD balance, and the difference shows up as that account's drift instead of as a wrong total. A profile that holds one currency never sees any of this.

## Choose a home currency

`home_currency` is a setting in the profile's database, not in `config.yaml`, so the report guards can read it. It starts unset on every profile:

```console
$ uv run moneybin profile set home_currency EUR
Using profile: demo
Restating 1 transform model(s)
Transform restatement completed in 9.78s
✅ Set home_currency=EUR
```

The restatement rebuilds the currency-lot models that depend on which currency is home (see [What is not built yet](#what-is-not-built-yet)) — concretely, when cached rates and prior conversions already exist, `core.bridge_currency_conversions.home_value`, `core.fct_currency_lots.cost_basis_total`/`cost_basis_remaining`, and `core.fct_realized_fx_gains` are recomputed and persisted in the new home currency. Setting it converts nothing at the source: every raw and core transaction and balance amount keeps its original currency and value. Two layers price into home currency, and only two: those derived FX-accounting tables the restatement just rebuilt, and the read-time `--display-currency` conversion described below — the setting names the default target for the second layer. `profile show` lists it under `Settings (database)`, the setting takes any ISO 4217 code, and there is no dedicated unset command; changing it means setting another code. Each `profile set` writes an audit row, so `moneybin system audit undo <operation_id>` reverses one — including back to unset, when it is the first home-currency write on the profile — subject to the normal later-write guard (undo refuses once a later operation touched the same setting).

## Gather rates

Rates are fetched during `moneybin refresh`, never during a report read, because a read opens the database read-only and a fetch has to write to the cache. The `rates` step runs last in the cascade and asks the provider for one span per foreign currency, from the earliest date that currency appears in your rows through today, into the home currency and into every currency declared with `profile set display_currency_targets` (see [Read a report in one currency](#read-a-report-in-one-currency) below). `--step rates` runs that step alone:

```console
$ uv run moneybin refresh --step rates
Using profile: demo
No AED/EUR series published for the requested range
No exchange rate series is published for AED/EUR
Discarded 1 out-of-window rate(s) for CAD/EUR
Discarded 1 out-of-window rate(s) for USD/EUR
Rate backfill: 4 pair(s) planned, 2091 rate(s) written, 0 pair(s) failed, 1 pair(s) unsupported, 2 pair(s) with discarded rate(s)
⚠️  Exchange rate coverage is short for CAD/EUR, USD/EUR. Conversion may be incomplete on those dates.
Pipeline:
  Rates: 2091 written
Partial refresh complete (steps: rates; best-effort failures above)
```

One warning line is trimmed above: `⚠️  No exchange rate series is published for AED/EUR. Record these rates yourself with moneybin fx set.` A pair the step could not fill is reported as one of three kinds, because the remedies differ. A *failed* pair is a provider call that raised; the next refresh retries it. An *unsupported* pair is a currency the provider does not publish at all, so it will answer the same way forever and the report points at `moneybin fx set`. A *discarded* pair answered, but the answer did not cover the whole window, or a rate in it fell outside the requested dates; the warning says coverage may be short rather than naming a remedy. Here AED is unsupported, and the two discards are single rates the provider returned for a date outside the window, which is routine.

Only currency codes and dates leave the machine. The provider is Frankfurter, which republishes the ECB's daily reference rates without a credential, and a code that does not look like ISO 4217 is never sent. A profile with neither a home currency nor a declared display target fetches nothing.

## Read one rate

`fx rate` answers for one pair on one date and names where the number came from. Precedence is your own correction, then the cached provider rate, then one live fetch, which is also cached. A weekend resolves to the last business day the provider published, and the answer says so rather than presenting it as the weekend's own rate:

```console
$ uv run moneybin fx rate USD EUR 2025-06-30
Using profile: demo
1 USD = 0.85324000 EUR on 2025-06-30  (frankfurter)
$ uv run moneybin fx rate USD EUR 2025-06-28
Using profile: demo
1 USD = 0.85441000 EUR on 2025-06-27, the last rate published on or before 2025-06-28  (frankfurter)
```

`fx list` prints a pair's stored series newest first and never fetches. One row per date, showing the rate that applied and the source that won it:

```console
$ uv run moneybin fx list GBP EUR --since 2025-12-22
Using profile: demo
┏━━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━━━━━━┓
┃ date       ┃ rate       ┃ source      ┃
┡━━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━━━━━━┩
│ 2026-09-11 │ 1.16530000 │ frankfurter │
│ 2026-09-10 │ 1.16390000 │ frankfurter │
│ 2026-09-09 │ 1.16420000 │ frankfurter │
│ 2025-12-24 │ 1.14560000 │ frankfurter │
│ 2025-12-23 │ 1.14560000 │ frankfurter │
│ 2025-12-22 │ 1.14390000 │ frankfurter │
└────────────┴────────────┴─────────────┘
```

Rates are stored to 8 decimal places. `moneybin refresh`'s rate step never checks the cache before fetching: it re-asks the provider for the whole implied span, per pair, on every run, because reading stored dates as coverage would strand any gap between two separately-fetched spans, and a date-set model would re-request every weekend and holiday forever. The store then keeps only the first answer written for a given pair and date — a conflicting re-offer is discarded — so a later revision to an already-stored date never arrives no matter how many times refresh re-asks for it. Its coverage check only looks at the span's two edges (whether the earliest stored rate reaches the window's opening day, and the latest reaches near its closing day), never at whether every date between them landed a row, so a clean, warning-free backfill can still hide an interior weekday gap exactly as a discarded or interrupted one can. `fx list` reads only what's on disk and never fetches, so it cannot tell that gap apart from an ordinary weekend or ECB holiday either way.

## Record your own rate

`fx set` records a rate for one pair and date that outranks every provider rate for that date and leaves every other date alone. It is the remedy for an unsupported pair and for a day your bank priced differently from the ECB mid-rate. The UAE dirham is pegged at 3.6725 per US dollar, so its euro rate on a date is the ECB's USD/EUR rate divided by the peg:

```console
$ uv run moneybin fx rate USD EUR 2025-12-19
Using profile: demo
1 USD = 0.85383000 EUR on 2025-12-19  (frankfurter)
$ uv run moneybin fx set AED EUR 2025-12-19 0.23249 --note "AED is pegged at 3.6725 per USD; ECB USD/EUR of 2025-12-19 divided by the peg"
Using profile: demo
Restating 1 transform model(s)
Transform restatement completed in 8.81s
✅ Recorded 1 AED = 0.23249 EUR on 2025-12-19
$ uv run moneybin fx list AED EUR
Using profile: demo
┏━━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━━━┓
┃ date       ┃ rate       ┃ source   ┃
┡━━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━━━┩
│ 2025-12-19 │ 0.23249000 │ override │
└────────────┴────────────┴──────────┘
```

The rate is units of the second currency per one unit of the first, must be positive, and is written with an audit-log row. A correction is one rate per pair per date, not a per-transaction spread: two same-day conversions at different effective rates are one rate here, and the difference is realized FX gain or loss, which `moneybin reports realized-fx` reports per consumed lot (see [What is not built yet](#what-is-not-built-yet) for the tie-out still open). `fx delete AED EUR 2025-12-19` withdraws the correction; it is the only way to withdraw one, since `set` can only change the number. A provider rate reappears for that date only if the backfill already cached one before the override was set — deleting AED/EUR here leaves the date unpriced, because AED is not a pair Frankfurter publishes and the backfill stores nothing for an unsupported pair. Reports that need it fall back to per-currency sub-totals until another rate is recorded. The deletion is recorded like every other write: `moneybin system audit undo <operation_id>` restores the removed override, subject to the normal later-write guard.

## Read a report in one currency

Five reports convert, because each of their rows is one event on one date: the three net-worth reports (`net-worth`, `net-worth-currencies`, `net-worth-accounts`) at their balance date, `large-transactions` at its transaction date, and `balance-drift` at its assertion date. Each row prices at the rate for its own date, never at one as-of rate applied across the range. With a home currency set they convert by default; `--display-currency` on any report command, and `display_currency` on the `reports` MCP tool, name another target. With every rate the read needs on disk — here after `moneybin transform apply` has picked up the refreshed rates and the AED override above — net worth collapses to one figure:

```console
$ uv run moneybin reports net-worth --from-date 2025-12-19 --to-date 2025-12-19
Using profile: demo
┏━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━┓
┃ balance_date ┃ unpriced_currency_count ┃ net_worth ┃
┡━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━┩
│ 2025-12-19   │ 0                       │ 94,915.31 │
└──────────────┴─────────────────────────┴───────────┘
3 of 9 columns shown — --wide for all
$ uv run moneybin reports net-worth-accounts --from-date 2025-12-19 --to-date 2025-12-19
Using profile: demo
┏━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━┓
┃                    ┃               ┃ original_currency_ ┃                 ┃                      ┃
┃ account_name       ┃ currency_code ┃ code               ┃ account_balance ┃ account_balance_home ┃
┡━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━┩
│ Barclays checking  │ EUR           │ GBP                │       10,051.45 │            10,051.45 │
│ Chase Bank         │ EUR           │ USD                │        5,342.58 │             5,342.58 │
│ checking …0005     │               │                    │                 │                      │
│ Emirates NBD       │ EUR           │ AED                │        9,568.07 │             9,568.07 │
│ checking           │               │                    │                 │                      │
│ ING checking …0001 │ EUR           │ EUR                │       61,265.33 │            61,265.33 │
│ RBC Royal Bank     │ EUR           │ CAD                │        8,687.88 │             8,687.88 │
│ checking …0003     │               │                    │                 │                      │
└────────────────────┴───────────────┴────────────────────┴─────────────────┴──────────────────────┘
5 of 15 columns shown — --wide for all
💱 Converted from AED, CAD, GBP, USD using 4 stored rates; run 'moneybin fx rate <from> EUR <date>' for one of them, or --output json for all
```

Every converted figure traces to a stored rate. Under `--output json`, `summary.applied_rates` lists one entry per pair and date the read used, with the rate, its source, the date requested, and the date actually priced, and each row carries `original_currency_code` and `rate_source` so a figure can be tied back to the entry that priced it.

### When a rate is missing

A converting report needs a rate for every foreign row it fetches, not only the ones it returns: a capped read pulls one extra row past the cap to detect truncation, converts the whole fetched set first, then drops that extra row — so a missing rate on the discarded row alone still degrades the response. When any row's rate is missing, the whole report falls back to per-currency sub-totals rather than converting the rows it can and leaving the rest, because a figure that mixes converted and unconverted rows is worse than no figure. The home-currency default falls back quietly, so a profile that has set one is not warned on every read it cannot price; ask for a currency explicitly and the reason is printed, and lands in `summary.degraded_reason` under `--output json`:

```console
$ uv run moneybin reports large-transactions --top 2 --display-currency EUR
Using profile: demo
┏━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━━━━┓
┃ account_name     ┃ description      ┃ currency_code ┃ txn_date   ┃    amount ┃
┡━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━━━━┩
│ Emirates NBD     │ SALARY TRANSFER  │ AED           │ 2025-04-01 │ +2,448.00 │
│ checking         │ Gulf Data        │               │            │           │
│                  │ Partners FZ LLC  │               │            │           │
│ RBC Royal Bank   │ EFT DEPOSIT      │ CAD           │ 2025-11-01 │   +816.00 │
│ checking …0003   │ Maple Line       │               │            │           │
│                  │ Advisory Inc     │               │            │           │
│ ING checking     │ SALARIS Meridian │ EUR           │ 2025-05-01 │ +4,715.00 │
│ …0001            │ Analytics BV     │               │            │           │
└──────────────────┴──────────────────┴───────────────┴────────────┴───────────┘
5 of 13 columns shown — --wide for all
⚠️  no stored AED->EUR rate for some of the dates in this report; run 'moneybin refresh' to gather the missing dates
```

Seven of the ten rows are cut above. The AED rows are dated on days that carry no AED rate, since the one recorded so far covers 2025-12-19 only, so the report stays in original currencies. `--top` ranks within each currency and interleaves the results, so a truncated list still holds every currency; a converted read also returns the two z-score columns and `is_top_100` as `null` for every repriced row, because those were scored against the row's original currency and a per-date conversion is not one scaling of them.

`refresh` gathers rates into the home currency and into every currency declared with `profile set display_currency_targets` (comma-separated, e.g. `display_currency_targets USD`), and only for the currencies your rows hold. A target that has never been declared — home or otherwise — falls back until its own rates are stored: the closing warning's "run `moneybin refresh`" half only helps once USD is a declared target or the home currency, and this profile has declared neither, so EUR, its home currency, is the only target refresh already reaches. Declaring USD and refreshing again would fetch USD rates for every currency the provider covers, without touching `fx rate` or `fx set`. Short of declaring a target, a supported pair still has a manual-free path: `fx rate <from> USD <date>` fetches and caches a live rate on demand, one pair and date at a time. AED is not that case — the provider does not publish it at all, so `fx rate AED USD` fails the same way `refresh` does even once USD is a declared target, and `fx set` is the only remedy for AED specifically:

```console
$ uv run moneybin reports net-worth-currencies --from-date 2025-12-19 --to-date 2025-12-19 --display-currency USD
Using profile: demo
┏━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━━━━━━━━━┓
┃ currency_code ┃ balance_date ┃ net_worth ┃ net_worth_home ┃
┡━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━━━━━━━━━┩
│ AED           │ 2025-12-19   │ 41,154.76 │       9,568.07 │
│ CAD           │ 2025-12-19   │ 14,035.34 │       8,687.88 │
│ EUR           │ 2025-12-19   │ 61,265.33 │      61,265.33 │
│ GBP           │ 2025-12-19   │  8,804.70 │      10,051.45 │
│ USD           │ 2025-12-19   │  6,257.19 │       5,342.58 │
└───────────────┴──────────────┴───────────┴────────────────┘
4 of 13 columns shown — --wide for all
```

The closing warning is trimmed above; it reads `⚠️  no stored AED->USD rates at all; run 'moneybin refresh' to gather them, and record one with 'moneybin fx set' if refresh reports the pair unsupported`. `net_worth` stays in each row's own currency; `net_worth_home` is still the EUR home-currency figure. `moneybin export report` never converts: an export outlives the rate that made it, so it always carries the original amount and currency.

### A published date, not just a business day

The cache resolves a weekend back to the preceding Friday, but it never hops a weekday, because a missing weekday is ambiguous between a market holiday and a date nobody fetched yet. The demo's last balance date is Saturday 2025-12-27, and the ECB published nothing on 25 or 26 December, so the default `reports net-worth` on this profile reports no combined figure even with every pair covered, and the explicit ask names `CAD->EUR` as missing for some dates. `fx rate CAD EUR 2025-12-27` still answers, because the live path resolves back to 24 December; the report's read-only path cannot. Two ways through: `--from-date` and `--to-date` on a published date, as the transcripts above do, or `fx set` for that exact date, which an override always wins. Pricing a holiday-dated row from the last published day before it, offline, is designed and not built.

### Reports that stay per currency

The other four reports aggregate with the currency in their grouping key, so a row is already a sub-total for one currency and stays one whatever the home currency is. Net worth over time is not one of them: `net-worth --interval` buckets the single home-currency total only, so a bucket whose last day holds an unpriced currency reports no figure — on this profile, AED on every date but the one override:

```console
$ uv run moneybin reports net-worth --interval monthly --from-date 2025-10-01 --to-date 2025-12-31
Using profile: demo
┏━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━━━━━┓
┃ balance_date ┃ unpriced_currency_count ┃ net_worth ┃ change_abs ┃
┡━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━━━━━┩
│ 2025-10-31   │ 1                       │         - │          - │
│ 2025-11-30   │ 1                       │         - │          - │
│ 2025-12-27   │ 1                       │         - │          - │
└──────────────┴─────────────────────────┴───────────┴────────────┘
4 of 11 columns shown — --wide for all
```

`spending`, `cashflow`, `recurring`, and `merchants` carry `currency_code` in the same position, and each ranks within a currency before ordering across them, so a `--limit` smaller than the number of currencies still holds every currency that fits. A report you save with `reports create` inherits whatever its own SQL does; the built-ins group by `currency_code`, and keeping currencies apart in your own query is on you. `moneybin sql query` over any `reports.*` view returns the stored, unconverted figures, which is the way to read a converting report in original currencies on a home-currency profile.

## From an AI client

The `profile` tool reads the home currency, which is `null` until you choose one, and `profile_set(home_currency="EUR")` sets it with an audit row that `system_audit_undo` reverses. `reports(report_id="core:net_worth", display_currency="EUR")` is the converted read, with `summary.applied_rates` and `summary.degraded_reason` exactly as above. `investments(view="holdings")` totals a mixed-currency portfolio into the home currency at each position's own close rate and publishes `data.applied_rates` beside the total; with no covering rate the total is `null` and `data.market_value_by_currency` gives the split. There is no rate tool: `fx rate`, `fx set`, and `fx delete` are CLI-only, and so is `refresh --step rates`, which `refresh_run` includes as the last step of its full cascade. The [MCP tool reference](../reference/mcp-tools.md) lists every parameter.

## What is not built yet

- **Realized FX gain or loss lacks a deliberate statement tie-out.** `moneybin reports realized-fx` (`reports.realized_fx` in SQL, `reports(report_id='core:realized_fx')` from an AI client) reports it per consumed lot: an accepted transfer between accounts in two currencies becomes a row in `core.bridge_currency_conversions`, foreign currency you hold is tracked as lots in `core.fct_currency_lots` under the account's cost-basis election, and disposing it writes `core.fct_realized_fx_gains`. What remains is a deliberate EUR/USD bank-statement expectation checked to within $0.01; which conversions the engine can and cannot cover is recorded with a reason per row, so a query over those tables says where its own coverage stops.
- **Investment positions do not count toward net worth**, in any currency. `investments holdings` values them; `reports net-worth` reads balances only.
- **Coverage is checked at a span's edges.** `refresh` reports a series that starts late or stops early; a single weekday missing from the middle of a provider's answer passes unnoticed and surfaces only when a read on that date falls back.
- **Two currencies on one account are drift.** A transaction denominated differently from its account sits out of that account's carried balance, as the doctor says. Converting it into the carry needs a rate at the `core` layer, which is not where conversion runs.

The design and the record of what shipped when are in the [multi-currency spec](../specs/multi-currency.md).
