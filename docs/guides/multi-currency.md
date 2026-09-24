<!-- Last reviewed: 2026-09-23 -->
# Multi-currency

Every transaction, balance, and investment event keeps the currency it arrived
in. No report adds currencies into one figure without a stored rate behind it.
Set a home currency to price the reports whose rows are single dated events at
read time; aggregated reports retain a subtotal for each currency. Provider
rates come from Frankfurter's ECB reference series. Your own corrections
outrank them, and report-time conversions do not alter source amounts.

## Where a currency comes from

| Source | Currency read from |
|---|---|
| OFX / QFX / QBO | the file's `CURDEF` |
| Plaid transactions | `iso_currency_code` |
| Plaid balances and investments | `iso_currency_code`, then the unofficial code when needed |
| CSV, Excel, Parquet | a `currency` column, else the account's currency |
| Manual transactions or investments | `--currency`, else the account's currency |
| The account | `accounts set <id> --currency`, else what its source reported |

An account with no currency in that chain is unknown. `moneybin system doctor`
reports it rather than guessing. Set the account currency and run
`moneybin refresh` (or `moneybin transform apply`) to rebuild the canonical
account table.

## A reproducible synthetic profile

The transcript in this guide was captured from a fresh, isolated profile with
the `international` persona and seed 42. It contains five accounts in AED,
CAD, EUR, GBP, and USD. Create the profile before generating into it:

```bash
uv run moneybin profile create cli-ux-international --no-init-inbox
uv run moneybin --profile cli-ux-international synthetic generate --persona international --profile cli-ux-international --seed 42
```

The capture generated history from 2024-01-01 through 2025-12-31. The profile
started with no home currency, so net worth stayed split by currency.
`reports net-worth-currencies` prints one row per currency and
`reports net-worth-accounts` one row per account:

```console
$ uv run moneybin --profile cli-ux-international reports net-worth-currencies --no-pager
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

› Run reports(report_id='core:net_worth') for the single home-currency total
› Run reports(report_id='core:net_worth_accounts') for the account-level
breakdown

$ uv run moneybin --profile cli-ux-international reports net-worth-accounts --no-pager
┏━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━┓
┃ account_name        ┃ currency_code ┃ account_balance ┃ account_balance_home ┃
┡━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━┩
│ Barclays checking   │ GBP           │        8,786.52 │                    - │
│ Chase Bank checking │ USD           │        6,294.20 │                    - │
│ …0005               │               │                 │                      │
│ Emirates NBD        │ AED           │       40,748.33 │                    - │
│ checking            │               │                 │                      │
│ ING checking …0001  │ EUR           │       61,072.11 │                    - │
│ RBC Royal Bank      │ CAD           │       14,035.34 │                    - │
│ checking …0003      │               │                 │                      │
└─────────────────────┴───────────────┴─────────────────┴──────────────────────┘
4 of 14 columns shown — --wide for all
```

Each report's third hint, which points to `moneybin profile set home_currency`
because this profile has none, and the account report's other two hints are
trimmed above. The `*_home` columns stay `-` until a home currency is set.

The doctor reports this as a warning: the profile is internally coherent, but
it cannot produce one combined figure until it has rates for the requested
conversion.

## Choose a home currency

`home_currency` is stored in the profile database. Setting it does not rewrite
raw or core amounts. It restates the derived FX-accounting tables whose meaning
depends on the selected home currency, and becomes the default target for
read-time conversion.

```console
$ uv run moneybin --profile cli-ux-international profile set home_currency USD
Profile setting saved
Profile:       cli-ux-international
home_currency: USD
```

Each setting write has an audit row. `moneybin system audit undo <operation_id>`
can reverse one subject to the normal later-write guard.

## Rates and fixture overrides

`moneybin refresh --step rates` fetches rate series during a write operation;
reports only read stored rates. The normal refresh command contacts Frankfurter,
so this guide does not present a fixture run as a live-provider transcript.
It fetches one span for each foreign currency used by the profile, into the
home currency and any declared display-currency target. An unsupported pair or
a missing date remains visible to the report as incomplete coverage.

`moneybin fx set` records an audited correction for one pair and date. It
outranks a provider value for that date. The following captured value is a
synthetic fixture override, not a provider rate:

```console
$ uv run moneybin --profile cli-ux-international fx set AED USD 2025-12-27 0.27200000 --note 'synthetic transcript fixture'
FX override recorded
Pair:    AED/USD
Date:    2025-12-27
Rate:    0.27200000
Outcome: Recorded 1 AED = 0.27200000 USD on 2025-12-27

$ uv run moneybin --profile cli-ux-international fx list AED USD --since 2025-12-27 --no-pager
Exchange rates
Scope:        AED/USD since 2025-12-27
Stored rates: 1
┏━━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━━━┓
┃ date       ┃ rate       ┃ source   ┃
┡━━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━━━┩
│ 2025-12-27 │ 0.27200000 │ override │
└────────────┴────────────┴──────────┘
```

`fx rate` answers a single pair and date. Its precedence is an override, then
a cached provider rate, then a live provider fetch that it caches. `fx list`
never fetches. `fx delete <from> <to> <date>` withdraws one override.

## Read a report in one currency

The fixture uses four explicit overrides at its latest report date: EUR/USD
1.10000000, GBP/USD 1.25000000, CAD/USD 0.75000000, and AED/USD 0.27200000.
With all four stored, net worth collapses into one USD figure, and each
currency's row keeps its source currency beside it:

```console
$ uv run moneybin --profile cli-ux-international reports net-worth --from-date 2025-12-27 --to-date 2025-12-27 --no-pager
┏━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━┓
┃ balance_date ┃ unpriced_currency_count ┃  net_worth ┃
┡━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━┩
│ 2025-12-27   │ 0                       │ 106,066.73 │
└──────────────┴─────────────────────────┴────────────┘
3 of 9 columns shown — --wide for all

› Run reports(report_id='core:net_worth', parameters={'interval': 'monthly'})
for period-over-period change
› Run reports(report_id='core:net_worth_currencies') for the currency-level
breakdown

$ uv run moneybin --profile cli-ux-international reports net-worth-currencies --from-date 2025-12-27 --to-date 2025-12-27 --no-pager
┏━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━━━━━━━━━┓
┃               ┃ original_currenc ┃              ┃           ┃                ┃
┃ currency_code ┃ y_code           ┃ balance_date ┃ net_worth ┃ net_worth_home ┃
┡━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━━━━━━━━━┩
│ USD           │ AED              │ 2025-12-27   │ 11,083.55 │      11,083.55 │
│ USD           │ CAD              │ 2025-12-27   │ 10,526.51 │      10,526.51 │
│ USD           │ EUR              │ 2025-12-27   │ 67,179.32 │      67,179.32 │
│ USD           │ GBP              │ 2025-12-27   │ 10,983.15 │      10,983.15 │
│ USD           │ USD              │ 2025-12-27   │  6,294.20 │       6,294.20 │
└───────────────┴──────────────────┴──────────────┴───────────┴────────────────┘
5 of 14 columns shown — --wide for all
```

The currency report's closing disclosure and two next-step hints are trimmed
above. The disclosure reads "Converted from AED, CAD, EUR, GBP using 4 stored
rates" and points to `moneybin fx rate AED USD 2025-12-27` for any one of them,
or `--output json` for all.
`reports net-worth-accounts` takes the same dates and prices each account the
same way.

`--output json` carries `summary.applied_rates`, including each requested date,
the date actually priced, rate, and source. Every converted row retains
`original_currency_code`, and a row of the currency or account report names
the `rate_source` that priced it. A row on
the three net-worth reports carries two units — its own currency and a
home-currency column beside it (`net_worth_home` and its siblings) — and
`applied_rates` names each rate's own pair, never which of the two it priced.
`summary.home_currency` closes that: it names the home currency actually priced
into a home-basis column on this response, and is absent when no conversion put
a value in one.

If any required rate is missing, a converting report falls back to
per-currency subtotals rather than mixing converted and original values. An
explicitly requested display currency discloses the degradation in text and
`summary.degraded_reason` in JSON.

Five reports convert because their rows are dated events: the three net-worth
reports (`net-worth`, `net-worth-currencies`, `net-worth-accounts`),
`large-transactions`, and `balance-drift`. `cash-flow`, `spending-trend`,
`recurring-subscriptions`, and `merchant-activity` retain `currency_code` in
their grouping key, so their totals remain per currency whatever home currency
is set.

Net worth over time is not per currency: `net-worth --interval` buckets the
single home-currency total, so a bucket whose last day holds an unpriced
currency reports no figure. Here the four overrides cover 2025-12-27 only:

```console
$ uv run moneybin --profile cli-ux-international reports net-worth --interval monthly --from-date 2025-10-01 --to-date 2025-12-31 --no-pager
┏━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━━━━━┓
┃ balance_date ┃ unpriced_currency_count ┃  net_worth ┃ change_abs ┃
┡━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━━━━━┩
│ 2025-10-31   │ 4                       │          - │          - │
│ 2025-11-30   │ 4                       │          - │          - │
│ 2025-12-27   │ 0                       │ 106,066.73 │          - │
└──────────────┴─────────────────────────┴────────────┴────────────┘
4 of 11 columns shown — --wide for all
```

The three next-step hints are trimmed above.

## From an AI client

The `profile` tool reads the home currency. `profile_set(home_currency="EUR")`
sets it with an audit row. `reports(report_id="core:net_worth",
display_currency="EUR")` is the converted read and returns applied rates and
any degraded reason. FX writes and provider refresh are CLI-only; the [MCP tool
reference](../reference/mcp-tools.md) lists the agent-facing report parameters.

## What is not built yet

- **Realized FX gain or loss lacks a deliberate statement tie-out.**
  `moneybin reports realized-fx` reports per consumed lot, but it still needs a
  bank-statement expectation checked within $0.01.
- **Investment positions do not count toward net worth.** `investments holdings`
  values them; `reports net-worth` reads balances only.
- **Coverage is checked at span edges.** A missing interior provider weekday can
  surface only when a report needs that date.
- **Two currencies on one account are drift.** A transaction whose currency
  differs from its account stays out of that account's carried balance.

The design and shipped-record boundary are in the
[multi-currency spec](../specs/multi-currency.md).
