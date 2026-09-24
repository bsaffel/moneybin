<!-- Last reviewed: 2026-09-14 -->
# Reports

Nine built-in reports answer the standing questions — what am I worth, where
does the money go, what recurs, what is unusual, and what did I gain or lose
converting currency — from the canonical tables, and you can save your own SQL
beside them. One catalog serves every surface: `moneybin reports …` on the CLI,
the `reports` MCP tool, and `moneybin export report`. Same report ids, same
parameters, same masking.

Every transcript below is real output from the family demo persona, trimmed only by whole lines:

```bash
uv run moneybin demo --persona family
```

That build ends with `✅ Demo profile 'demo' ready (4 accounts, 2886 transactions, 2473 categorized).` — three calendar years ending on the last complete one, two bank accounts and two credit cards, seed 42. The seed fixes the figures within one calendar year: the window ends on the last complete year and `demo` has no option to pin it, so a rerun after January 1 shifts every date-bound number a year forward while the shapes hold.

## The ten built-in reports

| Command | Report id | Answers |
|---|---|---|
| [`reports net-worth`](../reference/cli/reports.md#moneybin-reports-net-worth) | `core:net_worth` | What am I worth in my home currency, and how has it moved? |
| [`reports net-worth-currencies`](../reference/cli/reports.md#moneybin-reports-net-worth-currencies) | `core:net_worth_currencies` | What am I worth per currency? |
| [`reports net-worth-accounts`](../reference/cli/reports.md#moneybin-reports-net-worth-accounts) | `core:net_worth_accounts` | What is each account's balance? |
| [`reports spending-trend`](../reference/cli/reports.md#moneybin-reports-spending-trend) | `core:spending_trend` | What goes out, by category and month, against last month, last year, and the trailing quarter? |
| [`reports cash-flow`](../reference/cli/reports.md#moneybin-reports-cash-flow) | `core:cash_flow` | In, out, and net, by month and account or category? |
| [`reports recurring-subscriptions`](../reference/cli/reports.md#moneybin-reports-recurring-subscriptions) | `core:recurring_subscriptions` | What recurs, how often, and what does it cost a year? |
| [`reports merchant-activity`](../reference/cli/reports.md#moneybin-reports-merchant-activity) | `core:merchant_activity` | Who gets paid, how much, how often, how recently? |
| [`reports large-transactions`](../reference/cli/reports.md#moneybin-reports-large-transactions) | `core:large_transactions` | What is large, and what is large *for this account or category*? |
| [`reports balance-drift`](../reference/cli/reports.md#moneybin-reports-balance-drift) | `core:balance_drift` | Where does a balance I asserted disagree with the transactions? |
| [`reports realized-fx`](../reference/cli/reports.md#moneybin-reports-realized-fx) | `core:realized_fx` | What gain or loss did a deliberate currency conversion realize? |

Each command's reference page lists every flag with its type and default. The flow reports exclude transfers between your own accounts and archived accounts.

### Net worth

```console
$ uv run moneybin reports net-worth-accounts
Using profile: demo
┏━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━┓
┃ account_name              ┃ currency_code ┃ account_balance ┃ account_balance_home ┃
┡━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━┩
│ Ally Bank savings …0002   │ USD           │       33,000.00 │                    - │
│ Chase Bank checking …0001 │ USD           │      387,080.77 │                    - │
│ Chase Bank credit card    │ USD           │            0.00 │                    - │
│ Citi credit card          │ USD           │            0.00 │                    - │
└───────────────────────────┴───────────────┴─────────────────┴──────────────────────┘
4 of 14 columns shown — --wide for all
💡 Run reports(report_id='core:net_worth') for the single home-currency total
💡 Run reports(report_id='core:net_worth_currencies') for the currency-level breakdown
```

With no range the report reads the latest balance date; `--from-date 2025-06-30 --to-date 2025-06-30` reads that day instead, carrying each balance forward from the last one on or before it. `net-worth-currencies` sums the same rows per currency, and `net-worth` into one home-currency total; none of the three takes an account filter. An account excluded from net worth (`accounts set <id> --exclude`) drops out of both after the next `moneybin refresh` or `moneybin transform apply`, because the exclusion is a setting the canonical account table picks up when it is rebuilt. Holdings in investment accounts do not count toward net worth yet.

### Net worth over time

```console
$ uv run moneybin profile set home_currency USD
$ uv run moneybin reports net-worth --interval monthly --from-date 2025-01-01 --to-date 2025-12-31
Using profile: demo
┏━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━━━━━┓
┃ balance_date ┃ unpriced_currency_count ┃  net_worth ┃ change_abs ┃
┡━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━━━━━┩
│ 2025-01-31   │ 0                       │ 300,133.10 │          - │
│ 2025-02-28   │ 0                       │ 310,383.89 │ +10,250.79 │
│ 2025-03-31   │ 0                       │ 320,763.38 │ +10,379.49 │
│ 2025-04-30   │ 0                       │ 331,318.81 │ +10,555.43 │
│ 2025-05-31   │ 0                       │ 342,064.37 │ +10,745.56 │
│ 2025-06-30   │ 0                       │ 352,756.07 │ +10,691.70 │
│ 2025-07-31   │ 0                       │ 362,791.60 │ +10,035.53 │
│ 2025-08-31   │ 0                       │ 381,166.67 │ +18,375.07 │
│ 2025-09-30   │ 0                       │ 390,658.51 │  +9,491.84 │
│ 2025-10-31   │ 0                       │ 400,767.89 │ +10,109.38 │
│ 2025-11-30   │ 0                       │ 410,775.08 │ +10,007.19 │
│ 2025-12-31   │ 0                       │ 420,080.77 │  +9,305.69 │
└──────────────┴─────────────────────────┴────────────┴────────────┘
4 of 11 columns shown — --wide for all
```

`net-worth` is a home-currency total, so it reports no figure until the profile has a home currency; the `profile set` output is trimmed above. `--interval` is `daily`, `weekly` (ISO weeks starting Monday), or `monthly`; each row is the bucket's last available balance date, and `change_abs` (with `change_pct` under `--wide`) compares it to the bucket before it. Both bounds are optional.

### Spending

```console
$ uv run moneybin reports spending-trend --from-month 2025-01 --to-month 2025-12
Using profile: demo
┏━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━┓
┃ category            ┃ currency_code ┃ year_month ┃ total_spend ┃ yoy_pct                ┃
┡━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━┩
│                     │ USD           │ 2025-12    │    3,151.16 │ -0.050429258914392296  │
│ Housing & Utilities │ USD           │ 2025-12    │    2,641.06 │ -0.0016783216783216785 │
│ Shopping            │ USD           │ 2025-12    │    1,659.53 │ 0.4079444128651299     │
│ Food & Drink        │ USD           │ 2025-12    │      967.39 │ 0.05113383242967197    │
│ Other               │ USD           │ 2025-12    │      451.23 │ 0.321820898145707      │
│ Services            │ USD           │ 2025-12    │      250.00 │ 0.0                    │
│ Transportation      │ USD           │ 2025-12    │      241.04 │ -0.3999950215318746    │
│ Entertainment       │ USD           │ 2025-12    │      112.04 │ -0.27076282218172354   │
│ Healthcare          │ USD           │ 2025-12    │       66.92 │ -0.6928302579638299    │
│ Personal Care       │ USD           │ 2025-12    │        0.00 │ -1.0                   │
└─────────────────────┴───────────────┴────────────┴─────────────┴────────────────────────┘
5 of 12 columns shown — --wide for all
💡 Run reports(report_id='core:spending_trend', parameters={'category': '<name>'}) to filter to one category
💡 Run reports(report_id='core:cash_flow') for inflow, outflow, and net
💡 Run reports(report_id='core:recurring_subscriptions') for recurring charge patterns
```

The full year is 120 rows; the eleven earlier months are trimmed here. `total_spend` is a positive absolute outflow, biggest category first within each month. The blank category is money nobody has categorized yet, which on this persona is the largest line — the [categorization guide](categorization.md) is how it shrinks. `yoy_pct` is a fraction: `-0.05` is 5% less than the same month a year earlier, `-1.0` means the category spent nothing this month. The comparison columns are computed over all history, so narrowing the window never blanks them.

Omit both bounds for the last 12 calendar months. `--compare mom` or `--compare trailing` swaps the comparison column shown; `--category` keeps one line:

```console
$ uv run moneybin reports spending-trend --from-month 2025-01 --to-month 2025-12 --category "Food & Drink" --compare mom
Using profile: demo
┏━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━┓
┃ category     ┃ currency_code ┃ year_month ┃ total_spend ┃ mom_pct               ┃
┡━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━┩
│ Food & Drink │ USD           │ 2025-01    │      876.71 │ -0.047396042723805584 │
│ Food & Drink │ USD           │ 2025-02    │    1,390.84 │ 0.58643108895758      │
│ Food & Drink │ USD           │ 2025-03    │      814.71 │ -0.41423168732564497  │
│ Food & Drink │ USD           │ 2025-04    │    1,154.29 │ 0.416810889764456     │
│ Food & Drink │ USD           │ 2025-05    │      800.06 │ -0.3068812863318577   │
│ Food & Drink │ USD           │ 2025-06    │      885.48 │ 0.10676699247556434   │
│ Food & Drink │ USD           │ 2025-07    │      670.95 │ -0.24227537606721777  │
│ Food & Drink │ USD           │ 2025-08    │      940.29 │ 0.4014308070646098    │
│ Food & Drink │ USD           │ 2025-09    │    1,140.32 │ 0.21273224218060385   │
│ Food & Drink │ USD           │ 2025-10    │    1,058.10 │ -0.07210256770029466  │
│ Food & Drink │ USD           │ 2025-11    │      991.80 │ -0.06265948398072016  │
│ Food & Drink │ USD           │ 2025-12    │      967.39 │ -0.02461181689856826  │
└──────────────┴───────────────┴────────────┴─────────────┴───────────────────────┘
5 of 12 columns shown — --wide for all
💡 Run reports(report_id='core:spending_trend', parameters={'category': '<name>'}) to filter to one category
💡 Run reports(report_id='core:cash_flow') for inflow, outflow, and net
💡 Run reports(report_id='core:recurring_subscriptions') for recurring charge patterns
```

### Cash flow

```console
$ uv run moneybin reports cash-flow --from-month 2025-07 --to-month 2025-12 --by category
Using profile: demo
┏━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━━━━━┓
┃ category            ┃ currency_code ┃ year_month ┃        net ┃
┡━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━━━━━┩
│ Income              │ USD           │ 2025-12    │ +16,055.80 │
│ Housing & Utilities │ USD           │ 2025-12    │  −2,641.06 │
│ Shopping            │ USD           │ 2025-12    │  −1,659.53 │
│ Food & Drink        │ USD           │ 2025-12    │    −967.39 │
│ Other               │ USD           │ 2025-12    │    −451.23 │
│                     │ USD           │ 2025-12    │    −360.90 │
│ Services            │ USD           │ 2025-12    │    −250.00 │
│ Transportation      │ USD           │ 2025-12    │    −241.04 │
│ Entertainment       │ USD           │ 2025-12    │    −112.04 │
│ Healthcare          │ USD           │ 2025-12    │     −66.92 │
└─────────────────────┴───────────────┴────────────┴────────────┘
4 of 7 columns shown — --wide for all
💡 Rerun reports(report_id='core:cash_flow', parameters={'by': 'category'}) to regroup by category
💡 Run reports(report_id='core:spending_trend') for outflow-only MoM and YoY trends
```

Six months is 64 rows; July through November are trimmed here. Cash flow is signed — income positive, spending negative — where `spending` is outflow only and unsigned. `--by account` groups by account instead, and the default `account-and-category` gives one row per pair. `inflow` and `outflow` are among the `--wide` columns.

### Recurring

```console
$ uv run moneybin reports recurring-subscriptions
Using profile: demo
```

Empty. `--status` defaults to `active`, and a stream counts as active while its last charge is within 60 days or two cadence intervals, whichever is longer; the demo's data ends on the last December 31, and this transcript was captured in September, when every stream had lapsed — rerun the demo in January and the same command lists them as active. On live data the default is what you want. Here, ask for everything:

```console
$ uv run moneybin reports recurring-subscriptions --status all
Using profile: demo
┏━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━┳━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━━━━━━━━━━┓
┃ merchant_normalized ┃ currency_code ┃ cadence ┃ status   ┃ annualized_cost ┃
┡━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━╇━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━━━━━━━━━━┩
│ Mortgage Payment    │ USD           │ monthly │ inactive │       25,200.00 │
│ (uncategorized)     │ USD           │ monthly │ inactive │        6,000.00 │
│ Auto Insurance      │ USD           │ monthly │ inactive │        2,220.00 │
│ Phone Plan          │ USD           │ monthly │ inactive │        1,740.00 │
│ Internet Service    │ USD           │ monthly │ inactive │        1,068.00 │
│ Life Insurance      │ USD           │ monthly │ inactive │          780.00 │
│ Netflix             │ USD           │ monthly │ inactive │          239.88 │
│ Spotify             │ USD           │ monthly │ inactive │          203.88 │
│ Disney+             │ USD           │ monthly │ inactive │          167.88 │
└─────────────────────┴───────────────┴─────────┴──────────┴─────────────────┘
5 of 13 columns shown — --wide for all
```

A row is a merchant whose charges land at a steady interval; `cadence` names the interval, `confidence` (a `--wide` column, 0 to 1) says how steady, and `--min-confidence` filters on it, `0.5` by default. `annualized_cost` is the average charge scaled to a year. `(uncategorized)` collects recurring charges with no merchant match. There is no "mark as cancelled" yet; a cancelled subscription goes inactive on its own once its last charge is more than 60 days or two intervals old, whichever is longer.

### Merchants

```console
$ uv run moneybin reports merchant-activity --top 10
Using profile: demo
┏━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━━━━━━┓
┃ merchant_normalized ┃ currency_code ┃ last_seen  ┃ txn_count ┃ total_spend ┃
┡━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━━━━━━┩
│ (uncategorized)     │ USD           │ 2025-12-30 │ 413       │   89,334.18 │
│ Mortgage Payment    │ USD           │ 2025-12-01 │ 36        │   75,600.00 │
│ Auto Insurance      │ USD           │ 2025-12-05 │ 36        │    6,660.00 │
│ Costco              │ USD           │ 2025-12-10 │ 45        │    6,441.18 │
│ Electric Company    │ USD           │ 2025-12-15 │ 36        │    5,492.22 │
│ Phone Plan          │ USD           │ 2025-12-08 │ 36        │    5,220.00 │
│ Trader Joe's        │ USD           │ 2025-12-29 │ 54        │    3,260.58 │
│ Internet Service    │ USD           │ 2025-12-10 │ 36        │    3,204.00 │
│ Apple Store         │ USD           │ 2025-12-11 │ 21        │    3,147.23 │
│ Gas Utility         │ USD           │ 2025-12-18 │ 36        │    3,110.78 │
└─────────────────────┴───────────────┴────────────┴───────────┴─────────────┘
5 of 14 columns shown — --wide for all
```

Lifetime totals, `--top 25` by default, sorted by `spend` or by `count` or `recent`. `total_spend` is absolute outflow; `--wide` adds the signed decomposition (`total_inflow`, `total_outflow`, `avg_amount`, `median_amount`), first seen, active months, account count, and the modal category. The `(uncategorized)` row is every transaction with no merchant resolved, which is why it leads on a persona that has had no merchant curation.

### Large transactions

```console
$ uv run moneybin reports large-transactions --top 10
Using profile: demo
┏━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━━━━┓
┃ account_name              ┃ description          ┃ currency_code ┃ txn_date   ┃    amount ┃
┡━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━━━━┩
│ Chase Bank checking …0001 │ DIRECT DEP Acme Corp │ USD           │ 2025-01-03 │ +4,455.78 │
│ Chase Bank checking …0001 │ DIRECT DEP Acme Corp │ USD           │ 2025-08-01 │ +4,455.78 │
│ Chase Bank checking …0001 │ DIRECT DEP Acme Corp │ USD           │ 2025-08-29 │ +4,455.78 │
│ Chase Bank checking …0001 │ DIRECT DEP Acme Corp │ USD           │ 2025-09-26 │ +4,455.78 │
│ Chase Bank checking …0001 │ DIRECT DEP Acme Corp │ USD           │ 2025-03-28 │ +4,455.78 │
│ Chase Bank checking …0001 │ DIRECT DEP Acme Corp │ USD           │ 2025-12-19 │ +4,455.78 │
│ Chase Bank checking …0001 │ DIRECT DEP Acme Corp │ USD           │ 2025-01-17 │ +4,455.78 │
│ Chase Bank checking …0001 │ DIRECT DEP Acme Corp │ USD           │ 2025-06-06 │ +4,455.78 │
│ Chase Bank checking …0001 │ DIRECT DEP Acme Corp │ USD           │ 2025-04-11 │ +4,455.78 │
│ Chase Bank checking …0001 │ DIRECT DEP Acme Corp │ USD           │ 2025-02-28 │ +4,455.78 │
└───────────────────────────┴──────────────────────┴───────────────┴────────────┴───────────┘
5 of 13 columns shown — --wide for all
```

Ranked by absolute amount, so on a salaried persona the top ten are ten paychecks. The useful lens is `--anomaly`, which keeps only rows more than 2.5 robust standard deviations above the typical size in their own account or category:

```console
$ uv run moneybin reports large-transactions --anomaly category
Using profile: demo
┏━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━━━━┓
┃ account_name              ┃ description      ┃ currency_code ┃ txn_date   ┃    amount ┃
┡━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━━━━┩
│ Chase Bank checking …0001 │ Mortgage Payment │ USD           │ 2023-03-01 │ −2,100.00 │
│ Chase Bank checking …0001 │ Mortgage Payment │ USD           │ 2025-02-01 │ −2,100.00 │
│ Chase Bank checking …0001 │ Mortgage Payment │ USD           │ 2025-10-01 │ −2,100.00 │
│ Chase Bank checking …0001 │ Mortgage Payment │ USD           │ 2025-04-01 │ −2,100.00 │
│ Chase Bank checking …0001 │ Mortgage Payment │ USD           │ 2023-12-01 │ −2,100.00 │
└───────────────────────────┴──────────────────┴───────────────┴────────────┴───────────┘
5 of 13 columns shown — --wide for all
```

Twenty-five rows come back on this persona (the default `--top`); twenty are trimmed here. Every one is the mortgage, because in the `Housing & Utilities` category a 2,100 payment is an outlier next to the utility bills — which is exactly the question this lens answers. The two z-scores and `is_top_100` are `--wide` columns.

### Balance drift

```console
$ uv run moneybin reports balance-drift
Using profile: demo
💡 Rerun reports(report_id='core:balance_drift', parameters={'account': '<name or id>'}) to filter to one account
💡 Rerun reports(report_id='core:balance_drift', parameters={'status': 'drift'}) to show drift rows
```

Empty on the demo, because drift needs an assertion: a balance you typed from a statement, recorded with `accounts balance assert`. Each assertion becomes one row comparing the asserted figure to the balance the transactions imply on that date, bucketed `clean`, `warning`, `drift`, `no-data`, or `currency-mismatch`. It is the report that tells you an import is missing rows.

### Realized FX

```console
$ uv run moneybin reports realized-fx --currency EUR --coverage complete
Using profile: demo
```

Empty on the demo, because the family persona holds only USD accounts and
never converts currency — no Currency lot is ever consumed. Each complete row
is one consumed Currency lot from a deliberate conversion, so
one disposal can produce several rows with different acquisition dates and bases.
An unmatched-inventory placeholder has no `currency_lot_id` and remains visible
as an incomplete row instead of disappearing from the total.
The default table shows `realized_fx_gain_id`, `currency_code`,
`home_currency`, `coverage_status`, `disposal_date`, and `gain_loss` —
`realized_fx_gain_id` is the row's own stable identifier, so it distinguishes
the several rows a multi-lot disposal produces even when two lots land on the
same gain or loss. Add `--wide` for `disposed_amount` plus the
conversion, lot, account, source, rate, and transfer lineage. Filter with
`--from-date`, `--to-date`, `--currency`, and
`--coverage complete|incomplete|all`; incomplete rows stay visible by default
and carry a closed `coverage_reason` in the wide output.

The row is deliberately mixed-unit. `disposed_amount` is in `currency_code`;
`proceeds`, `cost_basis`, `fee_amount`, and `gain_loss` are in `home_currency`.
`--display-currency` does not re-price these historical accounting amounts. The
same report is available to an agent as
`reports(report_id='core:realized_fx')`.

## Reading the output

- **Default columns.** A text table shows the columns that answer the question; the footer (`5 of 12 columns shown — --wide for all`) counts the rest. `--wide` renders all of them on every report command and on `reports run`. JSON always carries all of them.
- **Signs.** `spending-trend`, `merchant-activity`, and `recurring-subscriptions` report outflow as positive absolute amounts. `cash-flow`, `large-transactions`, and every transaction listing are signed: negative is money out.
- **Currency.** Every ordinary built-in row carries a `currency_code`, and a built-in never blends two known currencies into one figure; `realized_fx` additionally names `home_currency` because its row is deliberately mixed-unit. A saved report inherits whatever its own SQL does. Rows with no currency at all pool into one unknown segment and are summed together, because nothing can tell two unknowns apart; `system doctor` fails on any such account and `accounts set --currency` followed by `moneybin refresh` or `moneybin transform apply` is the fix, because the account table is rebuilt rather than read live; set them before trusting a total. A multi-currency profile gets its rows interleaved per currency, best-ranked first within each, so a capped result holds every currency that fits inside the cap — a `--limit` smaller than the number of currencies still drops some, and `summary.has_more` says the cap cut the result — a report has no page after the first, so raise the limit to see the rest. See [One display currency](#one-display-currency).
- **The `💡` lines.** Each one is the MCP tool call an assistant would make next, written out so you can read it as the CLI's own next move — with one exception: a report that masked one of its columns adds a `Run moneybin reports explain <id>` hint, which names the CLI command by design. The parameter a tool-call hint names maps to a flag on the dedicated command, not always under the same name (`from_date` is `--from`), and the [reference page](../reference/cli/reports.md) lists each command's flags.
- **Freshness.** Every built-in reads views over the canonical tables, so it reflects the last import or `moneybin refresh` the moment that finishes, and nothing is cached between runs. The one deferral is an import run with `--no-refresh`, whose rows reach the canonical tables only after `moneybin refresh` or `moneybin transform apply`. `balance-drift` has one live side: an `accounts balance assert` shows up on its next run, while the computed balance it is compared against comes from the last rebuild. A saved report is as fresh as what it reads: over `raw.*` or the `prep.*` views it sees an import at once, over `core.*` or `reports.*` it waits for that same transform.
- **Rows, not aggregates.** When the question is "show me the transactions", `moneybin transactions list` filters by `--account`, `--from`/`--to`, `--category`, `--amount-min`/`--amount-max`, and `--description`, and `moneybin sql query` takes a `SELECT`, `WITH`, `DESCRIBE`, or `SHOW` over the `core`, `app`, `reports`, `raw`, and `prep` schemas.

## Any report by id: list, run, explain

`reports list` prints the whole catalog — name, id, tier, parameters, description. Tiers are `builtin` (the ten above, ids prefixed `core:`), `extension` (reports a MoneyBin extension package registers), and `user` (yours, prefixed `user:`). `--tier` filters, `--include-archived` adds saved reports you have archived.

`reports run HANDLE` executes any of them by id or name, with `--param key=value` for each parameter and `--limit` for a row cap. It prints the rows through the shared renderer — default columns, the footer, and the `💡` hints — without the dedicated command's own layout, such as `spending-trend`'s chosen comparison column, so the dedicated command is the better read when one exists:

```console
$ uv run moneybin reports run core:net_worth_accounts
Using profile: demo
┏━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━┓
┃ account_name              ┃ currency_code ┃ account_balance ┃ account_balance_home ┃
┡━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━┩
│ Ally Bank savings …0002   │ USD           │       33,000.00 │            33,000.00 │
│ Chase Bank checking …0001 │ USD           │      387,080.77 │           387,080.77 │
│ Chase Bank credit card    │ USD           │            0.00 │                 0.00 │
│ Citi credit card          │ USD           │            0.00 │                 0.00 │
└───────────────────────────┴───────────────┴─────────────────┴──────────────────────┘
4 of 14 columns shown — --wide for all
💡 Run reports(report_id='core:net_worth') for the single home-currency total
💡 Run reports(report_id='core:net_worth_currencies') for the currency-level breakdown
```

`reports explain HANDLE` runs nothing. It prints the report's description, every output column with its privacy class and where it comes from, the tables it reads, and, for a report that is a `SELECT`, the SQL in bound and template form:

```console
$ uv run moneybin reports explain core:spending_trend
Using profile: demo
core:spending_trend  (builtin)
Monthly spending trend with MoM, YoY, and 3-month-trailing deltas.

Defaults to the last 12 calendar months when both bounds are omitted. YoY columns come from the underlying view (all history), so narrowing the window does not null out yoy_pct. Spending amounts are positive absolute outflows; comparison deltas are current spend minus comparison-period spend. Monetary values are denominated in each row's own currency_code.
┏━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━━━┓
┃ column           ┃ class      ┃ origin   ┃ upstream ┃
┡━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━━━┩
│ year_month       │ txn_date   │ upstream │ -        │
│ category         │ category   │ upstream │ -        │
│ currency_code    │ currency   │ upstream │ -        │
│ total_spend      │ txn_amount │ upstream │ -        │
│ txn_count        │ aggregate  │ upstream │ -        │
│ prev_month_spend │ txn_amount │ upstream │ -        │
│ mom_delta        │ txn_amount │ upstream │ -        │
│ mom_pct          │ aggregate  │ upstream │ -        │
│ prev_year_spend  │ txn_amount │ upstream │ -        │
│ yoy_delta        │ txn_amount │ upstream │ -        │
│ yoy_pct          │ aggregate  │ upstream │ -        │
│ trailing_3mo_avg │ txn_amount │ upstream │ -        │
└──────────────────┴────────────┴──────────┴──────────┘
Reads: reports.spending_trend
Graduation: already_materialized
```

The SQL that follows is trimmed here. It reads the `reports.spending_trend` view, which the [data model](../reference/data-model.md) documents column by column, and you can run it yourself with `moneybin sql query` once you replace each `?` with a literal: the date bounds stay withheld as `?` because their values carry a privacy class, and `sql query` binds nothing. The `class` column is what decides masking when the report leaves the machine through MCP or an export. `Graduation` says whether the report could be materialized as a view of its own; `Fingerprint`, on a saved report, is a hash over the SQL text, the classes of every column it reads, and the current masking policy for each of those classes; a run whose recomputed hash differs — the SQL was rewritten, even to the same shape, or a policy moved — re-derives the classes before serving anything.

## JSON

`--output json` on any report returns the standard envelope with every column, not the default set:

```console
$ uv run moneybin reports run core:recurring_subscriptions --param status=all --limit 2 --output json | jq .
{
  "status": "ok",
  "summary": {
    "total_count": 3,
    "returned_count": 2,
    "has_more": true,
    "sensitivity": "high",
    "display_currency": "USD"
  },
  "data": [
    {
      "merchant_id": "8b8e28c76d39",
      "merchant_normalized": "Mortgage Payment",
      "currency_code": "USD",
      "cadence": "monthly",
      "status": "inactive",
      "first_seen": "2025-04-01",
      "last_seen": "2025-12-01",
      "interval_days_avg": 30.5,
      "interval_days_stddev": 0.5345224838248488,
      "confidence": 0.9618198225839394,
      "occurrence_count": 9,
      "avg_amount": 2100.0,
      "annualized_cost": 25200.0
    },
    {
      "merchant_id": null,
      "merchant_normalized": "(uncategorized)",
      "currency_code": "USD",
      "cadence": "monthly",
      "status": "inactive",
      "first_seen": "2025-03-05",
      "last_seen": "2025-12-05",
      "interval_days_avg": 30.555555555555557,
      "interval_days_stddev": 0.5270462766947299,
      "confidence": 0.9623538373789479,
      "occurrence_count": 10,
      "avg_amount": 500.0,
      "annualized_cost": 6000.0
    }
  ],
  "actions": []
}
```

`has_more` says the `--limit` cut the result, and while it is true `total_count` is a lower bound rather than the total: execution fetches one row past the cap and reports that, so the `3` above means at least three — the uncapped `--status all` run earlier on this page shows nine — and the exact count means a `--limit` above it (`reports run` caps at 1,000,000 rows even with no flag) or a `COUNT(*)` through `moneybin sql query`. Every column carries a privacy class — what it reveals: a merchant, an amount, an account id — that decides what is masked when the value leaves the machine through MCP or an export; `sensitivity` is the highest class among the returned columns, and the [MCP server guide](mcp-server.md) says what the server does with it. `display_currency` names the currency the amounts are in, and a converted read adds `applied_rates` and, when a row could not be priced, `degraded_reason`. The `💡` lines become `actions`. The envelope's full contract is in the [CLI reference](cli-reference.md#output-envelopes).

## Save your own report

A saved report is a read-only `SELECT` stored in the profile, with typed parameters, run and exported through the same catalog as the built-ins. Write `$name` where a value goes and declare it with `--param name:type` (`str`, `int`, `float`, `bool`, `date`, or `decimal`), optionally with a default (`name:type=value`); a parameter with no default is required:

```console
$ uv run moneybin reports create coffee --sql "SELECT merchant_name, currency_code, SUM(amount) AS spend FROM core.fct_transactions WHERE category = \$category GROUP BY merchant_name, currency_code QUALIFY ROW_NUMBER() OVER (PARTITION BY currency_code ORDER BY spend) BETWEEN 1 AND 5 ORDER BY currency_code, spend" --param category:str --description "Top merchants in one category"
Using profile: demo
user_report.create report_id=user:r6ebf7dcd4ba6 outcome=saved
✅ Saved coffee (user:r6ebf7dcd4ba6)
```

`currency_code` sits in the grouping key and the rank is taken within it, so a profile holding two currencies gets a top five per currency rather than one sum across both. Saving a report derives its privacy classes and checks nothing about its arithmetic, so keeping currencies apart in your own SQL is on you; the built-ins do it this same way. `--sql-file` takes the query from a file instead. Run it by name:

```console
$ uv run moneybin reports run coffee --param "category=Food & Drink"
Using profile: demo
┏━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━┳━━━━━━━━━━┓
┃ merchant_name ┃ currency_code ┃ spend    ┃
┡━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━╇━━━━━━━━━━┩
│ Costco        │ USD           │ -6441.18 │
│ Trader Joe's  │ USD           │ -3260.58 │
│ Kroger        │ USD           │ -2842.24 │
│ Instacart     │ USD           │ -2391.18 │
│ Whole Foods   │ USD           │ -2329.06 │
└───────────────┴───────────────┴──────────┘
```

You never declare privacy classes. MoneyBin derives them from the SQL at save time — a column read from `core.fct_transactions.merchant_name` is a `merchant_name`, a sum over `amount` is a `txn_amount` — and stores them, so on `run`, `explain`, and the MCP tool a saved report masks exactly as a built-in does. A redacted export goes further, because a saved report's column names, parameter names, and SQL are your own text; the [Export](#export) section says what it withholds:

```console
$ uv run moneybin reports explain coffee --param "category=Food & Drink"
Using profile: demo
user:r6ebf7dcd4ba6  (user)
Top merchants in one category
┏━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃ column        ┃ class         ┃ origin   ┃ upstream                            ┃
┡━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
│ merchant_name │ merchant_name │ upstream │ core.fct_transactions.merchant_name │
│ currency_code │ currency      │ upstream │ core.fct_transactions.currency_code │
│ spend         │ txn_amount    │ computed │ -                                   │
└───────────────┴───────────────┴──────────┴─────────────────────────────────────┘
Reads: core.fct_transactions
Graduation: eligible
Updated: 2026-09-05 00:14:05.940839
Fingerprint: a5ff30ae3f62c0dd6368d61fbc135744746672b11e247952a2d8abfba8b35a71

SQL:
SELECT merchant_name, currency_code, SUM(amount) AS spend FROM core.fct_transactions WHERE category = 'Food & Drink' GROUP BY merchant_name, currency_code QUALIFY ROW_NUMBER() OVER (PARTITION BY currency_code ORDER BY spend) BETWEEN 1 AND 5 ORDER BY currency_code, spend

Template:
SELECT merchant_name, currency_code, SUM(amount) AS spend FROM core.fct_transactions WHERE category = $category GROUP BY merchant_name, currency_code QUALIFY ROW_NUMBER() OVER (PARTITION BY currency_code ORDER BY spend) BETWEEN 1 AND 5 ORDER BY currency_code, spend
```

A report may read `raw.*` or `prep.*` too, but those schemas declare classes for few columns, so masking there falls back to scanning values by shape — an account number of fewer than eight digits passes through. Keep saved reports on `core.*` and `reports.*` unless you have read [what the AI provider sees](what-the-ai-sees.md). When a derived class is stricter than the column deserves, `reports reclassify` lowers it for one column, with a `--reason`, and the change is audited.

The rest of the lifecycle:

```console
$ uv run moneybin reports list --tier user
Using profile: demo
┏━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━┳━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃ name   ┃ report_id          ┃ tier ┃ parameters ┃ description                   ┃
┡━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━╇━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
│ coffee │ user:r6ebf7dcd4ba6 │ user │ category   │ Top merchants in one category │
└────────┴────────────────────┴──────┴────────────┴───────────────────────────────┘
$ uv run moneybin reports set coffee --archive
Using profile: demo
user_report.set report_id=user:r6ebf7dcd4ba6 fields=1 outcome=updated
✅ Updated coffee (user:r6ebf7dcd4ba6)
$ uv run moneybin reports list --include-archived --tier user
Using profile: demo
┏━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃ name   ┃ report_id          ┃ tier            ┃ parameters ┃ description                   ┃
┡━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
│ coffee │ user:r6ebf7dcd4ba6 │ user [archived] │ category   │ Top merchants in one category │
└────────┴────────────────────┴─────────────────┴────────────┴───────────────────────────────┘
$ uv run moneybin reports delete coffee --yes
Using profile: demo
user_report.delete report_id=user:r6ebf7dcd4ba6 outcome=removed
✅ Deleted coffee (user:r6ebf7dcd4ba6)
```

`reports set` also renames (`--name`), re-describes, and replaces the SQL or the parameters, re-deriving the privacy classes when it does. `--restore` unarchives. A delete is audited; `system audit undo` brings the report back.

## One display currency

See the [multi-currency guide](multi-currency.md) for where a row's currency comes from and what a mixed-currency profile reads like before it has a home currency set.

`--display-currency EUR` — `display_currency` on the MCP tool — prices a report into one currency at read time. Omit the flag and the target is the profile's home currency (`profile set home_currency EUR`): a profile that has set one gets the five converting reports named below priced into it whenever the rates are on disk, and falls back quietly when they are not. A profile with no home currency, which is how every profile starts, reads each row in its own currency, and so does any report that cannot convert: the four that aggregate per currency always, the mixed-unit realized-FX report always, and the five converting ones whenever a rate is missing. The three net-worth reports are the exception to that last clause: their home-currency columns come back NULL rather than in the row's own currency, because the view computes them and a missing rate leaves nothing to show. Nothing converted is ever stored — the original amount and currency stay in every table — and because `home_currency` takes an ISO code and has no unset, the unconverted read on a home-currency profile is `moneybin sql query` over the view: `reports.net_worth`, `reports.net_worth_currencies`, `reports.net_worth_accounts`, `reports.large_transactions`, or `reports.balance_drift`. Rates come from `moneybin refresh`, which caches the direct provider pairs your own rows imply for the home currency and declared display targets (`profile set display_currency_targets EUR,GBP`); a target with no stored rates falls back, and the report says so instead of guessing:

```console
$ uv run moneybin reports net-worth-accounts --display-currency EUR
Using profile: demo
┏━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━┓
┃ account_name              ┃ currency_code ┃ account_balance ┃ account_balance_home ┃
┡━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━┩
│ Ally Bank savings …0002   │ USD           │       33,000.00 │            33,000.00 │
│ Chase Bank checking …0001 │ USD           │      387,080.77 │           387,080.77 │
│ Chase Bank credit card    │ USD           │            0.00 │                 0.00 │
│ Citi credit card          │ USD           │            0.00 │                 0.00 │
└───────────────────────────┴───────────────┴─────────────────┴──────────────────────┘
4 of 14 columns shown — --wide for all
💡 Run reports(report_id='core:net_worth') for the single home-currency total
💡 Run reports(report_id='core:net_worth_currencies') for the currency-level breakdown
```

Between the table and the hints the command prints the reason, trimmed from the transcript above: `⚠️  no stored USD->EUR rates at all; run 'moneybin refresh' to gather them, and record one with 'moneybin fx set' if refresh reports the pair unsupported`. In JSON the same sentence is `summary.degraded_reason`. On this profile, declare the target before refreshing (`profile set display_currency_targets EUR`); refresh then gathers the direct USD→EUR pair for currencies your rows hold. You can also make EUR the home currency (`profile set home_currency EUR`, then `moneybin refresh`) or record the pair yourself with `moneybin fx set`.

Five reports convert, because each of their rows is one event on one date: the three net-worth reports (`net-worth`, `net-worth-currencies`, `net-worth-accounts`) at the balance date, `large-transactions` at the transaction date, `balance-drift` at the assertion date. Four aggregate with the currency in their grouping key, so a row is already a per-currency subtotal and stays one. `realized-fx` stays mixed-unit by design: `disposed_amount` retains `currency_code` while `proceeds`, `cost_basis`, `fee_amount`, and `gain_loss` retain `home_currency`. The three net-worth reports build their home-currency columns in the view itself, by joining each balance to the profile's home currency, so a profile that has never set one gets no total from them — `net_worth`, `net_worth_home`, and `account_balance_home` are NULL and `unpriced_currency_count` is at least 1, in a single currency as much as in five. Set it with `profile set home_currency <CODE>`. A display currency re-prices those columns afterwards, from the home currency rather than from each row's own; it cannot stand in for a home currency that was never set. [Features](../features.md#reading-a-report-in-one-currency) has the full rule set, including which anomaly columns a converted read blanks and why.

## From an AI client

The `reports` MCP tool is the same catalog. Called with no `report_id` it returns the catalog; with one it runs the report, taking `parameters` as a dictionary keyed by the names `reports explain` lists — `reports(report_id='core:spending_trend', parameters={'category': 'Food & Drink', 'from_month': '2025-01'})` — plus `display_currency` and a row cap (`MONEYBIN_MCP__MAX_ROWS`, 1,000 by default). The response is the JSON envelope above. Saving, editing, and deleting reports is CLI-only; the tool reads the catalog and runs it. It never takes SQL — `sql_query` does, read-only, over `core`, `reports`, and `app`, plus `raw` and `prep` under the weaker value-shape masking described above. The [MCP server guide](mcp-server.md) covers the envelope and sensitivity tiers and the [tool reference](../reference/mcp-tools.md#reports) lists every parameter.

## Export

`moneybin export report REPORT_ID` writes any catalog report — built-in or saved, `--param` bound the same way — to a named local destination as CSV, Parquet, or XLSX, or to a Google Sheet in the sheet's own format. Masking applies on the way out; `--unredacted` is an explicit per-run choice. A redacted export of a saved report also withholds what you authored: its columns become `redacted_column_1`, `redacted_column_2`, and so on, its parameters `redacted_parameter_*`, and its SQL is left out of the receipt, while a built-in's export keeps its real names, and its SQL. The [export section of the CLI reference](cli-reference.md#export) and the [Google Sheets guide](connect-gsheet.md) cover destinations.

## What is not built yet

- **Holdings in net worth.** Investment holdings do not count toward the net-worth total; only account balances do. Read positions with `moneybin investments holdings`. ([Net worth](#net-worth))
- **Marking a subscription cancelled.** There is no "mark as cancelled"; a cancelled subscription goes inactive on its own once its last charge is more than 60 days or two intervals old, whichever is longer. ([Recurring](#recurring))
- **Balance drift without an assertion.** `balance-drift` returns no rows until you record a statement balance with `accounts balance assert`, and the computed balance it compares against comes from the last rebuild, so run `moneybin refresh` or `moneybin transform apply` after an import. ([Balance drift](#balance-drift), [Reading the output](#reading-the-output))
- **Converting historical accounting amounts.** `--display-currency` does not re-price `proceeds`, `cost_basis`, `fee_amount`, or `gain_loss` on `realized-fx`; those stay in `home_currency` while `disposed_amount` stays in `currency_code`. ([Realized FX](#realized-fx))
- **A second page of a report.** A report has no page after the first: when `summary.has_more` says the cap cut the result, raise `--limit`. ([Reading the output](#reading-the-output))
- **Saving a report from an agent.** Saving, editing, and deleting reports is CLI-only; the `reports` tool reads the catalog and runs it. ([From an AI client](#from-an-ai-client))
