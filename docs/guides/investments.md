<!-- Last reviewed: 2026-09-11 -->
# Investments

One ledger of investment events is the source of truth. Tax lots, positions, and realized gain or loss are derived from it on every refresh, under one of four cost-basis methods, and nothing derived is ever stored as authoritative. Prices come from the broker's own closes, from Tiingo and CoinGecko, from the trades you recorded, or from a mark you set by hand, and a position with no usable price says so rather than reporting zero.

Every transcript below is real output from the family demo persona, trimmed only by whole lines:

```bash
uv run moneybin demo --persona family
```

That build ends with `✅ Demo profile 'demo' ready (4 accounts, 2886 transactions, 2473 categorized).` — two bank accounts and two credit cards, no investments. The guide adds a brokerage account by hand and records seven events into it, so every number on this page is traceable to a command above it. A brokerage connected through [Plaid sync](data-import.md#live-banking-sync-plaid) lands in the same ledger with no manual step; everything from [Positions and lots](#positions-and-lots) down applies to both.

## Get a brokerage account

There is no `accounts create`. An account comes from an import or a sync, so a brokerage you keep by hand starts from its cash-activity export, which every broker offers as a CSV. This one is two deposits:

```text
Date,Description,Amount
2024-01-05,ACH deposit from checking,10000.00
2024-07-01,ACH deposit from checking,5000.00
```

A bare Date, Description, Amount file names no account, so the import stops twice: once to confirm the column mapping, then to ask which account the rows belong to. `--account-name` names the new account and `@0=new` mints it rather than adopting one of the four the demo already has:

```console
$ uv run moneybin import confirm brokerage-cash.csv --accept --account-name Brokerage --account-binding '@0=new' --account-meta '@0:account_subtype=brokerage'
Using profile: demo
Importing CSV file: brokerage-cash.csv
⚠️  Sign convention is ambiguous (all amounts appear positive). Proceeding with 'negative_is_expense' — use --sign to override if expense amounts look wrong.
Created import batch: aca4a218...
Transform complete: 2 accepted, 0 rejected
Loaded 2 transactions
Loaded 1 accounts
Import aca4a218... finalized: complete (2 imported, 0 rejected)
Auto-saved format 'brokerage' for future imports
✅ Imported brokerage-cash.csv: 2 rows (import_id: aca4a218-8c60-445c-8308-88df18b30d65)
👀 Created account: Brokerage (f2b870002664)
```

The first `import files brokerage-cash.csv` and the `import confirm brokerage-cash.csv --accept --account-name Brokerage` that raised the account question are cut above; each returns a `confirmation_required` envelope naming the next command. The [data import guide](data-import.md#by-file-format) has the whole ladder.

One more step this file forces. It carries no currency column, so the account has no currency, and `moneybin system doctor` fails `currency_integrity` naming it rather than assuming USD: every event recorded into the account would inherit that unknown, and its amounts would sit outside every total. Assign it once:

```console
$ uv run moneybin accounts set f2b870002664 --currency USD
Using profile: demo
Updated settings for account f2b****...2664: fields=['currency_code']
Restating 1 transform model(s)
Transform restatement completed in 31.33s
✅ Updated settings for f2b870002664: fields=['currency_code']
```

A brokerage that arrives through OFX or Plaid states its currency and skips this. The [multi-currency guide](multi-currency.md#where-a-currency-comes-from) has the resolution order.

## Catalog the securities

The catalog is yours to maintain by hand; a Plaid sync mints entries into it as it meets new securities. An entry is keyed on a stable id, and ticker, CUSIP, ISIN, FIGI, and CoinGecko id are attributes, because tickers get reused and crypto has none of the others. `--method` elects a cost-basis method for one security; omitted, the account's default applies, and the account's default is FIFO until `accounts set --default-cost-basis-method` says otherwise:

```console
$ uv run moneybin investments securities add --name "Northwind Industries" --type equity --ticker NWND --exchange NYSE
Using profile: demo
securities.upsert security_id=55ab9e0ce69f type=equity actor=cli
✅ Added security 55ab9e0ce69f
$ uv run moneybin investments securities add --name "Broad Market Index ETF" --type etf --ticker BMKT --method average
Using profile: demo
securities.upsert security_id=f1f00a4e0562 type=etf actor=cli
✅ Added security f1f00a4e0562
$ uv run moneybin investments securities list
Using profile: demo
┏━━━━━━━━━━━━━━┳━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━┓
┃ security     ┃ ticker ┃ name                   ┃ type   ┃
┡━━━━━━━━━━━━━━╇━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━┩
│ f1f00a4e0562 │ BMKT   │ Broad Market Index ETF │ etf    │
│ 55ab9e0ce69f │ NWND   │ Northwind Industries   │ equity │
└──────────────┴────────┴────────────────────────┴────────┘
```

Seven types are accepted: `equity`, `etf`, `mutual_fund`, `bond`, `crypto`, `cash`, `other`. `average` cost is accepted for `mutual_fund` and `etf` only, which is where the IRS allows it. `securities set <id>` changes any attribute except the type, and every command below takes a ticker, CUSIP, ISIN, name, or id wherever it asks for a security.

## Record the ledger

`investments add` writes one event. Fourteen types cover a brokerage statement: `buy`, `sell`, `reinvest`, `dividend`, `interest`, `capital_gain_distribution`, `transfer_in`, `transfer_out`, `deposit`, `withdrawal`, `split`, `fee`, `return_of_capital`, and `other`. Quantity is signed with the position, positive for an acquisition and negative for a disposal; amount is signed with the cash, negative leaving the account and positive arriving, and includes fees. Seven events, in trade-date order:

```bash
uv run moneybin investments add --account Brokerage --type buy --date 2024-01-08 --security NWND --quantity 100 --price 42.00 --amount -4200.00
uv run moneybin investments add --account Brokerage --type buy --date 2024-03-15 --security NWND --quantity 50 --price 50.00 --amount -2500.00
uv run moneybin investments add --account Brokerage --type buy --date 2024-06-20 --security BMKT --quantity 40 --price 210.00 --amount -8400.00
uv run moneybin investments add --account Brokerage --type dividend --date 2024-09-30 --security NWND --amount 60.00 --subtype qualified
uv run moneybin investments add --account Brokerage --type reinvest --date 2024-12-16 --security BMKT --quantity 1.5 --price 220.00 --amount -330.00 --subtype dividend
uv run moneybin investments add --account Brokerage --type sell --date 2025-02-10 --security NWND --quantity -120 --price 55.00 --amount 6595.00 --fees 5.00
uv run moneybin investments add --account Brokerage --type buy --date 2025-08-01 --security NWND --quantity 30 --price 48.00 --amount -1440.00
```

Each prints `✅ Recorded <id>` with the event's id; the sale's, `e23b609336ab824d`, is used below. A `reinvest` is the one event that writes two rows, the acquisition and its paired income, so income reports sum only income-typed rows and a reinvested dividend is never counted twice. `--currency` denominates an event in something other than the account's currency, `--acquired` and `--basis` carry the original date and cost on a `transfer_in` so the holding period travels with the shares, and `split` takes the multiplier in `--quantity`: `2` for 2-for-1, `0.5` for a 1-for-2 reverse. There is no edit and no delete for a recorded event; each write is its own import batch, so `import revert <import_id>` is the undo.

The ledger is the only authored surface; lots, holdings, and gains are rebuilt from it by `moneybin refresh`:

```console
$ uv run moneybin refresh --step transform
Using profile: demo
Running transforms
Transforms completed in 31.13s
Pipeline:
  Transforms: rebuilt
✅ Refresh complete in 31.13s
$ uv run moneybin investments list
Using profile: demo
┏━━━━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━━━┓
┃ date       ┃ type     ┃ security    ┃ quantity        ┃    amount ┃ currency ┃
┡━━━━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━━━┩
│ 2024-01-08 │ buy      │ 55ab9e0ce69 │ 100.0000000000  │ −4,200.00 │ USD      │
│            │          │ f           │                 │           │          │
│ 2024-03-15 │ buy      │ 55ab9e0ce69 │ 50.0000000000   │ −2,500.00 │ USD      │
│            │          │ f           │                 │           │          │
│ 2024-06-20 │ buy      │ f1f00a4e056 │ 40.0000000000   │ −8,400.00 │ USD      │
│            │          │ 2           │                 │           │          │
│ 2024-09-30 │ dividend │ 55ab9e0ce69 │                 │    +60.00 │ USD      │
│            │          │ f           │                 │           │          │
│ 2024-12-16 │ reinvest │ f1f00a4e056 │ 1.5000000000    │   −330.00 │ USD      │
│            │          │ 2           │                 │           │          │
│ 2024-12-16 │ dividend │ f1f00a4e056 │                 │   +330.00 │ USD      │
│            │          │ 2           │                 │           │          │
│ 2025-02-10 │ sell     │ 55ab9e0ce69 │ -120.0000000000 │ +6,595.00 │ USD      │
│            │          │ f           │                 │           │          │
│ 2025-08-01 │ buy      │ 55ab9e0ce69 │ 30.0000000000   │ −1,440.00 │ USD      │
│            │          │ f           │                 │           │          │
└────────────┴──────────┴─────────────┴─────────────────┴───────────┴──────────┘
```

Every table on this page names a security by its id, not its ticker; `securities list` is the lookup. Quantities carry ten decimal places because a mutual fund or a crypto position is fractional.

## Positions and lots

Each acquisition opens a lot; each disposal consumes lots in the order the elected method dictates. Under FIFO the February sale of 120 shares consumed the whole January lot of 100 and 20 of the March 50, which is why one lot is closed and 30 of the March lot remain:

```console
$ uv run moneybin investments lots list
Using profile: demo
┏━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━━━━━━━━┳━━━━━━━━━━┳━━━━━━┓
┃ lot            ┃ security     ┃ acquired   ┃ remaining     ┃    basis ┃ note ┃
┡━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━━━━━━━━╇━━━━━━━━━━╇━━━━━━┩
│ lot_7618ff9ca1 │ 55ab9e0ce69f │ 2024-03-15 │ 30.0000000000 │ 1,500.00 │      │
│ d1221c         │              │            │               │          │      │
│ lot_e4d69c1977 │ f1f00a4e0562 │ 2024-06-20 │ 40.0000000000 │ 8,414.46 │      │
│ e130d3         │              │            │               │          │      │
│ lot_7fd47e48c6 │ f1f00a4e0562 │ 2024-12-16 │ 1.5000000000  │   315.54 │      │
│ 4e5ca1         │              │            │               │          │      │
│ lot_550e73310e │ 55ab9e0ce69f │ 2025-08-01 │ 30.0000000000 │ 1,440.00 │      │
│ de4b66         │              │            │               │          │      │
└────────────────┴──────────────┴────────────┴───────────────┴──────────┴──────┘
6 of 9 columns shown — --wide for all
```

The two ETF lots show average cost at work. They were bought at 210.00 and 220.00, but the pool is 8,730.00 over 41.5 shares, 210.36 a share, and each lot carries its share of the pool rather than its own price: 8,414.46 for 40 shares, 315.54 for 1.5. `--all` adds the closed January lot and a `state` column; `--wide` adds the currency and the method each lot was derived under. The `note` column marks a lot whose basis is incomplete, which happens when a `transfer_in` arrives with no `--basis`: the lot opens at zero basis and flagged, never rejected, because the basis genuinely may be unknown at transfer time.

Holdings are the open lots summed per position, valued at the most recent close on or before today:

```console
$ uv run moneybin investments holdings
Using profile: demo
┏━━━━━━━━━━━┳━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━━━━┓
┃ security  ┃ quantity      ┃ market value ┃ unrealized ┃ currency ┃ status    ┃
┡━━━━━━━━━━━╇━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━━━━┩
│ 55ab9e0ce │ 60.0000000000 │     2,880.00 │     −60.00 │ USD      │ carried_f │
│ 69f       │               │              │            │          │ orward    │
│ f1f00a4e0 │ 41.5000000000 │     9,130.00 │    +400.00 │ USD      │ carried_f │
│ 562       │               │              │            │          │ orward    │
└───────────┴───────────────┴──────────────┴────────────┴──────────┴───────────┘
6 of 9 columns shown — --wide for all
portfolio market_value=12,010.00 USD max_days_since_observed=634
```

No price has been fetched or set, yet both positions carry a value, because an executed trade is itself a price observation: the 60 shares are valued at the 48.00 paid on 2025-08-01, the ETF at the 220.00 of the December reinvestment. The closing line says how stale that is, 634 days for the ETF, and the `status` column says `carried_forward` because the close used predates today. A position with no observation at all reads `-` with status `unpriced`; a known-wrong share count or lots that disagree on currency read `withheld`; an account whose ledger arrives from two sources at once reads `source_overlap`. All three are `-` and never zero, since zero and unknown would otherwise be the same figure in every total.

## Prices

Five sources compete for each date, in precedence order: a mark you set by hand, the close a connected broker sent, Tiingo, CoinGecko, and the trade-implied price above. `investments prices pull` refreshes Tiingo and CoinGecko for every open position; Tiingo needs a free token stored with `investments prices token`, CoinGecko needs none, and a symbol the provider might resolve to a different security is queued under `investments securities links pending` rather than bound to a guess. A mark records the number on a statement, or a valuation no feed will ever carry, and outranks every provider close for its own date:

```console
$ uv run moneybin investments prices set NWND 2026-09-10 52.25 --note "closing price from the broker statement"
Using profile: demo
✅ Marked 55ab9e0ce69f at 52.25 USD on 2026-09-10
💡 This mark values holdings once the models rebuild — run 'moneybin refresh', or pass --refresh next time
$ uv run moneybin investments prices set BMKT 2026-09-10 231.40 --note "closing price from the broker statement" --refresh
Using profile: demo
Running transforms
Transforms completed in 7.50s
✅ Marked f1f00a4e0562 at 231.40 USD on 2026-09-10
$ uv run moneybin investments holdings
Using profile: demo
┏━━━━━━━━━━━┳━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━━━━┓
┃ security  ┃ quantity      ┃ market value ┃ unrealized ┃ currency ┃ status    ┃
┡━━━━━━━━━━━╇━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━━━━┩
│ 55ab9e0ce │ 60.0000000000 │     3,135.00 │    +195.00 │ USD      │ carried_f │
│ 69f       │               │              │            │          │ orward    │
│ f1f00a4e0 │ 41.5000000000 │     9,603.10 │    +873.10 │ USD      │ carried_f │
│ 562       │               │              │            │          │ orward    │
└───────────┴───────────────┴──────────────┴────────────┴──────────┴───────────┘
6 of 9 columns shown — --wide for all
portfolio market_value=12,738.10 USD max_days_since_observed=1
```

`prices list` shows the resolved series, one winner per date with its source, which is how a figure on the holdings page is tied back to the observation behind it:

```console
$ uv run moneybin investments prices list NWND
Using profile: demo
┏━━━━━━━━━━━━┳━━━━━━━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━━━━━━━━┳━━━━━━━┓
┃ date       ┃ close         ┃ currency ┃ source        ┃ basis ┃
┡━━━━━━━━━━━━╇━━━━━━━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━━━━━━━━╇━━━━━━━┩
│ 2026-09-10 │ 52.2500000000 │ USD      │ override      │ raw   │
│ 2025-08-01 │ 48.0000000000 │ USD      │ trade_implied │ raw   │
│ 2025-02-10 │ 55.0000000000 │ USD      │ trade_implied │ raw   │
│ 2024-03-15 │ 50.0000000000 │ USD      │ trade_implied │ raw   │
│ 2024-01-08 │ 42.0000000000 │ USD      │ trade_implied │ raw   │
└────────────┴───────────────┴──────────┴───────────────┴───────┘
```

`basis` is the adjustment basis the source declared. Only raw closes value a position: an adjusted series restates itself after every later split, so it cannot stand as a historical fact, and the ledger already carries splits as quantity. `prices delete <security> <date>` withdraws a mark and returns that date to provider pricing; a non-positive mark is refused, because a worthless position is a ledger event, not a price. `moneybin system doctor` runs four checks over the series: two sources more than 2% apart on one date, a position still valued from a close older than its security type allows, a held position with no usable price, and a price row whose source it cannot resolve.

## Realized gain and loss

`investments gains` is the 1099-B surface: one row per consumed lot, so a sale that drew on two lots is two rows, each with its own holding term. Held one year or less is `short`; longer is `long`. The 5.00 fee on the sale came off the proceeds before they were split across the lots:

```console
$ uv run moneybin investments gains
Using profile: demo
┏━━━━━━━━━━━━┳━━━━━━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━┓
┃ disposed   ┃ security     ┃ proceeds ┃      gain ┃ currency ┃ term  ┃
┡━━━━━━━━━━━━╇━━━━━━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━┩
│ 2025-02-10 │ 55ab9e0ce69f │ 1,099.17 │    +99.17 │ USD      │ short │
│ 2025-02-10 │ 55ab9e0ce69f │ 5,495.83 │ +1,295.83 │ USD      │ long  │
└────────────┴──────────────┴──────────┴───────────┴──────────┴───────┘
6 of 9 columns shown — --wide for all
```

`--wide` adds the quantity and basis each row was computed from and a `note` marking an incomplete basis; `--term`, `--security`, `--account`, `--from`, and `--to` narrow the table to one tax year or one position. A disposal that exceeds the lots on record, because history before the first import is missing, realizes the excess at zero basis, the worst case for the taxpayer, and says so on stderr even under `-q`.

### Choosing lots

Four methods are computations over the same lots: FIFO consumes oldest first, HIFO highest basis first, average cost pools the basis, and specific identification consumes the lots you name and falls back to FIFO for any remainder. Only specific identification reads a lot selection, so the selection is refused until the security elects it, and the refusal names the command:

```console
$ uv run moneybin investments lots select e23b609336ab824d --lot lot_7618ff9ca1d1221c:50 --lot lot_c79097c308e877ea:70
Using profile: demo
❌ This disposal replays under 'fifo' cost basis, which ignores lot selections; only 'specific' identification consumes them.
💡 Elect specific identification first — 'moneybin investments securities set 55ab9e0ce69f --method specific' (MCP: investments_securities_set) — then retry the selection.
$ uv run moneybin investments securities set 55ab9e0ce69f --method specific
Using profile: demo
securities.upsert security_id=55ab9e0ce69f type=equity actor=cli
✅ Updated security 55ab9e0ce69f
$ uv run moneybin investments lots select e23b609336ab824d --lot lot_7618ff9ca1d1221c:50 --lot lot_c79097c308e877ea:70
Using profile: demo
lot_selections.set disposal=e23b609336ab824d count=2 actor=cli
✅ Set lot selection for e23b609336ab824d: 2 lot(s)
$ uv run moneybin refresh --step transform
Using profile: demo
Running transforms
Transforms completed in 15.16s
Pipeline:
  Transforms: rebuilt
✅ Refresh complete in 15.16s
$ uv run moneybin investments gains
Using profile: demo
┏━━━━━━━━━━━━┳━━━━━━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━┓
┃ disposed   ┃ security     ┃ proceeds ┃    gain ┃ currency ┃ term  ┃
┡━━━━━━━━━━━━╇━━━━━━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━┩
│ 2025-02-10 │ 55ab9e0ce69f │ 2,747.92 │ +247.92 │ USD      │ short │
│ 2025-02-10 │ 55ab9e0ce69f │ 3,847.08 │ +907.08 │ USD      │ long  │
└────────────┴──────────────┴──────────┴─────────┴──────────┴───────┘
6 of 9 columns shown — --wide for all
```

The same sale now draws all 50 March shares and 70 of the January 100. Total gain is 1,395.00 either way; what moved is 148.75 from long-term into short-term, and the lot that stays open is now 30 January shares at 42.00 rather than 30 March shares at 50.00, so the position's basis fell from 2,940.00 to 2,700.00 and its unrealized gain rose by the same 240.00. The selection is declarative: the listed pairs replace any earlier selection, an omitted lot is dropped, and `--clear` submits the empty set and returns the sale to FIFO. A method change is retroactive too. `securities set --method` and `accounts set --default-cost-basis-method` re-derive every past disposal on the next refresh; MoneyBin mirrors whatever method your broker reports and enforces no IRS election lock-in, average-cost switching rule, or wash-sale adjustment.

## Net worth

`reports networth` reads balance observations, not positions. A brokerage synced through Plaid reports a balance that already is its total position value, so it counts once at that figure. A brokerage you keep by hand has no balance until you assert one, and until then it is absent from net worth entirely:

```console
$ uv run moneybin accounts balance assert f2b870002664 2025-12-31 17125.00 --notes "year-end statement: cash plus positions" --yes
Using profile: demo
Asserted balance for account f2b****...2664 on 2025-12-31
✅ Asserted balance for f2b870002664 on 2025-12-31: 17125.00 USD
$ uv run moneybin reports networth
Using profile: demo
USD as of 2025-12-31
Net worth:   437,205.77
Assets:      437,205.77
Liabilities: 0.00
Accounts:    5
┏━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━━━━┓
┃ account                   ┃    balance ┃ currency ┃ source    ┃
┡━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━━━━┩
│ Ally Bank savings …0002   │  33,000.00 │ USD      │           │
│ Brokerage                 │  17,125.00 │ USD      │ assertion │
│ Chase Bank checking …0001 │ 387,080.77 │ USD      │           │
│ Chase Bank credit card    │       0.00 │ USD      │ tabular   │
│ Citi credit card          │       0.00 │ USD      │ tabular   │
└───────────────────────────┴────────────┴──────────┴───────────┘
```

The asserted figure is what the statement says the account is worth on that date, cash and positions together; MoneyBin does not derive it from the ledger, and the ledger's own cash legs are not transactions, so `reports balance-drift` compares the assertion against the two deposits alone. Folding market value into net worth without counting a brokerage twice is designed and not built; see below.

## From an AI client

One read tool and three write tools cover the surface. `investments(view=...)` returns `events`, `holdings`, `lots`, `gains`, or `securities` with the same filters as the commands above, and its holdings payload carries `total_market_value`, `market_value_by_currency`, `max_days_since_observed`, and a `warnings` list that counts the unpriced, withheld, and overlapping rows. `investments_record(events=[...])` writes a batch of events atomically, resolving every account and security before the first write, so a retry after a failure cannot double-insert; `investments_securities_set` creates or updates a catalog entry including its method; `investments_lots_select` is the selection above. The per-account default method is a field on `accounts_set`. Pulling prices and storing the Tiingo token are CLI-only, the token because a credential must not travel through a model. The [MCP tool reference](../reference/mcp-tools.md#investments) lists every parameter.

## What is not built yet

- **No real-broker 1099-B tie-out.** The four methods reconcile against a hand-labeled full-tax-year fixture. Until they tie to a real broker's 1099-B, treat `investments gains` as a number to check against the form, not a replacement for it.
- **Positions do not fold into net worth.** `reports networth` reads balances; a brokerage counts at its reported or asserted balance, and `investments holdings` is the only place market value appears. There is no daily series of what a position was worth on a past date.
- **No wash-sale detection, Schedule D, or Form 8949.** Lot selection is the tax-loss-harvesting primitive; the workflow around it is planned as a reference package on top of this ledger.
- **Options, margin, short positions, and derivatives** are outside the ledger. A merger or spin-off is recorded as a `transfer_out` and `transfer_in` pair carrying the basis; there is no single command for either.
- **A recorded event has no edit or delete.** `import revert` on the event's batch is the undo, and a wrong `--currency` on a recorded event has no in-product remedy yet.
- **Manual entry and Plaid sync are not deduplicated on one account.** Recording by hand into an account Plaid also syncs makes every read on that account report `source_overlap` and `system doctor` fail until the redundant import is reverted.

The design, the shipped decisions, and what each of the above waits on are in the [investments overview](../specs/investments-overview.md) and its [data-model spec](../specs/investments-data-model.md).
