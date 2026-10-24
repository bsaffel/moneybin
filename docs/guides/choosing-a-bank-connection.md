# Choosing a bank connection

Choose a connection by institution, account type, data coverage, and cost.
Plaid and SimpleFIN can serve different accounts in the same MoneyBin profile;
there is no single provider that is best for every institution.

Provider references checked **October 24, 2026**. Prices and coverage can change.
MoneyBin's SimpleFIN delivery is in progress for Early Access; deployment and
independent live acceptance remain open. Confirm that your installation supports
it before purchasing a subscription specifically for MoneyBin.

## Check your institution first

| Provider | Official reference | What to check |
| --- | --- | --- |
| SimpleFIN Bridge | [Supported institution search](https://beta-bridge.simplefin.org/search-institutions) | Search the institution's exact name. Coverage is limited to US and Canadian institutions, including the US/Canadian versions of international banks. |
| Plaid | [US and Canada Coverage Explorer](https://plaid.com/docs/institutions/) | Select the country and required products: Transactions, Balance, or Investments. |
| Plaid — Europe | [European Coverage Explorer](https://plaid.com/docs/institutions/europe/) | Check institution and product coverage, then confirm that your MoneyBin connection service enables the relevant region and products. |

A directory listing does not guarantee every account or data product. Account
verification support alone is insufficient for transaction sync. Plaid's public
explorers are not updated in real time; the connection operator can check its
[Dashboard institution status](https://dashboard.plaid.com/activity/status) for
more current coverage and outages. Confirm the accounts offered during linking
and the data received afterward.

## Compare the connections

Each row describes one provider so this comparison can grow as MoneyBin adds
integrations. Data support below means **MoneyBin support**, not every product
the provider sells.

| Provider | Cost and who pays | Data supported in MoneyBin | History and freshness | Availability in MoneyBin |
| --- | --- | --- | --- | --- |
| Plaid | No direct Plaid consumer charge. The connection operator pays under its Plaid plan; self-hosters should check their own charges. | Accounts, balances, cash/credit transactions; investment holdings and transactions where supported. Merchant and category information when supplied. | Initial transaction history defaults to 90 days; up to 730 days can be requested. Institution and integration limits apply. Plaid typically checks for transaction updates 1–4 times daily; MoneyBin must also pull the data. | Existing integration through moneybin-sync. Availability depends on enabled products and institution coverage. |
| SimpleFIN Bridge | User subscription: **$1.50/month or $15/year, plus tax**, for up to 25 institutions and 25 apps. | Confirmed cash/credit accounts, balances, and posted transactions. Account classification needs review. Investment holdings and trades are outside the current MoneyBin integration's scope. | Available history varies by institution. Requests cover at most 90 days at a time. Bridge is designed for daily updates. | Early Access delivery in progress. Use a compatible client and connection service once deployment is available. |

Sources: [Plaid pricing](https://plaid.com/pricing/),
[Plaid transaction history and updates](https://plaid.com/docs/transactions/),
[SimpleFIN pricing](https://beta-bridge.simplefin.org/), and
[SimpleFIN limits](https://beta-bridge.simplefin.org/info/developers).

## Choose for the data you need

- **Checking, savings, or credit cards:** consider either provider. Compare the
  accounts, history, and balances actually delivered at your institution.
- **Investment holdings or trades:** check Plaid's Investments coverage. MoneyBin's
  current SimpleFIN path covers cash and credit accounts, not the investment ledger.
- **Predictable personal subscription pricing:** SimpleFIN publishes a flat price.
  Compare it with the actual cost of your Plaid-backed setup, including operation
  of the connection service. Plaid's lack of a direct consumer fee does not make
  a self-hosted service free.
- **Older history:** verify the earliest transaction received. Neither a requested
  date range nor an empty response proves that all history is present. Use
  [supported file imports](data-import.md) to fill cash/credit history gaps.
- **Only one provider connects successfully:** start there and validate the data.
  If neither delivers what you need, use supported file imports.

Plaid is a useful choice for its existing MoneyBin investment support and
provider-supplied merchant/category information. Those are reasons to choose it
for a particular need, not evidence that it is cheaper or more complete at every
institution. SimpleFIN's flat subscription and cash/credit scope may fit another
user better. Choose separately for each institution when necessary.

## Understand history and update limits

Plaid's 730-day figure is the maximum initial transaction history that an
integration can request, not a promise of two years for every account. Its default
request is 90 days, and an institution may return less. History accumulates while
the connection remains active. See
[Plaid's history guidance](https://plaid.com/docs/transactions/) and
[institution-specific limitations](https://plaid.com/docs/transactions/troubleshooting/).

SimpleFIN's 90-day range is a **per-request limit**, not a universal total-history
limit. Older history depends on the institution. MoneyBin retrieves completed UTC
days in overlapping windows; repeated pulls do not make the institution's data
more current. See [SimpleFIN history and recovery](data-import.md#simplefin-sync)
before requesting a backfill.

For either connection, distinguish the time MoneyBin pulled data from the time
the institution last observed a balance. A successful pull can contain an older
balance or incomplete history.

## Verify completeness after connecting

Compare MoneyBin with the institution's website or statement before relying on
totals:

1. **Accounts:** confirm every expected account is present, with the correct type,
   subtype, and currency. Review SimpleFIN account classifications explicitly.
2. **Balances:** compare amounts and observation dates. Keep current and available
   balances separate; confirm credit-card debt and overpayments have the right sign.
3. **Transactions:** compare recent posted transactions, amounts, dates, and any
   reversals. Pending activity is not a substitute for the posted ledger.
4. **History:** record the earliest transaction received and check for missing
   periods against statements.
5. **Investments:** where needed, confirm holdings and investment transactions,
   not just the presence of an investment account balance.
6. **Ongoing operation:** repeat a pull after new activity posts. Check
   `moneybin sync status` and `moneybin system doctor` for incomplete coverage,
   stale balances, and account-link decisions.

When adding another source for an existing account, review proposed account links
and overlapping transactions before trusting combined totals. A successful
connection is evidence of access, not proof of completeness.

## Connect and manage access

Both integrations use MoneyBin's connection service, moneybin-sync. Financial
data crosses that service before it reaches the local database; self-hosting
changes who operates it. Follow the setup flow rather than putting credentials
or SimpleFIN setup tokens into commands or an agent conversation.

- [Plaid setup](data-import.md#live-banking-sync-plaid)
- [SimpleFIN setup, recovery, and revocation](data-import.md#simplefin-sync)

## Maintaining this comparison

Add one row per supported integration and an official institution-lookup link.
Record who pays, MoneyBin's supported data, history/update limits, and delivery
status. Use **Unknown** for unverified facts, **Not supported** for confirmed
limitations, and **Planned** for future integrations. Recheck primary sources and
update the reference date when revising prices or coverage.
