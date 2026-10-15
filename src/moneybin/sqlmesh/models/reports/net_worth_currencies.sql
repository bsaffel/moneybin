/* One row per currency per day: the currency-grain rung of the net-worth
   ladder (reports-net-worth-sql-surface.md), between the account-grain rung
   and the top-level cross-currency total. It reads core.* and app.* only and
   never another reports.* view, because privacy-class derivation rejects a
   stacked ladder (assert_acyclic).
   Fails closed per currency: when an account in net worth holds value in this
   currency but has no balance observation, every measure on that row is NULL
   and unanchored_account_count says how many. */
MODEL (
  name reports.net_worth_currencies,
  kind VIEW
);

WITH home AS (
  SELECT
    (
      SELECT
        p.home_currency
      FROM app.profile_settings AS p
    ) AS home_currency_code
), per_currency AS (
  /* rate, rate_source, and r.published_date are each constant within a
     (currency_code, balance_date) group: the join key is
     (currency_code, home_currency_code, balance_date) and
     core.fct_exchange_rates_effective carries one row per that key, so
     grouping by them alongside the grain adds no extra split. */
  SELECT
    d.currency_code,
    h.home_currency_code,
    r.rate_source,
    d.balance_date,
    r.published_date AS rate_published_date,
    r.rate,
    COUNT(DISTINCT d.account_id) FILTER(WHERE
      NOT d.balance IS NULL) AS account_count,
    COUNT(DISTINCT d.account_id) FILTER(WHERE
      NOT d.balance IS NULL AND NOT d.is_observed) AS carried_forward_count,
    COUNT(DISTINCT d.account_id) FILTER(WHERE
      d.balance IS NULL) AS unanchored_account_count,
    SUM(CASE WHEN d.balance > 0 THEN d.balance ELSE 0 END) AS total_assets,
    SUM(CASE WHEN d.balance < 0 THEN d.balance ELSE 0 END) AS total_liabilities,
    SUM(d.balance) AS net_worth
  FROM core.fct_balances_daily AS d
  CROSS JOIN home AS h
  LEFT JOIN core.fct_exchange_rates_effective AS r
    ON r.from_currency = d.currency_code
    AND r.to_currency = h.home_currency_code
    AND r.effective_date = d.balance_date
  WHERE
    d.in_net_worth
  GROUP BY
    d.currency_code,
    h.home_currency_code,
    r.rate_source,
    d.balance_date,
    r.published_date,
    r.rate
), converted AS (
  /* A NULL balance withholds the whole segment, in its own unit and in home. */
  SELECT
    currency_code,
    home_currency_code,
    rate_source,
    balance_date,
    rate_published_date,
    account_count,
    carried_forward_count,
    unanchored_account_count,
    CASE WHEN unanchored_account_count = 0 THEN total_assets END AS total_assets,
    CASE WHEN unanchored_account_count = 0 THEN total_liabilities END AS total_liabilities,
    CASE WHEN unanchored_account_count = 0 THEN net_worth END AS net_worth,
    CASE
      WHEN unanchored_account_count = 0
      THEN ROUND(total_assets * rate, 2)::DECIMAL(18, 2)
    END AS total_assets_home,
    CASE
      WHEN unanchored_account_count = 0
      THEN ROUND(total_liabilities * rate, 2)::DECIMAL(18, 2)
    END AS total_liabilities_home
  FROM per_currency
)
SELECT
  currency_code, /* ISO 4217 currency this row's totals are denominated in; NULL is the unknown-currency segment, never resolved to the home currency (multi-currency.md Requirement 5). Rows sharing NULL pool into one segment and are summed: unknown is one bucket, not one bucket per real currency, so two accounts in genuinely different currencies that both lack one are added together. That is why an unknown currency is a `system doctor` FAILURE rather than a warning — the remedy is `accounts set --currency`, not a total MoneyBin could compute. Splitting the bucket is impossible by construction: nothing distinguishes two unknowns. Every other money-summing reports.* model pools the same way. */
  home_currency_code, /* app.profile_settings.home_currency; NULL until the user chooses one, and then every row is unpriced */
  rate_source, /* override / provider / identity; NULL when the pair is unpriced on this date */
  balance_date, /* Grain. Calendar date */
  rate_published_date, /* The day the applied rate was actually published; NULL when this currency is unpriced on this date */
  account_count::INT AS account_count, /* Accounts with a known balance contributing on this date in this currency */
  carried_forward_count::INT AS carried_forward_count, /* How many of them are carried forward, not observed */
  unanchored_account_count::INT AS unanchored_account_count, /* Accounts in net worth holding value in this currency with no balance observation on this date; every measure on the row is NULL while it is above 0 */
  total_assets::DECIMAL(18, 2) AS total_assets, /* Sum of positive balances, in currency_code; NULL when unanchored_account_count > 0 */
  total_liabilities::DECIMAL(18, 2) AS total_liabilities, /* Sum of negative balances, kept negative, in currency_code; NULL when unanchored_account_count > 0 */
  net_worth::DECIMAL(18, 2) AS net_worth, /* This currency's segment, in its own unit; NULL when unanchored_account_count > 0 */
  total_assets_home, /* Assets converted at rate; NULL when this currency is unpriced on this date; NULL when unanchored_account_count > 0 */
  total_liabilities_home, /* Liabilities converted at rate; NULL when this currency is unpriced on this date; NULL when unanchored_account_count > 0 */
  (
    total_assets_home + total_liabilities_home
  )::DECIMAL(18, 2) AS net_worth_home /* Headline. Assets plus liabilities in home currency, so the row's own columns always add up; deliberately the sum of the two converted components rather than ROUND(net_worth * rate, 2) — rounding each side independently lets a row's own columns disagree by a cent (the same reasoning _recompute_net_worth_and_change's docstring gives in reports/definitions/net_worth.py). NULL when unpriced; NULL when unanchored_account_count > 0 */
FROM converted
