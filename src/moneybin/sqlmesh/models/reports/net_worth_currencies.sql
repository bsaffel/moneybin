/* One row per currency per day: the currency-grain rung of the net-worth
   ladder (reports-net-worth-sql-surface.md), between the account-grain rung
   and the top-level cross-currency total. It reads core.* and app.* only and
   never another reports.* view, because privacy-class derivation rejects a
   stacked ladder (assert_acyclic). */
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
    COUNT(DISTINCT d.account_id) AS account_count,
    COUNT(DISTINCT CASE WHEN NOT d.is_observed THEN d.account_id END) AS carried_forward_count,
    SUM(CASE WHEN d.balance > 0 THEN d.balance ELSE 0 END) AS total_assets,
    SUM(CASE WHEN d.balance < 0 THEN d.balance ELSE 0 END) AS total_liabilities,
    SUM(d.balance) AS net_worth
  FROM core.fct_balances_daily AS d
  INNER JOIN core.dim_accounts AS a
    ON d.account_id = a.account_id
  CROSS JOIN home AS h
  LEFT JOIN core.fct_exchange_rates_effective AS r
    ON r.from_currency = d.currency_code
    AND r.to_currency = h.home_currency_code
    AND r.effective_date = d.balance_date
  WHERE
    a.include_in_net_worth
    AND (
      NOT a.archived
      OR (
        NOT a.archived_at IS NULL AND d.balance_date <= a.archived_at
      )
    )
  GROUP BY
    d.currency_code,
    h.home_currency_code,
    r.rate_source,
    d.balance_date,
    r.published_date,
    r.rate
), converted AS (
  SELECT
    currency_code,
    home_currency_code,
    rate_source,
    balance_date,
    rate_published_date,
    account_count,
    carried_forward_count,
    total_assets,
    total_liabilities,
    net_worth,
    ROUND(total_assets * rate, 2)::DECIMAL(18, 2) AS total_assets_home,
    ROUND(total_liabilities * rate, 2)::DECIMAL(18, 2) AS total_liabilities_home
  FROM per_currency
)
SELECT
  currency_code, /* ISO 4217 currency this row's totals are denominated in; NULL is the unknown-currency segment, never resolved to the home currency (multi-currency.md Requirement 5). Rows sharing NULL pool into one segment and are summed: unknown is one bucket, not one bucket per real currency, so two accounts in genuinely different currencies that both lack one are added together. That is why an unknown currency is a `system doctor` FAILURE rather than a warning — the remedy is `accounts set --currency`, not a total MoneyBin could compute. Splitting the bucket is impossible by construction: nothing distinguishes two unknowns. Every other money-summing reports.* model pools the same way. */
  home_currency_code, /* app.profile_settings.home_currency; NULL until the user chooses one, and then every row is unpriced */
  rate_source, /* override / provider / identity; NULL when the pair is unpriced on this date */
  balance_date, /* Grain. Calendar date */
  rate_published_date, /* The day the applied rate was actually published; NULL when this currency is unpriced on this date */
  account_count, /* Accounts contributing on this date in this currency */
  carried_forward_count, /* How many of them are carried forward, not observed */
  total_assets, /* Sum of positive balances, in currency_code */
  total_liabilities, /* Sum of negative balances, kept negative, in currency_code */
  net_worth, /* This currency's segment, in its own unit */
  total_assets_home, /* Assets converted at rate; NULL when this currency is unpriced on this date */
  total_liabilities_home, /* Liabilities converted at rate; NULL when this currency is unpriced on this date */
  total_assets_home + total_liabilities_home AS net_worth_home /* Headline. Assets plus liabilities in home currency, so the row's own columns always add up; deliberately the sum of the two converted components rather than ROUND(net_worth * rate, 2) — rounding each side independently lets a row's own columns disagree by a cent (the same reasoning _recompute_net_worth_and_change's docstring gives in reports/definitions/net_worth.py). NULL when unpriced */
FROM converted
