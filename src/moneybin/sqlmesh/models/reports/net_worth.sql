/* Net worth per day in the profile's home currency: one number per date.
   Fails closed — when any currency contributing on a date has no rate, all
   three measures are NULL and unpriced_currency_count says how many, because
   SUM() over converted segments would silently return the priced subset.
   Also fails closed when an account in net worth holds value but has no
   balance observation: its rows on core.fct_balances_daily carry a NULL
   balance, counted in unanchored_account_count, and the measures are NULL.
   Eligibility is the spine's in_net_worth, computed once there.
   The day-grain rung of the net-worth ladder (reports-net-worth-sql-surface.md,
   net-worth-guard-on-daily-balances.md); it repeats the rate join of the other
   two rungs rather than reading them, because privacy-class derivation rejects
   a reports.* model reading reports.* (assert_acyclic). */
MODEL (
  name reports.net_worth,
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
  /* Grouping by d.currency_code puts every NULL-currency balance in one
     group; it joins no rate (r.from_currency = NULL matches nothing) and so
     counts as one unpriced currency, same as every other money-summing
     reports.* model (see net_worth_currencies.sql). */
  SELECT
    h.home_currency_code,
    d.balance_date,
    d.currency_code,
    r.rate IS NULL AS is_unpriced,
    COUNT(DISTINCT d.account_id) FILTER(WHERE
      NOT d.balance IS NULL) AS account_count,
    COUNT(DISTINCT d.account_id) FILTER(WHERE
      NOT d.balance IS NULL AND NOT d.is_observed) AS carried_forward_count,
    COUNT(DISTINCT d.account_id) FILTER(WHERE
      d.balance IS NULL) AS unanchored_account_count,
    ROUND(SUM(CASE WHEN d.balance > 0 THEN d.balance ELSE 0 END) * r.rate, 2)::DECIMAL(18, 2) AS total_assets_home,
    ROUND(SUM(CASE WHEN d.balance < 0 THEN d.balance ELSE 0 END) * r.rate, 2)::DECIMAL(18, 2) AS total_liabilities_home
  FROM core.fct_balances_daily AS d
  CROSS JOIN home AS h
  LEFT JOIN core.fct_exchange_rates_effective AS r
    ON r.from_currency = d.currency_code
    AND r.to_currency = h.home_currency_code
    AND r.effective_date = d.balance_date
  WHERE
    d.in_net_worth
  GROUP BY
    h.home_currency_code,
    d.balance_date,
    d.currency_code,
    r.rate
), per_day AS (
  SELECT
    home_currency_code,
    balance_date,
    SUM(account_count) AS account_count,
    SUM(carried_forward_count) AS carried_forward_count,
    COUNT(*) AS currency_count,
    COUNT(*) FILTER(WHERE
      is_unpriced) AS unpriced_currency_count,
    SUM(unanchored_account_count) AS unanchored_account_count,
    SUM(total_assets_home) AS total_assets_home,
    SUM(total_liabilities_home) AS total_liabilities_home
  FROM per_currency
  GROUP BY
    home_currency_code,
    balance_date
)
SELECT
  home_currency_code, /* app.profile_settings.home_currency; NULL until the user chooses one, and then every measure below is NULL */
  balance_date, /* Grain. Calendar date */
  account_count::INT AS account_count, /* Accounts with a known balance contributing on this date, across every currency */
  carried_forward_count::INT AS carried_forward_count, /* How many of them are carried forward rather than observed */
  currency_count::INT AS currency_count, /* Distinct currencies held on this date; the unknown-currency segment counts as one */
  unpriced_currency_count::INT AS unpriced_currency_count, /* How many of them had no rate on this date; 0 means every currency is priced */
  unanchored_account_count::INT AS unanchored_account_count, /* Accounts in net worth on this date that hold value but have no balance observation; 0 means none */
  CASE
    WHEN unpriced_currency_count = 0 AND unanchored_account_count = 0
    THEN total_assets_home
  END::DECIMAL(18, 2) AS total_assets, /* Sum of positive balances converted to home_currency_code; NULL when unpriced_currency_count > 0 or unanchored_account_count > 0 */
  CASE
    WHEN unpriced_currency_count = 0 AND unanchored_account_count = 0
    THEN total_liabilities_home
  END::DECIMAL(18, 2) AS total_liabilities, /* Sum of negative balances converted to home_currency_code, kept negative; NULL when unpriced_currency_count > 0 or unanchored_account_count > 0 */
  CASE
    WHEN unpriced_currency_count = 0 AND unanchored_account_count = 0
    THEN total_assets_home + total_liabilities_home
  END::DECIMAL(18, 2) AS net_worth /* Headline: total_assets + total_liabilities in home_currency_code; NULL when unpriced_currency_count > 0 or unanchored_account_count > 0 */
FROM per_day
