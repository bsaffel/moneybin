/* Net worth per day in the profile's home currency: one number per date.
   Fails closed — when any currency contributing on a date has no rate, all
   three measures are NULL and unpriced_currency_count says how many, because
   SUM() over converted segments would silently return the priced subset.
   Also fails closed on Requirement 14: an eligible account with evidence of
   holding value and no balance row NULLs the measures, counted in
   unanchored_account_count.
   The day-grain rung of the net-worth ladder (reports-net-worth-sql-surface.md);
   it repeats the eligibility filter and rate join of the other two rungs
   rather than reading them, because privacy-class derivation rejects a
   reports.* model reading reports.* (assert_acyclic). */
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
    COUNT(DISTINCT d.account_id) AS account_count,
    COUNT(DISTINCT CASE WHEN NOT d.is_observed THEN d.account_id END) AS carried_forward_count,
    ROUND(SUM(CASE WHEN d.balance > 0 THEN d.balance ELSE 0 END) * r.rate, 2)::DECIMAL(18, 2) AS total_assets_home,
    ROUND(SUM(CASE WHEN d.balance < 0 THEN d.balance ELSE 0 END) * r.rate, 2)::DECIMAL(18, 2) AS total_liabilities_home
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
    SUM(total_assets_home) AS total_assets_home,
    SUM(total_liabilities_home) AS total_liabilities_home
  FROM per_currency
  GROUP BY
    home_currency_code,
    balance_date
), candidates AS (
  /* Requirement 14's candidates, joined to their eligibility inputs. Eligibility
     itself is date-scoped, so it is applied per row below, never here. */
  SELECT
    a.account_id,
    a.archived,
    a.archived_at
  FROM core.dim_unanchored_accounts AS u
  INNER JOIN core.dim_accounts AS a
    ON u.account_id = a.account_id
  WHERE
    a.include_in_net_worth
), unanchored_per_day AS (
  /* Requirement 9's predicate, correlated to each row's own balance_date. */
  SELECT
    p.balance_date,
    COUNT(c.account_id) AS unanchored_account_count
  FROM per_day AS p
  LEFT JOIN candidates AS c
    ON NOT c.archived
    OR (
      NOT c.archived_at IS NULL AND p.balance_date <= c.archived_at
    )
  GROUP BY
    p.balance_date
), balance_driven AS (
  SELECT
    p.home_currency_code,
    p.balance_date,
    p.account_count,
    p.carried_forward_count,
    p.currency_count,
    p.unpriced_currency_count,
    u.unanchored_account_count,
    p.total_assets_home,
    p.total_liabilities_home
  FROM per_day AS p
  INNER JOIN unanchored_per_day AS u
    ON u.balance_date = p.balance_date
), spine_max AS (
  /* The same date reports.net_worth_accounts dates a candidate at: the latest
     eligible balance day, else the spine's last date, else CURRENT_DATE. The
     first is NULL whenever this arm fires; kept so the two views stay identical. */
  SELECT
    COALESCE(
      (
        SELECT
          MAX(e.balance_date)
        FROM core.fct_balances_daily AS e
        INNER JOIN core.dim_accounts AS ea
          ON e.account_id = ea.account_id
        WHERE
          ea.include_in_net_worth
          AND (
            NOT ea.archived
            OR (
              NOT ea.archived_at IS NULL AND e.balance_date <= ea.archived_at
            )
          )
      ),
      (
        SELECT
          MAX(b.balance_date)
        FROM core.fct_balances_daily AS b
      ),
      CURRENT_DATE
    ) AS balance_date
), unanchored_at_spine_max AS (
  SELECT
    m.balance_date,
    COUNT(c.account_id) AS unanchored_account_count
  FROM spine_max AS m
  LEFT JOIN candidates AS c
    ON NOT c.archived
    OR (
      NOT c.archived_at IS NULL AND m.balance_date <= c.archived_at
    )
  GROUP BY
    m.balance_date
), measured AS (
  SELECT
    home_currency_code,
    balance_date,
    account_count,
    carried_forward_count,
    currency_count,
    unpriced_currency_count,
    unanchored_account_count,
    total_assets_home,
    total_liabilities_home
  FROM balance_driven
  UNION ALL
  /* No eligible balance row, but an eligible candidate: one row, every measure
     NULL, so a bare read never returns zero rows for a profile that holds
     value. Dated, and archive-tested, at the same date as
     reports.net_worth_accounts dates the same candidate. The runner covers an
     explicit range. */
  SELECT
    h.home_currency_code,
    t.balance_date,
    0 AS account_count,
    0 AS carried_forward_count,
    0 AS currency_count,
    0 AS unpriced_currency_count,
    t.unanchored_account_count,
    NULL::DECIMAL(18, 2) AS total_assets_home,
    NULL::DECIMAL(18, 2) AS total_liabilities_home
  FROM unanchored_at_spine_max AS t
  CROSS JOIN home AS h
  WHERE
    t.unanchored_account_count > 0
    AND NOT EXISTS(
      SELECT
        1
      FROM balance_driven
    )
)
SELECT
  home_currency_code, /* app.profile_settings.home_currency; NULL until the user chooses one, and then every measure below is NULL */
  balance_date, /* Grain. Calendar date */
  account_count::INT AS account_count, /* Accounts contributing on this date, across every currency */
  carried_forward_count::INT AS carried_forward_count, /* How many of them are carried forward rather than observed */
  currency_count::INT AS currency_count, /* Distinct currencies held on this date; the unknown-currency segment counts as one */
  unpriced_currency_count::INT AS unpriced_currency_count, /* How many of them had no rate on this date; 0 means every currency is priced */
  unanchored_account_count::INT AS unanchored_account_count, /* Eligible accounts with holdings or transaction activity and no balance observation at all; 0 means none */
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
FROM measured
