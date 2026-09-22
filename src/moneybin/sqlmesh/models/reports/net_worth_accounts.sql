/* One row per included account per day, in its own currency and in the
   profile's home currency. The account-grain rung of the net-worth ladder
   (reports-net-worth-sql-surface.md); it reads core.* and app.* only and
   never another reports.* view, because privacy-class derivation rejects
   a stacked ladder (assert_acyclic).
   This rung is not an exact summand of reports.net_worth_currencies:
   account_balance_home rounds once per account, while that view rounds once
   per currency per day, so SUM(account_balance_home) can differ from
   total_assets_home + total_liabilities_home by a fraction of a cent per
   currency per day. Expected and bounded — reconcile the rungs to the cent. */
MODEL (
  name reports.net_worth_accounts,
  kind VIEW
);

WITH home AS (
  SELECT
    (
      SELECT
        p.home_currency
      FROM app.profile_settings AS p
    ) AS home_currency_code
), spine AS (
  SELECT
    d.account_id,
    d.currency_code,
    d.observation_source,
    d.balance,
    d.reconciliation_delta,
    d.is_observed,
    d.balance_date,
    MAX(CASE WHEN d.is_observed THEN d.balance_date END) OVER (
      PARTITION BY d.account_id
      ORDER BY d.balance_date
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS last_observed_date
  FROM core.fct_balances_daily AS d
)
SELECT
  s.account_id, /* Grain. Foreign key to core.dim_accounts */
  a.display_name AS account_name, /* Resolved display label from core.dim_accounts */
  s.currency_code, /* The account's own denomination; NULL is the unknown segment and is never priced */
  h.home_currency_code, /* app.profile_settings.home_currency; NULL until the user chooses one */
  a.account_type, /* Canonical account classification: depository, credit, loan, investment, other */
  s.is_observed, /* FALSE means the balance is carried forward from an earlier observation */
  s.observation_source, /* ofx / tabular / assertion / plaid; NULL when interpolated */
  r.rate_source, /* override / provider / identity; NULL when the pair is unpriced on this date */
  s.balance_date, /* Grain. Calendar date */
  r.published_date AS rate_published_date, /* The day the applied rate was actually published */
  CAST(s.balance_date - s.last_observed_date AS INT) AS days_since_observed, /* 0 on an observed day */
  s.reconciliation_delta, /* Observed minus transaction-derived; NULL on interpolated days */
  s.balance AS account_balance, /* In currency_code */
  ROUND(s.balance * r.rate, 2)::DECIMAL(18, 2) AS account_balance_home /* In home_currency_code; NULL when the pair is unpriced */
FROM spine AS s
INNER JOIN core.dim_accounts AS a
  ON s.account_id = a.account_id
CROSS JOIN home AS h
LEFT JOIN core.fct_exchange_rates_effective AS r
  ON r.from_currency = s.currency_code
  AND r.to_currency = h.home_currency_code
  AND r.effective_date = s.balance_date
WHERE
  a.include_in_net_worth
  AND (
    NOT a.archived
    OR (
      NOT a.archived_at IS NULL AND s.balance_date <= a.archived_at
    )
  )
