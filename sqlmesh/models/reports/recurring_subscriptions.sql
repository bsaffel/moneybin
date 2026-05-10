/* Heuristic detection of likely-recurring outflows. Surfaces candidates with
   confidence scores; does NOT auto-classify. Algorithm (per spec):
   group by (merchant_normalized, ROUND(amount, 0)) with >= 3 occurrences in
   the last 18 months, infer cadence from interval mean+stddev, compute
   confidence = clamp(0,1) of (occurrence_count/6) * (1 - stddev/14). */
MODEL (
  name reports.recurring_subscriptions,
  kind VIEW
);

WITH eligible AS (
  SELECT
    COALESCE(merchant_name, '(unknown)') AS merchant_normalized,
    ROUND(amount, 0) AS amount_bucket,
    amount,
    transaction_date
  FROM core.fct_transactions
  WHERE amount < 0
    AND NOT is_transfer
    AND transaction_date >= current_date - INTERVAL '18 months'
), with_intervals AS (
  SELECT
    merchant_normalized,
    amount_bucket,
    amount,
    transaction_date,
    transaction_date - LAG(transaction_date) OVER (
      PARTITION BY merchant_normalized, amount_bucket
      ORDER BY transaction_date
    ) AS interval_days
  FROM eligible
), grouped AS (
  SELECT
    merchant_normalized,
    amount_bucket,
    AVG(ABS(amount)) AS avg_amount,
    AVG(interval_days) AS interval_days_avg,
    STDDEV(interval_days) AS interval_days_stddev,
    COUNT(*) AS occurrence_count,
    MIN(transaction_date) AS first_seen,
    MAX(transaction_date) AS last_seen
  FROM with_intervals
  GROUP BY merchant_normalized, amount_bucket
  HAVING COUNT(*) >= 3
)
SELECT
  merchant_normalized, /* Normalized merchant string */
  avg_amount, /* Mean absolute charge */
  CASE
    WHEN interval_days_avg BETWEEN 5 AND 9 AND interval_days_stddev < 2 THEN 'weekly'
    WHEN interval_days_avg BETWEEN 12 AND 16 AND interval_days_stddev < 3 THEN 'biweekly'
    WHEN interval_days_avg BETWEEN 27 AND 33 AND interval_days_stddev < 4 THEN 'monthly'
    WHEN interval_days_avg BETWEEN 85 AND 95 AND interval_days_stddev < 7 THEN 'quarterly'
    WHEN interval_days_avg BETWEEN 355 AND 375 AND interval_days_stddev < 14 THEN 'yearly'
    ELSE 'irregular'
  END AS cadence, /* weekly | biweekly | monthly | quarterly | yearly | irregular */
  interval_days_avg, /* Mean days between consecutive charges */
  interval_days_stddev, /* Stddev of inter-arrival intervals */
  occurrence_count, /* Number of matching charges in the last 18 months */
  first_seen, /* Earliest charge in this cluster */
  last_seen, /* Most recent charge */
  CASE
    WHEN current_date - last_seen <= 60 THEN 'active'
    ELSE 'inactive'
  END AS status, /* 'active' if last_seen within 60 days */
  CASE
    WHEN interval_days_avg BETWEEN 5 AND 9 AND interval_days_stddev < 2 THEN avg_amount * 52
    WHEN interval_days_avg BETWEEN 12 AND 16 AND interval_days_stddev < 3 THEN avg_amount * 26
    WHEN interval_days_avg BETWEEN 27 AND 33 AND interval_days_stddev < 4 THEN avg_amount * 12
    WHEN interval_days_avg BETWEEN 85 AND 95 AND interval_days_stddev < 7 THEN avg_amount * 4
    WHEN interval_days_avg BETWEEN 355 AND 375 AND interval_days_stddev < 14 THEN avg_amount * 1
    WHEN interval_days_avg > 0 THEN avg_amount * (365.25 / interval_days_avg)
    ELSE NULL
  END AS annualized_cost, /* Estimated yearly cost based on cadence */
  LEAST(1.0, occurrence_count / 6.0)
    * GREATEST(0.0, 1.0 - LEAST(1.0, COALESCE(interval_days_stddev, 14.0) / 14.0)) AS confidence
    /* 0.0-1.0; saturates at 1.0 with >=6 occurrences and 0 variance */
FROM grouped
