/* Canonical transactions fact view; reads from the deduplicated merged layer
   with categorization and merchant joins; negative amount = expense, positive = income */
MODEL (
  name core.fct_transactions,
  kind VIEW,
  grain transaction_id
);

WITH enriched AS (
  SELECT
    t.transaction_id,
    t.account_id,
    t.transaction_date,
    t.authorized_date,
    t.amount,
    ABS(t.amount) AS amount_absolute,
    CASE WHEN t.amount < 0 THEN 'expense' WHEN t.amount > 0 THEN 'income' ELSE 'zero' END AS transaction_direction,
    t.description,
    COALESCE(m.canonical_name, t.merchant_name) AS merchant_name,
    t.memo,
    COALESCE(c.category, t.category) AS category,
    COALESCE(c.subcategory, t.subcategory) AS subcategory,
    c.categorized_by,
    t.payment_channel,
    t.transaction_type,
    t.check_number,
    t.is_pending,
    t.pending_transaction_id,
    t.location_address,
    t.location_city,
    t.location_region,
    t.location_postal_code,
    t.location_country,
    t.location_latitude,
    t.location_longitude,
    t.currency_code,
    t.canonical_source_type AS source_type,
    t.source_count,
    t.match_confidence,
    t.source_extracted_at,
    COALESCE(bt_debit.transfer_id, bt_credit.transfer_id) AS transfer_pair_id,
    (
      NOT bt_debit.transfer_id IS NULL OR NOT bt_credit.transfer_id IS NULL
    ) AS is_transfer,
    t.loaded_at
  FROM prep.int_transactions__merged AS t
  LEFT JOIN app.transaction_categories AS c
    ON t.transaction_id = c.transaction_id
  LEFT JOIN app.merchants AS m
    ON c.merchant_id = m.merchant_id
  LEFT JOIN core.bridge_transfers AS bt_debit
    ON t.transaction_id = bt_debit.debit_transaction_id
  LEFT JOIN core.bridge_transfers AS bt_credit
    ON t.transaction_id = bt_credit.credit_transaction_id
)
SELECT
  transaction_id, /* Gold key: deterministic SHA-256 hash, unique per real-world transaction */
  account_id, /* Foreign key to core.dim_accounts */
  transaction_date, /* Date the transaction posted or settled; earliest across sources for merged records */
  authorized_date, /* Date the transaction was authorized; from highest-priority source */
  amount, /* Transaction amount; negative = expense, positive = income */
  amount_absolute, /* Absolute value of amount; avoids sign handling in aggregations */
  transaction_direction, /* Derived from amount sign: expense, income, or zero */
  description, /* Payee or merchant description from highest-priority source */
  merchant_name, /* Normalized merchant name from app.merchants; falls back to source value */
  memo, /* Additional notes from highest-priority source */
  category, /* Spending category; from app.transaction_categories when categorized, else source value */
  subcategory, /* Spending subcategory; from app.transaction_categories when categorized, else source value */
  categorized_by, /* How the category was assigned: rule, ai, user, or NULL if uncategorized */
  payment_channel, /* Payment channel (online, in store, other) */
  transaction_type, /* Source-specific transaction type code */
  check_number, /* Check number for check transactions; NULL otherwise */
  is_pending, /* True if any contributing source row is pending */
  pending_transaction_id, /* ID of the pending transaction this record resolved */
  location_address, /* Merchant street address */
  location_city, /* Merchant city */
  location_region, /* Merchant state or region */
  location_postal_code, /* Merchant postal code */
  location_country, /* Merchant country code */
  location_latitude, /* Merchant latitude coordinate */
  location_longitude, /* Merchant longitude coordinate */
  currency_code, /* ISO 4217 currency code */
  source_type, /* Canonical source type: highest-priority source in the merge group */
  source_count, /* Number of contributing source rows (1 for unmatched, 2+ for merged) */
  match_confidence, /* Match confidence score; NULL for unmatched records */
  source_extracted_at, /* When the data was parsed from the source file */
  loaded_at, /* When this record was last written */
  is_transfer, /* TRUE if this transaction is part of a confirmed transfer pair */
  transfer_pair_id, /* FK to core.bridge_transfers.transfer_id; NULL if not a transfer */
  DATE_PART('year', transaction_date) AS transaction_year, /* Calendar year */
  DATE_PART('month', transaction_date) AS transaction_month, /* Calendar month (1-12) */
  DATE_PART('day', transaction_date) AS transaction_day, /* Calendar day (1-31) */
  DATE_PART('dayofweek', transaction_date) AS transaction_day_of_week, /* Day of week: 0 = Sunday */
  STRFTIME(transaction_date, '%Y-%m') AS transaction_year_month, /* YYYY-MM for period grouping */
  STRFTIME(transaction_date, '%Y') || '-Q' || QUARTER(transaction_date) AS transaction_year_quarter /* YYYY-QN for period grouping */
FROM enriched
