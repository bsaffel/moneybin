MODEL (
  name prep.int_transactions__merged,
  kind VIEW
);

/* Collapses matched groups to one row per transaction_id using
   source-priority merge rules. For each field, the value from the
   highest-priority source with a non-NULL value wins. Exception:
   transaction_date takes the earliest non-NULL value. */
SELECT
  m.transaction_id,
  m.account_id,
  MIN(m.transaction_date) AS transaction_date,
  ARG_MIN(
    m.authorized_date,
    CASE WHEN NOT m.authorized_date IS NULL THEN sp.priority ELSE 2147483647 END
  ) AS authorized_date,
  ARG_MIN(m.amount, COALESCE(sp.priority, 2147483647)) AS amount,
  ARG_MIN(
    m.description,
    CASE WHEN NOT m.description IS NULL THEN sp.priority ELSE 2147483647 END
  ) AS description,
  ARG_MIN(
    m.merchant_name,
    CASE WHEN NOT m.merchant_name IS NULL THEN sp.priority ELSE 2147483647 END
  ) AS merchant_name,
  ARG_MIN(m.memo, CASE WHEN NOT m.memo IS NULL THEN sp.priority ELSE 2147483647 END) AS memo,
  ARG_MIN(m.category, CASE WHEN NOT m.category IS NULL THEN sp.priority ELSE 2147483647 END) AS category,
  ARG_MIN(
    m.subcategory,
    CASE WHEN NOT m.subcategory IS NULL THEN sp.priority ELSE 2147483647 END
  ) AS subcategory,
  ARG_MIN(
    m.payment_channel,
    CASE WHEN NOT m.payment_channel IS NULL THEN sp.priority ELSE 2147483647 END
  ) AS payment_channel,
  ARG_MIN(
    m.transaction_type,
    CASE WHEN NOT m.transaction_type IS NULL THEN sp.priority ELSE 2147483647 END
  ) AS transaction_type,
  ARG_MIN(
    m.check_number,
    CASE WHEN NOT m.check_number IS NULL THEN sp.priority ELSE 2147483647 END
  ) AS check_number,
  BOOL_OR(m.is_pending::BOOLEAN) AS is_pending,
  ARG_MIN(
    m.pending_transaction_id,
    CASE WHEN NOT m.pending_transaction_id IS NULL THEN sp.priority ELSE 2147483647 END
  ) AS pending_transaction_id,
  ARG_MIN(
    m.location_address,
    CASE WHEN NOT m.location_address IS NULL THEN sp.priority ELSE 2147483647 END
  ) AS location_address,
  ARG_MIN(
    m.location_city,
    CASE WHEN NOT m.location_city IS NULL THEN sp.priority ELSE 2147483647 END
  ) AS location_city,
  ARG_MIN(
    m.location_region,
    CASE WHEN NOT m.location_region IS NULL THEN sp.priority ELSE 2147483647 END
  ) AS location_region,
  ARG_MIN(
    m.location_postal_code,
    CASE WHEN NOT m.location_postal_code IS NULL THEN sp.priority ELSE 2147483647 END
  ) AS location_postal_code,
  ARG_MIN(
    m.location_country,
    CASE WHEN NOT m.location_country IS NULL THEN sp.priority ELSE 2147483647 END
  ) AS location_country,
  ARG_MIN(
    m.location_latitude,
    CASE WHEN NOT m.location_latitude IS NULL THEN sp.priority ELSE 2147483647 END
  ) AS location_latitude,
  ARG_MIN(
    m.location_longitude,
    CASE WHEN NOT m.location_longitude IS NULL THEN sp.priority ELSE 2147483647 END
  ) AS location_longitude,
  ARG_MIN(m.currency_code, COALESCE(sp.priority, 2147483647)) AS currency_code,
  ARG_MIN(m.source_type, COALESCE(sp.priority, 2147483647)) AS canonical_source_type,
  COUNT(*) AS source_count,
  MAX(m.match_confidence) AS match_confidence,
  MAX(m.source_extracted_at) AS source_extracted_at,
  MAX(m.loaded_at) AS loaded_at
FROM prep.int_transactions__matched AS m
LEFT JOIN app.seed_source_priority AS sp
  ON m.source_type = sp.source_type
GROUP BY
  m.transaction_id,
  m.account_id
