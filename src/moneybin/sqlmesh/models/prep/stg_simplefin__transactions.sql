MODEL (
  name prep.stg_simplefin__transactions,
  kind VIEW
);

/* Structural failures do not replace validated observations. Validated
   financial holds still win broker order, preventing stale event revival.
   Typed fields also catch structural errors hidden by legacy error precedence.
   Parenthesized UTC conversion preserves date-cast grouping through formatting. */
WITH latest AS (
  SELECT
    *
  FROM raw.simplefin_transactions
  WHERE
    (
      validation_error IS NULL
      OR validation_error IN ('pending', 'nonpositive_posted', 'unsupported_precision')
    )
    AND (
      NOT posted_at IS NULL
      AND posted_at <= fetched_at
      OR (
        JSON_TYPE(original_record, '$.posted') IN ('BIGINT', 'UBIGINT')
        AND TRY_CAST(original_record ->> '$.posted' AS INT128) <= 0
      )
    )
    AND (
      NOT JSON_EXISTS(original_record, '$.transacted_at')
      OR (
        NOT transacted_at IS NULL AND transacted_at <= fetched_at
      )
    )
    AND (
      NOT pending IS NULL OR (
        original_record ->> '$.pending'
      ) IS NULL
    )
  QUALIFY
    ROW_NUMBER() OVER (
      PARTITION BY transaction_reference
      ORDER BY fetched_at DESC, batch_id DESC, observation_version DESC
    ) = 1
)
SELECT
  links.account_id,
  t.account_id AS source_account_key,
  t.transaction_reference AS source_transaction_id,
  t.transaction_id AS native_transaction_id,
  t.batch_id,
  t.source_origin,
  t.observation_version,
  t.description,
  NULL::TEXT AS merchant_name,
  t.currency AS currency_code,
  t.native_scope,
  t.original_record,
  t.source_type,
  t.source_file,
  t.amount,
  COALESCE(t.pending, FALSE) AS is_pending,
  (
    t.posted_at AT TIME ZONE 'UTC'
  )::DATE AS posted_date,
  t.posted_at,
  t.transacted_at,
  t.fetched_at AS extracted_at,
  t.loaded_at
FROM latest AS t
JOIN app.account_links AS links
  ON links.status = 'accepted'
  AND links.ref_kind = 'source_native'
  AND links.source_type = t.source_type
  AND links.source_origin = t.source_origin
  AND links.ref_value = t.account_id
JOIN core.dim_accounts AS a
  ON a.account_id = links.account_id
JOIN seeds.iso_currencies AS c
  ON c.currency_code = t.currency
WHERE
  t.validation_error IS NULL
  AND NOT t.amount IS NULL
  AND t.posted_at > '1970-01-01 00:00:00+00'::TIMESTAMPTZ
  AND t.posted_at <= t.fetched_at
  AND NOT COALESCE(t.pending, FALSE)
  AND NOT c.minor_unit IS NULL
  AND (
    (
      a.account_type = 'depository'
      AND a.account_subtype IN ('checking', 'savings', 'cash')
    )
    OR (
      a.account_type = 'credit' AND a.account_subtype = 'credit card'
    )
  )
