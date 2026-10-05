MODEL (
  name prep.stg_simplefin__balances,
  kind VIEW
);

/* Revisions at one provider instant use broker order. The last provider instant
   of each UTC day wins before the daily spine's source-priority selection.
   Parenthesized UTC conversion preserves date-cast grouping through formatting. */
WITH revisions AS (
  SELECT
    *
  FROM raw.simplefin_balances
  WHERE
    (
      validation_error IS NULL OR validation_error = 'unsupported_precision'
    )
    AND NOT balance_at IS NULL
    AND balance_at <= fetched_at
  QUALIFY
    ROW_NUMBER() OVER (
      PARTITION BY source_origin, account_id, balance_at
      ORDER BY fetched_at DESC, batch_id DESC, observation_version DESC
    ) = 1
), daily AS (
  SELECT
    *
  FROM revisions
  QUALIFY
    ROW_NUMBER() OVER (
      PARTITION BY source_origin, account_id, (
        balance_at AT TIME ZONE 'UTC'
      )::DATE
      ORDER BY balance_at DESC, fetched_at DESC, batch_id DESC, observation_version DESC
    ) = 1
)
SELECT
  links.account_id,
  b.account_id AS source_account_key,
  b.batch_id,
  b.source_origin,
  b.observation_version,
  b.currency AS currency_code,
  b.native_scope,
  b.original_record,
  b.source_type,
  b.source_file,
  b.balance AS current_balance,
  b.available_balance,
  (
    b.balance_at AT TIME ZONE 'UTC'
  )::DATE AS balance_date,
  b.balance_at,
  b.fetched_at AS extracted_at,
  b.loaded_at
FROM daily AS b
JOIN app.account_links AS links
  ON links.status = 'accepted'
  AND links.ref_kind = 'source_native'
  AND links.source_type = b.source_type
  AND links.source_origin = b.source_origin
  AND links.ref_value = b.account_id
JOIN core.dim_accounts AS a
  ON a.account_id = links.account_id
JOIN seeds.iso_currencies AS c
  ON c.currency_code = b.currency
WHERE
  b.validation_error IS NULL
  AND NOT b.balance IS NULL
  AND NOT b.balance_at IS NULL
  AND b.balance_at <= b.fetched_at
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
QUALIFY
  ROW_NUMBER() OVER (
    PARTITION BY links.account_id, (
      b.balance_at AT TIME ZONE 'UTC'
    )::DATE
    ORDER BY b.balance_at DESC, b.fetched_at DESC, b.batch_id DESC, b.observation_version DESC, b.source_origin DESC, b.account_id DESC
  ) = 1
