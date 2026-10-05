MODEL (
  name prep.stg_simplefin__accounts,
  kind VIEW
);

/* Identity/currency evidence must not depend on financial eligibility: the
   account must exist before a user can classify it. Raw retains every hold. */
WITH latest AS (
  SELECT
    *
  FROM raw.simplefin_accounts
  QUALIFY
    ROW_NUMBER() OVER (
      PARTITION BY source_origin, account_id
      ORDER BY fetched_at DESC, batch_id DESC, observation_version DESC
    ) = 1
)
SELECT
  links.account_id,
  a.account_id AS source_account_key,
  a.batch_id,
  a.source_origin,
  a.observation_version,
  a.account_label,
  CASE
    WHEN (
      a.native_scope ->> '$.conn_id'
    ) IS NULL
    THEN a.native_scope ->> '$.name'
    ELSE a.institution_label
  END AS institution_name,
  NULL::TEXT AS account_type,
  NULL::TEXT AS account_subtype,
  a.currency AS currency_code,
  a.validation_error,
  a.identity_strength,
  a.native_scope,
  a.original_record,
  a.source_type,
  a.source_file,
  a.fetched_at AS extracted_at,
  a.loaded_at
FROM latest AS a
LEFT JOIN app.account_links AS links
  ON links.status = 'accepted'
  AND links.ref_kind = 'source_native'
  AND links.source_type = a.source_type
  AND links.source_origin = a.source_origin
  AND links.ref_value = a.account_id
