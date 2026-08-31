MODEL (
  name prep.stg_tabular__transactions,
  kind VIEW
);

WITH accepted_native_links AS (
  SELECT DISTINCT
    account_id,
    source_type,
    source_origin,
    ref_value
  FROM app.account_links
  WHERE
    status = 'accepted' AND ref_kind = 'source_native'
), legacy_pinned AS (
  SELECT
    t.transaction_id,
    t.account_id AS canonical_account_id,
    t.source_file,
    t.source_type,
    t.source_origin,
    t.transaction_date,
    t.amount,
    t.description,
    t.original_date_str,
    t.source_transaction_id,
    ROW_NUMBER() OVER (
      PARTITION BY t.account_id, t.source_file, t.source_type, t.source_origin, t.transaction_date, t.amount, t.description, t.original_date_str, t.source_transaction_id
      ORDER BY t.transaction_id
    ) AS occurrence
  FROM raw.tabular_transactions AS t
  /* Before #438, a pin wrote the canonical account id into this source-native
     column. Keep those rows authoritative: their source key is the input to the
     gold id, so replacing them would orphan app.* curation. The self-map is the
     proof that the raw value is legacy, not merely a string matching another
     account id. A reversed self-map after a merge cannot prove which later
     native row, if any, is its replacement, so it is retained without pairing. */
  JOIN app.account_links AS legacy_self_map
    ON legacy_self_map.ref_kind = 'source_native'
    AND legacy_self_map.account_id = t.account_id
    AND legacy_self_map.ref_value = t.account_id
    AND legacy_self_map.source_type = t.source_type
    AND legacy_self_map.source_origin IS NOT DISTINCT FROM t.source_origin
    AND legacy_self_map.status = 'accepted'
  WHERE
    t.deleted_from_source_at IS NULL
), corrected_pinned AS (
  SELECT
    t.transaction_id,
    t.account_id AS source_account_key,
    SUBSTRING(
      SHA256(
        t.source_type || '|' || t.source_origin || '|' || t.account_id || '|' || t.transaction_id
      ),
      1,
      16
    ) AS gold_transaction_id,
    link.account_id AS canonical_account_id,
    t.source_file,
    t.source_type,
    t.source_origin,
    t.transaction_date,
    t.amount,
    t.description,
    t.original_date_str,
    t.source_transaction_id,
    ROW_NUMBER() OVER (
      PARTITION BY link.account_id, t.source_file, t.source_type, t.source_origin, t.transaction_date, t.amount, t.description, t.original_date_str, t.source_transaction_id
      ORDER BY t.transaction_id
    ) AS occurrence
  FROM raw.tabular_transactions AS t
  JOIN accepted_native_links AS link
    ON link.source_type = t.source_type
    AND link.source_origin = t.source_origin
    AND link.ref_value = t.account_id
  WHERE
    t.deleted_from_source_at IS NULL
    AND t.account_id <> link.account_id
    AND NOT EXISTS(
      SELECT
        1
      FROM app.account_links AS legacy_self_map
      WHERE
        legacy_self_map.ref_kind = 'source_native'
        AND legacy_self_map.account_id = t.account_id
        AND legacy_self_map.ref_value = t.account_id
        AND legacy_self_map.source_type = t.source_type
        AND legacy_self_map.source_origin IS NOT DISTINCT FROM t.source_origin
    )
), stable_legacy_pinned AS (
  SELECT
    t.transaction_id,
    t.account_id AS canonical_account_id,
    t.source_file,
    t.source_type,
    t.source_origin,
    NULLIF(TRIM(t.source_transaction_id), '') AS source_transaction_id,
    ROW_NUMBER() OVER (
      PARTITION BY t.account_id, t.source_file, t.source_type, t.source_origin, NULLIF(TRIM(t.source_transaction_id), '')
      ORDER BY t.transaction_id
    ) AS occurrence
  FROM raw.tabular_transactions AS t
  JOIN app.account_links AS legacy_self_map
    ON legacy_self_map.ref_kind = 'source_native'
    AND legacy_self_map.account_id = t.account_id
    AND legacy_self_map.ref_value = t.account_id
    AND legacy_self_map.source_type = t.source_type
    AND legacy_self_map.source_origin IS NOT DISTINCT FROM t.source_origin
    AND legacy_self_map.status = 'accepted'
  WHERE
    t.deleted_from_source_at IS NULL
    AND NOT NULLIF(TRIM(t.source_transaction_id), '') IS NULL
), stable_corrected_pinned AS (
  SELECT
    t.transaction_id,
    t.account_id AS source_account_key,
    SUBSTRING(
      SHA256(
        t.source_type || '|' || t.source_origin || '|' || t.account_id || '|' || t.transaction_id
      ),
      1,
      16
    ) AS gold_transaction_id,
    link.account_id AS canonical_account_id,
    t.source_file,
    t.source_type,
    t.source_origin,
    NULLIF(TRIM(t.source_transaction_id), '') AS source_transaction_id,
    ROW_NUMBER() OVER (
      PARTITION BY link.account_id, t.source_file, t.source_type, t.source_origin, NULLIF(TRIM(t.source_transaction_id), '')
      ORDER BY t.transaction_id
    ) AS occurrence
  FROM raw.tabular_transactions AS t
  JOIN accepted_native_links AS link
    ON link.source_type = t.source_type
    AND link.source_origin = t.source_origin
    AND link.ref_value = t.account_id
  WHERE
    t.deleted_from_source_at IS NULL
    AND t.account_id <> link.account_id
    AND NOT NULLIF(TRIM(t.source_transaction_id), '') IS NULL
    AND NOT EXISTS(
      SELECT
        1
      FROM app.account_links AS legacy_self_map
      WHERE
        legacy_self_map.ref_kind = 'source_native'
        AND legacy_self_map.account_id = t.account_id
        AND legacy_self_map.ref_value = t.account_id
        AND legacy_self_map.source_type = t.source_type
        AND legacy_self_map.source_origin IS NOT DISTINCT FROM t.source_origin
    )
), stable_source_id_pairs AS MATERIALIZED (
  SELECT
    legacy.transaction_id AS legacy_transaction_id,
    legacy.canonical_account_id AS legacy_source_account_key,
    corrected.transaction_id AS corrected_transaction_id,
    corrected.source_account_key AS corrected_source_account_key,
    corrected.source_file,
    corrected.source_type,
    corrected.source_origin,
    corrected.canonical_account_id,
    corrected.gold_transaction_id
  FROM stable_legacy_pinned AS legacy
  JOIN stable_corrected_pinned AS corrected
    ON legacy.canonical_account_id = corrected.canonical_account_id
    AND legacy.source_file IS NOT DISTINCT FROM corrected.source_file
    AND legacy.source_type = corrected.source_type
    AND legacy.source_origin IS NOT DISTINCT FROM corrected.source_origin
    AND legacy.source_transaction_id = corrected.source_transaction_id
    AND legacy.occurrence = corrected.occurrence
), curated_transaction_ids AS (
  SELECT
    transaction_id
  FROM app.transaction_categories
  UNION
  SELECT
    transaction_id
  FROM app.transaction_notes
  UNION
  SELECT
    transaction_id
  FROM app.transaction_tags
  UNION
  SELECT
    transaction_id
  FROM app.transaction_splits
  UNION
  SELECT
    transaction_id
  FROM app.categorization_decisions
  UNION
  SELECT
    new_transaction_id AS transaction_id
  FROM app.transaction_id_aliases
  UNION
  SELECT
    UNNEST(sample_txn_ids) AS transaction_id
  FROM app.proposed_rules
), protected_corrected_match_endpoints AS (
  SELECT
    match.account_id,
    match.source_type_a AS source_type,
    match.source_origin_a AS source_origin,
    match.source_transaction_id_a AS transaction_id
  FROM app.match_decisions AS match
  WHERE
    match.match_status IN ('pending', 'accepted', 'rejected')
    AND match.reversed_at IS NULL
    AND match.match_type IN ('dedup', 'transfer')
  UNION
  SELECT
    CASE
      WHEN match.match_type = 'dedup'
      THEN match.account_id
      ELSE match.account_id_b
    END AS account_id,
    match.source_type_b AS source_type,
    match.source_origin_b AS source_origin,
    match.source_transaction_id_b AS transaction_id
  FROM app.match_decisions AS match
  WHERE
    match.match_status IN ('pending', 'accepted', 'rejected')
    AND match.reversed_at IS NULL
    AND match.match_type IN ('dedup', 'transfer')
), replaceable_stable_source_id_pairs AS MATERIALIZED (
  SELECT
    pair.legacy_transaction_id,
    pair.legacy_source_account_key,
    pair.corrected_transaction_id,
    pair.corrected_source_account_key,
    pair.source_file,
    pair.source_type,
    pair.source_origin
  FROM stable_source_id_pairs AS pair
  LEFT JOIN curated_transaction_ids AS curation
    ON curation.transaction_id = pair.gold_transaction_id
  LEFT JOIN protected_corrected_match_endpoints AS endpoint
    ON endpoint.account_id = pair.canonical_account_id
    AND endpoint.source_type = pair.source_type
    AND endpoint.source_origin IS NOT DISTINCT FROM pair.source_origin
    AND endpoint.transaction_id = pair.corrected_transaction_id
  WHERE
    curation.transaction_id IS NULL AND endpoint.transaction_id IS NULL
), ranked AS (
  SELECT
    transaction_id,
    account_id,
    transaction_date,
    post_date,
    amount,
    original_amount,
    original_date_str,
    TRIM(description) AS description,
    TRIM(memo) AS memo,
    category,
    subcategory,
    transaction_type,
    status,
    check_number,
    source_transaction_id,
    reference_number,
    balance,
    UPPER(NULLIF(TRIM(currency), '')) AS currency,
    member_name,
    source_file,
    source_type,
    source_origin,
    import_id,
    row_number,
    extracted_at,
    loaded_at,
    deleted_from_source_at,
    ROW_NUMBER() OVER (PARTITION BY transaction_id, account_id ORDER BY loaded_at DESC) AS _row_num
  FROM raw.tabular_transactions AS t
  /* Exclude soft-deleted rows BEFORE ranking: a soft-deleted row with a newer
     loaded_at would rank #1 and then be dropped by the outer filter, while a valid
     same-key row at #2 is also excluded — silently losing the transaction.
     Filtering pre-rank lets the valid row take #1. A stable source ID retains
     the corrected values under the legacy source-account key, preserving the
     legacy gold id. ID-less duplicate content instead keeps the legacy row and
     suppresses its corrected twin only when no app state references that twin.
     Both pairing paths retain the corrected row when curation or terminal match
     state references it. Duplicate content is paired by occurrence, so a reused
     path remains visible. */
  WHERE
    deleted_from_source_at IS NULL
    AND NOT EXISTS(
      SELECT
        1
      FROM corrected_pinned AS corrected
      JOIN legacy_pinned AS legacy
        ON legacy.canonical_account_id = corrected.canonical_account_id
        AND legacy.source_file IS NOT DISTINCT FROM corrected.source_file
        AND legacy.source_type = corrected.source_type
        AND legacy.source_origin IS NOT DISTINCT FROM corrected.source_origin
        AND NULLIF(TRIM(legacy.source_transaction_id), '') IS NULL
        AND NULLIF(TRIM(corrected.source_transaction_id), '') IS NULL
        AND legacy.transaction_date = corrected.transaction_date
        AND legacy.amount = corrected.amount
        AND legacy.description IS NOT DISTINCT FROM corrected.description
        AND legacy.original_date_str IS NOT DISTINCT FROM corrected.original_date_str
        AND legacy.occurrence = corrected.occurrence
      WHERE
        corrected.transaction_id = t.transaction_id
        AND NOT EXISTS(
          SELECT
            1
          FROM curated_transaction_ids AS curation
          WHERE
            curation.transaction_id = corrected.gold_transaction_id
        )
        AND NOT EXISTS(
          SELECT
            1
          FROM protected_corrected_match_endpoints AS endpoint
          WHERE
            endpoint.account_id = corrected.canonical_account_id
            AND endpoint.source_type = corrected.source_type
            AND endpoint.source_origin IS NOT DISTINCT FROM corrected.source_origin
            AND endpoint.transaction_id = corrected.transaction_id
        )
        AND corrected.source_file IS NOT DISTINCT FROM t.source_file
        AND corrected.source_type = t.source_type
        AND corrected.source_origin IS NOT DISTINCT FROM t.source_origin
        AND corrected.transaction_date = t.transaction_date
        AND corrected.amount = t.amount
        AND corrected.description IS NOT DISTINCT FROM t.description
        AND corrected.original_date_str IS NOT DISTINCT FROM t.original_date_str
        AND corrected.source_transaction_id IS NOT DISTINCT FROM t.source_transaction_id
    )
    AND NOT EXISTS(
      SELECT
        1
      FROM replaceable_stable_source_id_pairs AS pair
      WHERE
        pair.legacy_transaction_id = t.transaction_id
        AND pair.legacy_source_account_key = t.account_id
        AND pair.source_file IS NOT DISTINCT FROM t.source_file
        AND pair.source_type = t.source_type
        AND pair.source_origin IS NOT DISTINCT FROM t.source_origin
    )
)
SELECT
  COALESCE(links.account_id, ranked.account_id) AS account_id, /* canonical via the import-time resolver link; source-native only if unresolved */
  COALESCE(identity.legacy_source_account_key, ranked.account_id) AS source_account_key,
  COALESCE(identity.legacy_transaction_id, ranked.transaction_id) AS transaction_id,
  ranked.transaction_date,
  ranked.post_date,
  ranked.amount,
  ranked.original_amount,
  ranked.original_date_str,
  ranked.description,
  ranked.memo,
  ranked.category,
  ranked.subcategory,
  ranked.transaction_type,
  ranked.status,
  ranked.check_number,
  ranked.source_transaction_id,
  ranked.reference_number,
  ranked.balance,
  ranked.currency,
  ranked.member_name,
  ranked.source_file,
  ranked.source_type,
  ranked.source_origin,
  ranked.import_id,
  ranked.row_number,
  ranked.extracted_at,
  ranked.loaded_at
FROM ranked
LEFT JOIN replaceable_stable_source_id_pairs AS identity
  ON identity.corrected_transaction_id = ranked.transaction_id
  AND identity.corrected_source_account_key = ranked.account_id
  AND identity.source_file IS NOT DISTINCT FROM ranked.source_file
  AND identity.source_type = ranked.source_type
  AND identity.source_origin IS NOT DISTINCT FROM ranked.source_origin
LEFT JOIN app.account_links AS links
  ON links.status = 'accepted'
  AND links.ref_kind = 'source_native'
  AND links.source_type = ranked.source_type
  AND links.source_origin = ranked.source_origin
  AND links.ref_value = ranked.account_id
WHERE
  ranked._row_num = 1
