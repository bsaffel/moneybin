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
    current_link.account_id AS canonical_account_id,
    t.source_file,
    t.source_type,
    t.source_origin,
    t.transaction_date,
    t.amount,
    t.description,
    ROW_NUMBER() OVER (
      PARTITION BY t.account_id, t.source_file, t.source_type, t.source_origin, t.transaction_date, t.amount, t.description
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
  JOIN accepted_native_links AS current_link
    ON current_link.ref_value = legacy_self_map.ref_value
    AND current_link.source_type = legacy_self_map.source_type
    AND current_link.source_origin IS NOT DISTINCT FROM legacy_self_map.source_origin
  WHERE
    t.deleted_from_source_at IS NULL
), corrected_pinned AS (
  SELECT
    t.transaction_id,
    link.account_id AS canonical_account_id,
    t.source_file,
    t.source_type,
    t.source_origin,
    t.transaction_date,
    t.amount,
    t.description,
    ROW_NUMBER() OVER (
      PARTITION BY link.account_id, t.source_file, t.source_type, t.source_origin, t.transaction_date, t.amount, t.description
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
     Filtering pre-rank lets the valid row take #1. Corrected pinned rows are
     excluded only when an accepted legacy self-map proves the same canonical
     account, file, origin, and transaction content; duplicate content is
     paired by occurrence, so a reused path remains visible. */
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
        AND legacy.transaction_date = corrected.transaction_date
        AND legacy.amount = corrected.amount
        AND legacy.description IS NOT DISTINCT FROM corrected.description
        AND legacy.occurrence = corrected.occurrence
      WHERE
        corrected.transaction_id = t.transaction_id
        AND corrected.source_file IS NOT DISTINCT FROM t.source_file
        AND corrected.source_type = t.source_type
        AND corrected.source_origin IS NOT DISTINCT FROM t.source_origin
        AND corrected.transaction_date = t.transaction_date
        AND corrected.amount = t.amount
        AND corrected.description IS NOT DISTINCT FROM t.description
    )
)
SELECT
  COALESCE(links.account_id, ranked.account_id) AS account_id, /* canonical via the import-time resolver link; source-native only if unresolved */
  ranked.account_id AS source_account_key,
  ranked.transaction_id,
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
LEFT JOIN app.account_links AS links
  ON links.status = 'accepted'
  AND links.ref_kind = 'source_native'
  AND links.source_type = ranked.source_type
  AND links.source_origin = ranked.source_origin
  AND links.ref_value = ranked.account_id
WHERE
  ranked._row_num = 1
