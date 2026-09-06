MODEL (
  name prep.stg_tabular__transactions,
  kind VIEW
);

/* category and subcategory are nulled out when blank for the reason
   stg_plaid__accounts states for its own free-text columns, and the reason
   currency below already is: '' passes a NULL check while rendering as a
   malformed label. Here the NULL check that matters is
   core.uncategorized_queue's `category IS NULL` — a category of spaces hides a
   transaction nobody ever categorized from the queue built to surface it.

   These two use a regex rather than the bare TRIM their siblings use, because
   they are the only staging columns with a Python-side counterpart that has to
   agree with them: services._validators.validate_category_text refuses on
   str.strip(). The class is defined to equal str.strip() exactly — \p{Z} every
   Unicode space separator, \s the C0 whitespace RE2 includes, \x0B the vertical
   tab it excludes, and \x1C-\x1F\x85 the information separators and NEXT LINE.
   Do not maintain it by appending the character that last leaked;
   test_blank_whitespace_definition.py enumerates all 29 codepoints
   str.isspace() accepts, fails naming any the class misses, and holds all three
   copies of it (both staging models and V054) identical. Bare TRIM is not a
   substitute in either direction: it strips the space separators but leaves
   every control character, so a tab survives it.

   A blanked category then takes its subcategory with it, in the projection
   below. Nulling the two independently manufactures an orphan the write path
   forbids: a subcategory is a child of a category here, so resolve_category_id
   short-circuits on a NULL category and no lone subcategory can ever resolve
   to a category_id. core.fct_transaction_lines coalesces the two columns
   independently, so the orphan would render this row's subcategory beside the
   *parent transaction's* category — a pair nobody chose. The cascade runs one
   way only: a blank subcategory under a real category nulls just itself,
   because a top-level category is a legitimate state (17 of the seeded
   categories are exactly that). */
WITH ranked AS (
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
    NULLIF(
      REGEXP_REPLACE(category, '^[\p{Z}\s\x0B\x1C-\x1F\x85]+|[\p{Z}\s\x0B\x1C-\x1F\x85]+$', '', 'g'),
      ''
    ) AS category,
    NULLIF(
      REGEXP_REPLACE(subcategory, '^[\p{Z}\s\x0B\x1C-\x1F\x85]+|[\p{Z}\s\x0B\x1C-\x1F\x85]+$', '', 'g'),
      ''
    ) AS subcategory,
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
  FROM raw.tabular_transactions
  /* Exclude soft-deleted rows BEFORE ranking: a soft-deleted row with a newer
     loaded_at would rank #1 and then be dropped by the outer filter, while a valid
     same-key row at #2 is also excluded — silently losing the transaction.
     Filtering pre-rank lets the valid row take #1. */
  WHERE
    deleted_from_source_at IS NULL
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
  CASE WHEN ranked.category IS NULL THEN NULL ELSE ranked.subcategory END AS subcategory, /* a blanked category takes its subcategory with it; see the header */
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
