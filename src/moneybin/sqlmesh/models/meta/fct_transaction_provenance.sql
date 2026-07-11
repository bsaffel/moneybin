MODEL (
  name meta.fct_transaction_provenance,
  kind VIEW
);

/* Links every gold record in core.fct_transactions to every contributing
   source row. Unmatched records have exactly one provenance row (match_id = NULL).
   Matched groups have one row per contributing source. */
SELECT
  m.transaction_id, /* FK to gold record in core.fct_transactions */
  m.source_transaction_id, /* Source-native ID, joinable to raw/prep */
  m.source_type, /* Import pathway / origin system */
  m.source_origin, /* Institution/connection/format that produced this row */
  m.source_file, /* File that produced this source row */
  m.source_extracted_at, /* When the source row was parsed */
  md.match_id /* FK to app.match_decisions; NULL for unmatched records */
FROM prep.int_transactions__matched AS m
LEFT JOIN app.match_decisions AS md
  ON md.match_status = 'accepted'
  AND md.reversed_at IS NULL
  AND md.match_type = 'dedup'
  AND (
    (
      m.source_type = md.source_type_a
      AND m.source_transaction_id = md.source_transaction_id_a
      AND m.account_id = md.account_id
    )
    OR (
      m.source_type = md.source_type_b
      AND m.source_transaction_id = md.source_transaction_id_b
      AND m.account_id = md.account_id
    )
  )
