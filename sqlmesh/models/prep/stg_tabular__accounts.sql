MODEL (
  name prep.stg_tabular__accounts,
  kind VIEW
);

SELECT
  account_id, /* Source-system account identifier */
  account_name, /* Human-readable label */
  account_number, /* Full account number (encrypted at rest) */
  account_number_masked, /* Last 4 digits for display */
  account_type, /* Account classification */
  institution_name, /* Financial institution name */
  currency, /* Default currency */
  NULL::TEXT AS routing_number, /* Not available from tabular imports */
  NULL::TEXT AS institution_fid, /* Not available from tabular imports */
  source_file,
  source_type,
  source_origin,
  import_id,
  extracted_at,
  loaded_at
FROM raw.tabular_accounts
