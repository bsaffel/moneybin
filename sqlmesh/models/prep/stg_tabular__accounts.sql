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
  source_file, /* Path to source file */
  source_type, /* Import pathway: csv, tsv, excel, parquet, feather, pipe */
  source_origin, /* Institution/format that produced this data */
  import_id, /* UUID linking to import batch */
  extracted_at, /* When data was parsed from source */
  loaded_at /* When record was loaded into database */
FROM raw.tabular_accounts
