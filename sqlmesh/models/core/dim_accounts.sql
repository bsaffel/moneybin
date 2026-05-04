/* Canonical accounts dimension; deduplicated accounts from all sources, with
   user-controlled settings (display_name, archive, include_in_net_worth, Plaid-
   parity metadata) joined in as the single resolved source of truth.
   Per .claude/rules/database.md, no consumer joins app.account_settings directly. */
-- Query examples for the LLM: see src/moneybin/services/schema_catalog.py (EXAMPLES dict)
MODEL (
  name core.dim_accounts,
  kind FULL,
  grain account_id
);

WITH ofx_accounts AS (
  SELECT
    account_id,
    routing_number,
    account_type,
    institution_org AS institution_name,
    institution_fid,
    'ofx' AS source_type,
    source_file,
    extracted_at,
    loaded_at
  FROM prep.stg_ofx__accounts
), tabular_accounts AS (
  SELECT
    account_id,
    routing_number,
    account_type,
    institution_name,
    institution_fid,
    source_type,
    source_file,
    extracted_at,
    loaded_at
  FROM prep.stg_tabular__accounts
), all_accounts AS (
  SELECT * FROM ofx_accounts
  UNION ALL
  SELECT * FROM tabular_accounts
), deduplicated AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY extracted_at DESC) AS _row_num
  FROM all_accounts
), winners AS (
  SELECT * FROM deduplicated WHERE _row_num = 1
)
SELECT
  w.account_id, /* Unique account identifier; stable across imports; foreign key in fct_transactions */
  w.routing_number, /* ABA bank routing number; NULL when not provided by source */
  w.account_type, /* Account classification from source, e.g. CHECKING, SAVINGS, CREDITLINE */
  w.institution_name, /* Human-readable name of the financial institution */
  w.institution_fid, /* OFX financial institution identifier; NULL for tabular sources */
  w.source_type, /* Origin of the winning record after deduplication: ofx, csv, tsv, excel, etc. */
  w.source_file, /* Path to the source file from which this record was loaded */
  w.extracted_at, /* When the data was parsed from the source file */
  w.loaded_at, /* When the record was written to the raw table */
  CURRENT_TIMESTAMP AS updated_at, /* When this core record was last refreshed by SQLMesh */
  COALESCE(
    s.display_name,
    w.institution_name || ' ' || w.account_type || ' …' || RIGHT(w.account_id, 4),
    w.account_id
  ) AS display_name, /* Resolved display label: user override → derived default → bare account_id */
  s.official_name, /* Institution's formal name (mirrors Plaid official_name); user-set or future Plaid sync */
  s.last_four, /* Last 4 digits of account number (mirrors Plaid mask); user-set or future Plaid sync */
  s.account_subtype, /* Plaid-style subtype (checking, savings, credit card, mortgage, ...) */
  s.holder_category, /* 'personal' / 'business' / 'joint' */
  COALESCE(s.iso_currency_code, 'USD') AS iso_currency_code, /* ISO-4217 currency code; defaults to USD until multi-currency.md ships */
  s.credit_limit, /* User-asserted credit limit on credit cards / lines */
  COALESCE(s.archived, FALSE) AS archived, /* Hides account from default list and from agg_net_worth */
  COALESCE(s.include_in_net_worth, TRUE) AS include_in_net_worth /* Whether this account contributes to agg_net_worth */
FROM winners AS w
LEFT JOIN app.account_settings AS s ON w.account_id = s.account_id
