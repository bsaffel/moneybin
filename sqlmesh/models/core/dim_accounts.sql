/* Canonical accounts dimension; deduplicated accounts from all sources, with
   user-controlled settings (display_name, archive, include_in_net_worth, Plaid-
   parity metadata) joined in as the single resolved source of truth.
   Per .claude/rules/database.md, no consumer joins app.account_settings directly.
   Query examples for the LLM: see src/moneybin/services/schema_catalog.py (EXAMPLES dict). */
MODEL (
  name core.dim_accounts,
  kind FULL,
  grain account_id
);

WITH ofx_accounts AS (
  SELECT
    account_id,
    source_account_key,
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
    source_account_key,
    routing_number,
    account_type,
    institution_name,
    institution_fid,
    source_type,
    source_file,
    extracted_at,
    loaded_at
  FROM prep.stg_tabular__accounts
), plaid_accounts AS (
  SELECT
    account_id,
    source_account_key,
    NULL::TEXT AS routing_number,
    account_type,
    institution_name,
    NULL::TEXT AS institution_fid,
    'plaid' AS source_type,
    source_file,
    extracted_at,
    loaded_at
  FROM prep.stg_plaid__accounts
), all_accounts AS (
  SELECT
    *
  FROM ofx_accounts
  UNION ALL
  SELECT
    *
  FROM tabular_accounts
  UNION ALL
  SELECT
    *
  FROM plaid_accounts
), ranked AS (
  /* grain_key: account_id is the CANONICAL opaque id when the account has an
     accepted app.account_links row; it is NULL for accounts not yet re-imported
     through AccountResolver (pre-M1S.3 raw data). COALESCE falls back to the
     source-native key so those unlinked accounts stay DISTINCT instead of every
     NULL collapsing into one bad row. Safety net: the durable path is re-import,
     not a migration — once all data is re-imported every account is linked, so
     the COALESCE always takes the canonical id and this fallback is inert.
     source_rank: bank-field authority ordering for the golden-record merge
     (ofx > plaid > tabular, lower rank wins); manual/gsheet contribute no
     structured bank fields. */
  SELECT
    *,
    COALESCE(account_id, source_account_key) AS grain_key,
    CASE source_type WHEN 'ofx' THEN 0 WHEN 'plaid' THEN 1 ELSE 2 END AS source_rank
  FROM all_accounts
), merged AS (
  /* Per-field COALESCE-across-group merge (Decision 4), replacing last-write-wins.
     A later weaker-source NULL can no longer clobber a stronger source's value.
       - Structured bank fields (routing_number, institution_fid): first non-null
         by source strength then recency — ARG_MIN over (source_rank ASC,
         extracted_at DESC); negating epoch_us flips the timestamp to descending
         within the composite ordering key.
       - Descriptive fields (institution_name, account_type): first non-null by
         recency — ARG_MAX over extracted_at.
       - Display provenance (source_type, source_file): the winning (strength then
         recency) row's value; the full contributing set is recoverable from
         app.account_links.
       - Representative timestamps (extracted_at, loaded_at): MAX over the merged
         group; keeps updated_at monotone. */
  SELECT
    grain_key AS account_id,
    ARG_MIN(routing_number, (source_rank, -EPOCH_US(extracted_at))) FILTER(WHERE
      NOT routing_number IS NULL) AS routing_number,
    ARG_MIN(institution_fid, (source_rank, -EPOCH_US(extracted_at))) FILTER(WHERE
      NOT institution_fid IS NULL) AS institution_fid,
    ARG_MAX(institution_name, extracted_at) FILTER(WHERE
      NOT institution_name IS NULL) AS institution_name,
    ARG_MAX(account_type, extracted_at) FILTER(WHERE
      NOT account_type IS NULL) AS account_type,
    ARG_MIN(source_type, (source_rank, -EPOCH_US(extracted_at))) AS source_type,
    ARG_MIN(source_file, (source_rank, -EPOCH_US(extracted_at))) AS source_file,
    MAX(extracted_at) AS extracted_at,
    MAX(loaded_at) AS loaded_at
  FROM ranked
  GROUP BY
    grain_key
)
SELECT
  w.account_id, /* Canonical account identifier; opaque and stable across imports; foreign key in fct_transactions */
  w.routing_number, /* ABA bank routing number; merged first-non-null by source strength then recency; NULL when no source provided it */
  w.account_type, /* Account classification from source, e.g. CHECKING, SAVINGS, CREDITLINE */
  w.institution_name, /* Human-readable name of the financial institution */
  w.institution_fid, /* OFX financial institution identifier; NULL for tabular/plaid sources */
  w.source_type, /* Origin of the winning record after the cross-source merge: ofx, csv, tsv, excel, plaid, etc. */
  w.source_file, /* Path to the source file from which the winning record was loaded */
  w.extracted_at, /* Latest time the data was parsed from a contributing source */
  w.loaded_at, /* Latest time a contributing record was written to the raw table */
  GREATEST(w.loaded_at, s.updated_at) AS updated_at, /* Latest of all per-row input timestamps contributing to this row's current values. Does not advance on idempotent SQLMesh re-applies. See docs/specs/core-updated-at-convention.md. */
  COALESCE(
    s.display_name,
    w.institution_name || ' ' || w.account_type || ' …' || s.last_four,
    w.institution_name || ' ' || w.account_type,
    w.institution_name,
    w.account_type,
    'Account ' || w.account_id
  ) AS display_name, /* Resolved display label: user override → derived (institution+type[+last4]) → institution or type alone → 'Account <id>' terminal so it is never NULL */
  s.official_name, /* Institution's formal name (mirrors Plaid official_name); user-set or future Plaid sync */
  s.last_four, /* Last 4 digits of account number (mirrors Plaid mask); user-set or future Plaid sync */
  s.account_subtype, /* Plaid-style subtype (checking, savings, credit card, mortgage, ...) */
  s.holder_category, /* 'personal' / 'business' / 'joint' */
  COALESCE(s.iso_currency_code, 'USD') AS iso_currency_code, /* ISO-4217 currency code; defaults to USD until multi-currency.md ships */
  s.credit_limit, /* User-asserted credit limit on credit cards / lines */
  COALESCE(s.archived, FALSE) AS archived, /* Hides account from default list and from agg_net_worth */
  COALESCE(s.include_in_net_worth, TRUE) AS include_in_net_worth /* Whether this account contributes to agg_net_worth */
FROM merged AS w
LEFT JOIN app.account_settings AS s
  ON w.account_id = s.account_id
