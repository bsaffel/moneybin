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
  /* OFX <ORG> is a routing code, not a name — Chase publishes "B1", Wells Fargo
     "WF" — so resolve a display name from the exact <FID> via seeds.institutions
     and fall back to the raw <ORG> when the FID is unregistered. This is a
     display concern only: the import-time institution slug (source_origin) is
     deliberately untouched, because it feeds the transaction_id content hash. */
  SELECT
    a.account_id,
    a.source_account_key,
    a.routing_number,
    a.account_type,
    COALESCE(i.display_name, a.institution_org) AS institution_name,
    a.institution_fid,
    'ofx' AS source_type,
    a.source_file,
    a.extracted_at,
    a.loaded_at,
    NULL::TEXT AS official_name,
    a.account_subtype,
    CASE
      WHEN LENGTH(REGEXP_REPLACE(a.source_account_key, '[^0-9]', '', 'g')) >= 4
      THEN RIGHT(REGEXP_REPLACE(a.source_account_key, '[^0-9]', '', 'g'), 4)
    END AS last_four_raw
  FROM prep.stg_ofx__accounts AS a
  LEFT JOIN seeds.institutions AS i
    ON i.fid = a.institution_fid
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
    loaded_at,
    NULL::TEXT AS official_name,
    account_subtype,
    CASE
      WHEN LENGTH(
        REGEXP_REPLACE(COALESCE(account_number, account_number_masked), '[^0-9]', '', 'g')
      ) >= 4
      THEN RIGHT(
        REGEXP_REPLACE(COALESCE(account_number, account_number_masked), '[^0-9]', '', 'g'),
        4
      )
    END AS last_four_raw
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
    loaded_at,
    official_name,
    account_subtype,
    CASE
      WHEN LENGTH(REGEXP_REPLACE(mask, '[^0-9]', '', 'g')) >= 4
      THEN RIGHT(REGEXP_REPLACE(mask, '[^0-9]', '', 'g'), 4)
    END AS last_four_raw
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
       - Descriptive fields (institution_name, account_type, official_name,
         account_subtype): first non-null by recency — ARG_MAX over extracted_at.
         account_type arrives already normalized to one canonical vocabulary by
         the three stg_*__accounts views (seeds.account_type_map), so this merge
         compares like with like; before that normalization a later 'depository'
         could out-rank an earlier 'CHECKING' for the same account and silently
         rename it. official_name comes only from Plaid staging today; the merge
         keeps it source-agnostic for future providers.
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
    ARG_MAX(official_name, extracted_at) FILTER(WHERE
      NOT official_name IS NULL) AS official_name,
    ARG_MAX(account_subtype, extracted_at) FILTER(WHERE
      NOT account_subtype IS NULL) AS account_subtype,
    ARG_MIN(source_type, (source_rank, -EPOCH_US(extracted_at))) AS source_type,
    ARG_MIN(source_file, (source_rank, -EPOCH_US(extracted_at))) AS source_file,
    MAX(extracted_at) AS extracted_at,
    MAX(loaded_at) AS loaded_at,
    ARG_MIN(last_four_raw, (source_rank, -EPOCH_US(extracted_at))) FILTER(WHERE
      NOT last_four_raw IS NULL) AS last_four_derived
  FROM ranked
  GROUP BY
    grain_key
)
SELECT
  w.account_id, /* Canonical account identifier; opaque and stable across imports; foreign key in fct_transactions */
  w.routing_number, /* ABA bank routing number; merged first-non-null by source strength then recency; NULL when no source provided it */
  w.account_type, /* Canonical account classification, normalized across all sources via seeds.account_type_map: depository, credit, loan, investment, other. NULL when the source spelling is unrecognized — the finer source distinction is preserved in account_subtype */
  w.institution_name, /* Human-readable name of the financial institution */
  w.institution_fid, /* OFX financial institution identifier; NULL for tabular/plaid sources */
  w.source_type, /* Origin of the winning record after the cross-source merge: ofx, csv, tsv, excel, plaid, etc. */
  w.source_file, /* Path to the source file from which the winning record was loaded */
  w.extracted_at, /* Latest time the data was parsed from a contributing source */
  w.loaded_at, /* Latest time a contributing record was written to the raw table */
  GREATEST(w.loaded_at, s.updated_at) AS updated_at, /* Latest of all per-row input timestamps contributing to this row's current values. Does not advance on idempotent SQLMesh re-applies. See docs/specs/core-updated-at-convention.md. */
  COALESCE(
    s.display_name,
    w.institution_name || ' ' || COALESCE(s.account_subtype, w.account_subtype, w.account_type) || ' …' || COALESCE(s.last_four, w.last_four_derived),
    w.institution_name || ' …' || COALESCE(s.last_four, w.last_four_derived),
    w.institution_name || ' ' || COALESCE(s.account_subtype, w.account_subtype, w.account_type),
    w.institution_name,
    w.account_type,
    'Account ' || w.account_id
  ) AS display_name, /* Resolved display label: user override → derived (institution+subtype-or-type[+last4]; the subtype is preferred because 'checking' reads to a human where the canonical 'depository' does not) → institution+last4 → institution or type alone → 'Account <id>' terminal so it is never NULL. The institution+last4 branch is what keeps two typeless accounts at one institution distinguishable; without it both collapse to the bare institution name. */
  COALESCE(s.official_name, w.official_name) AS official_name, /* Institution's formal account name: user override (app.account_settings) else Plaid official_name */
  COALESCE(s.last_four, w.last_four_derived) AS last_four, /* Last 4 of account number: user-set app.account_settings.last_four, else derived per source (OFX source_account_key digits, Plaid mask, tabular account_number/masked). Never the full number. */
  COALESCE(s.account_subtype, w.account_subtype) AS account_subtype, /* Plaid-style subtype (checking, savings, credit card, mortgage, ...): user override else Plaid subtype */
  s.holder_category, /* 'personal' / 'business' / 'joint' */
  COALESCE(s.currency_code, 'USD') AS currency_code, /* ISO-4217 currency code; NULL falls back to USD (M1K.1 Part B adds the true no-blend guard) */
  s.credit_limit, /* User-asserted credit limit on credit cards / lines */
  COALESCE(s.archived, FALSE) AS archived, /* Hides account from default list and from agg_net_worth */
  COALESCE(s.include_in_net_worth, TRUE) AS include_in_net_worth /* Whether this account contributes to agg_net_worth */
FROM merged AS w
LEFT JOIN app.account_settings AS s
  ON w.account_id = s.account_id
