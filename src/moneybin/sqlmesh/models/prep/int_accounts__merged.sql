/* Materialized half of core.dim_accounts: the cross-source account merge plus
   the app.account_settings fields that materialized models read. Those fields
   restate on write (docs/specs/materialization-freshness-contract.md); the
   display-only settings are joined by the core.dim_accounts view instead, so a
   rename never waits on a rebuild. Materialized models read this model, not the
   dim, so no read-through column reaches a FULL model. */
MODEL (
  name prep.int_accounts__merged,
  kind FULL,
  grain account_id
);

WITH ofx_balance_currency AS (
  /* Account currency per source, taken from the balance rows because no source
     puts it on the account record: OFX carries <CURDEF> on the statement, Plaid
     returns iso_currency_code on the balance. Most recent non-NULL wins, matching
     how `merged` resolves the other descriptive fields. Without this the account
     grain has no currency of its own, and the terminal COALESCE below would have
     nothing to fall back on but a literal — which is exactly what
     multi-currency.md Requirement 3 forbids ("never a blind 'USD'").

     Scoped by (source_account_key, source_origin), like every other join onto
     these staging views: a source-native <ACCTID> is unique per institution,
     not globally. On the key alone, two banks that both call an account "1001"
     collapse into one lookup row and the most recent one's currency is handed
     to both canonical accounts — mislabeling one institution's money as the
     other's, inside the mechanism built to stop exactly that. */
  SELECT
    source_account_key,
    source_origin,
    ARG_MAX(currency_code, extracted_at) FILTER(WHERE
      NOT currency_code IS NULL) AS source_currency
  FROM prep.stg_ofx__balances
  GROUP BY
    source_account_key,
    source_origin
), plaid_balance_currency AS (
  SELECT
    source_account_key,
    source_origin,
    ARG_MAX(COALESCE(iso_currency_code, unofficial_currency_code), extracted_at) FILTER(WHERE
      NOT COALESCE(iso_currency_code, unofficial_currency_code) IS NULL) AS source_currency
  FROM prep.stg_plaid__balances
  GROUP BY
    source_account_key,
    source_origin
), institution_alias AS (
  /* Normalized spelling -> canonical registry slug, indexing both halves of every
     seeds.institutions row because sources arrive carrying either: OFX resolves a
     display name, a sheet's Institution column is written by hand, the filename
     heuristic already emits the slug. Punctuation is stripped rather than replaced,
     which is what lets those meet — the registry's slug is curated, not derived, so
     no amount of slugifying turns "U.S. Bank" into "us_bank". Grouped so a future
     registry row normalizing onto an existing alias cannot fan one account into two.
     Mirrors extractors/institution_resolution.py::slug_for_institution_name, which
     the resolver applies to the *source* side; the two must agree, and
     test_registry_alias_lookup_matches_the_sql_model pins that. */
  SELECT
    alias,
    MIN(slug) AS slug
  FROM (
    SELECT
      REGEXP_REPLACE(LOWER(slug), '[^a-z0-9]', '', 'g') AS alias,
      slug
    FROM seeds.institutions
    UNION ALL
    SELECT
      REGEXP_REPLACE(LOWER(display_name), '[^a-z0-9]', '', 'g') AS alias,
      slug
    FROM seeds.institutions
  )
  WHERE
    alias <> ''
  GROUP BY
    alias
), ofx_accounts AS (
  /* OFX <ORG> is a routing code, not a name — Chase publishes "B1", Wells Fargo
     "WF" — so resolve from the exact <FID> via seeds.institutions, then from the
     <ORG> as a name for issuers that publish one, and only then fall back to the
     raw <ORG>. Two columns come out of that join and they are NOT
     interchangeable: institution_name is for display,
     institution_slug is what account matching compares. Slugifying the display
     name is not the inverse of the registry ("U.S. Bank" -> "u-s-bank", not
     "us_bank"), so a consumer that matches on the name drops candidates.
     The import-time institution slug (source_origin) stays untouched either
     way, because it feeds the transaction_id content hash. */
  SELECT
    a.account_id,
    a.source_account_key,
    a.routing_number,
    a.account_type,
    COALESCE(i.display_name, a.institution_org) AS institution_name,
    COALESCE(i.slug, ia.slug, a.institution_org) AS institution_slug,
    NOT COALESCE(i.slug, ia.slug) IS NULL AS institution_slug_resolved,
    a.institution_fid,
    'ofx' AS source_type,
    a.source_file,
    a.extracted_at,
    a.loaded_at,
    NULL::TEXT AS official_name,
    NULL /* OFX has no account-name element at all: <BANKACCTFROM> carries the
       number, the type and the routing number, and nothing a person wrote. */::TEXT AS account_label,
    a.account_subtype,
    CASE
      WHEN LENGTH(REGEXP_REPLACE(a.source_account_key, '[^0-9]', '', 'g')) >= 4
      THEN RIGHT(REGEXP_REPLACE(a.source_account_key, '[^0-9]', '', 'g'), 4)
    END AS last_four_raw,
    c.source_currency
  FROM prep.stg_ofx__accounts AS a
  LEFT JOIN seeds.institutions AS i
    ON i.fid = a.institution_fid
  /* Only reached when the FID is unregistered: some issuers put a real name in
     <ORG> ("WELLS FARGO") rather than a routing code, and that name resolves. */
  LEFT JOIN institution_alias AS ia
    ON ia.alias = REGEXP_REPLACE(LOWER(a.institution_org), '[^a-z0-9]', '', 'g')
  LEFT JOIN ofx_balance_currency AS c
    ON c.source_account_key = a.source_account_key AND c.source_origin = a.source_origin
), tabular_accounts AS (
  SELECT
    account_id,
    source_account_key,
    routing_number,
    account_type,
    institution_name,
    COALESCE(ia.slug, institution_name) AS institution_slug, /* Resolved, never aliased across: a sheet's Institution column and the filename/format chain both yield display text, so passing it straight through would make one column mean two things by source and stop an OFX row for the same bank from matching it */
    NOT ia.slug IS NULL AS institution_slug_resolved,
    institution_fid,
    source_type,
    source_file,
    extracted_at,
    loaded_at,
    NULL::TEXT AS official_name,
    account_label,
    account_subtype,
    CASE
      WHEN LENGTH(
        REGEXP_REPLACE(COALESCE(account_number, account_number_masked), '[^0-9]', '', 'g')
      ) >= 4
      THEN RIGHT(
        REGEXP_REPLACE(COALESCE(account_number, account_number_masked), '[^0-9]', '', 'g'),
        4
      )
    END AS last_four_raw,
    currency AS source_currency /* the one source that carries it on the account itself */
  FROM prep.stg_tabular__accounts
  LEFT JOIN institution_alias AS ia
    ON ia.alias = REGEXP_REPLACE(LOWER(institution_name), '[^a-z0-9]', '', 'g')
), plaid_accounts AS (
  /* Every column is alias-qualified because the balance-currency join makes
     bare source_account_key ambiguous. */
  SELECT
    a.account_id,
    a.source_account_key,
    NULL::TEXT AS routing_number,
    a.account_type,
    a.institution_name,
    COALESCE(ia.slug, a.institution_name) AS institution_slug, /* Plaid carries no FID, so its display name is all there is to resolve from; unregistered institutions keep the name and rely on both sides being slugified when compared */
    NOT ia.slug IS NULL AS institution_slug_resolved,
    NULL::TEXT AS institution_fid,
    'plaid' AS source_type,
    a.source_file,
    a.extracted_at,
    a.loaded_at,
    a.official_name,
    a.account_label,
    a.account_subtype,
    CASE
      WHEN LENGTH(REGEXP_REPLACE(a.mask, '[^0-9]', '', 'g')) >= 4
      THEN RIGHT(REGEXP_REPLACE(a.mask, '[^0-9]', '', 'g'), 4)
    END AS last_four_raw,
    c.source_currency
  FROM prep.stg_plaid__accounts AS a
  LEFT JOIN institution_alias AS ia
    ON ia.alias = REGEXP_REPLACE(LOWER(a.institution_name), '[^a-z0-9]', '', 'g')
  LEFT JOIN plaid_balance_currency AS c
    ON c.source_account_key = a.source_account_key AND c.source_origin = a.source_origin
), simplefin_accounts AS (
  SELECT
    a.account_id,
    a.source_account_key,
    NULL::TEXT AS routing_number,
    a.account_type,
    a.institution_name,
    COALESCE(ia.slug, a.institution_name) AS institution_slug,
    NOT ia.slug IS NULL AS institution_slug_resolved,
    NULL::TEXT AS institution_fid,
    a.source_type,
    a.source_file,
    a.extracted_at AS extracted_at,
    a.loaded_at AS loaded_at,
    NULL::TEXT AS official_name,
    a.account_label,
    a.account_subtype,
    NULL::TEXT AS last_four_raw,
    c.currency_code AS source_currency
  FROM prep.stg_simplefin__accounts AS a
  LEFT JOIN institution_alias AS ia
    ON ia.alias = REGEXP_REPLACE(LOWER(a.institution_name), '[^a-z0-9]', '', 'g')
  LEFT JOIN seeds.iso_currencies AS c
    ON c.currency_code = a.currency_code AND NOT c.minor_unit IS NULL
  WHERE
    NOT a.account_id IS NULL
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
  UNION ALL
  SELECT
    *
  FROM simplefin_accounts
), ranked AS (
  /* grain_key: account_id is the CANONICAL opaque id when the account has an
     accepted app.account_links row, and the source-native key when it does not.
     The COALESCE here never fires: all three stg_*__accounts models already
     project COALESCE(links.account_id, a.account_id), so account_id arrives
     non-null either way. It is kept as a second line of defence — were a
     staging model to stop falling back, every NULL would collapse into one bad
     row — but nothing downstream can read it as "was this account resolved?",
     because that fact is spent in prep and never projected. See the terminal
     arm of display_name, which is fail-closed for exactly this reason.
     source_rank: bank-field authority ordering for the golden-record merge
     (ofx > plaid > tabular, lower rank wins); manual/gsheet contribute no
     structured bank fields. */
  SELECT
    *,
    COALESCE(account_id, source_account_key) AS grain_key,
    CASE source_type
      WHEN 'ofx'
      THEN 0
      WHEN 'plaid'
      THEN 1
      WHEN 'simplefin'
      THEN 3
      ELSE 2
    END AS source_rank
  FROM all_accounts
), merged AS (
  /* Per-field COALESCE-across-group merge (Decision 4), replacing last-write-wins.
     A later weaker-source NULL can no longer clobber a stronger source's value.
       - Structured bank fields (routing_number, institution_fid): first non-null
         by source strength then recency — ARG_MIN over (source_rank ASC,
         extracted_at DESC); negating epoch_us flips the timestamp to descending
         within the composite ordering key.
       - institution_slug: resolved-first, then recency. Only some sources can
         resolve one — OFX has a <FID>, a spreadsheet has whatever its
         Institution column was typed as — so ranking this by recency alone lets
         an unregistered spelling overwrite a registry slug the moment a sheet
         for the same account arrives later. Matching then compares the canonical
         slug against that raw text, misses the account, and mints a duplicate.
         Source rank is the wrong key here: a tabular row that DID resolve is
         more useful than an OFX row that fell back to a raw <ORG>.
       - Descriptive fields (institution_name, account_type, official_name,
         account_label, account_subtype): first non-null by recency — ARG_MAX
         over extracted_at.
         account_label by recency and not by source rank on purpose: only two
         sources have one at all, and neither is more authoritative than the
         other — a spreadsheet's Account column and Plaid's per-account name are
         both what a person calls this account, so the newer spelling is the
         better answer. Ranking would instead pin the first source that ever
         named it and make re-exporting under a new name a no-op.
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
    ARG_MIN(
      institution_slug,
      (CASE WHEN institution_slug_resolved THEN 0 ELSE 1 END, -EPOCH_US(extracted_at))
    ) FILTER(WHERE
      NOT institution_slug IS NULL) AS institution_slug,
    ARG_MAX(account_type, extracted_at) FILTER(WHERE
      NOT account_type IS NULL) AS account_type,
    ARG_MAX(official_name, extracted_at) FILTER(WHERE
      NOT official_name IS NULL) AS official_name,
    ARG_MAX(account_label, extracted_at) FILTER(WHERE
      NOT account_label IS NULL) AS account_label,
    ARG_MAX(account_subtype, extracted_at) FILTER(WHERE
      NOT account_subtype IS NULL) AS account_subtype,
    ARG_MIN(source_type, (source_rank, -EPOCH_US(extracted_at))) AS source_type,
    ARG_MIN(source_file, (source_rank, -EPOCH_US(extracted_at))) AS source_file,
    MAX(extracted_at) AS extracted_at,
    MAX(loaded_at) AS loaded_at,
    ARG_MIN(last_four_raw, (source_rank, -EPOCH_US(extracted_at))) FILTER(WHERE
      NOT last_four_raw IS NULL) AS last_four_derived,
    ARG_MAX(source_currency, extracted_at) FILTER(WHERE
      NOT source_currency IS NULL) AS source_currency
  FROM ranked
  GROUP BY
    grain_key
)
SELECT
  w.account_id,
  w.account_label,
  w.official_name,
  COALESCE(s.account_type, w.account_type) AS account_type,
  s.account_type AS account_type_override,
  w.account_type AS source_account_type,
  COALESCE(s.account_subtype, w.account_subtype) AS account_subtype,
  w.institution_slug,
  w.institution_fid,
  w.routing_number,
  w.institution_name,
  w.last_four_derived,
  w.source_type,
  w.source_file,
  s.investment_source_type,
  s.default_cost_basis_method,
  COALESCE(s.currency_code, w.source_currency) AS currency_code,
  COALESCE(s.archived, FALSE) AS archived,
  COALESCE(s.include_in_net_worth, TRUE) AS include_in_net_worth,
  s.archived_at,
  w.extracted_at,
  w.loaded_at,
  s.investment_source_type_changed_at,
  s.archived_changed_at,
  s.account_type_changed_at,
  s.account_subtype_changed_at,
  s.currency_code_changed_at,
  s.default_cost_basis_method_changed_at,
  GREATEST(
    w.loaded_at,
    s.investment_source_type_changed_at,
    s.archived_changed_at,
    s.account_type_changed_at,
    s.account_subtype_changed_at,
    s.currency_code_changed_at,
    s.default_cost_basis_method_changed_at
  ) AS updated_at
FROM merged AS w
LEFT JOIN app.account_settings AS s
  ON w.account_id = s.account_id
