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
    CASE source_type WHEN 'ofx' THEN 0 WHEN 'plaid' THEN 1 ELSE 2 END AS source_rank
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
  w.account_id, /* Canonical account identifier; opaque and stable across imports; foreign key in fct_transactions */
  w.routing_number, /* ABA bank routing number; merged first-non-null by source strength then recency; NULL when no source provided it */
  w.account_type, /* Canonical account classification, normalized across all sources via seeds.account_type_map: depository, credit, loan, investment, other. NULL when the source spelling is unrecognized — the finer source distinction is preserved in account_subtype */
  w.institution_name, /* Human-readable name of the financial institution */
  w.institution_slug, /* Canonical institution slug used to match accounts across sources; institution_name is for display only and does not slugify back to this */
  w.institution_fid, /* OFX financial institution identifier; NULL for tabular/plaid sources */
  w.source_type, /* Origin of the winning record after the cross-source merge: ofx, csv, tsv, excel, plaid, etc. */
  w.source_file, /* Path to the source file from which the winning record was loaded */
  w.extracted_at, /* Latest time the data was parsed from a contributing source */
  w.loaded_at, /* Latest time a contributing record was written to the raw table */
  GREATEST(w.loaded_at, s.updated_at) AS updated_at, /* Latest of all per-row input timestamps contributing to this row's current values. Does not advance on idempotent SQLMesh re-applies. See docs/specs/core-updated-at-convention.md. */
  COALESCE(
    s.display_name,
    CASE
      WHEN REGEXP_MATCHES(w.account_label, '\p{L}')
      AND NOT REGEXP_MATCHES(w.account_label, '[0-9]{4}')
      THEN w.account_label || ' …' || COALESCE(s.last_four, w.last_four_derived)
    END /* The name a person wrote — a sheet's Account column, --account-name, or
       Plaid's per-account name — outranks every label assembled below it. It is
       the only rung whose text a human chose, and `moneybin accounts` already
       prints institution and type in their own columns beside the name, so
       naming a row "Test Bank depository" restates what is on screen and
       discards what is not.
       A label with no digits of its own carries the last four, like every rung
       that can. A label is chosen, not unique: Plaid sends the institution's
       own account name, and a household's two checking accounts routinely
       carry one product name. Naming both of them that collides two accounts
       onto one string, which is the defect this rung was added to fix, and
       resolve_strict then refuses a name reference that resolved before.
       A label already holding four digits takes nothing more. Four digits is
       the last-four unit, so such a label either states the account's own or is
       what the masker left of a longer number, and "Checking ****5678" joined
       with "…9012" publishes eight digits of a twelve-digit one. A year inside
       a name cannot be told from a number's tail, so neither is joined.
       The bare arm below is that case and the no-last-four one.
       The letter test is what keeps this rung for names. An Account column
       mapped straight from the account number is ordinary in a hand-rolled
       export; the importer masks it, which makes it safe to show but not a
       name, and "****1098" identifies the account strictly worse than
       "Test Bank …1098" does. Mirrored by
       services/account_display_name.py::usable_source_label, which the mint
       report derives through before any of this has run. */,
    CASE WHEN REGEXP_MATCHES(w.account_label, '\p{L}') THEN w.account_label END,
    w.institution_name || ' ' || COALESCE(s.account_subtype, w.account_subtype, w.account_type) || ' …' || COALESCE(s.last_four, w.last_four_derived),
    w.institution_name || ' …' || COALESCE(s.last_four, w.last_four_derived),
    w.institution_name || ' ' || COALESCE(s.account_subtype, w.account_subtype, w.account_type),
    w.institution_name,
    COALESCE(s.account_subtype, w.account_subtype, w.account_type) || ' …' || COALESCE(s.last_four, w.last_four_derived),
    COALESCE(s.account_subtype, w.account_subtype, w.account_type),
    '…' || COALESCE(s.last_four, w.last_four_derived),
    'Unnamed account' /* Nothing left to name it by: no institution, subtype, type or last four.
       The id is not a fallback name — for an account with no accepted link it
       IS the institution's own account number, and the dim cannot tell that
       case apart (see grain_key above). Naming no account at all beats naming
       one with a number, and this column feeds reports.* as account_name. */
  ) AS display_name, /* Resolved display label: user override → the source's own account label when it holds a letter → institution+subtype-or-type+last4 → institution+last4 → institution+subtype-or-type → institution → subtype-or-type+last4 → subtype-or-type → last4 alone → the literal 'Unnamed account' terminal, so it is never NULL and never an id. The subtype is preferred over the type because 'checking' reads to a human where the canonical 'depository' does not. A last four outranks the category it sits beside at every level: 'checking' is shared by every checking account, while the last four is what tells two of them apart, and it is already published in its own column and printed as confirm evidence. */
  COALESCE(s.official_name, w.official_name) AS official_name, /* Institution's formal account name: user override (app.account_settings) else Plaid official_name */
  COALESCE(s.last_four, w.last_four_derived) AS last_four, /* Last 4 of account number: user-set app.account_settings.last_four, else derived per source (OFX source_account_key digits, Plaid mask, tabular account_number/masked). Never the full number. */
  COALESCE(s.account_subtype, w.account_subtype) AS account_subtype, /* Plaid-style subtype (checking, savings, credit card, mortgage, ...): user override else Plaid subtype */
  s.holder_category, /* 'personal' / 'business' / 'joint' */
  COALESCE(s.currency_code, w.source_currency) AS currency_code, /* ISO 4217 currency this account is denominated in: user override (app.account_settings) else the currency its own source reported (OFX CURDEF, Plaid iso_currency_code, tabular currency column). NULL means genuinely unknown and stays that way — every monetary grain COALESCEs onto this column, so a literal default here would relabel the whole ledger and make the unknown-currency segment unreachable (multi-currency.md Requirements 3 and 8). system doctor's currency_integrity check surfaces NULLs for the user to resolve with `accounts set --currency`. */
  s.credit_limit, /* User-asserted credit limit on credit cards / lines */
  COALESCE(s.archived, FALSE) AS archived, /* Hides account from default list and from agg_net_worth */
  COALESCE(s.include_in_net_worth, TRUE) AS include_in_net_worth /* Whether this account contributes to agg_net_worth */
FROM merged AS w
LEFT JOIN app.account_settings AS s
  ON w.account_id = s.account_id
