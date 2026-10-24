/* Canonical accounts dimension; deduplicated accounts from all sources, with
   user-controlled settings (display_name, archive, include_in_net_worth, Plaid-
   parity metadata) joined in as the single resolved source of truth.
   Per .claude/rules/database.md, no consumer joins app.account_settings directly.
   A view over the materialized prep.int_accounts__merged: the merge and the
   settings materialized models read live there, and the display-only settings
   are joined here so an edit to them shows on the next read
   (docs/specs/materialization-freshness-contract.md).
   Query examples for the LLM: see src/moneybin/services/schema_catalog.py (EXAMPLES dict). */
MODEL (
  name core.dim_accounts,
  kind VIEW,
  grain account_id
);

SELECT
  b.account_id, /* Canonical account identifier; opaque and stable across imports; foreign key in fct_transactions */
  COALESCE(
    s.display_name,
    CASE
      WHEN REGEXP_MATCHES(b.account_label, '\p{L}')
      AND NOT REGEXP_MATCHES(b.account_label, '\p{Nd}{4}')
      AND b.account_label <> 'Unnamed account'
      THEN b.account_label || ' …' || COALESCE(s.last_four, b.last_four_derived)
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
       \p{Nd} rather than [0-9], so a run written in another script counts as
       one too: the importer's masker keeps whatever digits it matched, so a
       non-Latin account number reaches this column masked to four non-ASCII
       digits that [0-9]{4} read as none at all. The Python mirror spells this
       \d, which is already that category, while RE2 reads \d as ASCII -- the
       two sides mirror each other here only under different source.
       The bare arm below is that case and the no-last-four one.
       The letter test is what keeps this rung for names. An Account column
       mapped straight from the account number is ordinary in a hand-rolled
       export; the importer masks it, which makes it safe to show but not a
       name, and "****1098" identifies the account strictly worse than
       "Test Bank …1098" does. Mirrored by
       extractors/account_identity.py::usable_source_label, which the mint
       report derives through before any of this has run.
       'Unnamed account' is excluded from both label arms because it is this
       ladder's own terminal arm — the one string that says nothing could name
       the account — and it reaches a source label by an ordinary route, since
       reports.* publish it as account_name and an export can be re-imported.
       It holds letters, so the test above cannot tell it apart. Promoting it
       would hand is_a_name a label it must discard, leaving the account
       unresolvable by the name it displays while the institution-derived
       fallthrough below would have named it. */,
    CASE
      WHEN REGEXP_MATCHES(b.account_label, '\p{L}') AND b.account_label <> 'Unnamed account'
      THEN b.account_label
    END,
    b.institution_name || ' ' || COALESCE(s.account_subtype, b.account_subtype, b.account_type) || ' …' || COALESCE(s.last_four, b.last_four_derived),
    b.institution_name || ' …' || COALESCE(s.last_four, b.last_four_derived),
    b.institution_name || ' ' || COALESCE(s.account_subtype, b.account_subtype, b.account_type),
    b.institution_name,
    COALESCE(s.account_subtype, b.account_subtype, b.account_type) || ' …' || COALESCE(s.last_four, b.last_four_derived),
    COALESCE(s.account_subtype, b.account_subtype, b.account_type),
    '…' || COALESCE(s.last_four, b.last_four_derived),
    'Unnamed account' /* Nothing left to name it by: no institution, subtype, type or last four.
       The id is not a fallback name — for an account with no accepted link it
       IS the institution's own account number, and the dim cannot tell that
       case apart (see grain_key in prep.int_accounts__merged). Naming no account at all beats naming
       one with a number, and this column feeds reports.* as account_name. */
  ) AS display_name, /* Resolved display label: user override → the source's own account label when it holds a letter and is not the 'Unnamed account' sentinel → institution+subtype-or-type+last4 → institution+last4 → institution+subtype-or-type → institution → subtype-or-type+last4 → subtype-or-type → last4 alone → the literal 'Unnamed account' terminal, so it is never NULL and never an id. The subtype is preferred over the type because 'checking' reads to a human where the canonical 'depository' does not. A last four outranks the category it sits beside at every level: 'checking' is shared by every checking account, while the last four is what tells two of them apart, and it is already published in its own column and printed as confirm evidence. */
  COALESCE(s.official_name, b.official_name) AS official_name, /* Institution's formal account name: user override (app.account_settings) else Plaid official_name */
  b.account_type, /* Canonical account classification, normalized across all sources via seeds.account_type_map: depository, credit, loan, investment, other. NULL when the source spelling is unrecognized — the finer source distinction is preserved in account_subtype */
  COALESCE(s.account_subtype, b.account_subtype) AS account_subtype, /* Plaid-style subtype (checking, savings, credit card, mortgage, ...): user override else Plaid subtype */
  s.holder_category, /* 'personal' / 'business' / 'joint' */
  b.institution_slug, /* Canonical institution slug used to match accounts across sources; institution_name is for display only and does not slugify back to this */
  b.institution_fid, /* OFX financial institution identifier; NULL for tabular/plaid sources */
  b.routing_number, /* ABA bank routing number; merged first-non-null by source strength then recency; NULL when no source provided it */
  b.institution_name, /* Human-readable name of the financial institution */
  COALESCE(s.last_four, b.last_four_derived) AS last_four, /* Last 4 of account number: user-set app.account_settings.last_four, else derived per source (OFX source_account_key digits, Plaid mask, tabular account_number/masked). Never the full number. */
  b.source_type, /* Origin of the winning record after the cross-source merge: ofx, csv, tsv, excel, plaid, etc. */
  b.source_file, /* Path to the source file from which the winning record was loaded */
  b.investment_source_type, /* The source type whose investment rows feed this account's ledger (manual or plaid); NULL means every source. Read by core.fct_investment_transactions; see docs/specs/investment-source-choice.md */
  b.currency_code, /* ISO 4217 currency this account is denominated in: user override (app.account_settings) else the currency its own source reported (OFX CURDEF, Plaid iso_currency_code, tabular currency column). NULL means genuinely unknown and stays that way — every monetary grain COALESCEs onto this column, so a literal default here would relabel the whole ledger and make the unknown-currency segment unreachable (multi-currency.md Requirements 3 and 8). system doctor's currency_integrity check surfaces NULLs for the user to resolve with `accounts set --currency`. */
  s.credit_limit, /* User-asserted credit limit on credit cards / lines */
  COALESCE(
    NOT s.display_name IS NULL
    OR (
      REGEXP_MATCHES(b.account_label, '\p{L}') AND b.account_label <> 'Unnamed account'
    ),
    FALSE
  ) AS display_name_is_user_set, /* Whether display_name came from a human-authored rung -- the user-set app.account_settings override, or the source's own account_label (a sheet's Account column, --account-name, Plaid's per-account name) -- rather than one of the generated-descriptor fallback arms below them. The account_label condition mirrors the union of the two account_label arms above: the digit-run distinction between them only decides which one supplies the text, not whether it is human-authored. The outer COALESCE matters: REGEXP_MATCHES and <> both propagate NULL for a NULL account_label (every OFX-sourced row, per the NULL::TEXT AS account_label cast above), which would otherwise leave this column NULL rather than FALSE for that whole population, unlike the model's other two booleans (archived, include_in_net_worth), which both default through COALESCE. Provenance signal only -- never for display. AccountResolver's weak "name" merge signal reads this to require a human-chosen name, because two unrelated accounts whose *generated* descriptors coincide (e.g. two typeless checking accounts at the same bank both resolving to "checking") is a coincidence of shared attributes already compared separately, not name evidence. */
  b.archived, /* Hides account from default list and from reports.net_worth */
  b.include_in_net_worth, /* Whether this account contributes to reports.net_worth */
  b.archived_at, /* The date the account stopped being part of the position; NULL while active. Lets a stock-measure report exclude the account only for dates after this one, instead of retroactively */
  b.extracted_at, /* Latest time the data was parsed from a contributing source */
  b.loaded_at, /* Latest time a contributing record was written to the raw table */
  b.investment_source_type_changed_at, /* When investment_source_type last changed, including a clear and an undo; folded into the investment ledger's and dim_holdings' updated_at so rows that re-enter the ledger do not rewind it */
  b.archived_changed_at, /* When archived last changed, including an undo; folded into updated_at here and on the account's core.fct_transactions rows, whose is_categorizable it moves */
  GREATEST(b.updated_at, s.updated_at) AS updated_at /* Latest of all per-row input timestamps contributing to this row's current values. Folds prep.int_accounts__merged.updated_at, which carries every setting change time (investment source, archive, currency, cost-basis method), because an undo restores the settings row's own updated_at from the old image and advances only the change time. Does not advance on idempotent SQLMesh re-applies. See docs/specs/core-updated-at-convention.md. */
FROM prep.int_accounts__merged AS b
LEFT JOIN app.account_settings AS s
  ON b.account_id = s.account_id
