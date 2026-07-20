MODEL (
  name prep.stg_security_prices,
  kind VIEW
);

/* Resolve the provider's own key to the canonical security_id through the same
   accepted bindings SecurityResolver writes. An INNER JOIN is deliberate: an
   unresolved observation stays in raw and reappears here once its security binds,
   rather than being dropped or carried forward as an orphan FK.

   The backlog is only partly reported: investment_unreported_holdings catches an
   unresolved security that is currently held, and investment_unresolved_securities
   catches one carrying modeled transactions. A price-only observation for a security
   that is neither held nor transacted has no doctor coverage — it simply waits in raw.

   ref_kind is mapped per source rather than hardcoded, so C.2's stooq_ticker and
   coingecko_slug extend the CASE instead of forking a second resolution path.

   COVERAGE — read this before adding a price adapter. The CASE below maps exactly
   ONE source: 'plaid' -> 'plaid_security_id'. That is the complete set that resolves
   today. Any other value of raw.security_prices.source makes the CASE return NULL,
   `links.ref_kind = NULL` evaluates to UNKNOWN, and this INNER JOIN discards the row
   silently — no error, no doctor check, no counter.

   That drop is PERMANENT, not deferred, and this is the one way it differs from the
   unresolved-binding case described above. An unresolved observation waits in raw and
   reappears here the moment its security binds. A row whose source has no ref_kind
   mapping never reappears no matter how many bindings are accepted, because the
   failure is in the mapping, not the binding. It is invisible and unrecoverable until
   someone edits this file.

   Nothing upstream prevents it: raw.security_prices.source carries no CHECK constraint
   (unlike price_basis), its own schema comment names stooq and coingecko as expected
   values, and core.fct_security_prices already ranks override, stooq, coingecko, and
   trade_implied. So a new adapter MUST extend this CASE in the SAME change that starts
   writing its rows — and, because app.security_links.ref_kind is itself CHECK-
   constrained to ('plaid_security_id', 'institution_security_id'), must widen that
   constraint in the same change too. tests/moneybin/test_stg_security_prices.py drives
   this CASE's mapped set directly and fails if either half is missing. */
SELECT
  links.security_id AS security_id,
  p.provider_security_key,
  p.price_date,
  UPPER(p.quote_currency) AS quote_currency,
  p.source,
  p.source_origin,
  p.close,
  p.price_basis,
  p.extracted_at,
  p.loaded_at
FROM raw.security_prices AS p
JOIN app.security_links AS links
  ON links.status = 'accepted'
  AND links.source_type = p.source
  AND links.ref_value = p.provider_security_key
  AND links.ref_kind = CASE p.source WHEN 'plaid' THEN 'plaid_security_id' END
WHERE
  p.close > 0
