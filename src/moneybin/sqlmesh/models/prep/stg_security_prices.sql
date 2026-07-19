MODEL (
  name prep.stg_security_prices,
  kind VIEW
);

/* Resolve the provider's own key to the canonical security_id through the same
   accepted bindings SecurityResolver writes. An INNER JOIN is deliberate: an
   unresolved observation stays in raw and reappears here once its security binds,
   rather than being dropped or carried forward as an orphan FK.
   investment_unresolved_securities already reports that backlog.

   ref_kind is mapped per source rather than hardcoded, so C.2's stooq_ticker and
   coingecko_slug extend the CASE instead of forking a second resolution path. */
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
