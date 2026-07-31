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

   ref_kind is mapped per source rather than hardcoded, so C.2's tiingo_ticker and
   coingecko_slug extend the CASE instead of forking a second resolution path.

   RETIRED KEYS still resolve their own history. A reversed link is one of two very
   different things, and only reversed_by tells them apart. PriceService retires an
   auto-derived feed key ('auto') when the catalog value it came from moves — a ticker
   rename, FB to META — which is bookkeeping: the closes stored under the old symbol
   were still this security's prices, and admitting only 'accepted' would erase the
   entire pre-rename series from here and from core on an ordinary corporate action,
   with no error. A reversal by anyone else is a judgement that the pairing was wrong,
   so its observations must stay dropped; restoring them would reinstate exactly the
   valuation the user rejected.

   The price_date bound is not optional. A rename frees the old symbol, and tickers get
   recycled — whoever lists under FB next binds it accepted. Without the bound the
   retired link would go on claiming every future FB close, valuing this security from a
   different company's series: the precise failure retiring the binding existed to
   prevent. Observations dated before the retirement resolve through it; nothing after
   does. The 'auto' literal is pinned to price_service._AUTO_REVERSAL by
   test_price_service.py::test_the_staging_model_retires_the_actor_this_service_writes,
   because a silent drift here reads as "this security has no history".

   COVERAGE — read this before adding a price adapter. The CASE below maps three
   sources: 'plaid', 'tiingo', and 'coingecko'. That is the complete set that resolves
   today. Any other value of raw.security_prices.source_type makes the CASE return NULL,
   `links.ref_kind = NULL` evaluates to UNKNOWN, and this INNER JOIN discards the row
   silently — no error and no counter. The doctor check
   investment_unmapped_price_source is the safety net: it reports any source_type
   present in raw.security_prices that this CASE does not map. It reports rows already
   written, so it cannot prevent the drop — extending this CASE in the same change that
   starts writing a source is still the requirement.

   That drop is PERMANENT, not deferred, and this is the one way it differs from the
   unresolved-binding case described above. An unresolved observation waits in raw and
   reappears here the moment its security binds. A row whose source_type has no ref_kind
   mapping never reappears no matter how many bindings are accepted, because the
   failure is in the mapping, not the binding. It is invisible and unrecoverable until
   someone edits this file.

   Nothing upstream prevents it: raw.security_prices.source_type carries no CHECK constraint
   (unlike price_basis), and core.fct_security_prices already ranks override and
   trade_implied, neither of which passes through this view at all. So a new adapter MUST
   extend this CASE in the SAME change that starts writing its rows — the tiingo and
   coingecko arms below were added one commit late, and every row those adapters wrote in
   between was discarded here.

   Two tests guard the two directions, because one alone cannot see both. This model's own
   coverage test reads the CASE and grows itself when a mapping is added, so it catches a
   mapping whose ref_kind or CHECK constraint is wrong. It cannot catch a writer shipping
   ahead of its mapping — the CASE is unchanged, so the test is unchanged. That direction is
   tests/moneybin/test_services/test_price_service.py, which asserts every source_type
   PriceService writes appears here.

   No close-positivity filter follows: raw.security_prices enforces CHECK (close > 0) at
   write, so a zero or negative close can never reach this view — the guard lives at the
   write boundary of the append-only source, not as a read-time filter that could mask a
   bad row already stored. */
SELECT
  links.security_id AS security_id,
  p.provider_security_key,
  p.price_date,
  UPPER(p.quote_currency) AS quote_currency,
  p.source_type,
  p.source_origin,
  p.close,
  p.price_basis,
  p.extracted_at,
  p.loaded_at
FROM raw.security_prices AS p
JOIN app.security_links AS links
  ON links.source_type = p.source_type
  AND links.ref_value = p.provider_security_key
  AND links.ref_kind = CASE p.source_type
    WHEN 'plaid'
    THEN 'plaid_security_id'
    WHEN 'tiingo'
    THEN 'tiingo_ticker'
    WHEN 'coingecko'
    THEN 'coingecko_slug'
  END
  AND (
    links.status = 'accepted'
    OR (
      links.status = 'reversed'
      AND links.reversed_by = 'auto'
      AND p.price_date < links.reversed_at::DATE
    )
  )
