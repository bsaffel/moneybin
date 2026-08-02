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

   ONE KEY, ONE OWNER PER DATE. A provider key is not owned by a security outright, only
   for an interval, because a rename frees the old symbol and tickers get recycled —
   whoever lists under FB next binds it accepted. So each link owns the closes from its
   predecessor's retirement up to its own, and the handover CTE below is what supplies
   that lower edge. Both bounds are load-bearing and neither is redundant with the other:

     - Without the UPPER bound, a retired link keeps claiming every future FB close and
       values its security from a different company's series — the precise failure that
       retiring the binding existed to prevent.
     - Without the LOWER bound, the next owner's accepted link claims every close stored
       before it ever listed. Those are the previous owner's rows, which its own retired
       link still resolves, so ONE raw observation becomes TWO securities' price history
       rather than merely going missing. The same gap lets a key retired twice — bound,
       retired, rebound, retired again — resolve its earliest rows through both retired
       links at once, duplicating them under a single security.

   A user reversal deliberately creates no handover edge. It means the pairing was never
   real, so it transfers nothing: the next holder is the rightful owner of the whole
   series, not just of the part after a boundary that describes a mistake. That is also
   why the handover CTE filters on reversed_by rather than on status alone.

   Every 'auto' literal here is pinned to price_service._AUTO_REVERSAL by
   test_price_service.py::test_the_staging_model_retires_the_actor_this_service_writes,
   which fails if the two arms ever name different actors — a silent drift in the join
   arm reads as "this security has no history", and one in the handover CTE silently
   turns a user's rejection into a transfer of ownership.

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
WITH handover AS (
  SELECT
    link.link_id,
    MAX(prior.reversed_at)::DATE AS owned_from
  FROM app.security_links AS link
  JOIN app.security_links AS prior
    ON prior.source_type = link.source_type
    AND prior.ref_kind = link.ref_kind
    AND prior.ref_value = link.ref_value
    AND prior.link_id <> link.link_id
    AND prior.status = 'reversed'
    AND prior.reversed_by = 'auto'
    AND prior.reversed_at <= link.decided_at
  GROUP BY
    link.link_id
)
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
LEFT JOIN handover AS h
  ON h.link_id = links.link_id
WHERE
  (
    h.owned_from IS NULL OR p.price_date >= h.owned_from
  )
  AND (
    links.status = 'accepted'
    OR (
      links.status = 'reversed'
      AND links.reversed_by = 'auto'
      AND p.price_date < links.reversed_at::DATE
    )
  )
