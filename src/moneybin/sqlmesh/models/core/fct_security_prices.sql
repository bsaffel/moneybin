/* The resolved price series: one close per security, date, and quote currency, with
   the source that supplied it carried as provenance. Only price_basis = 'raw' is
   eligible — an adjusted series states a price relative to the corporate actions known
   when it was fetched, so a row fetched as split_adjusted stops being correctly
   adjusted after the next split. Adjusted observations remain visible in
   prep.stg_security_prices and raw.security_prices; they are excluded from valuation
   rather than silently valued.

   Source rank breaks same-date ties and is a TOTAL order, not a grouping: two sources
   sharing a tier would leave several winners and let a rebuild pick a different price
   each time. source_origin and provider_security_key close most remaining ties (two
   Plaid connections differ only by origin). One case survives those three keys:
   prep.stg_security_prices normalizes quote_currency with UPPER(), so a provider
   observation stored as 'usd' and a duplicate stored as 'USD' reach this model with
   identical security_id, source, source_origin, and provider_security_key — the
   only raw column that differed is gone by the time it gets here. close is the final
   tiebreak for that case: it is the one remaining column guaranteed to hold the
   colliding rows' own content, so ordering by it is deterministic and stable across
   rebuilds. If close also ties, the candidate rows are identical in every column this
   model exposes, so either pick is indistinguishable to a reader. A new adapter takes
   the next free rank. See docs/specs/investments-price-feeds.md. */
MODEL (
  name core.fct_security_prices,
  kind FULL,
  grain (security_id, price_date, quote_currency)
);

WITH ranked AS (
  SELECT
    p.security_id,
    p.price_date,
    p.quote_currency,
    p.close,
    p.source,
    p.source_origin,
    p.provider_security_key,
    p.price_basis,
    p.extracted_at,
    CASE p.source
      WHEN 'override'
      THEN 1
      WHEN 'plaid'
      THEN 2
      WHEN 'stooq'
      THEN 3
      WHEN 'coingecko'
      THEN 4
      WHEN 'trade_implied'
      THEN 5
      ELSE 99
    END AS source_rank
  FROM prep.stg_security_prices AS p
  WHERE
    p.price_basis = 'raw'
)
SELECT
  security_id, /* FK to core.dim_securities (grain) */
  price_date, /* The date this close applies to (grain) */
  quote_currency, /* ISO 4217 the close is expressed in (grain); this model converts nothing — M1K.2 owns FX */
  close, /* The winning close for one unit, in quote_currency */
  source, /* Which source supplied the winning close: override, plaid, stooq, coingecko, or trade_implied */
  price_basis, /* Always 'raw' here; adjusted observations are excluded upstream and stay visible in prep.stg_security_prices */
  extracted_at AS updated_at /* When the winning observation was served by its provider */
FROM ranked
QUALIFY
  ROW_NUMBER() OVER (
    PARTITION BY security_id, price_date, quote_currency
    ORDER BY source_rank, source_origin, provider_security_key, close
  ) = 1
