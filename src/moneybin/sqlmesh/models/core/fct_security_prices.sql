/* The resolved price series: one close per security, date, and quote currency, with
   the source that supplied it carried as provenance. Only price_basis = 'raw' is
   eligible — an adjusted series states a price relative to the corporate actions known
   when it was fetched, so a row fetched as split_adjusted stops being correctly
   adjusted after the next split. Adjusted observations remain visible in
   prep.stg_security_prices and raw.security_prices; they are excluded from valuation
   rather than silently valued.

   The ORDER BY is a deterministic pick over every column this model exposes — not an
   unqualified total order over the raw input. source_rank picks a preferred provider;
   source (the string, not just the rank) separates two sources that share the ELSE 99
   bucket, since a bucket is a grouping and would otherwise leave two unranked sources
   tied; source_origin and provider_security_key separate two connections or two
   provider keys for the same security; extracted_at DESC — freshest observation wins —
   separates two rows still tied on all four. close is kept only as a final,
   effectively unreachable backstop.

   One duplicate shape survives all of the above: prep.stg_security_prices normalizes
   quote_currency with UPPER(), so a provider observation stored as 'usd' and a
   duplicate stored as 'USD' carry distinct raw primary keys (quote_currency is part of
   raw.security_prices' PK) and both reach this model with identical security_id,
   source, source_origin, and provider_security_key. extracted_at resolves that case by
   freshness, but the raw casing that distinguished the two rows is discarded by
   staging and is not recoverable at this layer — the ordering is deterministic, not
   exhaustive over information staging already threw away. A new adapter takes the
   next free rank. See docs/specs/investments-price-feeds.md. */
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
  source, /* Which source supplied the winning close: plaid today; override, stooq, coingecko, and trade_implied are planned — see docs/specs/investments-price-feeds.md */
  price_basis, /* Always 'raw' here; adjusted observations are excluded upstream and stay visible in prep.stg_security_prices */
  extracted_at AS updated_at /* When the winning observation was served by its provider */
FROM ranked
QUALIFY
  ROW_NUMBER() OVER (
    PARTITION BY security_id, price_date, quote_currency
    ORDER BY source_rank, source, source_origin, provider_security_key, extracted_at DESC, close
  ) = 1
