/* The resolved price series: one close per security, date, and quote currency, with
   the source that supplied it carried as provenance. Only price_basis = 'raw' is
   eligible — an adjusted series states a price relative to the corporate actions known
   when it was fetched, so a row fetched as split_adjusted stops being correctly
   adjusted after the next split. Adjusted observations remain visible in
   prep.stg_security_prices and raw.security_prices; they are excluded from valuation
   rather than silently valued.

   THREE INPUTS, ONE GRAIN. Provider observations arrive from prep.stg_security_prices
   (the only branch stored in raw.security_prices); user marks from
   app.security_price_overrides; and trade-implied prices from
   core.fct_investment_transactions. The two derived branches are computed at model
   build and carry price_basis 'raw' by construction — a user states an as-traded price,
   and an executed trade is a raw observation by definition. Neither passes through
   app.security_links, which is what lets a security no feed covers — a restricted
   grant, a pre-IPO position, a private fund — carry a price at all.

   Trade-implied rows are filtered to price > 0 and a non-NULL security_id.
   raw.security_prices and app.security_price_overrides both enforce CHECK (close > 0),
   but fct_investment_transactions.price carries no such constraint: a vesting grant or
   a stock dividend legitimately records price 0, and a cash-only event (deposit,
   withdrawal, fee) records neither security nor price. Unioned unfiltered, that zero
   becomes a resolved close and values the whole position at nothing while reporting
   valuation_status 'valued' — the precise outcome the positivity checks on the other
   two sources exist to refuse.

   NO FIRST-AVAILABLE FLOOR HERE, deliberately. This model's grain is the observation
   date, not a valuation date, so a floor of MIN(price_date) per (security, source)
   would be taken over the very set it filters and admit every row by construction.
   The failure a floor guards — valuing a 2018 position from a 2024 listing price —
   needs resolution to reach FORWARD in time, which the bounded-lookback rule forbids:
   core.dim_holdings takes `price_date <= CURRENT_DATE` ordered `price_date DESC`. A
   floor becomes meaningful only if C.3's fct_holdings_daily fills its spine backward,
   and that model should not. See docs/specs/investments-price-feeds.md.

   The ORDER BY is a deterministic pick over every column this model exposes — not an
   unqualified total order over the raw input. source_rank picks a preferred source;
   source_type (the string, not just the rank) separates two sources that share the ELSE 99
   bucket, since a bucket is a grouping and would otherwise leave two unranked sources
   tied; extracted_at DESC — freshest observation wins — then decides, and it must come
   BEFORE the two identifier keys, not after. app.security_links is N:1 (one security_id
   may own many provider refs, because Plaid retires a security_id on a corporate action
   and re-binds the successor to the same canonical security), so on a changeover day the
   retired ref and its successor both carry an observation for one price_date and quote
   currency, tied on security_id, source_rank, and source_type. Ordering by
   observation_key first would settle a 10:1 split by ASCII sort — the retired
   ref's PRE-split close winning over the successor's post-split one — and dim_holdings
   would then multiply the post-split quantity by the pre-split price and publish a
   market_value overstated by the split factor, with valuation_status 'valued'. Freshness
   is the only key that carries the right answer there. source_origin and
   observation_key follow as the deterministic backstop for rows tied even on
   extracted_at, and close is the final tiebreak — see below.

   Freshness fails to decide when the retired and successor refs arrive in ONE sync: the
   extractor stamps a single batch-level extracted_at per pull, so both refs tie on it and
   the ASCII backstop would settle a split by key sort again. That grain is WITHHELD
   instead (same_pull_key_conflict below emits no row for it), so dim_holdings falls back to
   an earlier close under its own split-staleness guards rather than publishing an
   arbitrary, possibly pre-split close as 'valued'. The withhold is scoped to DIFFERENT
   provider refs disagreeing on close within one pull — a same-ref casing duplicate (one
   observation_key, 'usd' vs 'USD') is not a churn and keeps its close tiebreak.

   THE WITHHOLD IS SCOPED BY SOURCE IDENTITY, NOT BY RANK. Two partial fills of one
   security on one day share source_type, source_origin, and extracted_at while carrying
   different transaction ids and different prices — every condition the conflict tests
   for. Withholding there would blank a routine grain and report the position unpriced.
   A rank threshold cannot express the distinction (override is rank 1 and trade_implied
   rank 5, so the two derived sources bracket the three provider ones) and a rank RANGE
   would break silently the first time a new adapter takes rank 6. The provider set is
   therefore enumerated; keep it in step with investment_price_disagreement, which
   restricts its comparison to the same three sources for the same reason.

   This is a total order over the emitted columns. The model exposes security_id,
   price_date, and quote_currency (the partition), plus close, source_type, price_basis,
   and extracted_at; source_rank is a pure function of source_type, and price_basis is
   constant ('raw') across all three branches. Two rows tied on source_type,
   extracted_at, and close are therefore identical in every column this model publishes,
   whichever the QUALIFY picks.

   One duplicate shape reaches that final tiebreak: prep.stg_security_prices normalizes
   quote_currency with UPPER(), so a provider observation stored as 'usd' and a
   duplicate stored as 'USD' carry distinct raw primary keys (quote_currency is part of
   raw.security_prices' PK) and both reach this model with identical security_id,
   source_type, source_origin, and observation_key. extracted_at usually resolves that
   case by freshness; when both casing variants arrive in the same sync and therefore
   share one extracted_at, close breaks the tie instead. Either way, the raw casing
   that distinguished the two rows is discarded by staging and is not recoverable at
   this layer — the ordering is deterministic, not exhaustive over information staging
   already threw away. app.security_price_overrides has the same shape for the same
   reason (quote_currency is in its primary key), and resolves it the same way. A new
   adapter takes the next free rank. See
   docs/specs/investments-price-feeds.md. */
MODEL (
  name core.fct_security_prices,
  kind FULL,
  grain (security_id, price_date, quote_currency)
);

WITH provider AS (
  SELECT
    p.security_id,
    p.price_date,
    p.quote_currency,
    p.close,
    p.source_type,
    p.source_origin,
    p.provider_security_key AS observation_key,
    p.price_basis,
    p.extracted_at
  FROM prep.stg_security_prices AS p
  WHERE
    p.price_basis = 'raw'
), marks AS (
  /* source_origin and observation_key are empty because a mark has no connection
     that produced it and no identifier beyond its own primary key, which is exactly
     this model's partition. Two marks can therefore never tie. */
  SELECT
    o.security_id,
    o.price_date,
    UPPER(o.quote_currency) AS quote_currency,
    o.close,
    'override' AS source_type,
    '' AS source_origin,
    '' AS observation_key,
    'raw' AS price_basis,
    o.updated_at AS extracted_at
  FROM app.security_price_overrides AS o
), trade_implied AS (
  SELECT
    t.security_id,
    t.trade_date AS price_date,
    UPPER(t.currency_code) AS quote_currency,
    t.price AS close,
    'trade_implied' AS source_type,
    t.source_origin,
    t.investment_transaction_id AS observation_key,
    'raw' AS price_basis,
    t.updated_at AS extracted_at
  FROM core.fct_investment_transactions AS t
  WHERE
    NOT t.security_id IS NULL AND t.price > 0
), candidates AS (
  SELECT
    security_id,
    price_date,
    quote_currency,
    close,
    source_type,
    source_origin,
    observation_key,
    price_basis,
    extracted_at
  FROM provider
  UNION ALL
  SELECT
    security_id,
    price_date,
    quote_currency,
    close,
    source_type,
    source_origin,
    observation_key,
    price_basis,
    extracted_at
  FROM marks
  UNION ALL
  SELECT
    security_id,
    price_date,
    quote_currency,
    close,
    source_type,
    source_origin,
    observation_key,
    price_basis,
    extracted_at
  FROM trade_implied
), ranked AS (
  SELECT
    c.security_id,
    c.price_date,
    c.quote_currency,
    c.close,
    c.source_type,
    c.source_origin,
    c.observation_key,
    c.price_basis,
    c.extracted_at,
    CASE c.source_type
      WHEN 'override'
      THEN 1
      WHEN 'plaid'
      THEN 2
      WHEN 'tiingo'
      THEN 3
      WHEN 'coingecko'
      THEN 4
      WHEN 'trade_implied'
      THEN 5
      ELSE 99
    END AS source_rank,
    (
      c.source_type IN ('plaid', 'tiingo', 'coingecko')
      AND MIN(c.observation_key) OVER same_pull <> MAX(c.observation_key) OVER same_pull
      AND MIN(c.close) OVER same_pull <> MAX(c.close) OVER same_pull
    ) AS same_pull_key_conflict
  FROM candidates AS c
  WINDOW same_pull AS (
    PARTITION BY c.security_id, c.price_date, c.quote_currency, c.source_type, c.source_origin, c.extracted_at
  )
)
SELECT
  security_id, /* FK to core.dim_securities (grain) */
  price_date, /* The date this close applies to (grain) */
  quote_currency, /* ISO 4217 the close is expressed in (grain); this model converts nothing — M1K.2 owns FX */
  close, /* The winning close for one unit, in quote_currency */
  source_type, /* Which source supplied the winning close: plaid, override, or trade_implied today; tiingo and coingecko join when their adapters land — see docs/specs/investments-price-feeds.md */
  price_basis, /* Always 'raw' here; adjusted provider observations are excluded upstream and stay visible in prep.stg_security_prices, and the two derived sources are raw by construction */
  extracted_at AS updated_at /* When the winning observation was served: the provider's own timestamp, the mark's last edit, or the trade's ledger timestamp */
FROM ranked
QUALIFY
  ROW_NUMBER() OVER (
    PARTITION BY security_id, price_date, quote_currency
    ORDER BY source_rank, source_type, extracted_at DESC, source_origin, observation_key, close
  ) = 1
  AND NOT same_pull_key_conflict
