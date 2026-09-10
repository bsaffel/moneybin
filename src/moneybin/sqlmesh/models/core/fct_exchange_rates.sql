/* Observation grain (from_currency, to_currency, rate_date), where rate_date
   keeps the meaning raw.exchange_rates already gives it: the business day the
   provider published the rate for. Unions the staged provider rows with
   app.exchange_rate_overrides and resolves precedence — an override outranks
   every provider row for its own pair and date, mirroring the tie-break
   CurrencyService._stored_rate already applies (override first, then
   freshest write, then source_type as a deterministic backstop). Mirrors
   core.fct_balances (a VIEW unioning observation sources) and the
   provider/override precedence already resolved by core.fct_security_prices.

   rate_source carries the real provenance value here — the provider's own
   source_type ('frankfurter', ...) or 'override' — rather than the coarser
   provider/identity/override vocabulary core.fct_exchange_rates_daily and
   core.fct_exchange_rates_effective use. Those two exist to be joined at a
   predictable, closed-vocabulary grain; this one exists to answer "what did
   we actually observe," the same job core.fct_security_prices does for
   prices, and it answers that job the same way: real source_type values.

   This view is NOT what core.fct_exchange_rates_daily densifies — see that
   model's header for why the two must stay independent. */
MODEL (
  name core.fct_exchange_rates,
  kind VIEW,
  grain (from_currency, to_currency, rate_date)
);

WITH provider AS (
  SELECT
    from_currency,
    to_currency,
    rate_date,
    rate,
    source_type,
    loaded_at AS updated_at
  FROM prep.stg_exchange_rates
), overrides AS (
  SELECT
    from_currency,
    to_currency,
    rate_date,
    rate,
    'override' AS source_type,
    updated_at
  FROM app.exchange_rate_overrides
), candidates AS (
  SELECT
    from_currency,
    to_currency,
    rate_date,
    rate,
    source_type,
    updated_at,
    0 AS source_rank
  FROM overrides
  UNION ALL
  SELECT
    from_currency,
    to_currency,
    rate_date,
    rate,
    source_type,
    updated_at,
    1 AS source_rank
  FROM provider
)
SELECT
  from_currency, /* ISO 4217, upper (grain) */
  to_currency, /* ISO 4217, upper (grain) */
  rate_date, /* The business day this rate was published for (grain) */
  rate, /* Multiply a from_currency amount by this to get to_currency */
  source_type AS rate_source, /* Provider source_type, or 'override' when a user correction won */
  updated_at /* When the winning row was recorded: loaded_at for a provider row, updated_at for an override */
FROM candidates
QUALIFY
  ROW_NUMBER() OVER (
    PARTITION BY from_currency, to_currency, rate_date
    ORDER BY source_rank, updated_at DESC, source_type
  ) = 1
