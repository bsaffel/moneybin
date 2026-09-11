/* Observation grain (from_currency, to_currency, rate_date), where rate_date
   keeps the meaning raw.exchange_rates already gives it: the business day the
   provider published the rate for. Unions the staged provider rows with
   app.exchange_rate_overrides and resolves precedence — an override outranks
   every provider row for its own pair and date, mirroring the tie-break
   CurrencyService._stored_rate already applies (override first, then
   freshest write, then provider name as a deterministic backstop). Mirrors
   core.fct_balances (a VIEW unioning observation sources) and the
   provider/override precedence already resolved by core.fct_security_prices.

   TWO COLUMNS, NOT ONE, CARRY PROVENANCE — and this split is shared with
   core.fct_exchange_rates_daily and core.fct_exchange_rates_effective, so a
   caller joining any of the three reads the same two names the same way.
   rate_source is the closed vocabulary ('provider' / 'override'; 'identity'
   only appears on the two downstream models, never here) that every
   consumer branches on. provider carries the specific name behind a
   'provider' row (raw.exchange_rates.source_type, e.g. 'frankfurter') and is
   NULL whenever rate_source is not 'provider' — an override is user-authored,
   not sourced from a named feed. Splitting the two is what lets one column
   name mean one thing everywhere: a single rate_source column that sometimes
   held 'frankfurter' and sometimes held the coarse 'provider' would be two
   vocabularies wearing one name, exactly the coherence failure
   design-principles.md's "one way to do each thing" rule exists to catch.

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
    source_type AS provider_name,
    loaded_at AS updated_at
  FROM prep.stg_exchange_rates
), overrides AS (
  SELECT
    from_currency,
    to_currency,
    rate_date,
    rate,
    NULL::TEXT AS provider_name,
    updated_at
  FROM app.exchange_rate_overrides
), candidates AS (
  SELECT
    from_currency,
    to_currency,
    rate_date,
    rate,
    provider_name,
    updated_at,
    0 AS source_rank,
    'override' AS rate_source_value
  FROM overrides
  UNION ALL
  SELECT
    from_currency,
    to_currency,
    rate_date,
    rate,
    provider_name,
    updated_at,
    1 AS source_rank,
    'provider' AS rate_source_value
  FROM provider
)
SELECT
  from_currency, /* ISO 4217, upper (grain) */
  to_currency, /* ISO 4217, upper (grain) */
  rate_source_value AS rate_source, /* provider / override */
  provider_name AS provider, /* The named feed behind a provider row (e.g. 'frankfurter'); NULL when rate_source = 'override' */
  rate, /* Multiply a from_currency amount by this to get to_currency */
  rate_date, /* The business day this rate was published for (grain) */
  updated_at /* When the winning row was recorded: loaded_at for a provider row, updated_at for an override */
FROM candidates
QUALIFY
  ROW_NUMBER() OVER (
    PARTITION BY from_currency, to_currency, rate_date
    ORDER BY source_rank, updated_at DESC, provider_name
  ) = 1
