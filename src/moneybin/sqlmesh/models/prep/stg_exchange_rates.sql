MODEL (
  name prep.stg_exchange_rates,
  kind VIEW
);

/* Normalizes raw.exchange_rates. Mirrors prep.stg_security_prices in spirit,
   but a rate needs no provider-key resolution against a links table the way a
   security does — from_currency/to_currency are already the canonical join
   key, so this view only upper-cases the codes raw.exchange_rates documents
   but does not CHECK-constrain. */
SELECT
  UPPER(r.from_currency) AS from_currency,
  UPPER(r.to_currency) AS to_currency,
  r.rate_date,
  r.rate,
  r.source_type,
  r.loaded_at
FROM raw.exchange_rates AS r
