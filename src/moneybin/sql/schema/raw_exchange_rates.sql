/* Reference rates published by an exchange-rate feed, one row per pair, business
   day, and provider. APPEND-ONLY: a rate a provider published for a date is a
   historical fact, so a refetch never rewrites one. User corrections live in
   app.exchange_rate_overrides and outrank every row here — nothing in this table
   is ever edited to record a disagreement with it.

   Keyed by the provider's own resolution, not the caller's request: a rate asked
   for on a weekend resolves back to the last published business day, and
   rate_date stores the day the provider answered with. Storing the requested day
   instead would file a rate under a day no provider ever published, which is
   what the whole conversion path is later audited against. See
   docs/specs/multi-currency.md. */
CREATE TABLE IF NOT EXISTS raw.exchange_rates (
    from_currency VARCHAR NOT NULL,           -- ISO 4217, upper; the currency being converted out of
    to_currency VARCHAR NOT NULL,             -- ISO 4217, upper; the currency being converted into
    rate_date DATE NOT NULL,                  -- The business day the provider published this rate for, which may precede the date requested
    rate DECIMAL(18, 8) NOT NULL              -- Multiply a from_currency amount by this to get to_currency; (18,8) is the exchange-rate precision in database.md
        CHECK (rate > 0),                     -- A zero or negative rate is never a real quote, and this table is append-only, so a bad row could never be corrected in place
    source_type VARCHAR NOT NULL,             -- Provider only: 'frankfurter' | ...; never 'override', which is a different table. Named source_type to match the canonical provenance column across layers (database.md)
    loaded_at TIMESTAMP                       -- When this record was inserted locally; the feeds serve no publication timestamp of their own
        DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (from_currency, to_currency, rate_date, source_type)
);
