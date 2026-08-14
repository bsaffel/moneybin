/* User corrections to provider reference rates (multi-currency.md Requirement 14).
   An override outranks every cached provider rate for its own pair and date, and a
   later refetch never overwrites it — raw.exchange_rates is append-only precisely
   so a disagreement is recorded here instead of edited into the cache.

   The grain is one DAILY reference rate, not one rate per transaction: two
   same-day conversions at different effective rates (spread, fee) are out of
   scope, and a genuine per-transaction rate belongs on the M1K.3 conversion-pair
   model. Written only through ExchangeRateOverridesRepo, which pairs every
   mutation with app.audit_log (Invariant 10). No provider write ever touches
   this table. */
CREATE TABLE IF NOT EXISTS app.exchange_rate_overrides (
    from_currency VARCHAR NOT NULL,           -- ISO 4217, upper; the currency being converted out of
    to_currency VARCHAR NOT NULL,             -- ISO 4217, upper; the currency being converted into
    rate_date DATE NOT NULL,                  -- The business day this correction applies to, not the day it was entered; per-date scoping is what lets an override survive a refetch without suppressing other days
    rate DECIMAL(18, 8) NOT NULL              -- The user's rate; multiply a from_currency amount by this to get to_currency. (18,8) is the exchange-rate precision in database.md
        CHECK (rate > 0),                     -- Mirrors raw.exchange_rates: a zero or negative rate is never a real quote, and this one outranks the provider, so it would convert every balance to zero
    note VARCHAR,                             -- Why the user overrode the provider rate; the only provenance an override carries beyond its audit row
    created_at TIMESTAMP                      -- When first entered; preserved across a later correction to the same date
        DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP                      -- When this override last changed value
        DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (from_currency, to_currency, rate_date)
);
