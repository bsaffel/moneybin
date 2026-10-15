/* Date spans an exchange-rate feed has answered for completely, one row per
   answer (multi-currency.md Requirement 13). Inside a span, every date the
   provider published is in raw.exchange_rates, so a date with no row there was
   a closed market rather than a day nobody fetched — the proof the offline
   market-closure rule needs before it prices that date from the publication
   before it. raw.exchange_rates alone cannot give that proof: two separate
   `moneybin fx rate` lookups leave the same rows as a full range fetch with a
   real gap between them.

   Written by the refresh backfill (the first to last publication a range
   answer kept, and only when it dropped nothing from the middle) and by a
   single `fx rate` fetch (the publication the provider resolved a date back to,
   through that date). APPEND-ONLY, like the rate cache it vouches for. */
CREATE TABLE IF NOT EXISTS raw.exchange_rate_coverage (
    from_currency VARCHAR NOT NULL,           -- ISO 4217, upper; matches raw.exchange_rates.from_currency
    to_currency VARCHAR NOT NULL,             -- ISO 4217, upper; matches raw.exchange_rates.to_currency
    start_date DATE NOT NULL,                 -- First date of the span the provider answered for completely
    end_date DATE NOT NULL,                   -- Last date of that span, inclusive
    source_type VARCHAR NOT NULL,             -- Provider that answered; matches raw.exchange_rates.source_type
    loaded_at TIMESTAMP                       -- When this answer was recorded locally
        DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (from_currency, to_currency, start_date, end_date, source_type),
    CHECK (start_date <= end_date)
);
