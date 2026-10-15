/* The currencies each exchange-rate feed publishes at all, as of the last time
   MoneyBin read its list (multi-currency.md Requirement 18). Written when a
   refresh or `moneybin fx rate` reads the provider's list to tell an unsupported
   pair from a missing date, and replaced whole per provider on each read, so a
   currency the provider stops carrying stops counting as published.

   It exists so a report read, which never reaches the network, can still say
   whether a missing rate needs `moneybin fx set` (the provider never publishes
   that currency) or `moneybin refresh` (it does, and the date was not fetched).
   No list for a provider means neither is claimed. */
CREATE TABLE IF NOT EXISTS raw.exchange_rate_currencies (
    source_type VARCHAR NOT NULL,             -- Provider whose list this is: 'frankfurter' | ...; matches raw.exchange_rates.source_type
    currency_code VARCHAR NOT NULL,           -- ISO 4217, upper; a currency that provider publishes rates for
    loaded_at TIMESTAMP                       -- When this list was read from the provider
        DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (source_type, currency_code)
);
