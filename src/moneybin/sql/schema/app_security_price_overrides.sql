/* User price marks (investments-price-feeds.md phase C.2). One row per security,
   date, and quote currency. A mark exists to state a price the feeds either got
   wrong or never covered — a restricted grant, a private fund, a security no
   provider quotes — and a later provider fetch never overwrites it: source
   precedence ranks 'override' above every provider for its own date, while
   freshness still lets a newer close win across dates. Written only through
   SecurityPriceRepo, which pairs every mutation with app.audit_log (Invariant 10).
   No provider write ever touches this table. */
CREATE TABLE IF NOT EXISTS app.security_price_overrides (
    security_id VARCHAR NOT NULL,             -- FK to app.securities; the canonical id, never a provider key — a mark is authored against MoneyBin's own catalog
    price_date DATE NOT NULL,                 -- The date this mark applies to, not the date it was entered; scoping per-date is what lets a mark survive re-fetch without suppressing newer closes
    quote_currency VARCHAR NOT NULL,          -- ISO 4217; in the key for the same reason as raw.security_prices — a dual-quoted security has two legitimate prices for one date
    close DECIMAL(28, 10) NOT NULL            -- The user's price for one unit in quote_currency; (28,10) matches raw.security_prices and the investments quantity/price precedent
        CHECK (close > 0),                    -- Mirrors raw.security_prices: zero is the value 'an unpriced holding is NULL, never zero' exists to refuse, so a mark must not be able to assert it
    note VARCHAR,                             -- Why the user set it; optional, and the only provenance a mark carries beyond the audit row
    created_at TIMESTAMP                      -- When this mark was first entered; preserved across a later correction to the same date
        DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP                      -- When this mark last changed value
        DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (security_id, price_date, quote_currency)
);
