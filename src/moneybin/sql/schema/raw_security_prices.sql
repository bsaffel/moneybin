/* Daily price observations per security, date, quote currency, and source. APPEND-ONLY:
   a stored row is never updated or deleted, because a historical close is an immutable
   fact. Writers pass on_conflict="ignore" so a re-reported observation keeps the row
   first written — "insert" would raise on the second sync and "upsert" would silently
   mutate history. Deliberately unlike raw.plaid_securities, whose close_price is
   upsert-overwritten on every pull and therefore cannot carry a history.

   Keyed by the PROVIDER's own identifier, not the canonical security_id: the extractor
   writes during ingestion, before SecurityResolver has minted one, and on a first pull
   for a new security no canonical id exists to write. Resolution happens in staging.
   See docs/specs/investments-price-feeds.md. */
CREATE TABLE IF NOT EXISTS raw.security_prices (
    provider_security_key VARCHAR NOT NULL,   -- The provider's own id: Plaid security_id, a ticker, a CoinGecko slug; resolved to canonical security_id in prep.stg_security_prices
    price_date DATE NOT NULL,                 -- The date the close applies to, not the date it was fetched
    quote_currency VARCHAR NOT NULL,          -- ISO 4217; in the key so an ADR and its ordinary listing keep both prices instead of colliding
    source VARCHAR NOT NULL,                  -- plaid, stooq, coingecko — stored provider observations only; override and trade_implied are derived at model build and never land here
    source_origin VARCHAR NOT NULL,           -- The connection that produced it (Plaid item_id); '' for single-tenant feeds. Mirrors raw.plaid_securities
    close DECIMAL(28, 10) NOT NULL,           -- Price of one unit in quote_currency; (28,10) matches the investments quantity/price precedent
    price_basis VARCHAR NOT NULL              -- Declared by the adapter, never inferred from the data; only 'raw' is eligible to value a holding
        CHECK (price_basis IN ('raw', 'split_adjusted', 'split_and_dividend_adjusted')),
    extracted_at TIMESTAMP                    -- When the provider served this observation (for Plaid, metadata.synced_at)
        DEFAULT CURRENT_TIMESTAMP,
    loaded_at TIMESTAMP                       -- When this record was inserted into the local database
        DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (source, source_origin, provider_security_key, price_date, quote_currency)
);
