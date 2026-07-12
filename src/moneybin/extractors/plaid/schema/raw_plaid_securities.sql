/* Securities referenced by Plaid holdings/investment transactions; one record per security per sync payload */
CREATE TABLE IF NOT EXISTS raw.plaid_securities (
    security_id VARCHAR NOT NULL,             -- Plaid security_id; adopt-quality (churns on corporate actions), not immutable
    institution_security_id VARCHAR,          -- Institution's own identifier; unique only per institution; often NULL
    institution_id VARCHAR,                   -- Plaid institution_id; scopes institution_security_id
    ticker_symbol VARCHAR,                    -- Ticker as reported; may carry exchange suffix
    market_identifier_code VARCHAR,           -- ISO-10383 MIC of the listing exchange/market; the exchange signal for ticker+exchange resolution
    security_name VARCHAR,                    -- Plaid name
    security_type VARCHAR,                    -- Plaid Security.type; prose enum, not schema-enforced — staging maps defensively
    close_price DECIMAL(28, 10),              -- Plaid close_price; point-in-time convenience, NOT the Pillar C price history
    close_price_as_of DATE,                   -- Date of close_price
    iso_currency_code VARCHAR,                -- ISO 4217; mutually exclusive with unofficial_currency_code
    unofficial_currency_code VARCHAR,         -- Non-ISO (crypto) currency; staging COALESCEs the pair
    cusip VARCHAR,                            -- License-gated; NULL in practice since 2024-03
    isin VARCHAR,                             -- License-gated; NULL in practice
    is_cash_equivalent BOOLEAN,               -- Pairs with security_type = 'cash' (money-market/sweep)
    source_file VARCHAR NOT NULL,             -- Logical identifier: sync_{job_id}
    source_type VARCHAR NOT NULL              -- Always 'plaid' for this table
        DEFAULT 'plaid',
    source_origin VARCHAR NOT NULL,           -- Plaid item_id; scopes dedup to the institution connection; part of the PK
    extracted_at TIMESTAMP                    -- When the server fetched this data from Plaid (metadata.synced_at)
        DEFAULT CURRENT_TIMESTAMP,
    loaded_at TIMESTAMP                       -- When this record was inserted into the local database
        DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (security_id, source_origin)
);
