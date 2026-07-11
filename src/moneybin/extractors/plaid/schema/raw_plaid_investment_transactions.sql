/* Investment ledger events from Plaid investments/transactions/get; one record per transaction per sync payload */
CREATE TABLE IF NOT EXISTS raw.plaid_investment_transactions (
    investment_transaction_id VARCHAR NOT NULL, -- Plaid investment_transaction_id; stable unique identifier
    account_id VARCHAR NOT NULL,                -- Plaid account_id; foreign key to raw.plaid_accounts
    security_id VARCHAR,                        -- Plaid security_id; NULL for cash-only events (deposit, withdrawal, account fee)
    transaction_date DATE NOT NULL,             -- Plaid date; POSTING date ("typically the settlement date" per Plaid docs) — NOT the trade date; staging derives trade_date
    transaction_datetime TIMESTAMP,             -- Plaid transaction_datetime; trade-initiation timestamp (select institutions only); preferred trade-date source
    transaction_name VARCHAR,                   -- Plaid name; broker's description of the event
    quantity DECIMAL(28, 10),                   -- Plaid quantity; already signed per ledger convention (+ acquire, − dispose)
    amount DECIMAL(18, 2) NOT NULL,             -- Plaid amount (required field → NOT NULL faithful); CAUTION: positive = cash out (opposite of ledger); staging flips sign and maps basis-unknown transfers (amount 0) to a NULL ledger amount
    price DECIMAL(28, 10),                      -- Per-unit price
    fees DECIMAL(18, 2),                        -- Fee/commission component
    iso_currency_code VARCHAR,                  -- ISO 4217; mutually exclusive with unofficial_currency_code
    unofficial_currency_code VARCHAR,           -- Non-ISO (crypto) currency
    investment_transaction_type VARCHAR,        -- Plaid type (6-value: buy, sell, cash, fee, transfer, cancel)
    investment_transaction_subtype VARCHAR,     -- Plaid subtype (48-value); preserved to core as provider_subtype
    source_file VARCHAR NOT NULL,               -- Logical identifier: sync_{job_id}
    source_type VARCHAR NOT NULL                -- Always 'plaid' for this table
        DEFAULT 'plaid',
    source_origin VARCHAR NOT NULL,             -- Plaid item_id; part of the PK
    extracted_at TIMESTAMP                      -- When the server fetched this data from Plaid
        DEFAULT CURRENT_TIMESTAMP,
    loaded_at TIMESTAMP                         -- When this record was inserted into the local database
        DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (investment_transaction_id, source_origin)
);
