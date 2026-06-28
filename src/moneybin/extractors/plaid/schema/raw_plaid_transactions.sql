/* Transactions fetched from Plaid /transactions/sync; one record per transaction per institution */
CREATE TABLE IF NOT EXISTS raw.plaid_transactions (
    transaction_id VARCHAR NOT NULL,    -- Plaid transaction_id; globally unique per Plaid
    account_id VARCHAR NOT NULL,        -- Plaid account_id; foreign key to raw.plaid_accounts
    transaction_date DATE NOT NULL,     -- Date the transaction posted; from Plaid date field
    amount DECIMAL(18, 2) NOT NULL,     -- CAUTION: Plaid convention is positive = expense; sign flip is in stg_plaid__transactions
    description VARCHAR,                -- Plaid name field
    merchant_name VARCHAR,              -- Plaid merchant_name; NULL when Plaid cannot identify
    category VARCHAR,                   -- Plaid personal_finance_category.primary
    original_description VARCHAR,       -- Plaid original_description; raw bank text, distinct from description=name; NULL for non-Plaid
    iso_currency_code VARCHAR,          -- Plaid iso_currency_code (ISO 4217)
    authorized_date DATE,               -- Plaid authorized_date
    pending_transaction_id VARCHAR,     -- Plaid pending_transaction_id; links pending -> posted
    payment_channel VARCHAR,            -- Plaid payment_channel: online, in store, other
    check_number VARCHAR,               -- Plaid check_number; NULL for non-check
    merchant_entity_id VARCHAR,         -- Plaid merchant_entity_id; stable merchant id (Tier-2a, not yet wired to core)
    location_address VARCHAR,           -- Plaid location.address
    location_city VARCHAR,              -- Plaid location.city
    location_region VARCHAR,            -- Plaid location.region
    location_postal_code VARCHAR,       -- Plaid location.postal_code
    location_country VARCHAR,           -- Plaid location.country
    location_latitude DOUBLE,           -- Plaid location.lat
    location_longitude DOUBLE,          -- Plaid location.lon
    category_detailed VARCHAR,          -- Plaid personal_finance_category.detailed (Tier-2b)
    category_confidence VARCHAR,        -- Plaid personal_finance_category.confidence_level (Tier-2b)
    pending BOOLEAN DEFAULT FALSE,
    source_file VARCHAR NOT NULL,       -- Logical identifier: sync_{job_id} (last sync to touch this row)
    source_type VARCHAR NOT NULL DEFAULT 'plaid',
    source_origin VARCHAR NOT NULL,     -- provider_item_id; scopes dedup to the institution connection
    extracted_at TIMESTAMP,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (transaction_id, source_origin)
);
