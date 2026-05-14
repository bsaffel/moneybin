/* Transactions fetched from Plaid /transactions/sync; one record per transaction per institution */
CREATE TABLE IF NOT EXISTS raw.plaid_transactions (
    transaction_id VARCHAR NOT NULL,    -- Plaid transaction_id; globally unique per Plaid
    account_id VARCHAR NOT NULL,        -- Plaid account_id; foreign key to raw.plaid_accounts
    transaction_date DATE NOT NULL,     -- Date the transaction posted; from Plaid date field
    amount DECIMAL(18, 2) NOT NULL,     -- CAUTION: Plaid convention is positive = expense; sign flip is in stg_plaid__transactions
    description VARCHAR,                -- Plaid name field
    merchant_name VARCHAR,              -- Plaid merchant_name; NULL when Plaid cannot identify
    category VARCHAR,                   -- Plaid personal_finance_category.primary
    pending BOOLEAN DEFAULT FALSE,
    source_file VARCHAR NOT NULL,       -- Logical identifier: sync_{job_id} (last sync to touch this row)
    source_type VARCHAR NOT NULL DEFAULT 'plaid',
    source_origin VARCHAR NOT NULL,     -- provider_item_id; scopes dedup to the institution connection
    extracted_at TIMESTAMP,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (transaction_id, source_origin)
);
