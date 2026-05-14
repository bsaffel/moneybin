/* Bank accounts connected via Plaid; one record per account per institution */
CREATE TABLE IF NOT EXISTS raw.plaid_accounts (
    account_id VARCHAR NOT NULL,        -- Plaid account_id; globally unique per Plaid
    account_type VARCHAR,               -- depository, credit, loan, investment, other
    account_subtype VARCHAR,            -- checking, savings, credit card, etc.
    institution_name VARCHAR,           -- Human-readable name from Plaid
    official_name VARCHAR,              -- Official account name from the institution
    mask VARCHAR,                       -- Last 4 digits of the account number
    source_file VARCHAR NOT NULL,       -- Logical identifier: sync_{job_id} (last sync to touch this row)
    source_type VARCHAR NOT NULL DEFAULT 'plaid',
    source_origin VARCHAR NOT NULL,     -- provider_item_id; scopes dedup to the institution connection
    extracted_at TIMESTAMP,             -- From metadata.synced_at
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (account_id, source_origin)
);
