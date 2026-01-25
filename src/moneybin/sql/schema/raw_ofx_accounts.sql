-- Raw OFX accounts table
-- Stores account details from OFX/QFX files
CREATE TABLE IF NOT EXISTS raw.ofx_accounts (
    account_id VARCHAR,
    routing_number VARCHAR,
    account_type VARCHAR,
    institution_org VARCHAR,
    institution_fid VARCHAR,
    source_file VARCHAR,
    extracted_at TIMESTAMP,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (account_id, source_file, extracted_at)
);
