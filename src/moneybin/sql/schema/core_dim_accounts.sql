-- Core accounts dimension table
-- Canonical deduplicated account records from all data sources
CREATE TABLE IF NOT EXISTS core.dim_accounts (
    account_id VARCHAR PRIMARY KEY,
    routing_number VARCHAR,
    account_type VARCHAR,
    institution_name VARCHAR,
    institution_fid VARCHAR,
    source_system VARCHAR,
    source_file VARCHAR,
    extracted_at TIMESTAMP,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
