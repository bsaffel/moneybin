-- Core accounts dimension table
-- Canonical deduplicated account records from all data sources
-- Schema mirrors the output of dbt/models/core/dim_accounts.sql
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
    dbt_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
