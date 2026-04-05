CREATE TABLE IF NOT EXISTS raw.csv_accounts (
    account_id VARCHAR NOT NULL,
    account_type VARCHAR,
    institution_name VARCHAR NOT NULL,
    source_file VARCHAR NOT NULL,
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (account_id, source_file)
);
