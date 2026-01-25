-- Raw OFX transactions table
-- Stores transaction records from OFX/QFX files
CREATE TABLE IF NOT EXISTS raw.ofx_transactions (
    transaction_id VARCHAR,
    account_id VARCHAR,
    transaction_type VARCHAR,
    date_posted TIMESTAMP,
    amount DECIMAL(18, 2),
    payee VARCHAR,
    memo VARCHAR,
    check_number VARCHAR,
    source_file VARCHAR,
    extracted_at TIMESTAMP,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (transaction_id, account_id, source_file)
);
