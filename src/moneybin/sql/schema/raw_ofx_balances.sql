-- Raw OFX balances table
-- Stores account balance snapshots from OFX/QFX files
CREATE TABLE IF NOT EXISTS raw.ofx_balances (
    account_id VARCHAR,
    statement_start_date TIMESTAMP,
    statement_end_date TIMESTAMP,
    ledger_balance DECIMAL(18, 2),
    ledger_balance_date TIMESTAMP,
    available_balance DECIMAL(18, 2),
    source_file VARCHAR,
    extracted_at TIMESTAMP,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (account_id, statement_end_date, source_file)
);
