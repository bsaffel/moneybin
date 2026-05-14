/* Account balance snapshots from Plaid; one record per account per balance date per institution */
CREATE TABLE IF NOT EXISTS raw.plaid_balances (
    account_id VARCHAR NOT NULL,        -- Plaid account_id
    balance_date DATE NOT NULL,         -- Date the balance was reported
    current_balance DECIMAL(18, 2),     -- Current balance including pending transactions
    available_balance DECIMAL(18, 2),   -- Available balance; NULL for credit accounts
    source_file VARCHAR NOT NULL,
    source_type VARCHAR NOT NULL DEFAULT 'plaid',
    source_origin VARCHAR NOT NULL,
    extracted_at TIMESTAMP,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (account_id, balance_date, source_origin)
);
