/* Account balance snapshots from Plaid; one record per account per balance date per institution */
CREATE TABLE IF NOT EXISTS raw.plaid_balances (
    account_id VARCHAR NOT NULL,        -- Plaid account_id
    balance_date DATE NOT NULL,         -- Date the balance was reported
    current_balance DECIMAL(18, 2),     -- Current balance including pending transactions
    available_balance DECIMAL(18, 2),   -- Available balance; NULL for credit accounts
    balance_limit DECIMAL(18, 2),       -- Plaid limit: credit limit, or overdraft limit on depository; distinct from the user-asserted app.account_settings.credit_limit
    margin_loan_amount DECIMAL(18, 2),  -- Borrowed funds on a margin account; investment accounts only, NULL elsewhere. current_balance is the gross value of assets, so core.fct_balances subtracts this
    iso_currency_code VARCHAR,          -- ISO 4217; mutually exclusive with unofficial_currency_code
    unofficial_currency_code VARCHAR,   -- Non-ISO (crypto) currency; core COALESCEs the pair
    source_file VARCHAR NOT NULL,
    source_type VARCHAR NOT NULL DEFAULT 'plaid',
    source_origin VARCHAR NOT NULL,
    last_updated_datetime TIMESTAMP,    -- Provider "as-of" time for the balance; populated only by some institutions
    extracted_at TIMESTAMP,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (account_id, balance_date, source_origin)
);
