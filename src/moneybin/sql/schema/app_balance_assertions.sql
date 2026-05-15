/* User-entered balance anchors for accounts; primary observation source alongside
   OFX statement balances and tabular running balances. Composite PK enforces
   one assertion per account per date. */
CREATE TABLE IF NOT EXISTS app.balance_assertions (
    account_id     VARCHAR NOT NULL,                                  -- Foreign key to core.dim_accounts.account_id
    assertion_date DATE NOT NULL,                                     -- Date the balance was observed
    balance        DECIMAL(18, 2) NOT NULL,                           -- Asserted balance amount
    notes          VARCHAR,                                            -- Optional user notes (e.g., "from paper statement")
    created_at     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,       -- When the assertion was first entered
    updated_at     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,       -- When the assertion was last edited; refreshed by BalanceService.assert_balance on conflict
    PRIMARY KEY (account_id, assertion_date)
);
