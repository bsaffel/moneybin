/* Account balance snapshots from OFX/QFX files; one record per account per statement period per source file */
CREATE TABLE IF NOT EXISTS raw.ofx_balances (
    account_id VARCHAR, -- Account this balance snapshot belongs to; foreign key to raw.ofx_accounts; part of primary key
    statement_start_date TIMESTAMP, -- Start of the statement period covered by this OFX file
    statement_end_date TIMESTAMP, -- End of the statement period covered by this OFX file; part of primary key
    ledger_balance DECIMAL(18, 2), -- OFX LEDGERBAL: settled balance including pending items
    ledger_balance_date TIMESTAMP, -- OFX DTASOF for the ledger balance snapshot
    available_balance DECIMAL(18, 2), -- OFX AVAILBAL: funds available for withdrawal; NULL when not provided
    source_file VARCHAR, -- Path to the OFX/QFX file this record was loaded from; part of primary key
    extracted_at TIMESTAMP, -- Timestamp when the OFX file was parsed
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- Timestamp when this record was inserted into the database
    import_id VARCHAR, -- UUID of the import batch this row belongs to; NULL for rows imported before V003
    source_type VARCHAR DEFAULT 'ofx', -- Format taxonomy marker; always 'ofx' for OFX/QFX/QBO files
    PRIMARY KEY (account_id, statement_end_date, source_file)
);
