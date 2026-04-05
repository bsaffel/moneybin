-- Raw OFX transactions table
-- Stores transaction records from OFX/QFX files
CREATE TABLE IF NOT EXISTS raw.ofx_transactions (
    transaction_id VARCHAR, -- Unique transaction identifier from OFX <FITID> element; part of primary key
    account_id VARCHAR, -- Account this transaction belongs to; foreign key to raw.ofx_accounts; part of primary key
    transaction_type VARCHAR, -- OFX TRNTYPE element, e.g. DEBIT, CREDIT, CHECK, INT, DIV
    date_posted TIMESTAMP, -- OFX DTPOSTED element; mapped to transaction_date in core
    amount DECIMAL(18, 2), -- OFX TRNAMT element; negative = expense, positive = income
    payee VARCHAR, -- OFX NAME element (payee/merchant); mapped to description in core
    memo VARCHAR, -- OFX MEMO element; supplemental transaction notes from the institution
    check_number VARCHAR, -- OFX CHECKNUM element; check number for paper checks; NULL for electronic transactions
    source_file VARCHAR, -- Path to the OFX/QFX file this record was loaded from; part of primary key
    extracted_at TIMESTAMP, -- Timestamp when the OFX file was parsed
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- Timestamp when this record was inserted into the database
    PRIMARY KEY (transaction_id, account_id, source_file)
);
