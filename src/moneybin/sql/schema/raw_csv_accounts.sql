/* Account metadata from CSV imports; account_id, account_type, and institution_name are caller-supplied since CSV files lack account context */
CREATE TABLE IF NOT EXISTS raw.csv_accounts (
    account_id VARCHAR NOT NULL, -- Caller-supplied identifier; not present in the CSV file itself; part of primary key
    account_type VARCHAR, -- Account classification, e.g. CHECKING, SAVINGS; caller-supplied alongside account_id
    institution_name VARCHAR NOT NULL, -- Human-readable name of the financial institution; caller-supplied
    source_file VARCHAR NOT NULL, -- Path to the CSV file this record was loaded from; part of primary key
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- Timestamp when the CSV file was parsed
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- Timestamp when this record was inserted into the database
    PRIMARY KEY (account_id, source_file)
);
