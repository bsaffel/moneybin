-- Raw OFX accounts table
-- Stores account details from OFX/QFX files
CREATE TABLE IF NOT EXISTS raw.ofx_accounts (
    account_id VARCHAR, -- Unique account identifier from OFX <ACCTID> element; part of primary key
    routing_number VARCHAR, -- ABA bank routing number from OFX <BANKID> element; NULL when not present in file
    account_type VARCHAR, -- Account classification from OFX <ACCTTYPE> element, e.g. CHECKING, SAVINGS, CREDITLINE
    institution_org VARCHAR, -- Financial institution name from OFX <ORG> element; mapped to institution_name in core
    institution_fid VARCHAR, -- Financial institution identifier from OFX <FID> element; used in OFX routing
    source_file VARCHAR, -- Path to the OFX/QFX file this record was loaded from; part of primary key
    extracted_at TIMESTAMP, -- Timestamp when the OFX file was parsed; part of primary key to allow re-extraction
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- Timestamp when this record was inserted into the database
    PRIMARY KEY (account_id, source_file, extracted_at)
);
