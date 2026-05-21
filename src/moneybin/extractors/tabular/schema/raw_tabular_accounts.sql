/* Accounts discovered during tabular file imports. For single-account files, one
   record is created from the --account-name flag. For multi-account files (Tiller,
   Mint), one record per unique account found in the data. Account numbers are stored
   here (not per-transaction) and masked at the application layer for display. */
CREATE TABLE IF NOT EXISTS raw.tabular_accounts (
    account_id VARCHAR NOT NULL,                -- Source-system account identifier
    account_name VARCHAR NOT NULL,              -- Human-readable account label provided by user or extracted from multi-account file
    account_number VARCHAR,                     -- Full account number if available in source; stored encrypted at rest, masked at application layer for all output
    account_number_masked VARCHAR,              -- Last 4 digits for display (e.g. "...4521"); derived from account_number or extracted directly if source only provides masked
    account_type VARCHAR,                       -- Account type if known (e.g. checking, savings, credit, brokerage, investment)
    institution_name VARCHAR,                   -- Financial institution name from format metadata, source file content, or user input
    currency VARCHAR,                           -- Default currency for this account if known (ISO 4217 code)
    source_file VARCHAR NOT NULL,               -- Absolute path to the imported file that created or updated this account record
    source_type VARCHAR NOT NULL,               -- Import pathway that produced this record: csv, tsv, excel, parquet, feather, pipe
    source_origin VARCHAR NOT NULL,             -- Institution/connection/format that produced this data; matches the format name for tabular imports
    import_id VARCHAR NOT NULL,                 -- UUID linking this row to its import batch in raw.import_log; enables import reverting and history
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- Timestamp when the extraction pipeline processed this record
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,    -- Timestamp when this record was written to the raw table
    PRIMARY KEY (account_id, source_file)
);
