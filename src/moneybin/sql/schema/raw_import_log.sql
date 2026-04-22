/* Audit log of every tabular file import. Each import batch gets a UUID that is
   stamped on every raw row it produces, enabling import history, reverting, and
   diagnostics. */
CREATE TABLE IF NOT EXISTS raw.import_log (
    import_id VARCHAR PRIMARY KEY,              -- UUID generated at the start of each import batch
    source_file VARCHAR NOT NULL,               -- Absolute path to the imported file
    source_type VARCHAR NOT NULL,               -- File format: csv, tsv, excel, parquet, feather, pipe
    source_origin VARCHAR NOT NULL,             -- Format/institution that produced this data
    format_name VARCHAR,                        -- Name of the matched or saved format (NULL if no format matched)
    format_source VARCHAR,                      -- How the format was resolved: "built-in", "saved", "detected", "override"
    account_names JSON NOT NULL,                -- List of account names affected by this import
    status VARCHAR NOT NULL DEFAULT 'importing' CHECK (status IN ('importing', 'complete', 'partial', 'failed', 'reverted')), -- Lifecycle: importing → complete | partial | failed | reverted
    rows_total INTEGER,                         -- Total rows in source file (before filtering)
    rows_imported INTEGER,                      -- Rows successfully written to raw tables
    rows_rejected INTEGER DEFAULT 0,            -- Rows that failed validation (with reasons in rejection_details)
    rows_skipped_trailing INTEGER DEFAULT 0,    -- Trailing junk rows removed by skip patterns
    rejection_details JSON,                     -- Per-rejected-row: [{row_number, reason}]
    detection_confidence VARCHAR,               -- Confidence tier of the column mapping: high, medium, low (NULL if format matched)
    number_format VARCHAR,                      -- Detected number convention: us, european, swiss_french, zero_decimal
    date_format VARCHAR,                        -- Date format string used for parsing
    sign_convention VARCHAR,                    -- Sign convention applied: negative_is_expense, negative_is_income, split_debit_credit
    balance_validated BOOLEAN,                  -- Whether running balance validation passed (NULL if no balance column)
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- When the import batch began
    completed_at TIMESTAMP,                     -- When the import batch finished (NULL if still running or failed)
    reverted_at TIMESTAMP                       -- When the import was reverted (NULL if not reverted)
);
