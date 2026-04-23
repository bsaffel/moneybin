/* Imported financial transactions from tabular file sources (CSV, TSV, Excel,
   Parquet, Feather). Each row represents a single transaction as extracted from the
   source file with minimal transformation — amounts are sign-normalized but all
   original values are preserved for audit. */
CREATE TABLE IF NOT EXISTS raw.tabular_transactions (
    transaction_id VARCHAR NOT NULL,            -- Deterministic identifier: source_transaction_id when available, else SHA-256 hash of date|amount|description|account_id|row_number
    account_id VARCHAR NOT NULL,                -- Source-system account identifier; for multi-account files extracted from per-row account column, for single-account files provided or generated
    transaction_date DATE NOT NULL,             -- Primary transaction date parsed from source using detected or specified date format
    post_date DATE,                             -- Settlement or posting date when distinct from transaction date; NULL if source provides only one date
    amount DECIMAL(18, 2) NOT NULL,             -- Normalized amount: negative = expense, positive = income regardless of source sign convention
    original_amount VARCHAR,                    -- Raw amount string exactly as it appeared in the source file before sign normalization and parsing
    original_date_str VARCHAR,                  -- Raw date string exactly as it appeared in the source file before format parsing
    description VARCHAR,                        -- Primary transaction description, payee, or merchant name from source
    memo VARCHAR,                               -- Supplementary transaction details, extended description, or notes from source
    category VARCHAR,                           -- Source-provided transaction category if present; preserved as-is for migration bootstrap, not MoneyBin categorization
    subcategory VARCHAR,                        -- Source-provided transaction subcategory if present; preserved as-is
    transaction_type VARCHAR,                   -- Source-provided transaction type (e.g. Sale, Return, Payment, Dividend, Fee, Transfer)
    status VARCHAR,                             -- Source-provided transaction status (e.g. Cleared, Pending, Posted, Reconciled)
    check_number VARCHAR,                       -- Check or cheque number for check-based transactions
    source_transaction_id VARCHAR,              -- Institution-assigned unique transaction identifier if present; strongest dedup signal for same-source re-imports
    reference_number VARCHAR,                   -- Institution-assigned reference, confirmation, or receipt number; not guaranteed unique across transactions
    balance DECIMAL(18, 2),                     -- Running account balance after this transaction if provided by source
    currency VARCHAR,                           -- ISO 4217 currency code if present in source (e.g. USD, EUR); captured now, multi-currency processing deferred
    member_name VARCHAR,                        -- Account holder, cardholder, or member name if present in source
    source_file VARCHAR NOT NULL,               -- Absolute path to the imported file at time of extraction
    source_type VARCHAR NOT NULL,               -- Import pathway that produced this record: csv, tsv, excel, parquet, feather, pipe
    source_origin VARCHAR NOT NULL,             -- Institution/connection/format that produced this data (e.g. "chase_credit", "tiller", Plaid item_id); scopes Tier 2b dedup
    import_id VARCHAR NOT NULL,                 -- UUID linking this row to its import batch in raw.import_log; enables import reverting and history
    row_number INTEGER,                         -- 1-based row/line number in the source file; invaluable for debugging import issues and deterministic hash generation
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- Timestamp when the extraction pipeline processed this record
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,    -- Timestamp when this record was written to the raw table
    PRIMARY KEY (transaction_id, account_id, source_file)
);
