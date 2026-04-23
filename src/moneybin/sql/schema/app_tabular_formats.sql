/* Saved column mappings for known tabular file formats. Built-in formats (Chase,
   Citi, Tiller, Mint, YNAB) are seeded from YAML files on db init. User formats
   are auto-saved after successful heuristic detection or created via --override +
   --save-format. User formats override built-ins of the same name. */
CREATE TABLE IF NOT EXISTS app.tabular_formats (
    name VARCHAR PRIMARY KEY,                   -- Machine identifier for this format (e.g. "chase_credit", "tiller", "mint")
    institution_name VARCHAR NOT NULL,          -- Human-readable institution or tool name (e.g. "Chase", "Tiller", "Mint")
    file_type VARCHAR NOT NULL DEFAULT 'auto',  -- Expected file type: csv, tsv, xlsx, parquet, feather, pipe, or "auto" for any type
    delimiter VARCHAR,                          -- Explicit delimiter character for text formats; NULL means auto-detected at import time
    encoding VARCHAR NOT NULL DEFAULT 'utf-8',  -- Character encoding for text formats (e.g. utf-8, latin-1, windows-1252)
    skip_rows INTEGER NOT NULL DEFAULT 0,       -- Number of non-data rows to skip before the header row in the source file
    sheet VARCHAR,                              -- Excel sheet name to read; NULL means auto-select the sheet with the most data rows
    header_signature JSON NOT NULL,             -- Ordered list of column names that uniquely fingerprint this format for auto-detection (case-insensitive subset matching)
    field_mapping JSON NOT NULL,                -- Mapping of destination field names to source column names
    sign_convention VARCHAR NOT NULL,           -- How amounts are represented in the source: negative_is_expense, negative_is_income, split_debit_credit
    date_format VARCHAR NOT NULL,               -- strftime format string for parsing date values (e.g. "%m/%d/%Y", "%Y-%m-%d")
    number_format VARCHAR NOT NULL DEFAULT 'us', -- Number convention: us (1,234.56), european (1.234,56), swiss_french (1 234,56), zero_decimal (1,234)
    skip_trailing_patterns JSON,                -- Regex patterns for trailing non-data rows: NULL = use default patterns, [] = no patterns, ["^Total"] = custom
    multi_account BOOLEAN NOT NULL DEFAULT FALSE, -- Whether this format expects per-row account identification (Tiller, Mint, Monarch)
    source VARCHAR NOT NULL DEFAULT 'detected', -- How this format was created: "detected", "manual", "built-in-override"
    times_used INTEGER NOT NULL DEFAULT 0,      -- Number of successful imports completed using this format
    last_used_at TIMESTAMP,                     -- Timestamp of the most recent successful import using this format
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- Timestamp when this format was first created
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP  -- Timestamp when this format was last modified
);
