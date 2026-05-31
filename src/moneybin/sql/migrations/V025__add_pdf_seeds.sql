/* Row-level storage for the PDF seed (catch-all) path. JSON column holds the
   extracted row verbatim; per-alias auto-generated views in raw.pdf_<alias>
   project JSON paths into typed columns for ergonomic SQL. Identity is a
   content hash so re-importing the same statement is a no-op (idempotent). */
CREATE TABLE IF NOT EXISTS raw.pdf_seeds (
    alias VARCHAR NOT NULL,        -- Logical seed source; becomes view name raw.pdf_<alias>
    row_hash VARCHAR NOT NULL,     -- Content hash of the row (pdf_ prefix); stable identity for dedup
    data JSON NOT NULL,            -- Extracted row as a JSON object: field-name -> value
    source_file VARCHAR NOT NULL,  -- Original filename (basename only, no path)
    page INTEGER,                  -- Source page number (informational)
    import_id VARCHAR NOT NULL,    -- Import that wrote this row (FK to raw.import_log; reversibility)
    loaded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, -- First observed; does not change on re-import
    PRIMARY KEY (alias, row_hash)
);
