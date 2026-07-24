/* Saved local and Google Sheets export targets. All mutations route through ExportDestinationsRepo to emit paired app.audit_log rows per app-integrity-invariant.md. */
CREATE TABLE IF NOT EXISTS app.export_destinations (
    destination_id VARCHAR PRIMARY KEY, -- Opaque truncated UUID (uuid4().hex[:12]) identifying this saved destination
    name VARCHAR NOT NULL UNIQUE, -- User-facing destination name; unique across local and Google Sheets kinds
    kind VARCHAR NOT NULL CHECK (kind IN ('local', 'sheets')), -- Destination type: 'local' for a visible directory or 'sheets' for a workbook
    local_path VARCHAR, -- Visible local export directory; required only when kind='local'
    spreadsheet_id VARCHAR, -- Google Sheets workbook id; required only when kind='sheets'
    managed_tab_prefix VARCHAR, -- Prefix for tabs managed by MoneyBin in this workbook; required only for Sheets
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, -- Destination configuration creation timestamp
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, -- Last destination-configuration mutation timestamp
    UNIQUE (spreadsheet_id, managed_tab_prefix), -- A Sheets tab namespace belongs to only one destination; NULL local fields remain distinct
    CHECK (
        (kind = 'local' AND local_path IS NOT NULL
         AND spreadsheet_id IS NULL AND managed_tab_prefix IS NULL)
        OR
        (kind = 'sheets' AND local_path IS NULL
         AND spreadsheet_id IS NOT NULL AND managed_tab_prefix IS NOT NULL)
    )
);
