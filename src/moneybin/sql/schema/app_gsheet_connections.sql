/* One row per connected (Google Sheets tab, adapter). Pre-refresh pull replays the sheet's current state through this connection's pinned mapping. All mutations route through GSheetConnectionsRepo to emit paired app.audit_log rows per app-integrity-invariant.md. */
CREATE TABLE IF NOT EXISTS app.gsheet_connections (
    connection_id VARCHAR PRIMARY KEY, -- Truncated UUID (uuid4().hex[:12]) per identifiers.md strategy 3; stable across renames
    spreadsheet_id VARCHAR NOT NULL, -- Google Sheets workbook id (from URL); shared across tabs in the same workbook
    sheet_gid INTEGER NOT NULL, -- Numeric tab id (URL fragment gid=); stable across sheet renames
    sheet_name VARCHAR NOT NULL, -- Tab name at last successful pull; informational only — may drift, not used for joins
    workbook_name VARCHAR NOT NULL, -- Workbook title at last successful pull; informational
    adapter VARCHAR NOT NULL CHECK (adapter IN ('transactions', 'seed')), -- Adapter target: 'transactions' (Tiller-style ledger → raw.tabular_transactions) or 'seed' (catch-all → raw.gsheet_seeds). v1 values; future values add as new adapters ship.
    account_id VARCHAR, -- FK to dim_accounts (transactions adapter only); NULL for seed adapter (multi-account by design)
    account_name VARCHAR, -- As provided via --account-name at connect time; denormalized for display
    column_mapping JSON NOT NULL, -- {source_header: dest_field} pinned at connect/reconnect time; the contract for subsequent pulls
    header_signature JSON NOT NULL, -- Ordered list of source headers at connect/reconnect time; drift detection baseline
    date_format VARCHAR, -- Pinned strftime format (e.g. '%m/%d/%Y'); NULL means auto-detect on each pull
    sign_convention VARCHAR, -- One of: negative_is_expense, negative_is_income, split_debit_credit, all_positive; NULL = auto
    number_format VARCHAR, -- One of: us, european, swiss, zero_decimal; NULL = auto
    skip_rows INTEGER NOT NULL DEFAULT 0, -- Rows to skip before the header row (Tiller sheets often have a banner row)
    skip_trailing_patterns JSON, -- Optional list of regex strings flagging trailing junk rows; NULL = use default patterns, [] = none, ['^Total'] = custom
    status VARCHAR NOT NULL DEFAULT 'healthy' CHECK (status IN ('healthy', 'auth_expired', 'unreachable', 'drift_detected', 'rate_limited', 'failed', 'disconnected')), -- Connection health; drives pre-refresh hook decisions and the system_status gsheet block
    last_pull_at TIMESTAMP, -- Timestamp of most recent pull attempt (success or failure)
    last_pull_import_id VARCHAR, -- FK to raw.import_log.import_id for the most recent attempt; NULL before first pull
    last_success_at TIMESTAMP, -- Timestamp of most recent pull that ingested cleanly; NULL before first successful pull
    last_drift_reason TEXT, -- Human-readable explanation populated when status='drift_detected'
    consecutive_failure_count INTEGER NOT NULL DEFAULT 0, -- Number of consecutive failed pulls since the last success; resets to 0 on success
    alias VARCHAR, -- User-supplied slug; required for adapter='seed' (becomes view name raw.gsheet_<alias>); NULL for adapter='transactions'. Globally unique across all connections.
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, -- Connection-row creation timestamp
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, -- Last mutation timestamp (refreshed by GSheetConnectionsRepo on every audited write)
    UNIQUE (spreadsheet_id, sheet_gid),
    UNIQUE (alias)
);
