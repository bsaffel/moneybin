/* Manual investment events entered by the user via CLI or MCP. Mirrors
   raw.manual_transactions (per-provider raw pattern); importer children add
   their own provider-shaped raw tables. Account and security are resolved
   interactively at entry time, so this table carries resolved IDs plus the
   user's original security reference for audit. One raw.import_log batch is
   created per CLI call or MCP bulk call. */
CREATE TABLE IF NOT EXISTS raw.manual_investment_transactions (
    source_transaction_id VARCHAR PRIMARY KEY,          -- Truncated UUID4 (12 hex), prefixed with 'manual_' for source-clarity in joins
    source_type VARCHAR NOT NULL DEFAULT 'manual',      -- Discriminator; constant for this table
    source_origin VARCHAR NOT NULL DEFAULT 'user',      -- Origin tag; always 'user' for manual entries
    import_id VARCHAR NOT NULL,                         -- FK to raw.import_log.import_id; one batch per CLI call or MCP bulk call
    account_id VARCHAR NOT NULL,                        -- FK to core.dim_accounts; resolved at entry
    security_id VARCHAR,                                -- FK to app.securities; resolved at entry; NULL for cash-only events
    security_ref VARCHAR,                               -- User-supplied security reference as typed (audit trail for the resolution)
    type VARCHAR NOT NULL,                              -- Core taxonomy value (CLI/MCP validate at entry; manual rows arrive canonical)
    subtype VARCHAR,                                    -- Per-type refinement (tax character, reinvest funding source)
    event_group_id VARCHAR,                             -- Links legs of one economic event (reinvest pair, merger legs)
    trade_date DATE NOT NULL,                           -- Trade date (drives holding period); NOT settlement date
    settlement_date DATE,                               -- Settlement date if supplied; informational
    original_acquisition_date DATE,                     -- For transfer_in: shares' original acquisition date (holding period transfers in); NULL otherwise
    quantity DECIMAL(28, 10),                           -- Units (high precision for fractional shares / crypto); signed per spec Requirement 6
    price DECIMAL(28, 10),                              -- Per-unit price; NULL for non-priced events
    amount DECIMAL(18, 2),                              -- Cash effect; signed per spec Requirement 6
    fees DECIMAL(18, 2),                                -- Commissions/fees component; folded into cost basis
    currency_code VARCHAR DEFAULT 'USD',                -- Denominating currency as supplied
    description VARCHAR,                                -- Free-text description
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, -- When the row was inserted
    created_by VARCHAR NOT NULL,                        -- 'cli' or 'mcp'; future-extensible for multi-user identity
    investment_transaction_id VARCHAR                   -- Predicted gold-key (content hash, per raw.manual_transactions.transaction_id precedent); populated at INSERT
);
