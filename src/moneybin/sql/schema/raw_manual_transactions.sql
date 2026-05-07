/* Manual transactions entered by the user via CLI or MCP. Mirrors the column
   shape of raw.tabular_transactions; the source-type discriminator is
   'manual' and source-origin is 'user'. One raw.import_log batch is created
   per CLI call or MCP bulk call. */
CREATE TABLE IF NOT EXISTS raw.manual_transactions (
    source_transaction_id VARCHAR PRIMARY KEY,                        -- 'manual_' + truncated UUID4 (12 hex)
    source_type           VARCHAR NOT NULL DEFAULT 'manual',          -- Discriminator; matches matcher and auto-rule exemption predicates
    source_origin         VARCHAR NOT NULL DEFAULT 'user',            -- Origin tag; always 'user' for manual entries
    import_id             VARCHAR NOT NULL,                           -- FK to raw.import_log.import_id; one batch per CLI call or MCP bulk call
    account_id            VARCHAR NOT NULL,                           -- FK to core.dim_accounts
    transaction_date      DATE NOT NULL,                              -- Date of the transaction as the user reports it
    amount                DECIMAL(18, 2) NOT NULL,                    -- Signed; negative = expense, positive = income
    description           VARCHAR NOT NULL,                           -- User-supplied description (free text)
    merchant_name         VARCHAR,                                    -- Optional user-supplied merchant; resolved against app.merchants on next pipeline pass
    memo                  VARCHAR,                                    -- Additional free-text memo
    category              VARCHAR,                                    -- Optional user-supplied category at entry time
    subcategory           VARCHAR,                                    -- Optional user-supplied subcategory
    payment_channel       VARCHAR,                                    -- Optional: in_store, online, other
    transaction_type      VARCHAR,                                    -- Optional source-style type code
    check_number          VARCHAR,                                    -- Optional check number
    currency_code         VARCHAR DEFAULT 'USD',                      -- ISO 4217 currency code
    created_at            TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, -- When the row was inserted
    created_by            VARCHAR NOT NULL                            -- 'cli' or 'mcp'; future-extensible for multi-user identity
);
