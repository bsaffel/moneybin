/* Broker-reported holdings, one row per (position, snapshot) — store-don't-trust reconciliation reference, never authoritative.
   Each pull is a full snapshot keyed by source_file (the snapshot identity), NOT by date: two pulls on the same UTC day
   are distinct snapshots, so a position dropped from the newer full snapshot is correctly absent from it (not masked by a
   same-day survivor). Idempotent — re-loading the same job replaces its own rows. */
CREATE TABLE IF NOT EXISTS raw.plaid_investment_holdings (
    account_id VARCHAR NOT NULL,              -- Plaid account_id
    security_id VARCHAR NOT NULL,             -- Plaid security_id
    holdings_date DATE,                       -- Snapshot calendar date = extracted_at::DATE; informational (Plaid holdings carry no as-of date of their own)
    institution_price DECIMAL(28, 10),        -- Broker-reported price
    institution_price_as_of DATE,             -- Date of institution_price
    institution_value DECIMAL(18, 2),         -- Broker-reported market value
    cost_basis DECIMAL(18, 2),                -- Broker-reported cost basis; reconciliation reference ONLY — never overwrites ledger-derived basis
    quantity DECIMAL(28, 10),                 -- Broker-reported open quantity
    iso_currency_code VARCHAR,                -- ISO 4217; mutually exclusive with unofficial_currency_code
    unofficial_currency_code VARCHAR,         -- Non-ISO (crypto) currency
    vested_quantity DECIMAL(28, 10),          -- Vested units (equity compensation); NULL otherwise
    vested_value DECIMAL(18, 2),              -- Vested value (equity compensation); NULL otherwise
    transactions_window_start DATE NOT NULL,  -- Per-item metadata.transactions_window_start; this item's /investments/transactions/get start boundary; opening-lot bootstrap's pre-window/in-window discriminant; constant per source_origin within a snapshot
    source_file VARCHAR NOT NULL,             -- Logical identifier: sync_{job_id}; the SNAPSHOT identity (part of the PK)
    source_type VARCHAR NOT NULL              -- Always 'plaid' for this table
        DEFAULT 'plaid',
    source_origin VARCHAR NOT NULL,           -- Plaid item_id; scopes the snapshot to the institution connection; part of the PK
    extracted_at TIMESTAMP                    -- When the server fetched this snapshot from Plaid; orders snapshots for the newest-snapshot join
        DEFAULT CURRENT_TIMESTAMP,
    loaded_at TIMESTAMP                       -- When this record was inserted into the local database
        DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (account_id, security_id, source_origin, source_file)
);
