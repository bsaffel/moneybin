/* Receipt that an item's holdings were FETCHED, one row per (item, pull) — written even when the item returns ZERO positions.
   raw.plaid_investment_holdings records holding ROWS, so it cannot distinguish "this item reported nothing (every position sold)"
   from "this item never reported / the pull didn't cover it": an item whose pull returns an empty holdings array writes no rows at
   all, and a newest-snapshot join keyed on those rows silently keeps the last NON-EMPTY snapshot from an earlier pull. That reads
   the largest possible net-worth overstatement — a fully-liquidated broker — as "still holding the old positions." This table is
   the missing evidence: consumers derive "the newest snapshot for this item" from HERE (core.dim_holdings, system doctor), so an
   item that reported nothing produces an empty newest snapshot rather than a stale one, and an item that never reported produces
   no snapshot at all. Invariant: every (source_origin, source_file) in raw.plaid_investment_holdings has a row here.
   Idempotent — re-loading the same job replaces its own row. */
CREATE TABLE IF NOT EXISTS raw.plaid_investment_holdings_snapshots (
    source_origin VARCHAR NOT NULL,           -- Plaid item_id; the item that reported (part of the PK)
    source_file VARCHAR NOT NULL,             -- Logical identifier: sync_{job_id}; the SNAPSHOT identity (part of the PK), same as the holdings rows it accounts for
    holdings_date DATE,                       -- Snapshot calendar date = extracted_at::DATE (UTC); same derivation as raw.plaid_investment_holdings.holdings_date
    holdings_count INTEGER NOT NULL,          -- Positions this item returned in this snapshot; 0 = the item reported and holds NOTHING (the case this table exists to record)
    source_type VARCHAR NOT NULL              -- Always 'plaid' for this table
        DEFAULT 'plaid',
    extracted_at TIMESTAMP                    -- When the server fetched this snapshot from Plaid; orders snapshots for the newest-snapshot join
        DEFAULT CURRENT_TIMESTAMP,
    loaded_at TIMESTAMP                       -- When this record was inserted into the local database
        DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (source_origin, source_file)
);
