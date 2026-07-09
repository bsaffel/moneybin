/* Specific-identification lot-selection overrides (investments-data-model.md).
   Core app.* state — cost basis is core, not the us_tax package. Records, for
   a disposal, which lots to draw from and how much from each; unselected
   remainder falls back to FIFO. Written only through LotSelectionsRepo
   (declarative set-for-disposal; empty set clears). */
CREATE TABLE IF NOT EXISTS app.lot_selections (
    investment_transaction_id VARCHAR NOT NULL,         -- FK to the disposal row in core.fct_investment_transactions
    lot_id VARCHAR NOT NULL,                            -- FK to core.fct_investment_lots; the chosen lot (content-hash id, stable across rebuilds)
    quantity DECIMAL(28, 10) NOT NULL,                  -- Units to draw from this lot for this disposal
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, -- When the selection was recorded
    PRIMARY KEY (investment_transaction_id, lot_id)     -- One selection per (disposal, lot) pair
);
