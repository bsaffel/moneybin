/* Per-lot detail within a Plaid holdings snapshot (Holding.tax_lots[], HoldingTaxLot); basis/acquisition-date source for opening-lot bootstrap. One row per broker lot per snapshot. */
CREATE TABLE IF NOT EXISTS raw.plaid_investment_holding_lots (
    account_id VARCHAR NOT NULL,              -- Plaid account_id
    security_id VARCHAR NOT NULL,             -- Plaid security_id
    lot_index INTEGER NOT NULL,               -- Loader-assigned position within Holding.tax_lots[]; PK disambiguator (institution_lot_id is nullable)
    institution_lot_id VARCHAR,               -- Broker's lot identifier; NULL where the institution does not provide one
    original_purchase_datetime TIMESTAMP,     -- HoldingTaxLot.original_purchase_datetime; acquisition timestamp, NULL where absent
    quantity DECIMAL(28, 10),                 -- Units in this lot
    purchase_price DECIMAL(28, 10),           -- Per-unit acquisition price for this lot (matches the schema-wide price precision)
    cost_basis DECIMAL(18, 2),                -- Broker-reported total cost basis of this lot (Plaid documents it fee-inclusive)
    current_value DECIMAL(18, 2),             -- Broker-reported current market value of this lot
    position_type VARCHAR,                    -- HoldingTaxLotPositionType (e.g. 'long' / 'short')
    source_file VARCHAR NOT NULL,             -- Snapshot identity (part of the PK), same as the parent holdings table
    source_type VARCHAR NOT NULL DEFAULT 'plaid',
    source_origin VARCHAR NOT NULL,           -- Plaid item_id; part of the PK
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (account_id, security_id, source_origin, lot_index, source_file)
);
