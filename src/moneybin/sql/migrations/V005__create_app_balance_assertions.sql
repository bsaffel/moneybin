-- Create app.balance_assertions for net-worth user-asserted balance anchors.
-- Idempotent: matches the schema file definition.

CREATE TABLE IF NOT EXISTS app.balance_assertions (
    account_id     VARCHAR NOT NULL,
    assertion_date DATE NOT NULL,
    balance        DECIMAL(18, 2) NOT NULL,
    notes          VARCHAR,
    created_at     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (account_id, assertion_date)
);
