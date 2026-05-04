-- Create app.account_settings for the v2 accounts namespace.
-- Idempotent: matches the schema file definition; init_schemas creates this
-- on fresh installs, so this migration only fires for upgrades.

CREATE TABLE IF NOT EXISTS app.account_settings (
    account_id           VARCHAR NOT NULL PRIMARY KEY,
    display_name         VARCHAR,
    official_name        VARCHAR,
    last_four            VARCHAR,
    account_subtype      VARCHAR,
    holder_category      VARCHAR,
    iso_currency_code    VARCHAR,
    credit_limit         DECIMAL(18, 2),
    archived             BOOLEAN NOT NULL DEFAULT FALSE,
    include_in_net_worth BOOLEAN NOT NULL DEFAULT TRUE,
    updated_at           TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
