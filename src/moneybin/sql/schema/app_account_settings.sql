/* Per-account user-controlled settings: Plaid-parity metadata + lifecycle flags.
   One row per account_id; absence means all defaults.
   Joined by core.dim_accounts to surface as the canonical resolved view. */
CREATE TABLE IF NOT EXISTS app.account_settings (
    account_id           VARCHAR NOT NULL PRIMARY KEY,            -- Foreign key to core.dim_accounts.account_id
    display_name         VARCHAR,                                  -- User-supplied label override; NULL falls back to derived default
    official_name        VARCHAR,                                  -- Institution's formal account name (mirrors Plaid official_name); free text
    last_four            VARCHAR,                                  -- Last 4 digits of account number (mirrors Plaid mask); validated ^[0-9]{4}$ at service boundary
    account_subtype      VARCHAR,                                  -- Plaid-style subtype (checking, savings, credit card, mortgage, ...); open vocabulary
    holder_category      VARCHAR,                                  -- 'personal' / 'business' / 'joint'; open vocabulary
    iso_currency_code    VARCHAR,                                  -- ISO-4217 (USD, EUR, ...); NULL defaults to USD until multi-currency.md ships
    credit_limit         DECIMAL(18, 2),                           -- User-asserted credit limit on credit cards / lines (drives utilization metrics)
    archived             BOOLEAN NOT NULL DEFAULT FALSE,           -- Hides account from default list and from reports.net_worth
    include_in_net_worth BOOLEAN NOT NULL DEFAULT TRUE,            -- Whether this account contributes to reports.net_worth (independent toggle, but archive cascades to FALSE)
    updated_at           TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP -- Last modification time
);
