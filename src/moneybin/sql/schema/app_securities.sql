/* Manually-maintained security catalog (investments-data-model.md). Keyed on
   a stable surrogate security_id; ticker/CUSIP/ISIN/FIGI are attributes,
   never the key. Managed via CLI/MCP (investments securities add/set)
   through SecuritiesRepo only. */
CREATE TABLE IF NOT EXISTS app.securities (
    security_id VARCHAR NOT NULL PRIMARY KEY,           -- Stable surrogate (truncated UUID4, 12 hex); never derived from ticker
    name VARCHAR NOT NULL,                              -- Human-readable label ("Apple Inc.", "Bitcoin")
    security_type VARCHAR NOT NULL CHECK (security_type IN ('equity', 'etf', 'mutual_fund', 'bond', 'crypto', 'cash', 'other')), -- Instrument classification; 'cash' = money-market/sweep positions
    ticker VARCHAR,                                     -- Exchange ticker ("AAPL"); nullable, not unique (tickers get reused)
    exchange VARCHAR,                                   -- Listing exchange ("NASDAQ"); disambiguates duplicate tickers
    cusip VARCHAR,                                      -- 9-char CUSIP if supplied by user data; licensed — accepted, never redistributed
    isin VARCHAR,                                       -- ISIN if supplied; international identifier
    figi VARCHAR,                                       -- OpenFIGI identifier (open mapping aid)
    coingecko_id VARCHAR,                               -- CoinGecko slug for crypto price lookup (Pillar C)
    is_cash_equivalent BOOLEAN,                         -- Highly liquid, treat-like-cash flag (money-market/sweep); NULL = unknown
    cost_basis_method VARCHAR CHECK (cost_basis_method IN ('fifo', 'hifo', 'specific', 'average')), -- Per-security election override; NULL falls back to account default
    currency_code VARCHAR NOT NULL DEFAULT 'USD',       -- Instrument's denominating currency; no FX conversion in v1
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, -- When the catalog entry was created
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP  -- When last modified; service must set explicitly on UPDATE (DuckDB has no ON UPDATE trigger)
);
