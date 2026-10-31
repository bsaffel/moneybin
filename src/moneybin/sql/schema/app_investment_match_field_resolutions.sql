/* Explicit selected fields retain exact source provenance, not copied observations. */
CREATE TABLE IF NOT EXISTS app.investment_match_field_resolutions (
    resolution_id VARCHAR PRIMARY KEY, -- Identity of this selected field revision
    decision_id VARCHAR NOT NULL, -- Human-accepted Match owning this resolution
    investment_transaction_id VARCHAR NOT NULL, -- Golden semantic leg receiving the field
    field_name VARCHAR NOT NULL, -- Validated proposal-issued field
    source_type VARCHAR NOT NULL, -- Selected comparison adapter
    source_origin VARCHAR NOT NULL, -- Selected source scope
    native_reference VARCHAR NOT NULL, -- Selected stable source row
    observation_version VARCHAR NOT NULL, -- Selected exact immutable source revision
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP, -- Selection publication time
    retired_at TIMESTAMPTZ -- Retirement time; NULL while selected
);
