/* Immutable SimpleFIN balances evidence */
CREATE TABLE IF NOT EXISTS raw.simplefin_balances (
    batch_id UUID NOT NULL, -- Immutable broker batch identity
    source_origin VARCHAR NOT NULL, -- Scoped source identity
    account_id VARCHAR NOT NULL, -- Original native account identity
    native_scope JSON NOT NULL, -- Original protocol scope identity
    identity_strength VARCHAR NOT NULL, -- Strong native ID or weak v1 fallback
    observation_version VARCHAR NOT NULL, -- Full SHA-256 content revision
    original_record JSON NOT NULL, -- Original credential-free source record
    validation_error VARCHAR, -- Safe reason excluding this observation from projection
    fetched_at TIMESTAMPTZ NOT NULL, -- Broker fetch time, never client arrival time
    source_file VARCHAR NOT NULL, -- First delivery job lineage
    source_type VARCHAR NOT NULL DEFAULT 'simplefin', -- Provider source type
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP, -- Local observation time
    balance DECIMAL(18,2), -- Exact signed provider balance
    balance_original VARCHAR, -- Original balance numeric string
    available_balance DECIMAL(18,2), -- Optional exact available balance
    available_balance_original VARCHAR, -- Original available balance string
    balance_at TIMESTAMPTZ, -- Valid provider balance instant
    currency VARCHAR, -- Original account currency
    PRIMARY KEY (batch_id, source_origin, account_id)
);
