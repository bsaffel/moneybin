/* Immutable SimpleFIN transactions evidence */
CREATE TABLE IF NOT EXISTS raw.simplefin_transactions (
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
    transaction_id VARCHAR NOT NULL, -- Native transaction identity
    transaction_reference VARCHAR NOT NULL, -- Full framed source event identity
    amount DECIMAL(18,2), -- Exact supported signed monetary amount
    amount_original VARCHAR, -- Provider numeric string without normalization
    posted_at TIMESTAMPTZ, -- Valid provider posting instant
    transacted_at TIMESTAMPTZ, -- Optional provider transaction instant
    pending BOOLEAN, -- Explicit pending marker where supplied
    description VARCHAR, -- Original provider description
    currency VARCHAR, -- Original account currency
    PRIMARY KEY (batch_id, source_origin, account_id, transaction_id)
);
