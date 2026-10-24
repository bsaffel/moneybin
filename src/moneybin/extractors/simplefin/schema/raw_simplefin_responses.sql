/* Immutable SimpleFIN responses evidence */
CREATE TABLE IF NOT EXISTS raw.simplefin_responses (
    batch_id UUID PRIMARY KEY, -- Immutable broker batch identity
    connection_id UUID NOT NULL, -- Broker grant identity
    credential_generation INTEGER NOT NULL, -- Broker credential generation
    protocol_version VARCHAR NOT NULL, -- Pinned upstream protocol version
    fetched_at TIMESTAMPTZ NOT NULL, -- Broker fetch time
    window_start TIMESTAMPTZ NOT NULL, -- Inclusive requested UTC boundary
    window_end TIMESTAMPTZ NOT NULL, -- Exclusive requested UTC boundary
    original_payload JSON NOT NULL, -- Original credential-free Account Set
    source_errors JSON NOT NULL, -- Original source error objects or strings
    content_digest VARCHAR NOT NULL, -- Immutable complete batch digest
    source_file VARCHAR NOT NULL, -- First delivery job lineage
    source_type VARCHAR NOT NULL DEFAULT 'simplefin', -- Provider source type
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP -- Local observation time
);
