/* Immutable SimpleFIN sync receipts evidence */
CREATE TABLE IF NOT EXISTS raw.simplefin_sync_receipts (
    job_id UUID NOT NULL, -- Authenticated broker job identity
    connection_id UUID NOT NULL, -- Broker grant identity
    credential_generation INTEGER NOT NULL, -- Observed credential generation
    protocol_version VARCHAR NOT NULL, -- Pinned protocol version
    status VARCHAR NOT NULL, -- Authenticated terminal broker job status
    started_at TIMESTAMPTZ NOT NULL, -- Broker requested-job order
    completed_at TIMESTAMPTZ NOT NULL, -- Authenticated broker completion evidence
    batch_ids JSON NOT NULL, -- Immutable batch identities including an empty list
    source_metadata JSON NOT NULL, -- Complete accepted source metadata excluding payload batches
    local_outcomes JSON NOT NULL, -- Safe local validation outcomes including unscoped failures
    content_digest VARCHAR NOT NULL, -- Immutable job/source receipt digest
    source_type VARCHAR NOT NULL DEFAULT 'simplefin', -- Provider source type
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP, -- Local receipt observation time
    PRIMARY KEY (job_id, connection_id)
);
