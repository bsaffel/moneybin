/* Operational recovery for a durably ingested broker job; not user-editable state. */
CREATE TABLE IF NOT EXISTS app.sync_job_state (
    job_id UUID PRIMARY KEY, -- Immutable broker job identity
    consumer_id VARCHAR NOT NULL, -- Internal owning profile identity
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP, -- Local durable ingestion time
    ack_state VARCHAR NOT NULL DEFAULT 'pending' CHECK (ack_state IN ('pending', 'confirmed', 'unknown', 'unavailable')), -- Broker acknowledgement confirmation
    acked_at TIMESTAMPTZ, -- Broker-confirmed acknowledgement time
    setup_state VARCHAR NOT NULL DEFAULT 'pending' CHECK (setup_state IN ('pending', 'complete', 'failed')), -- Local account and security binding obligation
    refresh_state VARCHAR NOT NULL DEFAULT 'pending' CHECK (refresh_state IN ('pending', 'deferred', 'complete', 'failed')), -- Independent local derivation obligation
    job_details JSON NOT NULL, -- Protected typed source references, receipt and post-ack evidence
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP -- Last local recovery transition
);
