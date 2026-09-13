/* One immutable delivery receipt per transaction, Source origin and sync job. */
CREATE SEQUENCE IF NOT EXISTS raw.investment_ingestion_sequence;

CREATE TABLE IF NOT EXISTS raw.plaid_investment_transaction_receipts (
    investment_transaction_id VARCHAR NOT NULL, -- Native transaction reference
    source_origin VARCHAR NOT NULL,             -- Source origin that delivered the observation
    source_file VARCHAR NOT NULL,               -- Logical sync job identity
    observation_version VARCHAR NOT NULL,       -- Exact immutable Raw revision delivered by this job
    ingestion_sequence BIGINT NOT NULL DEFAULT nextval('raw.investment_ingestion_sequence'), -- Local first-ingestion ordering, preserved on replay
    extracted_at TIMESTAMP,                     -- Extraction timestamp reported by the server
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- First local receipt time
    PRIMARY KEY (investment_transaction_id, source_origin, source_file)
);
