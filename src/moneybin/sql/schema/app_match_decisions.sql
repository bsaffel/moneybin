/* Match decisions from the Python matcher and user review; one row per proposed pair */
CREATE TABLE IF NOT EXISTS app.match_decisions (
    match_id VARCHAR NOT NULL,                -- UUID primary key for this match decision
    source_transaction_id_a VARCHAR NOT NULL,  -- Source-native ID of first row in the pair
    source_type_a VARCHAR NOT NULL,            -- source_type of first row (ofx, csv, etc.)
    source_origin_a VARCHAR NOT NULL,          -- source_origin of first row (institution/format)
    source_transaction_id_b VARCHAR NOT NULL,  -- Source-native ID of second row in the pair
    source_type_b VARCHAR NOT NULL,            -- source_type of second row
    source_origin_b VARCHAR NOT NULL,          -- source_origin of second row
    account_id VARCHAR NOT NULL,               -- Shared account (blocking requirement for dedup)
    confidence_score DECIMAL(5, 4),            -- Match confidence 0.0000 to 1.0000
    match_signals JSON,                        -- Per-signal scores: {"date_distance": 0, "description_similarity": 0.87}
    match_type VARCHAR NOT NULL DEFAULT 'dedup', -- dedup or transfer (transfer added by matching-transfer-detection.md)
    match_tier VARCHAR,                        -- Dedup-specific: 2b (within-source overlap) or 3 (cross-source); NULL for transfers
    account_id_b VARCHAR,                      -- Second account; NULL for dedup (same account); populated for transfers
    match_status VARCHAR NOT NULL,             -- pending, accepted, rejected
    match_reason VARCHAR,                      -- Human-readable explanation of why this match was proposed
    decided_by VARCHAR NOT NULL,               -- auto, user, system
    decided_at TIMESTAMP NOT NULL,             -- When the decision was made
    reversed_at TIMESTAMP,                     -- When the match was undone; NULL if active
    reversed_by VARCHAR,                       -- Who reversed: user or system; NULL if active
    PRIMARY KEY (match_id)
);
