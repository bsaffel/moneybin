-- Create synthetic schema on demand (not during normal init)
CREATE SCHEMA IF NOT EXISTS synthetic;

/* Known-correct labels for scoring categorization and transfer detection accuracy against synthetic data */
CREATE TABLE IF NOT EXISTS synthetic.ground_truth (
    source_transaction_id VARCHAR NOT NULL, -- Joins to raw/core transaction identity; primary key
    account_id VARCHAR NOT NULL, -- Synthetic source-system account ID; joins to raw account tables
    expected_category VARCHAR, -- Ground-truth category label; NULL for transfers
    transfer_pair_id VARCHAR, -- Non-NULL for transfer pairs; both sides share the same ID
    persona VARCHAR NOT NULL, -- Which persona generated this row
    seed INTEGER NOT NULL, -- Seed used for reproducibility
    generated_at TIMESTAMP NOT NULL, -- When this ground truth was produced
    PRIMARY KEY (source_transaction_id)
);
