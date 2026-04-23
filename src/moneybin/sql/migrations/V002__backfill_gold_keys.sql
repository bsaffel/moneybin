-- Backfill transaction_id values in app.transaction_categories and
-- app.transaction_notes from source-native IDs to deterministic gold keys.
-- Gold key = first 16 chars of SHA-256(source_type || '|' || source_id || '|' || account_id).
-- This is a 1:1 mapping since no merges exist yet.

-- Build a mapping from old source-level IDs to gold keys.
-- OFX: source_transaction_id is the FITID (post-V001 rename).
-- Tabular: transaction_id is the content hash used as PK.
CREATE TEMPORARY TABLE _gold_key_mapping AS
SELECT
    old_id,
    substr(sha256(source_type || '|' || old_id || '|' || account_id), 1, 16) AS gold_id
FROM (
    SELECT DISTINCT
        source_transaction_id AS old_id,
        'ofx' AS source_type,
        account_id
    FROM raw.ofx_transactions
    UNION ALL
    SELECT DISTINCT
        transaction_id AS old_id,
        source_type,
        account_id
    FROM raw.tabular_transactions
) sources;

-- Update transaction_categories FK
UPDATE app.transaction_categories SET transaction_id = gm.gold_id
FROM _gold_key_mapping gm
WHERE app.transaction_categories.transaction_id = gm.old_id;

-- Update transaction_notes FK
UPDATE app.transaction_notes SET transaction_id = gm.gold_id
FROM _gold_key_mapping gm
WHERE app.transaction_notes.transaction_id = gm.old_id;

DROP TABLE _gold_key_mapping;
