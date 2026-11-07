MODEL (
  name prep.stg_plaid__investment_transactions,
  kind VIEW
);

/* Normalize every immutable revision once; current receipt order selects today's view. */
WITH current_receipts AS (
  SELECT
    *
  FROM raw.plaid_investment_transaction_receipts
  QUALIFY
    ROW_NUMBER() OVER (
      PARTITION BY investment_transaction_id, source_origin
      ORDER BY extracted_at DESC, ingestion_sequence DESC
    ) = 1
)
SELECT
  r.*
  REPLACE (c.source_file AS source_file, c.extracted_at AS extracted_at, c.loaded_at AS created_at)
FROM prep.stg_plaid__investment_revisions AS r
JOIN current_receipts AS c
  USING (investment_transaction_id, source_origin, observation_version)
