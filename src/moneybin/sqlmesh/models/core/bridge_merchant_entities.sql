/* Gold transaction → the source system's own merchant-entity reference.
   One row per transaction that carries an entity id; sources that issue none
   (OFX, tabular, manual) are absent, so consumers LEFT JOIN this bridge.
   The (merchant_entity_source_type, merchant_entity_id) pair is the key
   app.merchant_links binds to a canonical core.dim_merchants row. */
MODEL (
  name core.bridge_merchant_entities,
  kind VIEW,
  grain transaction_id
);

SELECT
  transaction_id, /* FK to core.fct_transactions.transaction_id */
  merchant_entity_id, /* The source system's stable merchant id; opaque, never an account number */
  merchant_entity_source_type, /* source_type of the merge member that issued the entity id; NOT the merge-winner canonical_source_type */
  merchant_name AS source_merchant_name /* Merchant name as the source stated it; core.fct_transactions.merchant_name has already replaced this with the resolved canonical name */
FROM prep.int_transactions__merged
WHERE
  NOT merchant_entity_id IS NULL /* `NOT x IS NULL` is sqlmesh-format's canonical form; do not rewrite to `IS NOT NULL`. */
