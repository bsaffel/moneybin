/* Confirmed transfer pairs linking two fct_transactions rows;
   derived from app.match_decisions where match_type = 'transfer' */
MODEL (
  name core.bridge_transfers,
  kind VIEW,
  grain transfer_id
);

WITH matched_ids AS (
  SELECT DISTINCT
    source_transaction_id,
    source_type,
    account_id,
    transaction_id
  FROM prep.int_transactions__matched
)
SELECT
  md.match_id AS transfer_id, /* UUID identifying this transfer pair; also FK to app.match_decisions */
  debit.transaction_id AS debit_transaction_id, /* FK to fct_transactions; the outgoing side (negative amount) */
  credit.transaction_id AS credit_transaction_id, /* FK to fct_transactions; the incoming side (positive amount) */
  ABS(credit_txn.transaction_date - debit_txn.transaction_date) AS date_offset_days, /* Days between the two post dates (0 = same day) */
  ABS(debit_txn.amount) AS amount /* Absolute transfer amount */
FROM app.match_decisions AS md
JOIN matched_ids AS debit
  ON md.source_transaction_id_a = debit.source_transaction_id
  AND md.source_type_a = debit.source_type
  AND md.account_id = debit.account_id
JOIN matched_ids AS credit
  ON md.source_transaction_id_b = credit.source_transaction_id
  AND md.source_type_b = credit.source_type
  AND md.account_id_b = credit.account_id
JOIN prep.int_transactions__merged AS debit_txn
  ON debit.transaction_id = debit_txn.transaction_id
JOIN prep.int_transactions__merged AS credit_txn
  ON credit.transaction_id = credit_txn.transaction_id
WHERE
  md.match_type = 'transfer'
  AND md.match_status = 'accepted'
  AND md.reversed_at IS NULL
