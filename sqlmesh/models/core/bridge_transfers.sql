/* Confirmed transfer pairs linking two fct_transactions rows;
   derived from app.match_decisions where match_type = 'transfer' */
MODEL (
  name core.bridge_transfers,
  kind VIEW,
  grain transfer_id
);

SELECT
  md.match_id AS transfer_id, /* UUID identifying this transfer pair */
  debit.transaction_id AS debit_transaction_id, /* FK to fct_transactions; the outgoing side (negative amount) */
  credit.transaction_id AS credit_transaction_id, /* FK to fct_transactions; the incoming side (positive amount) */
  md.match_id, /* FK to app.match_decisions */
  ABS(credit.transaction_date - debit.transaction_date) AS date_offset_days, /* Days between the two post dates (0 = same day) */
  ABS(debit.amount) AS amount /* Absolute transfer amount */
FROM app.match_decisions AS md
JOIN prep.int_transactions__matched AS debit
  ON md.source_transaction_id_a = debit.source_transaction_id
  AND md.source_type_a = debit.source_type
  AND md.account_id = debit.account_id
JOIN prep.int_transactions__matched AS credit
  ON md.source_transaction_id_b = credit.source_transaction_id
  AND md.source_type_b = credit.source_type
  AND md.account_id_b = credit.account_id
WHERE
  md.match_type = 'transfer'
  AND md.match_status = 'accepted'
  AND md.reversed_at IS NULL
