AUDIT (
  name bridge_transfers_balanced,
);
/* Returns the debit_transaction_id of any transfer pair whose debit+credit
   amounts don't cancel to within $0.01. First column is the violation
   entity ID (debit side). */
SELECT
  bt.debit_transaction_id
FROM core.bridge_transfers AS bt
  JOIN core.fct_transactions AS d
    ON bt.debit_transaction_id = d.transaction_id
  JOIN core.fct_transactions AS c
    ON bt.credit_transaction_id = c.transaction_id
WHERE
  ABS(d.amount + c.amount) > 0.01
ORDER BY
  bt.debit_transaction_id
