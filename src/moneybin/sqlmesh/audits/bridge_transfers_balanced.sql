AUDIT (
  name bridge_transfers_balanced,
  standalone TRUE
);

/* Returns the debit_transaction_id of any transfer pair whose two legs do not
   cancel EXACTLY. Not "within a cent": `amount` is DECIMAL(18,2) end to end,
   and the transfer matcher pairs on `ABS(a.amount) = b.amount`
   (matching/transfer.py), so every pair it builds nets to 0.00 by
   construction. A one-cent residue is money that went missing, not
   arithmetic, and the $0.01 slack this audit used to carry could only ever
   hide a real defect.

   LEFT JOIN on purpose. A pair whose leg has left core.fct_transactions is
   unbalanced too, and an inner join drops that case without a word.
   `IS DISTINCT FROM` folds it in: a missing leg contributes NULL, as does a
   NULL amount. First column is the violation entity ID (debit side). */
SELECT
  bt.debit_transaction_id
FROM core.bridge_transfers AS bt
LEFT JOIN core.fct_transactions AS d
  ON bt.debit_transaction_id = d.transaction_id
LEFT JOIN core.fct_transactions AS c
  ON bt.credit_transaction_id = c.transaction_id
WHERE
  d.amount + c.amount IS DISTINCT FROM 0
ORDER BY
  bt.debit_transaction_id
