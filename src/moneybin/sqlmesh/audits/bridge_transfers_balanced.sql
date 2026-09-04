AUDIT (
  name bridge_transfers_balanced,
  standalone TRUE
);

/* Returns the debit_transaction_id of a transfer missing either leg or currency,
   or whose same-currency legs do not cancel EXACTLY. Not "within a cent":
   `amount` is DECIMAL(18,2) end to end, so a one-cent same-currency residue is
   money that went missing. Cross-currency amounts are unlike units and therefore
   cannot be added; their executed terms live in core.bridge_currency_conversions.

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
  d.amount IS NULL
  OR c.amount IS NULL
  OR d.currency_code IS NULL
  OR c.currency_code IS NULL
  OR (
    d.currency_code = c.currency_code
    AND d.amount + c.amount IS DISTINCT FROM 0
  )
ORDER BY
  bt.debit_transaction_id
