AUDIT (
  name fct_transactions_sign_convention,
);
/* Returns transaction_ids where amount is zero or NULL — both violate the
   sign convention (negative=expense, positive=income; zero is undefined).
   First column is the violation entity ID. */
SELECT
  transaction_id
FROM core.fct_transactions
WHERE
  amount = 0 OR amount IS NULL
ORDER BY
  transaction_id
