AUDIT (
  name fct_transactions_sign_convention,
  standalone TRUE
);

/* Returns transaction_ids that break the ledger's sign convention
   (negative = expense, positive = income). Three states qualify:

     - amount IS NULL — the one value the convention cannot classify.
     - transaction_direction disagrees with the sign of amount. That column is
       the convention made explicit for every consumer that reads the label
       instead of the number, so drift there mislabels expenses as income
       downstream without touching a single amount.
     - amount_absolute disagrees with ABS(amount) — the same failure on the
       column aggregations use to skip sign handling.

   Zero is NOT a violation: 'zero' is a modeled third direction (see
   fct_transactions.sql's transaction_direction CASE and its column comment)
   for legitimate $0.00 rows — a waived fee, a $0 authorization.

   Category is deliberately NOT policed against the sign. A refund, a
   statement credit, and an unmatched card payment each carry a positive
   amount under a non-Income category, so a category-vs-sign predicate reports
   correct data as a defect; the Python assertion that carried one passed only
   because the synthetic generator emits no refunds. This matches the stance
   fct_investment_transactions_sign_convention takes on the same question:
   police only what the ledger can prove. First column is the violation
   entity ID. */
SELECT
  transaction_id
FROM core.fct_transactions
WHERE
  amount IS NULL
  OR transaction_direction IS DISTINCT FROM CASE
    WHEN amount < 0
    THEN 'expense'
    WHEN amount > 0
    THEN 'income'
    ELSE 'zero'
  END
  OR amount_absolute IS DISTINCT FROM ABS(amount)
ORDER BY
  transaction_id
