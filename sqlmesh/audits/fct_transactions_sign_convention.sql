AUDIT (
  name fct_transactions_sign_convention,
  standalone TRUE
);

/* Returns transaction_ids where amount is NULL — the one state the sign
   convention (negative=expense, positive=income) cannot classify. Zero is
   NOT a violation: core.fct_transactions models it as a first-class third
   direction ('zero', see fct_transactions.sql's transaction_direction CASE
   and its column comment) for legitimate $0.00 rows (a waived fee, a $0
   authorization) that flow through the extract/load/merge chain unfiltered.
   Matches the stance fct_investment_transactions_sign_convention takes on
   the same question. First column is the violation entity ID. */
SELECT
  transaction_id
FROM core.fct_transactions
WHERE
  amount IS NULL
ORDER BY
  transaction_id
