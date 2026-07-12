AUDIT (
  name fct_investment_transactions_sign_convention,
  standalone TRUE
);

/* Returns investment_transaction_ids whose amount contradicts the ledger's sign
   convention (negative = cash out). A buy or a reinvest that reads as cash IN,
   or a sale that reads as cash OUT, is the signature of a provider's amount sign
   being flipped a SECOND time downstream of prep.stg_plaid__investment_transactions
   (the single point of inversion) — which turns every purchase into income and
   corrupts cost basis silently. First column is the violation entity ID
   (required by DoctorService convention — see doctor_service.py).

   Strict inequalities on purpose: NULL is legitimate (a bootstrap lot of unknown
   basis) and 0 is degenerate but not INVERTED. Only the types whose direction is
   unambiguous are policed — a fee rebate or a dividend reversal can legitimately
   carry the opposite sign, and blocking those would trade a real signal for
   noise. Standalone audits are non-blocking in SQLMesh: a violation surfaces
   through `moneybin doctor` rather than taking a user's transform offline. */
SELECT
  investment_transaction_id
FROM core.fct_investment_transactions
WHERE
  (
    type IN ('buy', 'reinvest') AND amount > 0
  )
  OR (
    type = 'sell' AND amount < 0
  )
ORDER BY
  investment_transaction_id
