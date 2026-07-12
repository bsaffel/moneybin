AUDIT (
  name fct_investment_transactions_uniqueness,
  standalone TRUE
);

/* Returns investment_transaction_ids that appear more than once in
   core.fct_investment_transactions, violating its declared grain
   (investment_transaction_id). The table is three UNION ALL branches with no
   dedup and no PK/UNIQUE constraint — DuckDB tables don't enforce one — so a
   duplicate id would silently double a lot's quantity and cost basis in the
   cost-basis engine. Collision is implausible today (Plaid provider ids vs.
   plaid_opening_-prefixed hashes vs. bare 16-hex manual hashes don't overlap
   by construction), but the grain is a contract worth holding mechanically.
   First column is the violation entity ID (required by DoctorService
   convention — see doctor_service.py). */
SELECT
  investment_transaction_id
FROM core.fct_investment_transactions
GROUP BY
  investment_transaction_id
HAVING
  COUNT(*) > 1
ORDER BY
  investment_transaction_id
