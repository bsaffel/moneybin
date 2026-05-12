AUDIT (
  name fct_transactions_fk_integrity,
);
/* Returns orphaned transaction_ids — any transaction whose account_id has
   no matching row in dim_accounts. First column is the violation entity ID
   (required by DoctorService convention — see doctor_service.py). */
SELECT
  t.transaction_id
FROM core.fct_transactions AS t
  LEFT JOIN core.dim_accounts AS a
    ON t.account_id = a.account_id
WHERE
  a.account_id IS NULL
ORDER BY
  t.transaction_id
