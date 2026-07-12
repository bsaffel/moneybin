AUDIT (
  name fct_investment_transactions_fk_integrity,
  standalone TRUE
);

/* Returns orphaned investment_transaction_ids — any investment transaction
   whose account_id has no matching row in dim_accounts. The banking ledger's
   fct_transactions_fk_integrity holds the same contract; this is its
   investment twin.

   The orphan arises when the provider delivers an event for an account it
   never delivered in the accounts array (Plaid eventual-consistency drift):
   no app.account_links row is written for it, so staging's
   COALESCE(al.account_id, r.account_id) carries the raw provider id into
   core, where no dim_accounts row resolves it — invisible to every surface
   that joins through the dim. First column is the violation entity ID
   (required by DoctorService convention — see doctor_service.py). */
SELECT
  t.investment_transaction_id
FROM core.fct_investment_transactions AS t
LEFT JOIN core.dim_accounts AS a
  ON t.account_id = a.account_id
WHERE
  a.account_id IS NULL
ORDER BY
  t.investment_transaction_id
