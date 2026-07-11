MODEL (
  name prep.stg_plaid__investment_holdings,
  kind VIEW
);

/* account_id follows the accounts precedent (COALESCE to source-native when
   unresolved — every source account always has an accepted binding by the time
   staging runs, per app_account_links.sql). security_id has no such fallback: it
   resolves only through app.security_links (status = 'accepted'); unresolved
   yields NULL, never the provider id, so an unbound security can't masquerade as
   canonical in the dim_holdings reconciliation join (sync-plaid-investments.md).
   No extra dedup here — raw.plaid_investment_holdings' PK
   (account_id, security_id, source_origin, source_file) already guarantees a
   same-snapshot repull upserts in place, and distinct snapshots (different
   source_file) must both survive as separate rows for the newest-snapshot
   reconciliation join in core.dim_holdings. */
SELECT
  COALESCE(al.account_id, h.account_id) AS account_id,
  h.account_id AS source_account_key,
  sl.security_id AS security_id,
  h.security_id AS source_security_key,
  h.holdings_date,
  h.institution_price,
  h.institution_price_as_of,
  h.institution_value,
  h.cost_basis,
  h.quantity,
  COALESCE(h.iso_currency_code, h.unofficial_currency_code) AS currency_code,
  h.vested_quantity,
  h.vested_value,
  h.transactions_window_start,
  h.source_file,
  h.source_type,
  h.source_origin,
  h.extracted_at,
  h.loaded_at
FROM raw.plaid_investment_holdings AS h
LEFT JOIN app.account_links AS al
  ON al.status = 'accepted'
  AND al.ref_kind = 'source_native'
  AND al.source_type = h.source_type
  AND al.source_origin = h.source_origin
  AND al.ref_value = h.account_id
LEFT JOIN app.security_links AS sl
  ON sl.status = 'accepted'
  AND sl.ref_kind = 'plaid_security_id'
  AND sl.source_type = h.source_type
  AND sl.ref_value = h.security_id
