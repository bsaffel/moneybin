MODEL (
  name prep.stg_plaid__opening_lot_review,
  kind VIEW
);

/* Positions the opening-lot bootstrap refuses to synthesize — a visible gap, never
   silent corruption (sync-plaid-investments.md § Opening-lot bootstrap, Requirement
   13 guards + the negative reconciliation gap). system doctor surfaces these.

   Reasons, in the order the CASE resolves them:
     short_or_nonpositive — a short lot (the engine models only long lots) or a
                            held quantity that is <= 0 or absent
     in_window_split      — the gap is measured post-split but a synthetic row is
                            dated pre-window, so the engine would re-apply the
                            multiplier and double-scale the opened quantity
     negative_gap         — the ledger shows MORE shares than are held (a disposal
                            Plaid dropped from the window); never silently adjusted

   Ids resolve exactly as they do in the sibling stg_plaid__ views: account_id falls
   back to the source-native id, security_id is NULL-passthrough (no COALESCE — a
   provider id in the canonical column would masquerade as a real catalog entry).
   Both source keys are carried so the raw provider row stays addressable. */
SELECT
  COALESCE(al.account_id, p.source_account_key) AS account_id,
  p.source_account_key,
  sl.security_id AS security_id,
  p.source_security_key,
  p.source_origin,
  CASE
    WHEN p.is_short_or_nonpositive
    THEN 'short_or_nonpositive'
    WHEN p.has_in_window_split
    THEN 'in_window_split'
    ELSE 'negative_gap'
  END AS reason
FROM prep.int_plaid__opening_positions AS p
LEFT JOIN app.account_links AS al
  ON al.status = 'accepted'
  AND al.ref_kind = 'source_native'
  AND al.source_type = 'plaid'
  AND al.source_origin = p.source_origin
  AND al.ref_value = p.source_account_key
LEFT JOIN app.security_links AS sl
  ON sl.status = 'accepted'
  AND sl.ref_kind = 'plaid_security_id'
  AND sl.source_type = 'plaid'
  AND sl.ref_value = p.source_security_key
WHERE
  p.is_short_or_nonpositive OR p.has_in_window_split OR p.gap_qty < 0
