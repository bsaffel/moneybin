MODEL (
  name prep.stg_plaid__opening_lot_review,
  kind VIEW
);

/* Positions the opening-lot bootstrap refuses to synthesize — a visible gap, never
   silent corruption (sync-plaid-investments.md § Opening-lot bootstrap, Requirement
   13 guards + the negative reconciliation gap). system doctor surfaces these.

   Reasons:
     short_or_nonpositive — a short lot (the engine models only long lots) or a
                            held quantity that is <= 0 or absent
     in_window_split      — the gap is measured post-split but a synthetic row is
                            dated pre-window, so the engine would re-apply the
                            multiplier and double-scale the opened quantity
     negative_gap         — the ledger shows MORE shares than are held (a disposal
                            Plaid dropped from the window); never silently adjusted
     sold_out_prewindow   — an in-window sell/transfer_out for a position with NO
                            holdings row in ANY snapshot; Plaid never reports closed
                            positions, so a pre-window position fully sold before the
                            first snapshot never reaches int_plaid__opening_positions
                            and would otherwise appear in neither this view nor
                            stg_plaid__opening_lots — a zero-basis oversold disposal
                            that basis_incomplete flags downstream but never explains

   Ids resolve exactly as they do in the sibling stg_plaid__ views: account_id falls
   back to the source-native id, security_id is NULL-passthrough (no COALESCE — a
   provider id in the canonical column would masquerade as a real catalog entry).
   Both source keys are carried so the raw provider row stays addressable. */
WITH guard_reasons AS (
  SELECT
    p.source_account_key,
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
  WHERE
    p.is_short_or_nonpositive OR p.has_in_window_split OR p.gap_qty < 0
), first_snapshot_window AS (
  /* The first snapshot's window boundary per (account, item) — the same anchor
     int_plaid__opening_positions.first_snapshot uses, reduced to just window_start.
     A fully-sold-out security has no holdings row of its own to read it from, so
     this reads it off ANY row of that account's first snapshot instead (the column
     is constant across every row of one snapshot — raw DDL comment). */
  SELECT
    account_id AS source_account_key,
    source_origin,
    transactions_window_start AS window_start
  FROM (
    SELECT
      account_id,
      source_origin,
      transactions_window_start,
      ROW_NUMBER() OVER (PARTITION BY account_id, source_origin ORDER BY extracted_at, source_file) AS snapshot_rank
    FROM raw.plaid_investment_holdings
  )
  WHERE
    snapshot_rank = 1
), sold_out_prewindow AS (
  /* A pre-window position fully sold before the FIRST snapshot leaves NO holdings
     row in ANY snapshot — Plaid never reports closed positions. Scoped to
     sell/transfer_out (cost_basis.py's _DISPOSAL_TYPES) since those are the events
     that need an open lot to draw from; a disposal with nothing in the snapshot to
     explain it is exactly the silent-zero-basis case this view exists to surface. */
  SELECT DISTINCT
    t.source_account_key,
    t.source_security_key,
    t.source_origin,
    'sold_out_prewindow' AS reason
  FROM prep.stg_plaid__investment_transactions AS t
  JOIN first_snapshot_window AS w
    ON w.source_account_key = t.source_account_key AND w.source_origin = t.source_origin
  WHERE
    t.ledger_include
    AND t.type IN ('sell', 'transfer_out')
    AND t.trade_date >= w.window_start
    AND NOT EXISTS(
      SELECT
        1
      FROM raw.plaid_investment_holdings AS h
      WHERE
        h.account_id = t.source_account_key
        AND h.source_origin = t.source_origin
        AND h.security_id = t.source_security_key
    )
), flagged AS (
  SELECT
    *
  FROM guard_reasons
  UNION ALL
  SELECT
    *
  FROM sold_out_prewindow
)
SELECT
  COALESCE(al.account_id, f.source_account_key) AS account_id,
  f.source_account_key,
  sl.security_id AS security_id,
  f.source_security_key,
  f.source_origin,
  f.reason
FROM flagged AS f
LEFT JOIN app.account_links AS al
  ON al.status = 'accepted'
  AND al.ref_kind = 'source_native'
  AND al.source_type = 'plaid'
  AND al.source_origin = f.source_origin
  AND al.ref_value = f.source_account_key
LEFT JOIN app.security_links AS sl
  ON sl.status = 'accepted'
  AND sl.ref_kind = 'plaid_security_id'
  AND sl.source_type = 'plaid'
  AND sl.ref_value = f.source_security_key
