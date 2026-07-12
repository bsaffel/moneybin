MODEL (
  name prep.int_plaid__opening_positions,
  kind VIEW
);

/* Opening-lot bootstrap inputs, anchored to the FIRST snapshot per
   (account, item) — sync-plaid-investments.md § Opening-lot bootstrap. Shared by
   prep.stg_plaid__opening_lots (what to synthesize) and
   prep.stg_plaid__opening_lot_review (what to refuse), so the gap and the guards
   are computed once and can never disagree.

   Anchoring to the first snapshot is what makes the bootstrap deterministically
   recomputable: raw.plaid_investment_holdings keeps every snapshot (source_file is
   part of its PK), so a later sale that drops a lot from the NEWEST snapshot never
   retroactively rewrites a pre-window lot whose basis was known at connect.
   (core.dim_holdings deliberately reads the opposite anchor — the newest snapshot.)

   Ids here are Plaid-native throughout (source_account_key / source_security_key),
   never canonical — resolution happens in the two stg_ views above this one. Naming
   them source_*_key rather than account_id / security_id is deliberate: a column
   named account_id must carry the same values everywhere (database.md, "Column Name
   Consistency Across Layers"), and a provider id under that name would silently
   return nothing when joined to core. */
WITH first_snapshot AS (
  SELECT
    account_id,
    source_origin,
    source_file
  FROM (
    SELECT
      account_id,
      source_origin,
      source_file,
      ROW_NUMBER() OVER (PARTITION BY account_id, source_origin ORDER BY extracted_at, source_file) AS snapshot_rank
    FROM (
      SELECT DISTINCT
        account_id,
        source_origin,
        source_file,
        extracted_at
      FROM raw.plaid_investment_holdings
    )
  )
  WHERE
    snapshot_rank = 1
), positions AS (
  SELECT
    h.account_id AS source_account_key,
    h.security_id AS source_security_key,
    h.source_origin,
    h.source_file,
    h.quantity AS held_qty,
    h.cost_basis AS position_cost_basis,
    COALESCE(h.iso_currency_code, h.unofficial_currency_code) AS currency_code,
    h.holdings_date AS snapshot_date,
    h.extracted_at AS snapshot_extracted_at,
    h.transactions_window_start AS window_start
  FROM raw.plaid_investment_holdings AS h
  JOIN first_snapshot AS fs
    ON fs.account_id = h.account_id
    AND fs.source_origin = h.source_origin
    AND fs.source_file = h.source_file
), in_window AS (
  /* Net signed shares inside [W, snapshot date]. splits are excluded — their
     quantity is a multiplier, not a share count — and so are review-routed rows,
     which never reach the ledger the gap is measured against. Joined on Plaid-native
     keys because a position IS a provider-native fact at this layer. */
  SELECT
    p.source_account_key,
    p.source_security_key,
    p.source_origin,
    SUM(s.quantity) AS net_qty
  FROM positions AS p
  JOIN prep.stg_plaid__investment_transactions AS s
    ON s.source_account_key = p.source_account_key
    AND s.source_security_key = p.source_security_key
    AND s.source_origin = p.source_origin
  WHERE
    s.ledger_include
    AND s.type <> 'split'
    AND NOT s.quantity IS NULL
    AND s.trade_date >= p.window_start
    AND s.trade_date <= p.snapshot_date
  GROUP BY
    p.source_account_key,
    p.source_security_key,
    p.source_origin
)
/* The three guards. Each routes the position to review and synthesizes nothing —
   a visible gap, never a plausible-looking guess. held_qty is COALESCEd to 0 so an
   absent broker quantity lands in review rather than evaporating from both views
   (NULL > 0 and NULL < 0 are both false). */
SELECT
  p.*,
  COALESCE(iw.net_qty, 0) AS in_window_net,
  p.held_qty - COALESCE(iw.net_qty, 0) AS gap_qty,
  (
    COALESCE(p.held_qty, 0) <= 0
    OR EXISTS(
      SELECT
        1
      FROM raw.plaid_investment_holding_lots AS gl
      WHERE
        gl.account_id = p.source_account_key
        AND gl.security_id = p.source_security_key
        AND gl.source_origin = p.source_origin
        AND gl.source_file = p.source_file
        AND gl.position_type = 'short'
    )
  ) AS is_short_or_nonpositive,
  EXISTS(
    SELECT
      1
    FROM prep.stg_plaid__investment_transactions AS sp
    WHERE
      sp.source_account_key = p.source_account_key
      AND sp.source_security_key = p.source_security_key
      AND sp.source_origin = p.source_origin
      AND sp.type = 'split'
      AND sp.trade_date >= p.window_start
      AND sp.trade_date <= p.snapshot_date
  ) AS has_in_window_split
FROM positions AS p
LEFT JOIN in_window AS iw
  ON iw.source_account_key = p.source_account_key
  AND iw.source_security_key = p.source_security_key
  AND iw.source_origin = p.source_origin
