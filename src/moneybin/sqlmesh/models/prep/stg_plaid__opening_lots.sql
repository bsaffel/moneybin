MODEL (
  name prep.stg_plaid__opening_lots,
  kind VIEW
);

/* Synthetic opening transfer_in rows (subtype 'opening_bootstrap') that seed
   pre-window positions into the ledger — sync-plaid-investments.md § Opening-lot
   bootstrap. Plaid's transaction window is ~24 months but its holdings snapshot
   reports the whole position, so an established account's long-held shares have no
   acquiring transaction; without these rows a later Plaid sale is processed as an
   oversold zero-basis disposal (fully-taxed phantom gain, term forced short).

   This is a RECONSTRUCTION from incomplete data, so every part of it is either
   exact or explicitly flagged. Shares whose lot the broker still reports open at
   the broker's real basis and real acquisition date; shares the snapshot cannot
   explain open with amount = NULL, which the engine reads as basis_incomplete.
   Nothing in between is ever guessed. Positions the bootstrap will not touch at all
   go to prep.stg_plaid__opening_lot_review.

   Two dates, deliberately different. The engine keys lot-OPENING order on
   trade_date, so every row is dated W - 1 day and therefore opens before every
   in-window disposal; it keys holding-period TERM and FIFO consumption on
   acquisition_date, so original_acquisition_date carries the lot's real date. The
   residual is dated oldest of all, which makes FIFO consume the unknown-basis shares
   first and leaves the known tax_lots standing as the held position.

   NOT a second Plaid sign flip. The only inversion here is cost basis (a positive
   cost) -> ledger amount (negative = cash out, the buy convention). Plaid's own
   amount-sign flip lives exclusively in stg_plaid__investment_transactions and is
   not repeated, re-applied, or undone here — these rows carry no Plaid amount.

   Determinism. investment_transaction_id is a content hash over
   (source_origin, account, security, lot_key, acquisition_date, basis) with a
   plaid_opening_ prefix (identifiers.md). source_origin is in the hash because
   (account_id, security_id) is only unique WITHIN a Plaid item — it is part of the
   raw PK for exactly that reason — and security is in it because two securities in
   one account would otherwise collide onto one id, and from there onto one engine
   lot_id the moment a security merge unifies them. lot_key ALWAYS carries lot_index
   — institution_lot_id, when the broker supplies one, is appended to it, never
   substituted for it — so two lots sharing one broker-supplied institution_lot_id
   still hash to distinct ids instead of colliding onto one engine lot_id.

   Stability, precisely. Because the holdings-side inputs (lot_key,
   acquisition_date) are read from the frozen first snapshot, even the positional
   lot_key fallback ('idx_N') is stable across re-syncs though Plaid may reorder
   tax_lots[] later. That freezes the HOLDINGS side only — NOT basis_amount, which
   on a boundary lot derives from gap_qty, which reads
   prep.stg_plaid__investment_transactions (not frozen). So the ids, the engine
   lot_ids hashed from them, and any app.lot_selections pointing at them are stable
   against a LATER SNAPSHOT — never against an in-window transaction correction: a
   late-arriving in-window buy can still close the gap and make a bootstrap row
   change or vanish. */
WITH bootstrappable AS (
  SELECT
    *
  FROM prep.int_plaid__opening_positions
  WHERE
    gap_qty > 0 AND NOT is_short_or_nonpositive AND NOT has_in_window_split
), eligible_lots AS (
  /* Pre-window lots of the frozen first snapshot. Strict < W: the transaction window
     is inclusive of W, so a lot dated exactly on W belongs to the window and drawing
     it would double-count the in-window buy that already opened it. A lot with a real
     basis but NO date is still eligible — its basis is never discarded — but it sorts
     LAST and is dated at W (spec case G): that date is SYNTHETIC, not recovered, so
     the holding-period term computed from it is understated by design — nothing
     downstream marks the term itself uncertain. */
  SELECT
    b.source_account_key,
    b.source_security_key,
    b.source_origin,
    b.source_file,
    b.window_start,
    b.snapshot_extracted_at,
    b.currency_code,
    b.gap_qty,
    l.lot_index,
    l.institution_lot_id,
    l.original_purchase_datetime,
    l.quantity AS lot_qty,
    l.cost_basis AS lot_basis,
    SUM(l.quantity) OVER (
      PARTITION BY b.source_account_key, b.source_security_key, b.source_origin
      ORDER BY (
        l.original_purchase_datetime IS NULL
      ), l.original_purchase_datetime, l.lot_index
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_qty
  FROM bootstrappable AS b
  JOIN raw.plaid_investment_holding_lots AS l
    ON l.account_id = b.source_account_key
    AND l.security_id = b.source_security_key
    AND l.source_origin = b.source_origin
    AND l.source_file = b.source_file
  WHERE
    COALESCE(l.quantity, 0) > 0
    AND (
      l.original_purchase_datetime < b.window_start
      OR (
        l.original_purchase_datetime IS NULL AND NOT l.cost_basis IS NULL
      )
    )
), draw_plan AS (
  /* Draw the gap oldest-first; the boundary lot (the one the cumulative draw crosses)
     takes only what is left of the gap. */
  SELECT
    *,
    LEAST(lot_qty, gap_qty - (
      cumulative_qty - lot_qty
    )) AS drawn_qty
  FROM eligible_lots
  WHERE
    cumulative_qty - lot_qty < gap_qty
), drawn_rows AS (
  /* A fully-drawn lot keeps its basis EXACTLY — no arithmetic touches it. Only the
     boundary lot prorates, and only that row pays DuckDB's DECIMAL -> DOUBLE detour
     (it has no decimal division; `/` on two DECIMALs returns DOUBLE). The DOUBLE is
     cast to DECIMAL(28,10) BEFORE narrowing to DECIMAL(18,2): that first cast lands
     the double on the exact decimal tie, so the (28,10) -> (18,2) narrowing is exact
     decimal rounding, not float truncation — casting straight to DECIMAL(18,2)
     truncates a tie instead of rounding it (e.g. 150.015 -> 150.01, not 150.02).
     Prorating a full draw instead of short-circuiting it would put every lot's basis
     through that detour, which is exactly what the money-type rule forbids. A lot
     with no basis at all prorates to NULL -> basis_incomplete. */
  SELECT
    source_account_key,
    source_security_key,
    source_origin,
    window_start,
    snapshot_extracted_at,
    currency_code,
    'idx_' || lot_index::TEXT || COALESCE('_' || institution_lot_id, '') AS lot_key,
    drawn_qty AS quantity,
    CASE
      WHEN drawn_qty = lot_qty
      THEN lot_basis
      ELSE (
        (
          lot_basis * drawn_qty / lot_qty
        )::DECIMAL(28, 10)
      )::DECIMAL(18, 2)
    END AS basis_amount,
    COALESCE(original_purchase_datetime::DATE, window_start) AS acquisition_date
  FROM draw_plan
), residuals AS (
  /* The sliver the eligible lots cannot cover: shares bought pre-window and sold
     in-window (gone from the snapshot, and Plaid's sell row carries proceeds but no
     basis), or an institution reporting no lot for part of the position. amount NULL
     -> basis_incomplete, dated one day before every drawn lot so FIFO eats these
     first. A position with NO lots at all takes the whole-position fallback below
     instead. */
  SELECT
    b.source_account_key,
    b.source_security_key,
    b.source_origin,
    b.window_start,
    b.snapshot_extracted_at,
    b.currency_code,
    '__residual__' AS lot_key,
    b.gap_qty - COALESCE(d.total_drawn, 0) AS quantity,
    NULL::DECIMAL(18, 2) AS basis_amount,
    (
      LEAST(COALESCE(d.oldest_acquisition, b.window_start), b.window_start) - INTERVAL '1' DAY
    )::DATE AS acquisition_date
  FROM bootstrappable AS b
  LEFT JOIN (
    SELECT
      source_account_key,
      source_security_key,
      source_origin,
      SUM(quantity) AS total_drawn,
      MIN(acquisition_date) AS oldest_acquisition
    FROM drawn_rows
    GROUP BY
      source_account_key,
      source_security_key,
      source_origin
  ) AS d
    ON d.source_account_key = b.source_account_key
    AND d.source_security_key = b.source_security_key
    AND d.source_origin = b.source_origin
  WHERE
    b.gap_qty - COALESCE(d.total_drawn, 0) > 0
    AND EXISTS(
      SELECT
        1
      FROM raw.plaid_investment_holding_lots AS el
      WHERE
        el.account_id = b.source_account_key
        AND el.security_id = b.source_security_key
        AND el.source_origin = b.source_origin
        AND el.source_file = b.source_file
    )
), fallbacks AS (
  /* Empty tax_lots[]: one row for the whole gap. The position-level cost basis is
     attributable to the gap ONLY when the gap IS the whole position (G = H, i.e. no
     in-window activity); when G < H, position-level data cannot isolate the gap's
     share of the basis, so the honest answer is basis_incomplete. */
  SELECT
    b.source_account_key,
    b.source_security_key,
    b.source_origin,
    b.window_start,
    b.snapshot_extracted_at,
    b.currency_code,
    '__position__' AS lot_key,
    b.gap_qty AS quantity,
    CASE
      WHEN b.gap_qty = b.held_qty
      THEN b.position_cost_basis
      ELSE NULL::DECIMAL(18, 2)
    END AS basis_amount,
    (
      b.window_start - INTERVAL '1' DAY
    )::DATE AS acquisition_date
  FROM bootstrappable AS b
  WHERE
    NOT EXISTS(
      SELECT
        1
      FROM raw.plaid_investment_holding_lots AS el
      WHERE
        el.account_id = b.source_account_key
        AND el.security_id = b.source_security_key
        AND el.source_origin = b.source_origin
        AND el.source_file = b.source_file
    )
), all_rows AS (
  SELECT
    *
  FROM drawn_rows
  UNION ALL
  SELECT
    *
  FROM residuals
  UNION ALL
  SELECT
    *
  FROM fallbacks
)
SELECT
  'plaid_opening_' || SUBSTRING(
    SHA256(
      CONCAT_WS(
        '|',
        a.source_origin,
        a.source_account_key,
        a.source_security_key,
        a.lot_key,
        COALESCE(a.acquisition_date::TEXT, ''),
        COALESCE(a.basis_amount::TEXT, ''),
        'opening_bootstrap'
      )
    ),
    1,
    16
  ) AS investment_transaction_id,
  COALESCE(al.account_id, a.source_account_key) AS account_id,
  a.source_account_key,
  sl.security_id AS security_id,
  a.source_security_key,
  (
    a.window_start - INTERVAL '1' DAY
  )::DATE AS trade_date,
  NULL::DATE AS settlement_date,
  a.acquisition_date AS original_acquisition_date,
  'transfer_in' AS type,
  'opening_bootstrap' AS subtype,
  NULL::TEXT AS event_group_id,
  a.quantity::DECIMAL(28, 10) AS quantity,
  NULL::DECIMAL(28, 10) AS price,
  CASE
    WHEN a.basis_amount IS NULL
    THEN NULL::DECIMAL(18, 2)
    ELSE (
      -1 * a.basis_amount
    )::DECIMAL(18, 2)
  END AS amount,
  NULL::DECIMAL(18, 2) AS fees,
  a.currency_code,
  NULL::TEXT AS provider_type,
  NULL::TEXT AS provider_subtype,
  'Opening lot bootstrap (pre-window position)' AS description,
  'plaid' AS source_type,
  a.source_origin,
  a.snapshot_extracted_at AS created_at
FROM all_rows AS a
LEFT JOIN app.account_links AS al
  ON al.status = 'accepted'
  AND al.ref_kind = 'source_native'
  AND al.source_type = 'plaid'
  AND al.source_origin = a.source_origin
  AND al.ref_value = a.source_account_key
LEFT JOIN app.security_links AS sl
  ON sl.status = 'accepted'
  AND sl.ref_kind = 'plaid_security_id'
  AND sl.source_type = 'plaid'
  AND sl.ref_value = a.source_security_key
