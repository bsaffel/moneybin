/* What each Plaid item's NEWEST holdings snapshot says about an account's
   position, reduced to one row per account. The broker's claim, not
   MoneyBin's ledger: core.dim_holdings sums open lots and so has no row for a
   position the broker reports without a matching lot (an unbound security, a
   declined bootstrap, a snapshot that landed before its transactions). This is
   that direction.

   Invariant (reports-net-worth-sql-surface.md §Data Model): absence of evidence
   is never evidence of absence. has_position = FALSE only when every item
   competent to speak for the account (account_type = 'investment') pulled
   successfully and reported nothing nonzero; TRUE needs no such licence, so an
   account of any type — including an unresolved NULL type — reads TRUE on a
   nonzero row of its own. A relinked account (several source_origins) reduces
   in a fixed order: TRUE, then absent, then FALSE, then NULL.

   Reads quantity and institution_value only; cost_basis is a reconciliation
   reference and never evidence of a position. */
MODEL (
  name core.dim_holdings_broker_reported,
  kind VIEW,
  grain account_id
);

WITH newest_snapshot AS (
  /* Same receipt-scoped newest pull as core.dim_holdings: a liquidated item's
     newest pull writes zero holdings rows, so the newest snapshot comes from
     the receipts, never from the rows. */
  SELECT
    source_origin,
    source_file,
    extracted_at
  FROM (
    SELECT
      source_origin,
      source_file,
      extracted_at,
      ROW_NUMBER() OVER (PARTITION BY source_origin ORDER BY extracted_at DESC, ingestion_sequence DESC) AS snapshot_rank
    FROM prep.stg_plaid__investment_holdings_snapshots
  )
  WHERE
    snapshot_rank = 1
), mapped_origins AS (
  /* can_read_negative: only an investment-typed origin may license FALSE or
     absent; any origin may license TRUE. */
  SELECT
    account_id,
    source_origin,
    COALESCE(BOOL_OR(CAST(account_type = 'investment' AS BOOLEAN)), FALSE) AS can_read_negative
  FROM prep.stg_plaid__accounts
  WHERE
    NOT account_id IS NULL
  GROUP BY
    account_id,
    source_origin
), per_origin AS (
  SELECT
    o.account_id,
    o.can_read_negative,
    NOT s.source_origin IS NULL AS has_receipt,
    s.extracted_at::DATE AS as_of,
    COUNT(h.account_id) AS row_count,
    COUNT(h.account_id) FILTER(WHERE
      NOT h.quantity IS NULL OR NOT h.institution_value IS NULL) AS decisive_row_count,
    COALESCE(BOOL_OR(CAST(h.quantity <> 0 OR h.institution_value <> 0 AS BOOLEAN)), FALSE) AS any_nonzero
  FROM mapped_origins AS o
  LEFT JOIN newest_snapshot AS s
    ON s.source_origin = o.source_origin
  LEFT JOIN prep.stg_plaid__investment_holdings AS h
    ON h.account_id = o.account_id
    AND h.source_origin = s.source_origin
    AND h.source_file = s.source_file
  GROUP BY
    o.account_id,
    o.source_origin,
    o.can_read_negative,
    s.source_origin,
    s.extracted_at
), resolved AS (
  SELECT
    account_id,
    can_read_negative,
    has_receipt,
    as_of,
    CASE
      WHEN NOT has_receipt
      THEN NULL
      WHEN any_nonzero
      THEN TRUE
      WHEN decisive_row_count = row_count
      THEN FALSE
      ELSE NULL
    END AS origin_has_position
  FROM per_origin
), reduced AS (
  SELECT
    account_id,
    BOOL_OR(CAST(origin_has_position IS TRUE AS BOOLEAN)) AS any_true,
    COALESCE(BOOL_OR(CAST(can_read_negative AND NOT has_receipt AS BOOLEAN)), FALSE) AS any_negative_origin_unpulled,
    COUNT(*) FILTER(WHERE
      can_read_negative) AS negative_origin_count,
    BOOL_AND(CAST(origin_has_position IS FALSE AS BOOLEAN)) FILTER(WHERE
      can_read_negative) AS every_negative_origin_false,
    MIN(as_of) FILTER(WHERE
      origin_has_position IS TRUE) AS true_as_of,
    MIN(as_of) FILTER(WHERE
      can_read_negative) AS negative_as_of
  FROM resolved
  GROUP BY
    account_id
)
SELECT
  account_id, /* Grain. Foreign key to core.dim_accounts */
  CASE WHEN any_true THEN TRUE WHEN every_negative_origin_false THEN FALSE END AS has_position, /* TRUE: a newest snapshot reports a nonzero quantity or value. FALSE: every investment-typed item pulled and reported nothing nonzero. NULL: pulled, but every row is NULL on both figures */
  CASE WHEN any_true THEN true_as_of ELSE negative_as_of END AS as_of /* Date of the stalest receipt behind has_position (MIN across contributing items) */
FROM reduced
WHERE
  any_true OR (
    negative_origin_count > 0 AND NOT any_negative_origin_unpulled
  )
