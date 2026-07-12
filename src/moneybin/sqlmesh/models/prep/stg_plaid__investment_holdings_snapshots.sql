MODEL (
  name prep.stg_plaid__investment_holdings_snapshots,
  kind VIEW
);

/* One row per (item, holdings pull) — the receipt that the item reported, and
   as of when. Passthrough: the grain is the item and the snapshot, so there is
   no account or security id here to resolve. Every consumer that needs "the
   newest holdings snapshot for this item" (core.dim_holdings, the doctor's
   holdings checks) derives it from HERE and never from the presence of holdings
   rows: an item whose pull returned zero positions writes no holdings rows at
   all, so a row-derived newest snapshot silently keeps an earlier NON-EMPTY one
   and a fully-liquidated broker reads as still holding its old positions
   (raw_plaid_investment_holdings_snapshots.sql). */
SELECT
  s.source_origin,
  s.source_file,
  s.holdings_date,
  s.holdings_count,
  s.source_type,
  s.extracted_at,
  s.loaded_at
FROM raw.plaid_investment_holdings_snapshots AS s
