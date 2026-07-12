/* Canonical investment-transaction ledger; the only authored/ingested
   investment surface — lots, holdings, and realized gain/loss all derive
   from this table (Invariant 8). v1 selects from the single manual staging
   model; importer children union their own staging models in here, mapping
   provider type strings to the closed taxonomy. */
MODEL (
  name core.fct_investment_transactions,
  kind FULL,
  grain investment_transaction_id
);

SELECT
  investment_transaction_id, /* Canonical ID (source-provided or content hash) */
  account_id, /* FK to core.dim_accounts */
  security_id, /* FK to core.dim_securities; NULL for cash-only events (deposit, withdrawal, account fee, cash interest) */
  trade_date, /* Trade date; drives holding-period classification */
  settlement_date, /* Settlement date; informational */
  original_acquisition_date, /* transfer_in only: original acquisition date; lot uses COALESCE(this, trade_date) */
  type, /* Closed taxonomy (see investments-data-model.md Requirement 5) */
  subtype, /* Per-type refinement (tax character, reinvest source); nullable */
  event_group_id, /* Links legs of one decomposed economic event; nullable */
  quantity, /* Signed units: + acquire, − dispose, NULL cash-only */
  price, /* Per-unit price; NULL for non-priced events */
  amount, /* Signed cash effect: − out (buy), + in (sell/dividend) */
  fees, /* Fee/commission component folded into basis */
  currency_code, /* Denominating currency; no FX in v1 */
  source_type, /* Origin tag (manual | ofx | plaid) */
  source_origin, /* Institution/connection scope */
  description, /* Free-text description */
  created_at AS updated_at /* Latest of all per-row input timestamps contributing to this row's current values. NULL when all contributing inputs are model-level (seeds, reference tables) — query meta.model_freshness for those. Does not advance on idempotent SQLMesh re-applies. v1: the row's own staging created_at (single source; no app-layer joins yet). See docs/specs/core-updated-at-convention.md. */
FROM prep.stg_manual__investment_transactions /* Future: UNION ALL prep.stg_plaid__investment_transactions, prep.stg_ofx__investment_transactions, etc. */
