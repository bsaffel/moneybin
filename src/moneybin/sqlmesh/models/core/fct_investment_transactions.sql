/* Canonical investment-transaction ledger; the only authored/ingested
   investment surface — lots, holdings, and realized gain/loss all derive
   from this table (Invariant 8). Unions three staging branches: manual entry,
   Plaid transactions, and the Plaid opening-lot bootstrap; further importers
   union their own staging models in here, mapping provider type strings to the
   closed taxonomy and preserving the originals in provider_type/provider_subtype.

   NO SIGN FLIP HAPPENS HERE. Every branch arrives in ledger convention already
   (negative = cash out). Plaid's inversion lives exclusively in
   prep.stg_plaid__investment_transactions; flipping again here would turn every
   buy into income. The fct_investment_transactions_sign_convention audit stands
   guard over that (system doctor runs it). */
MODEL (
  name core.fct_investment_transactions,
  kind FULL,
  grain investment_transaction_id
);

WITH unioned AS (
  SELECT
    investment_transaction_id,
    account_id,
    security_id,
    trade_date,
    settlement_date,
    original_acquisition_date,
    type,
    subtype,
    event_group_id,
    quantity,
    price,
    amount,
    fees,
    currency_code,
    NULL::TEXT AS provider_type,
    NULL::TEXT AS provider_subtype,
    source_type,
    source_origin,
    description,
    created_at
  FROM prep.stg_manual__investment_transactions
  UNION ALL
  /* Review-routed rows (splits, unmapped security-bearing subtypes) stay
     visible in staging for the doctor but never become ledger events. */
  SELECT
    investment_transaction_id,
    account_id,
    security_id,
    trade_date,
    settlement_date,
    original_acquisition_date,
    type,
    subtype,
    event_group_id,
    quantity,
    price,
    amount,
    fees,
    currency_code,
    provider_type,
    provider_subtype,
    source_type,
    source_origin,
    description,
    created_at
  FROM prep.stg_plaid__investment_transactions
  WHERE
    ledger_include
  UNION ALL
  /* Requirement 13: without this branch the opening-lot bootstrap is built but
     never reaches the ledger, and a pre-window sell goes oversold (zero-basis
     phantom gain). These rows are RECONSTRUCTIONS, not observations — they are
     the only transfer_in carrying subtype 'opening_bootstrap', which is not
     user-authorable, so a consumer can always tell one from a real transfer. */
  SELECT
    investment_transaction_id,
    account_id,
    security_id,
    trade_date,
    settlement_date,
    original_acquisition_date,
    type,
    subtype,
    event_group_id,
    quantity,
    price,
    amount,
    fees,
    currency_code,
    provider_type,
    provider_subtype,
    source_type,
    source_origin,
    description,
    created_at
  FROM prep.stg_plaid__opening_lots
)
SELECT
  investment_transaction_id, /* Canonical ID (source-provided or content hash) */
  account_id, /* FK to core.dim_accounts */
  security_id, /* FK to core.dim_securities; NULL for cash-only events (deposit, withdrawal, account fee, cash interest) and for a synced security with no accepted binding */
  trade_date, /* Trade date; drives holding-period classification */
  settlement_date, /* Settlement date; informational */
  original_acquisition_date, /* transfer_in only: original acquisition date; lot uses COALESCE(this, trade_date) */
  type, /* Closed taxonomy (see investments-data-model.md Requirement 5) */
  subtype, /* Per-type refinement (tax character, reinvest source); nullable. 'opening_bootstrap' marks a reconstructed pre-window lot, never a real transfer */
  event_group_id, /* Links legs of one decomposed economic event; nullable */
  quantity, /* Signed units: + acquire, − dispose, NULL cash-only */
  price, /* Per-unit price; NULL for non-priced events */
  amount, /* Signed cash effect: − out (buy), + in (sell/dividend). Already in ledger convention on every branch — never re-flip a provider's sign here */
  fees, /* Fee/commission component folded into basis */
  currency_code, /* Denominating currency; no FX in v1 */
  provider_type, /* Provider's original type string (Plaid investment_transaction_type), preserved verbatim for audit; NULL for manual and bootstrap rows. Never a ledger input — `type` is the closed taxonomy */
  provider_subtype, /* Provider's original subtype string, preserved verbatim for audit; NULL for manual and bootstrap rows */
  source_type, /* Origin tag (manual | ofx | plaid) */
  source_origin, /* Institution/connection scope */
  description, /* Free-text description */
  created_at AS updated_at /* Latest of all per-row input timestamps contributing to this row's current values. NULL when all contributing inputs are model-level (seeds, reference tables) — query meta.model_freshness for those. Does not advance on idempotent SQLMesh re-applies. v1: the row's own staging created_at (single source; no app-layer joins yet). See docs/specs/core-updated-at-convention.md. */
FROM unioned
