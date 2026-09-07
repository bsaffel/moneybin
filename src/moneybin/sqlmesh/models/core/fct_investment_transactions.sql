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
  u.investment_transaction_id, /* Canonical ID (source-provided or content hash) */
  u.account_id, /* FK to core.dim_accounts */
  u.security_id, /* FK to core.dim_securities; NULL for cash-only events (deposit, withdrawal, account fee, cash interest) and for a synced security with no accepted binding */
  u.trade_date, /* Trade date; drives holding-period classification */
  u.settlement_date, /* Settlement date; informational */
  u.original_acquisition_date, /* transfer_in only: original acquisition date; lot uses COALESCE(this, trade_date) */
  u.type, /* Closed taxonomy (see investments-data-model.md Requirement 5) */
  u.subtype, /* Per-type refinement (tax character, reinvest source); nullable. 'opening_bootstrap' marks a reconstructed pre-window lot, never a real transfer */
  u.event_group_id, /* Links legs of one decomposed economic event; nullable */
  u.quantity, /* Signed units: + acquire, − dispose, NULL cash-only */
  u.price, /* Per-unit price; NULL for non-priced events */
  u.amount, /* Signed cash effect: − out (buy), + in (sell/dividend). Already in ledger convention on every branch — never re-flip a provider's sign here */
  u.fees, /* Fee/commission component folded into basis */
  COALESCE(u.currency_code, a.currency_code) AS currency_code, /* Denominating currency; no FX in v1. The event's own currency, else inherited from core.dim_accounts.currency_code — never a literal, which would relabel a foreign account's ledger and make the unknown-currency segment unreachable (multi-currency.md Requirement 3). NULL when neither is known. system doctor's currency_integrity check does not read this table; it surfaces the cause instead — the account's own NULL currency, or an account_id no dim_accounts row resolves (fct_investment_transactions_fk_integrity). */
  u.provider_type, /* Provider's original type string (Plaid investment_transaction_type), preserved verbatim for audit; NULL for manual and bootstrap rows. Never a ledger input — `type` is the closed taxonomy */
  u.provider_subtype, /* Provider's original subtype string, preserved verbatim for audit; NULL for manual and bootstrap rows */
  u.source_type, /* Origin tag (manual | ofx | plaid) */
  u.source_origin, /* Institution/connection scope */
  u.description, /* Free-text description */
  GREATEST(
    u.created_at,
    COALESCE(CASE WHEN u.currency_code IS NULL THEN a.updated_at END, u.created_at)
  ) AS updated_at /* Latest of all per-row input timestamps contributing to this row's current values. Advances on an Account Currency edit only when this event inherits that Currency; an event's own Currency keeps its own freshness. Does not advance on idempotent SQLMesh re-applies. See docs/specs/core-updated-at-convention.md. */
FROM unioned AS u
LEFT JOIN core.dim_accounts AS a
  ON u.account_id = a.account_id
