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
   guard over that (system doctor runs it).

   An account with app.account_settings.investment_source_type set keeps only that
   source type's rows (docs/specs/investment-source-choice.md); the filter is the
   final WHERE, and lots, gains and holdings inherit it.

   reconciliation_status is derived here and only here (docs/specs/reconciliation-status.md):
   an account whose rows, after that filter and excluding opening_bootstrap,
   carry more than one source_type is 'unreconciled'. Other models read the column. */
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
), scoped AS (
  SELECT
    u.investment_transaction_id,
    u.account_id,
    u.security_id,
    u.event_group_id,
    u.type,
    u.subtype,
    u.provider_type,
    u.provider_subtype,
    u.source_type,
    u.source_origin,
    u.description,
    COALESCE(u.currency_code, a.currency_code) AS currency_code,
    u.quantity,
    u.price,
    u.amount,
    u.fees,
    u.trade_date,
    u.settlement_date,
    u.original_acquisition_date,
    GREATEST(
      u.created_at,
      COALESCE(CASE WHEN u.currency_code IS NULL THEN a.updated_at END, u.created_at),
      COALESCE(a.investment_source_type_changed_at, u.created_at)
    ) AS row_updated_at
  FROM unioned AS u
  LEFT JOIN prep.int_accounts__merged AS a
    ON u.account_id = a.account_id
  /* investment-source-choice.md: an account with a source chosen keeps only that
     source's rows. The only place the choice changes data; every model derived from
     this ledger inherits it. NULL (no choice, or no account row) keeps every row. */
  WHERE
    a.investment_source_type IS NULL OR u.source_type = a.investment_source_type
), account_reconciliation AS (
  SELECT
    account_id,
    CASE
      WHEN COUNT(DISTINCT source_type) FILTER(WHERE
        COALESCE(subtype, '') <> 'opening_bootstrap') > 1
      THEN 'unreconciled'
      ELSE 'reconciled'
    END AS reconciliation_status,
    MAX(row_updated_at) AS account_updated_at
  FROM scoped
  GROUP BY
    account_id
)
SELECT
  s.investment_transaction_id, /* Canonical ID (source-provided or content hash) */
  s.account_id, /* FK to core.dim_accounts */
  s.security_id, /* FK to core.dim_securities; NULL for cash-only events (deposit, withdrawal, account fee, cash interest) and for a synced security with no accepted binding */
  s.event_group_id, /* Links legs of one decomposed economic event; nullable */
  s.type, /* Closed taxonomy (see investments-data-model.md Requirement 5) */
  s.subtype, /* Per-type refinement (tax character, reinvest source); nullable. 'opening_bootstrap' marks a reconstructed pre-window lot, never a real transfer */
  s.provider_type, /* Provider's original type string (Plaid investment_transaction_type), preserved verbatim for audit; NULL for manual and bootstrap rows. Never a ledger input — `type` is the closed taxonomy */
  s.provider_subtype, /* Provider's original subtype string, preserved verbatim for audit; NULL for manual and bootstrap rows */
  s.source_type, /* Origin tag (manual | ofx | plaid) */
  s.source_origin, /* Institution/connection scope */
  s.description, /* Free-text description */
  ar.reconciliation_status, /* Whether the account's ledger carries one source ('reconciled') or two ('unreconciled', opening_bootstrap excluded); derived, never authored — docs/specs/reconciliation-status.md */
  s.currency_code, /* Denominating currency; no FX in v1. The event's own currency, else inherited from the account's currency_code (prep.int_accounts__merged, the materialized base under core.dim_accounts) — never a literal, which would relabel a foreign account's ledger and make the unknown-currency segment unreachable (multi-currency.md Requirement 3). NULL when neither is known. system doctor's currency_integrity check does not read this table; it surfaces the cause instead — the account's own NULL currency, or an account_id no dim_accounts row resolves (fct_investment_transactions_fk_integrity). */
  s.quantity, /* Signed units: + acquire, − dispose, NULL cash-only */
  s.price, /* Per-unit price; NULL for non-priced events */
  s.amount, /* Signed cash effect: − out (buy), + in (sell/dividend). Already in ledger convention on every branch — never re-flip a provider's sign here */
  s.fees, /* Fee/commission component folded into basis */
  s.trade_date, /* Trade date; drives holding-period classification */
  s.settlement_date, /* Settlement date; informational */
  s.original_acquisition_date, /* transfer_in only: original acquisition date; lot uses COALESCE(this, trade_date) */
  CASE
    WHEN ar.reconciliation_status = 'unreconciled'
    THEN GREATEST(s.row_updated_at, ar.account_updated_at)
    ELSE s.row_updated_at
  END AS updated_at /* Latest of all per-row input timestamps contributing to this row's current values. Advances on an Account Currency edit only when this event inherits that Currency; an event's own Currency keeps its own freshness. Also advances when the account's investment_source_type changes, so a row that re-enters the ledger after a clear reports a time newer than the clear rather than its original created_at. Does not advance on idempotent SQLMesh re-applies. On an unreconciled row it also folds the account's latest row timestamp, so a second source's event advances every row the status flags. See docs/specs/core-updated-at-convention.md. */
FROM scoped AS s
INNER JOIN account_reconciliation AS ar
  ON s.account_id = ar.account_id
