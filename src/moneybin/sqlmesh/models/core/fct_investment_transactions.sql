/* Canonical investment ledger from published exact Golden membership.

   reconciliation_status is derived here and only here (docs/specs/reconciliation-status.md):
   an account whose rows, after the investment_source_type filter and excluding
   opening_bootstrap, carry more than one representative source_type is
   'unreconciled'. Other models read the column. */
MODEL (
  name core.fct_investment_transactions,
  kind FULL,
  grain investment_transaction_id
);

/* Membership is the sole identity and exact-evidence authority. Every input is
   already in ledger sign convention. Explicit source choice remains a user gate. */
WITH published AS (
  SELECT
    investment_transaction_id,
    account_id,
    security_id,
    event_group_id,
    type,
    subtype,
    provider_type,
    provider_subtype,
    source_type,
    source_origin,
    description,
    currency_code,
    quantity,
    price,
    amount,
    fees,
    trade_date,
    settlement_date,
    original_acquisition_date,
    created_at,
    updated_at
  FROM prep.int_investment_events__source_projection
  WHERE
    projection_scope = 'active'
  UNION ALL
  SELECT
    investment_transaction_id,
    account_id,
    security_id,
    event_group_id,
    type,
    subtype,
    provider_type,
    provider_subtype,
    source_type,
    source_origin,
    description,
    currency_code,
    quantity,
    price,
    amount,
    fees,
    trade_date,
    settlement_date,
    original_acquisition_date,
    created_at,
    updated_at
  FROM prep.int_investment_events__opening_projection
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
    u.currency_code,
    u.quantity,
    u.price,
    u.amount,
    u.fees,
    u.trade_date,
    u.settlement_date,
    u.original_acquisition_date,
    GREATEST(u.updated_at, COALESCE(a.investment_source_type_changed_at, u.updated_at)) AS row_updated_at
  FROM published AS u
  LEFT JOIN prep.int_accounts__merged AS a
    ON a.account_id = u.account_id
  /* investment-source-choice.md: an account with a source chosen keeps only that
     source's rows; every model derived from this ledger inherits it. */
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
  s.investment_transaction_id, /* Stable MoneyBin-owned Golden semantic-leg ID (UUID4 hex12); source IDs remain provenance */
  s.account_id, /* FK to core.dim_accounts */
  s.security_id, /* Bound canonical Security; NULL for cash-only events. Unresolved security-bearing evidence awaits identity review */
  s.event_group_id, /* Stable MoneyBin-owned Golden event ID; non-null for singleton and compound events */
  s.type, /* Closed taxonomy (see investments-data-model.md Requirement 5) */
  s.subtype, /* Per-type refinement (tax character, reinvest source); nullable. 'opening_bootstrap' marks a reconstructed pre-window lot, never a real transfer */
  s.provider_type, /* Provider's original type string (Plaid investment_transaction_type), preserved verbatim for audit; NULL for manual and bootstrap rows. Never a ledger input — `type` is the closed taxonomy */
  s.provider_subtype, /* Provider's original subtype string, preserved verbatim for audit; NULL for manual and bootstrap rows */
  s.source_type, /* Deterministic representative source type; membership retains every exact contributor */
  s.source_origin, /* Representative connection scope, paired with source_type */
  s.description, /* Free-text description */
  ar.reconciliation_status, /* Whether the account's ledger carries one representative source ('reconciled') or two ('unreconciled', opening_bootstrap excluded); derived, never authored — docs/specs/reconciliation-status.md */
  s.currency_code, /* Effective currency captured by immutable membership; historical projection never follows current routing */
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
  END AS updated_at /* Latest committed projection transition or explicit source-choice change; idempotent rebuilds do not advance it. On an unreconciled row it also folds the account's latest row timestamp, so a second source's event advances every row the status flags. See docs/specs/core-updated-at-convention.md */
FROM scoped AS s
INNER JOIN account_reconciliation AS ar
  ON s.account_id IS NOT DISTINCT FROM ar.account_id
