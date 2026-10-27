/* Canonical investment ledger from published exact Golden membership. */
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
)
SELECT
  u.investment_transaction_id, /* Stable MoneyBin-owned Golden semantic-leg ID (UUID4 hex12); source IDs remain provenance */
  u.account_id, /* FK to core.dim_accounts */
  u.security_id, /* Bound canonical Security; NULL for cash-only events. Unresolved security-bearing evidence awaits identity review */
  u.event_group_id, /* Stable MoneyBin-owned Golden event ID; non-null for singleton and compound events */
  u.type, /* Closed taxonomy (see investments-data-model.md Requirement 5) */
  u.subtype, /* Per-type refinement (tax character, reinvest source); nullable. 'opening_bootstrap' marks a reconstructed pre-window lot, never a real transfer */
  u.provider_type, /* Provider's original type string (Plaid investment_transaction_type), preserved verbatim for audit; NULL for manual and bootstrap rows. Never a ledger input — `type` is the closed taxonomy */
  u.provider_subtype, /* Provider's original subtype string, preserved verbatim for audit; NULL for manual and bootstrap rows */
  u.source_type, /* Deterministic representative source type; membership retains every exact contributor */
  u.source_origin, /* Representative connection scope, paired with source_type */
  u.description, /* Free-text description */
  u.currency_code, /* Effective currency captured by immutable membership; historical projection never follows current routing */
  u.quantity, /* Signed units: + acquire, − dispose, NULL cash-only */
  u.price, /* Per-unit price; NULL for non-priced events */
  u.amount, /* Signed cash effect: − out (buy), + in (sell/dividend). Already in ledger convention on every branch — never re-flip a provider's sign here */
  u.fees, /* Fee/commission component folded into basis */
  u.trade_date, /* Trade date; drives holding-period classification */
  u.settlement_date, /* Settlement date; informational */
  u.original_acquisition_date, /* transfer_in only: original acquisition date; lot uses COALESCE(this, trade_date) */
  GREATEST(u.updated_at, COALESCE(a.investment_source_type_changed_at, u.updated_at)) AS updated_at /* Latest committed projection transition or explicit source-choice change; idempotent rebuilds do not advance it */
FROM published AS u
LEFT JOIN prep.int_accounts__merged AS a
  ON a.account_id = u.account_id
WHERE
  a.investment_source_type IS NULL OR u.source_type = a.investment_source_type
