/* Current positions: the sum of open lots per (account, security). The "now"
   snapshot with no date dimension, rebuilt on every run. Cost basis only —
   unrealized gain/loss needs a current price, which Pillar C (price feeds)
   supplies. Uses cost_basis_remaining (not cost_basis_total) because under
   average cost the pooled remaining basis is the meaningful figure and can
   exceed a lot's own total. */
MODEL (
  name core.dim_holdings,
  kind VIEW,
  grain (account_id, security_id)
);

SELECT
  l.account_id, /* FK to core.dim_accounts (grain) */
  l.security_id, /* FK to core.dim_securities (grain) */
  SUM(l.remaining_quantity)::DECIMAL(28, 10) AS quantity, /* Total open units (Σ remaining_quantity); cast back to (28,10) — SUM widens to (38,10) */
  SUM(l.cost_basis_remaining)::DECIMAL(18, 2) AS cost_basis, /* Total open basis (Σ cost_basis_remaining); cast back to (18,2) — SUM widens to (38,2) */
  (
    SUM(l.cost_basis_remaining) / NULLIF(SUM(l.remaining_quantity), 0)
  )::DECIMAL(28, 10) AS average_cost, /* cost_basis / quantity; cast wraps the WHOLE division so the result is DECIMAL(28,10), not DOUBLE (DuckDB decimal / promotes to DOUBLE); (28,10) for crypto fractional-unit precision; NULL when quantity is 0 */
  MAX(l.currency_code) AS currency_code, /* Denominating currency (one per position) */
  MAX(l.updated_at) AS updated_at /* Latest of all per-row input timestamps contributing to this row's current values (MAX over the position's open lots). Does not advance on idempotent SQLMesh re-applies. See docs/specs/core-updated-at-convention.md. */
FROM core.fct_investment_lots AS l
WHERE
  l.is_open
GROUP BY
  l.account_id,
  l.security_id
