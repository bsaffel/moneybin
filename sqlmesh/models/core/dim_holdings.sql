/* Current positions: the sum of open lots per (account, security). The "now"
   snapshot with no date dimension, rebuilt on every run. Cost basis only —
   unrealized gain/loss needs a current price, which Pillar C (price feeds)
   supplies. Uses cost_basis_remaining (not cost_basis_total) because under
   average cost the pooled remaining basis is the meaningful figure and can
   exceed a lot's own total.

   The provider_reported_* columns are STORE-DON'T-TRUST: the broker's CLAIM
   about the same position, joined from its newest holdings snapshot and never
   blended into the ledger-derived figures above them. They exist to be
   reconciled against (system doctor warns on divergence), not to be read as
   MoneyBin's position. A position MoneyBin holds but the broker's newest
   snapshot omits shows NULL — that NULL is itself the signal. The converse is
   NOT covered: a position the broker reports but MoneyBin has no open lot
   for (unbound security, a declined bootstrap, or a holdings snapshot that
   landed before its transactions) produces no row here at all — a doctor
   check for that direction must scan prep.stg_plaid__investment_holdings
   directly, not this view. */
MODEL (
  name core.dim_holdings,
  kind VIEW,
  grain (account_id, security_id)
);

WITH positions AS (
  SELECT
    l.account_id,
    l.security_id,
    SUM(l.remaining_quantity)::DECIMAL(28, 10) AS quantity,
    SUM(l.cost_basis_remaining)::DECIMAL(18, 2) AS cost_basis,
    (
      SUM(l.cost_basis_remaining) / NULLIF(SUM(l.remaining_quantity), 0)
    )::DECIMAL(28, 10) AS average_cost,
    MAX(l.currency_code) AS currency_code,
    MAX(l.updated_at) AS updated_at
  FROM core.fct_investment_lots AS l
  WHERE
    l.is_open
  GROUP BY
    l.account_id,
    l.security_id
), newest_snapshot AS (
  /* ONE WHOLE SNAPSHOT per item — the source_file with the latest extracted_at,
     never "the latest row per position" and never "the latest holdings_date"
     (holdings_date is extracted_at::DATE, so two pulls on one UTC day tie on it).
     Scoping to a whole snapshot is what makes an omitted position read as NULL
     below instead of as a stale survivor from an earlier pull. */
  SELECT
    source_origin,
    source_file
  FROM (
    SELECT
      source_origin,
      source_file,
      ROW_NUMBER() OVER (PARTITION BY source_origin ORDER BY extracted_at DESC, source_file DESC) AS snapshot_rank
    FROM (
      SELECT DISTINCT
        source_origin,
        source_file,
        extracted_at
      FROM prep.stg_plaid__investment_holdings
    )
  )
  WHERE
    snapshot_rank = 1
), provider_reported AS (
  /* Aggregated to the position grain (account, security): a security merge can
     bind two provider security ids in one account onto one canonical id, and a
     canonical security can be held at several institutions. Summing here — not
     joining row-per-row — is what keeps the LEFT JOIN below from fanning the
     position out. provider_reported_as_of takes MIN, not MAX: when one item's
     connection breaks and its snapshot goes stale, the summed quantity/cost
     basis above still carries that stale contributor at full weight, so the
     honest freshness is "as fresh as the stalest contributor," not the newest
     one — MAX would let a healthy item's fresh timestamp mask a broken one. */
  SELECT
    h.account_id,
    h.security_id,
    SUM(h.quantity)::DECIMAL(28, 10) AS provider_reported_quantity,
    SUM(h.cost_basis)::DECIMAL(18, 2) AS provider_reported_cost_basis,
    SUM(h.institution_value)::DECIMAL(18, 2) AS provider_reported_value,
    MIN(h.extracted_at) AS provider_reported_as_of
  FROM prep.stg_plaid__investment_holdings AS h
  JOIN newest_snapshot AS ns
    ON ns.source_file = h.source_file AND ns.source_origin = h.source_origin
  WHERE
    NOT h.security_id IS NULL
  GROUP BY
    h.account_id,
    h.security_id
)
SELECT
  p.account_id, /* FK to core.dim_accounts (grain) */
  p.security_id, /* FK to core.dim_securities (grain) */
  p.quantity, /* Total open units (Σ remaining_quantity); cast back to (28,10) — SUM widens to (38,10) */
  p.cost_basis, /* Total open basis (Σ cost_basis_remaining); cast back to (18,2) — SUM widens to (38,2) */
  p.average_cost, /* cost_basis / quantity; cast wraps the WHOLE division so the result is DECIMAL(28,10), not DOUBLE (DuckDB decimal / promotes to DOUBLE); (28,10) for crypto fractional-unit precision; NULL when quantity is 0 */
  p.currency_code, /* Denominating currency (one per position) */
  pr.provider_reported_quantity, /* NON-AUTHORITATIVE: the broker's claimed open units in its newest snapshot. Reconciliation reference only — `quantity` above is MoneyBin's figure. NULL = the broker's newest snapshot does not report this position */
  pr.provider_reported_cost_basis, /* NON-AUTHORITATIVE: the broker's claimed cost basis. Never overwrites or feeds `cost_basis` above; system doctor warns when the two diverge */
  pr.provider_reported_value, /* NON-AUTHORITATIVE: the broker's claimed market value (MoneyBin computes no market value until price feeds land) */
  pr.provider_reported_as_of, /* Oldest extracted_at among the snapshots summed into the three columns above (MIN, not MAX) — a canonical position spanning multiple broker connections is only as fresh as its stalest contributor; NULL when the broker no longer reports this position */
  p.updated_at /* Latest of all per-row input timestamps contributing to this row's current values (MAX over the position's open lots). Provider-reported columns do not advance it — they are a reference, not an input. Does not advance on idempotent SQLMesh re-applies. See docs/specs/core-updated-at-convention.md. */
FROM positions AS p
LEFT JOIN provider_reported AS pr
  ON pr.account_id = p.account_id AND pr.security_id = p.security_id
