/* Accounts carrying evidence of holding value and no balance observation at all
   — Requirement 14's candidate set (reports-net-worth-sql-surface.md), stated
   once. Evidence is any of: an open lot (core.dim_holdings), a broker-reported
   nonzero position (core.dim_holdings_broker_reported.has_position), or any row
   ever on either ledger (core.fct_transactions, core.fct_investment_transactions)
   — read existentially, never by SUM: a zero net cash effect is never proof of
   zero cash held, however it was reached.

   The anchor test reads core.fct_balances_daily, the FULL table the totals sum,
   not the live core.fct_balances view: a balance asserted since the last
   refresh is not yet in the spine, so the account stays a candidate (and the
   total NULL) until a refresh carries it there, instead of vanishing from both
   the guard and the total at once.

   Carries NO eligibility. include_in_net_worth and archival are applied by each
   reader at its own date (the report rungs per balance_date, the runner
   fallbacks at the synthesized date, system doctor at the account's current
   state), so they cannot live here. An account with no evidence of any kind
   stays out: nothing distinguishes "never funded" from "not yet observed" for
   it, and flagging it would NULL every profile holding a freshly linked
   account. */
MODEL (
  name core.dim_unanchored_accounts,
  kind VIEW,
  grain account_id
);

WITH evidence AS (
  SELECT DISTINCT
    h.account_id,
    'holdings' AS evidence_source
  FROM core.dim_holdings AS h
  UNION ALL
  SELECT
    r.account_id,
    'broker_position' AS evidence_source
  FROM core.dim_holdings_broker_reported AS r
  WHERE
    r.has_position
  UNION ALL
  SELECT DISTINCT
    t.account_id,
    'transactions' AS evidence_source
  FROM core.fct_transactions AS t
  UNION ALL
  SELECT DISTINCT
    i.account_id,
    'investment_transactions' AS evidence_source
  FROM core.fct_investment_transactions AS i
)
SELECT
  e.account_id, /* Grain. Foreign key to core.dim_accounts */
  BOOL_OR(CAST(e.evidence_source = 'holdings' AS BOOLEAN)) AS has_holdings, /* Has an open lot in core.dim_holdings */
  BOOL_OR(CAST(e.evidence_source = 'broker_position' AS BOOLEAN)) AS has_broker_position, /* core.dim_holdings_broker_reported.has_position is TRUE */
  BOOL_OR(CAST(e.evidence_source = 'transactions' AS BOOLEAN)) AS has_transactions, /* Has any row in core.fct_transactions */
  BOOL_OR(CAST(e.evidence_source = 'investment_transactions' AS BOOLEAN)) AS has_investment_transactions /* Has any row in core.fct_investment_transactions */
FROM evidence AS e
WHERE
  NOT e.account_id IS NULL
  AND NOT EXISTS(
    SELECT
      1
    FROM core.fct_balances_daily AS b
    WHERE
      b.account_id = e.account_id
  )
GROUP BY
  e.account_id
