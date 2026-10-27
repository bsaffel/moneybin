/* Derived Golden transaction identity resolution; no mutable alias registry. */
MODEL (
  name core.alias_investment_transaction_ids,
  kind VIEW,
  grain requested_investment_transaction_id
);

/* Active identities self-resolve. Consolidations forward directly to a current
   ratified target; changed structures and multiple successors never guess. */
WITH requested AS (
  SELECT DISTINCT
    investment_transaction_id
  FROM app.investment_event_members
), active AS (
  SELECT DISTINCT
    investment_transaction_id
  FROM app.investment_event_members
  WHERE
    retired_at IS NULL
), successors AS (
  SELECT
    old.investment_transaction_id AS requested_id,
    COUNT(DISTINCT current.investment_transaction_id) AS target_count,
    MIN(current.investment_transaction_id) AS target_id,
    BOOL_AND(CAST(NOT current.decision_id IS NULL AS BOOLEAN)) AS ratified
  FROM app.investment_event_members AS old
  JOIN app.investment_event_members AS current
    ON current.retired_at IS NULL
    AND current.member_kind = old.member_kind
    AND current.source_type = old.source_type
    AND current.source_origin = old.source_origin
    AND current.native_reference = old.native_reference
    AND current.semantic_leg_role = old.semantic_leg_role
  GROUP BY
    old.investment_transaction_id
)
SELECT
  r.investment_transaction_id AS requested_investment_transaction_id,
  CASE
    WHEN NOT a.investment_transaction_id IS NULL
    THEN r.investment_transaction_id
    WHEN s.target_count = 1 AND s.ratified
    THEN s.target_id
  END AS active_investment_transaction_id,
  CASE
    WHEN NOT a.investment_transaction_id IS NULL
    THEN 'active'
    WHEN s.target_count = 1 AND s.ratified
    THEN 'forwarded'
    ELSE 'retired'
  END AS resolution_status
FROM requested AS r
LEFT JOIN active AS a
  USING (investment_transaction_id)
LEFT JOIN successors AS s
  ON s.requested_id = r.investment_transaction_id
