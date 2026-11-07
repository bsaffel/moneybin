/* Derived Golden event identity resolution; no mutable alias registry. */
MODEL (
  name core.alias_investment_event_ids,
  kind VIEW,
  grain requested_event_group_id
);

/* Active identities self-resolve. Consolidations forward directly to a current
   ratified target; changed structures and multiple successors never guess. */
WITH requested AS (
  SELECT DISTINCT
    m.event_group_id
  FROM app.investment_event_members AS m
), active AS (
  SELECT DISTINCT
    m.event_group_id
  FROM app.investment_event_members AS m
  WHERE
    m.retired_at IS NULL
), successors AS (
  SELECT
    old.event_group_id AS requested_id,
    COUNT(DISTINCT current.event_group_id) AS target_count,
    MIN(current.event_group_id) AS target_id,
    BOOL_AND(CAST(NOT current.decision_id IS NULL AS BOOLEAN)) AS ratified
  FROM app.investment_event_members AS old
  JOIN app.investment_event_members AS current
    ON current.retired_at IS NULL
    AND current.member_kind = old.member_kind
    AND current.source_type = old.source_type
    AND current.source_origin = old.source_origin
    AND current.native_reference = old.native_reference
  GROUP BY
    old.event_group_id
)
SELECT
  r.event_group_id AS requested_event_group_id,
  CASE
    WHEN NOT a.event_group_id IS NULL
    THEN r.event_group_id
    WHEN s.target_count = 1 AND s.ratified
    THEN s.target_id
  END AS active_event_group_id,
  CASE
    WHEN NOT a.event_group_id IS NULL
    THEN 'active'
    WHEN s.target_count = 1 AND s.ratified
    THEN 'forwarded'
    ELSE 'retired'
  END AS resolution_status
FROM requested AS r
LEFT JOIN active AS a
  USING (event_group_id)
LEFT JOIN successors AS s
  ON s.requested_id = r.event_group_id
