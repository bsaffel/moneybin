/* Model-level freshness for every registered SQLMesh model.
   Wraps sqlmesh._snapshots to expose a stable public contract; if SQLMesh
   renames its internal columns, only this view changes.
   See docs/specs/core-updated-at-convention.md. */
MODEL (
  name meta.model_freshness,
  kind VIEW
);

WITH latest_per_name AS (
  SELECT
    -- `name` in sqlmesh._snapshots is a quoted three-part FQN like
    -- "moneybin"."core"."dim_accounts". Strip the quotes, then strip the
    -- leading catalog component, leaving 'schema.entity' for the public
    -- contract. `updated_ts` is BIGINT milliseconds since epoch.
    regexp_replace(REPLACE(name, '"', ''), '^[^.]+\.', '') AS model_name,
    MAX(updated_ts) AS last_applied_ms
  FROM sqlmesh._snapshots
  GROUP BY name
),
latest_version_per_name AS (
  SELECT
    regexp_replace(REPLACE(name, '"', ''), '^[^.]+\.', '') AS model_name,
    version,
    MIN(updated_ts) AS version_first_seen_ms,
    MAX(updated_ts) AS version_last_touched_ms
  FROM sqlmesh._snapshots
  GROUP BY name, version
),
current_version_per_name AS (
  SELECT model_name, version, version_first_seen_ms
  FROM (
    SELECT
      model_name,
      version,
      version_first_seen_ms,
      ROW_NUMBER() OVER (PARTITION BY model_name ORDER BY version_last_touched_ms DESC, version DESC) AS rn
    FROM latest_version_per_name
  )
  WHERE rn = 1
)
SELECT
  l.model_name, /* Schema-qualified model name, e.g. 'core.dim_accounts', 'seeds.categories'. */
  CAST(epoch_ms(c.version_first_seen_ms) AS TIMESTAMP) AS last_changed_at, /* When the current content version of this model was first materialized. Advances only when model definition or dependencies change. */
  CAST(epoch_ms(l.last_applied_ms) AS TIMESTAMP) AS last_applied_at /* When SQLMesh last touched any snapshot row for this model. Advances on every apply, idempotent or not. */
FROM latest_per_name AS l
JOIN current_version_per_name AS c USING (model_name)
