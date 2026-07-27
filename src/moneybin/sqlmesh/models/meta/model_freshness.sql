/* Model-level freshness for every registered SQLMesh model.
   Wraps sqlmesh._snapshots to expose a stable public contract; if SQLMesh
   renames its internal columns, only this view changes.
   See docs/specs/core-updated-at-convention.md. */
MODEL (
  name meta.model_freshness,
  kind VIEW
);

WITH snapshots AS (
  /* `name` in sqlmesh._snapshots is a quoted three-part FQN like
     "moneybin"."core"."dim_accounts". Strip the quotes, then strip the leading
     catalog component, leaving 'schema.entity' for the public contract.
     `updated_ts` is BIGINT milliseconds since epoch. */
  SELECT
    REGEXP_REPLACE(REPLACE(name, '"', ''), '^[^.]+\.', '') AS model_name,
    version,
    updated_ts,
    kind_name
  FROM sqlmesh._snapshots
), latest_per_name AS (
  SELECT
    model_name,
    MAX(updated_ts) AS last_applied_ms
  FROM snapshots
  GROUP BY
    model_name
), latest_version_per_name AS (
  /* A version is a content fingerprint and the kind is part of that content, so
     every row in a (model, version) group carries the same `kind_name`; MAX()
     just picks it deterministically. */
  SELECT
    model_name,
    version,
    MIN(updated_ts) AS version_first_seen_ms,
    MAX(updated_ts) AS version_last_touched_ms,
    MAX(kind_name) AS kind_name
  FROM snapshots
  GROUP BY
    model_name,
    version
), current_version_per_name AS (
  SELECT
    model_name,
    version,
    version_first_seen_ms,
    kind_name
  FROM (
    SELECT
      model_name,
      version,
      version_first_seen_ms,
      kind_name,
      ROW_NUMBER() OVER (PARTITION BY model_name ORDER BY version_last_touched_ms DESC, version DESC) AS rn
    FROM latest_version_per_name
  )
  WHERE
    rn = 1
), executions AS (
  /* `_intervals` records the intervals a model was actually backfilled over,
     one row per execution. Unlike `_snapshots.updated_ts` it does not move for
     metadata-only touches, so it is the surface that distinguishes "this model
     was rebuilt" from "the environment was promoted". Dev and removed rows are
     excluded so only prod execution counts.

     Keyed by version as well as name, because an interval belongs to a
     (name, version) pair — `_intervals.version` is written as
     `snapshot.version`, and SQLMesh's own accessor is
     `hydrate_with_intervals_by_version`. A plan records its snapshot rows
     before backfilling them, so one interrupted in between leaves a current
     version with no interval beside a previous version that has them; grouped
     by name alone, the old version's execution is attributed to the new one
     and content that never ran reports as freshly built. */
  SELECT
    REGEXP_REPLACE(REPLACE(name, '"', ''), '^[^.]+\.', '') AS model_name,
    version,
    MAX(created_ts) AS last_executed_ms
  FROM sqlmesh._intervals
  WHERE
    NOT is_dev AND NOT is_removed
  GROUP BY
    model_name,
    version
)
SELECT
  l.model_name, /* Schema-qualified model name, e.g. 'core.dim_accounts', 'seeds.categories'. */
  EPOCH_MS(c.version_first_seen_ms)::TIMESTAMP AS last_changed_at, /* When the current content version of this model was first materialized. Advances only when model definition or dependencies change. */
  EPOCH_MS(l.last_applied_ms)::TIMESTAMP AS last_applied_at, /* When SQLMesh last wrote to any snapshot row for this model. Underlying source is `_snapshots.updated_ts`, which captures snapshot-record updates (push/touch/unpause/state changes), not strict model-execution events. Advances on every apply; metadata-only touches (restore, environment promotion) also bump it. Use `last_executed_at` for strict "was this model rebuilt?" semantics. */
  EPOCH_MS(e.last_executed_ms)::TIMESTAMP AS last_executed_at, /* When this model's CURRENT content version was last actually backfilled, from `_intervals.created_ts`. Unlike `last_applied_at` it does NOT advance when the environment is merely promoted, so a selective plan (`transform restate --model`, a seed-only plan) leaves untouched models reading their true age. NULL when the current version has no interval of its own: it was registered but never backfilled, a plan recorded it and failed before the backfill, or the model is symbolic and never executes at all — check `model_kind` to tell the symbolic case apart. An earlier version's intervals are never borrowed to fill this in. UTC, like its siblings here. */
  c.kind_name AS model_kind /* SQLMesh model kind for the current version: 'FULL', 'VIEW', 'SEED', 'EXTERNAL', 'EMBEDDED', an INCREMENTAL_* variant, etc. 'EXTERNAL' and 'EMBEDDED' are symbolic — SQLMesh never executes them, so their `last_executed_at` is always NULL and freshness consumers must exclude them. NULL for a snapshot written before SQLMesh recorded the kind. */
FROM latest_per_name AS l
JOIN current_version_per_name AS c
  USING (model_name)
LEFT JOIN executions AS e
  ON c.model_name = e.model_name AND c.version = e.version
