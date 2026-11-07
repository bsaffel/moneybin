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
  /* Compaction replaces created_ts without rebuilding the physical table.
     SQLMesh preserves last_altered_ts, set after evaluation and audit, through
     compaction. Legacy NULL alteration times remain unknown until a rebuild.
     Exclude dev, removed, and pending-restatement rows, which cannot prove a
     completed production execution. Keep the version join: a new unbuilt
     version must never borrow its predecessor's execution. */
  SELECT
    REGEXP_REPLACE(REPLACE(name, '"', ''), '^[^.]+\.', '') AS model_name,
    version,
    MAX(last_altered_ts) AS last_executed_ms
  FROM sqlmesh._intervals
  WHERE
    NOT is_dev AND NOT is_removed AND NOT is_pending_restatement
  GROUP BY
    model_name,
    version
)
SELECT
  l.model_name, /* Schema-qualified model name, e.g. 'core.dim_accounts', 'seeds.categories'. */
  MAKE_TIMESTAMPTZ(c.version_first_seen_ms * 1000) AS last_changed_at, /* When the current content version of this model was first materialized. Advances only when model definition or dependencies change. */
  MAKE_TIMESTAMPTZ(l.last_applied_ms * 1000) AS last_applied_at, /* When SQLMesh last wrote to any snapshot row for this model. Underlying source is `_snapshots.updated_ts`, which captures snapshot-record updates (push/touch/unpause/state changes), not strict model-execution events. Advances on every apply; metadata-only touches (restore, environment promotion) also bump it. Use `last_executed_at` for strict "was this model rebuilt?" semantics. */
  MAKE_TIMESTAMPTZ(e.last_executed_ms * 1000) AS last_executed_at, /* Physical execution time for the current version, preserved through interval compaction; NULL when execution evidence is unavailable. */
  c.kind_name AS model_kind /* SQLMesh model kind for the current version: 'FULL', 'VIEW', 'SEED', 'EXTERNAL', 'EMBEDDED', an INCREMENTAL_* variant, etc. 'EXTERNAL' and 'EMBEDDED' are symbolic — SQLMesh never executes them, so their `last_executed_at` is always NULL and freshness consumers must exclude them. NULL for a snapshot written before SQLMesh recorded the kind. */
FROM latest_per_name AS l
JOIN current_version_per_name AS c
  USING (model_name)
LEFT JOIN executions AS e
  ON c.model_name = e.model_name AND c.version = e.version
