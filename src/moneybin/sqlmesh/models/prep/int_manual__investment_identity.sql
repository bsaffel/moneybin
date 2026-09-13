MODEL (
  name prep.int_manual__investment_identity,
  dialect duckdb,
  kind VIEW,
  grain source_transaction_id
);

WITH RECURSIVE edges AS (
  SELECT
    provisional_account_id,
    MIN(candidate_account_id) AS candidate_account_id,
    COUNT(DISTINCT candidate_account_id) AS target_count
  FROM app.account_link_decisions
  WHERE
    status = 'accepted' AND reversed_at IS NULL
  GROUP BY
    provisional_account_id
), walk AS (
  SELECT DISTINCT
    account_id AS frozen_account_id,
    account_id,
    [account_id] AS path,
    FALSE AS cycle
  FROM raw.manual_investment_transactions
  UNION ALL
  SELECT
    w.frozen_account_id,
    e.candidate_account_id,
    LIST_APPEND(w.path, e.candidate_account_id),
    ARRAY_CONTAINS(w.path, e.candidate_account_id)
  FROM walk AS w
  JOIN edges AS e
    ON e.provisional_account_id = w.account_id
  WHERE
    NOT w.cycle AND e.target_count = 1
), terminals AS (
  SELECT
    w.frozen_account_id,
    w.path,
    CASE
      WHEN NOT w.cycle
      AND e.provisional_account_id IS NULL
      AND (
        LENGTH(w.path) = 1
        OR EXISTS(
          SELECT
            1
          FROM app.account_links AS a
          WHERE
            a.account_id = w.account_id
            AND a.ref_kind = 'source_native'
            AND a.status = 'accepted'
        )
      )
      THEN w.account_id
    END AS account_id
  FROM walk AS w
  LEFT JOIN edges AS e
    ON e.provisional_account_id = w.account_id
  WHERE
    w.cycle OR COALESCE(e.target_count, 0) <> 1
), account_mutations AS (
  SELECT
    UNNEST(
      [
        before_value ->> '$.account_id',
        after_value ->> '$.account_id',
        before_value ->> '$.provisional_account_id',
        after_value ->> '$.provisional_account_id'
      ]
    ) AS account_id,
    audit_id
  FROM app.audit_log
  WHERE
    target_schema = 'app'
    AND target_table IN ('account_links', 'account_link_decisions')
    AND (
      (
        before_value ->> '$.status'
      ) = 'accepted'
      OR (
        after_value ->> '$.status'
      ) = 'accepted'
    )
), account_generations AS (
  SELECT
    t.frozen_account_id,
    SHA256(COALESCE(LISTAGG(DISTINCT m.audit_id, ','
    ORDER BY
      m.audit_id), '')) AS generation
  FROM terminals AS t
  LEFT JOIN account_mutations AS m
    ON ARRAY_CONTAINS(t.path, m.account_id)
  GROUP BY
    t.frozen_account_id
), security_generations AS (
  SELECT
    COALESCE(after_value ->> '$.ref_value', before_value ->> '$.ref_value') AS source_transaction_id,
    SHA256(LISTAGG(audit_id, ','
    ORDER BY
      audit_id)) AS generation
  FROM app.audit_log
  WHERE
    target_schema = 'app'
    AND target_table = 'security_links'
    AND COALESCE(after_value ->> '$.source_type', before_value ->> '$.source_type') = 'manual'
    AND COALESCE(after_value ->> '$.ref_kind', before_value ->> '$.ref_kind') = 'manual_investment_transaction_id'
  GROUP BY
    source_transaction_id
), security_routes AS (
  SELECT
    ref_value AS source_transaction_id,
    MIN(security_id) AS security_id,
    COUNT(*) AS route_count
  FROM app.security_links
  WHERE
    source_type = 'manual'
    AND ref_kind = 'manual_investment_transaction_id'
    AND status = 'accepted'
  GROUP BY
    ref_value
), resolved AS (
  SELECT
    t.source_transaction_id,
    a.account_id,
    CASE
      WHEN s.route_count IS NULL
      THEN t.security_id
      WHEN s.route_count = 1
      THEN s.security_id
    END AS security_id,
    t.account_id AS frozen_account_id,
    t.security_id AS frozen_security_id
  FROM raw.manual_investment_transactions AS t
  LEFT JOIN terminals AS a
    ON a.frozen_account_id = t.account_id
  LEFT JOIN security_routes AS s
    ON s.source_transaction_id = t.source_transaction_id
)
SELECT
  r.source_transaction_id,
  r.account_id,
  r.security_id,
  r.frozen_account_id,
  r.frozen_security_id,
  COALESCE(a.generation, SHA256('')) AS account_identity_generation,
  COALESCE(s.generation, SHA256('')) AS security_identity_generation,
  NOT r.account_id IS NULL AS account_identity_resolved,
  EXISTS(
    SELECT
      1
    FROM app.securities AS s
    WHERE
      s.security_id = r.security_id
  ) AS security_identity_resolved
FROM resolved AS r
LEFT JOIN account_generations AS a
  ON a.frozen_account_id = r.frozen_account_id
LEFT JOIN security_generations AS s
  ON s.source_transaction_id = r.source_transaction_id
