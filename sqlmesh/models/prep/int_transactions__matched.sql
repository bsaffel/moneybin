MODEL (
  name prep.int_transactions__matched,
  kind VIEW
);

WITH active_matches AS (
  SELECT
    match_id,
    source_transaction_id_a,
    source_type_a,
    source_origin_a,
    source_transaction_id_b,
    source_type_b,
    source_origin_b,
    account_id,
    confidence_score
  FROM app.match_decisions
  WHERE
    match_status = 'accepted' AND reversed_at IS NULL AND match_type = 'dedup'
), node_min_match AS (
  SELECT
    st,
    stid,
    aid,
    MIN(match_id) AS initial_component
  FROM (
    SELECT
      source_type_a AS st,
      source_transaction_id_a AS stid,
      account_id AS aid,
      match_id
    FROM active_matches
    UNION ALL
    SELECT
      source_type_b AS st,
      source_transaction_id_b AS stid,
      account_id AS aid,
      match_id
    FROM active_matches
  ) AS sub
  GROUP BY
    st,
    stid,
    aid
), match_component AS (
  SELECT
    am.match_id,
    LEAST(n1.initial_component, n2.initial_component) AS component
  FROM active_matches AS am
  JOIN node_min_match AS n1
    ON am.source_type_a = n1.st
    AND am.source_transaction_id_a = n1.stid
    AND am.account_id = n1.aid
  JOIN node_min_match AS n2
    ON am.source_type_b = n2.st
    AND am.source_transaction_id_b = n2.stid
    AND am.account_id = n2.aid
/* NOTE: This single-pass connected-component algorithm is correct only when
   each transaction participates in at most one match (1:1 bipartite assignment).
   The greedy matcher enforces this invariant. If multi-hop matches are added
   (e.g., 3+ way merges), replace with a recursive CTE label-propagation. */
), match_groups AS (
  SELECT
    st AS source_type,
    stid AS source_transaction_id,
    aid AS account_id,
    MIN(mc.component) AS group_id
  FROM (
    SELECT
      source_type_a AS st,
      source_transaction_id_a AS stid,
      account_id AS aid,
      mc.component
    FROM active_matches AS am
    JOIN match_component AS mc
      ON am.match_id = mc.match_id
    UNION ALL
    SELECT
      source_type_b AS st,
      source_transaction_id_b AS stid,
      account_id AS aid,
      mc.component
    FROM active_matches AS am
    JOIN match_component AS mc
      ON am.match_id = mc.match_id
  ) AS sub
  JOIN match_component AS mc
    ON sub.component = mc.component
  GROUP BY
    st,
    stid,
    aid
), group_gold_keys AS (
  SELECT
    mg.group_id,
    SUBSTRING(
      SHA256(
        LISTAGG(
          mg.source_type || '|' || mg.source_transaction_id || '|' || mg.account_id, '|'
          ORDER BY
            mg.source_type,
            mg.source_transaction_id,
            mg.account_id
        )
      ),
      1,
      16
    ) AS transaction_id
  FROM match_groups AS mg
  GROUP BY
    mg.group_id
), group_confidence AS (
  SELECT
    mg.group_id,
    MAX(am.confidence_score) AS match_confidence
  FROM match_groups AS mg
  JOIN active_matches AS am
    ON (
      (
        mg.source_type = am.source_type_a
        AND mg.source_transaction_id = am.source_transaction_id_a
        AND mg.account_id = am.account_id
      )
      OR (
        mg.source_type = am.source_type_b
        AND mg.source_transaction_id = am.source_transaction_id_b
        AND mg.account_id = am.account_id
      )
    )
  GROUP BY
    mg.group_id
)
SELECT
  CASE
    WHEN gk.transaction_id IS NOT NULL
    THEN gk.transaction_id
    ELSE SUBSTRING(
      SHA256(u.source_type || '|' || u.source_transaction_id || '|' || u.account_id),
      1,
      16
    )
  END AS transaction_id,
  u.source_transaction_id,
  u.account_id,
  u.transaction_date,
  u.authorized_date,
  u.amount,
  u.description,
  u.merchant_name,
  u.memo,
  u.category,
  u.subcategory,
  u.payment_channel,
  u.transaction_type,
  u.check_number,
  u.is_pending,
  u.pending_transaction_id,
  u.location_address,
  u.location_city,
  u.location_region,
  u.location_postal_code,
  u.location_country,
  u.location_latitude,
  u.location_longitude,
  u.currency_code,
  u.source_type,
  u.source_origin,
  u.source_file,
  u.source_extracted_at,
  u.loaded_at,
  mg.group_id AS match_group_id,
  gc.match_confidence
FROM prep.int_transactions__unioned AS u
LEFT JOIN match_groups AS mg
  ON u.source_type = mg.source_type
  AND u.source_transaction_id = mg.source_transaction_id
  AND u.account_id = mg.account_id
LEFT JOIN group_gold_keys AS gk
  ON mg.group_id = gk.group_id
LEFT JOIN group_confidence AS gc
  ON mg.group_id = gc.group_id
