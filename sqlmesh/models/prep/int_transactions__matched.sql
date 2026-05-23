MODEL (
  name prep.int_transactions__matched,
  kind VIEW
);

WITH RECURSIVE active_matches AS (
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
), node_key AS (
  /* Emit both directions of each edge so the graph is undirected. */
  SELECT
    source_type_a AS st,
    source_transaction_id_a AS stid,
    account_id AS aid,
    source_type_b AS nst,
    source_transaction_id_b AS nstid
  FROM active_matches
  UNION ALL
  SELECT
    source_type_b,
    source_transaction_id_b,
    account_id,
    source_type_a,
    source_transaction_id_a
  FROM active_matches
), edges AS (
  /* Pack each node as 'source_type|source_transaction_id'.
     Delimiter '|' is safe: source_type is a closed vocabulary (csv, ofx, …)
     and source_transaction_id values are hex content hashes or
     alphanumeric source IDs — neither contains '|'. */
  SELECT
    aid,
    st || '|' || stid AS src,
    nst || '|' || nstid AS dst
  FROM node_key
), nodes AS (
  SELECT DISTINCT
    aid,
    src AS node
  FROM edges
), reach AS (
  /* Transitive closure via recursive label propagation.
     UNION (not UNION ALL) terminates at the fixpoint — no new
     (aid, node, member) triples to add. */
  SELECT
    aid,
    node,
    node AS member
  FROM nodes
  UNION
  SELECT
    r.aid,
    r.node,
    e.dst
  FROM reach AS r
  JOIN edges AS e
    ON r.aid = e.aid AND r.member = e.src
), match_groups AS (
  /* Each node's group_id is the lexicographic MIN of all reachable members.
     All nodes in the same connected component converge to the same MIN. */
  SELECT
    aid AS account_id,
    SPLIT_PART(node, '|', 1) AS source_type,
    SPLIT_PART(node, '|', 2) AS source_transaction_id,
    MIN(member) AS group_id
  FROM reach
  GROUP BY
    aid,
    node
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
    WHEN NOT gk.transaction_id IS NULL
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
