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
), edges AS (
  /* Undirected edges. Node identity is the (source_type, source_transaction_id)
     pair scoped by account_id, carried as SEPARATE columns — never packed into a
     delimited string. A source_transaction_id may itself contain '|' (tabular
     imports keep source IDs as raw strings), so delimiter-based packing + later
     SPLIT_PART would truncate the id and mis-group the row. */
  SELECT
    account_id AS aid,
    source_type_a AS src_st,
    source_transaction_id_a AS src_stid,
    source_type_b AS dst_st,
    source_transaction_id_b AS dst_stid
  FROM active_matches
  UNION ALL
  SELECT
    account_id,
    source_type_b,
    source_transaction_id_b,
    source_type_a,
    source_transaction_id_a
  FROM active_matches
), nodes AS (
  SELECT DISTINCT
    aid,
    src_st AS st,
    src_stid AS stid
  FROM edges
), reach AS (
  /* Transitive closure via recursive label propagation, comparing on the
     (source_type, source_transaction_id) pair. UNION (not UNION ALL) terminates
     at the fixpoint — no new (aid, node, member) rows to add. */
  SELECT
    aid,
    st,
    stid,
    st AS mem_st,
    stid AS mem_stid
  FROM nodes
  UNION
  SELECT
    r.aid,
    r.st,
    r.stid,
    e.dst_st,
    e.dst_stid
  FROM reach AS r
  JOIN edges AS e
    ON r.aid = e.aid AND r.mem_st = e.src_st AND r.mem_stid = e.src_stid
), match_groups AS (
  /* source_type and source_transaction_id are recovered from the carried
     columns (no splitting). group_id is account_id prefixed onto the
     lexicographic MIN packed member of the component — an opaque label that is
     only ever grouped/joined/counted on, never split back apart. The account
     prefix makes it GLOBALLY unique: source_transaction_id is only unique within
     an account (source-provided ids can repeat across accounts), so without it
     two accounts sharing a min member would collide into one group_id and
     conflate their gold keys, confidence, and match_group_id counts. */
  SELECT
    aid AS account_id,
    st AS source_type,
    stid AS source_transaction_id,
    aid || '|' || MIN(mem_st || '|' || mem_stid) AS group_id
  FROM reach
  GROUP BY
    aid,
    st,
    stid
), group_members AS (
  /* Each group member's immutable source identity + loaded_at + intrinsic
     stability rank, used to pick a single anchor member to derive the gold
     transaction_id from. account_id is deliberately ABSENT from the identity
     tuple: it is a mutable canonical surrogate (M1S), so including it would
     re-key every transaction whenever an account is re-minted and orphan all
     app.* curation. source_account_key (the raw source-native account key) is
     always present and is the NULL-safe immutable stand-in. */
  SELECT
    mg.group_id,
    u.source_type,
    u.source_origin,
    u.source_account_key,
    u.source_transaction_id,
    u.loaded_at,
    CASE u.source_type WHEN 'ofx' THEN 0 WHEN 'plaid' THEN 0 WHEN 'manual' THEN 1 ELSE 2 END AS stability_rank
  FROM match_groups AS mg
  JOIN prep.int_transactions__unioned AS u
    ON u.source_type = mg.source_type
    AND u.source_transaction_id = mg.source_transaction_id
    AND u.account_id = mg.account_id
), group_anchor AS (
  /* Anchor = argmin over members of (stability_rank, loaded_at, source_type,
     source_origin, source_account_key, source_transaction_id). Native ids
     (ofx/plaid, rank 0) outrank minted (manual, rank 1) outrank content-hashed
     (csv/tabular/gsheet, rank 2); loaded_at then prefers the earliest-seen
     member. The trailing identity columns make the order total/deterministic. */
  SELECT
    group_id,
    source_type,
    source_origin,
    source_account_key,
    source_transaction_id,
    ROW_NUMBER() OVER (
      PARTITION BY group_id
      ORDER BY stability_rank, loaded_at, source_type, source_origin, source_account_key, source_transaction_id
    ) AS _rn
  FROM group_members
), group_gold_keys AS (
  SELECT
    group_id,
    SUBSTRING(
      SHA256(
        source_type || '|' || source_origin || '|' || source_account_key || '|' || source_transaction_id
      ),
      1,
      16
    ) AS transaction_id
  FROM group_anchor
  WHERE
    _rn = 1
), group_confidence AS (
  SELECT
    mg.group_id,
    MIN(am.confidence_score) AS match_confidence
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
      SHA256(
        u.source_type || '|' || u.source_origin || '|' || u.source_account_key || '|' || u.source_transaction_id
      ),
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
