MODEL (
  name prep.int_investment_events__source_projection,
  dialect duckdb,
  kind VIEW
);

/* Active projection and retained opening contexts share exact revisions and choices.
   The scope is derived, never a second membership authority or value snapshot. */
WITH normalized AS (
  SELECT
    source_type,
    source_origin,
    native_reference,
    observation_version,
    type,
    subtype,
    trade_date,
    settlement_date,
    original_acquisition_date,
    quantity,
    price,
    amount,
    fees,
    description,
    trade_date_basis,
    NULL::TEXT AS provider_type,
    NULL::TEXT AS provider_subtype
  FROM prep.int_investment_events__observations
  WHERE
    source_type = 'manual'
  UNION ALL
  SELECT
    source_type,
    source_origin,
    investment_transaction_id AS native_reference,
    observation_version,
    type,
    subtype,
    trade_date,
    settlement_date,
    original_acquisition_date,
    quantity,
    price,
    amount,
    fees,
    description,
    trade_date_basis,
    provider_type,
    provider_subtype
  FROM prep.stg_plaid__investment_revisions
), member_scopes AS (
  SELECT
    'active' AS projection_scope,
    membership_id
  FROM app.investment_event_members
  WHERE
    retired_at IS NULL AND member_kind = 'source_event'
  UNION ALL
  SELECT
    o.membership_id,
    j.value ->> '$'
  FROM app.investment_event_members AS o, JSON_EACH(o.reconstruction_evidence, '$.source_member_ids') AS j
  WHERE
    o.retired_at IS NULL AND o.member_kind = 'opening_lot_reconstruction'
), scoped AS (
  SELECT
    c.projection_scope,
    m.membership_id,
    m.investment_transaction_id,
    m.event_group_id,
    m.bound_account_id AS account_id,
    m.bound_security_id AS security_id,
    m.bound_effective_currency AS currency_code,
    m.semantic_leg_role AS leg_role,
    n.*,
    m.created_at,
    m.projection_changed_at
  FROM member_scopes AS c
  JOIN app.investment_event_members AS m
    USING (membership_id)
  JOIN normalized AS n
    ON n.source_type = m.source_type
    AND n.source_origin = m.source_origin
    AND n.native_reference = m.native_reference
    AND n.observation_version = m.observation_version
), resolution_scopes AS (
  SELECT
    'active' AS projection_scope,
    resolution_id
  FROM app.investment_match_field_resolutions
  WHERE
    retired_at IS NULL
  UNION ALL
  SELECT
    o.membership_id,
    j.value ->> '$'
  FROM app.investment_event_members AS o, JSON_EACH(o.reconstruction_evidence, '$.resolution_ids') AS j
  WHERE
    o.retired_at IS NULL AND o.member_kind = 'opening_lot_reconstruction'
), explicit AS (
  SELECT
    c.projection_scope,
    r.investment_transaction_id,
    MAX(CASE WHEN r.field_name = 'trade_date' THEN s.trade_date END) AS trade_date,
    COUNT_IF(r.field_name = 'trade_date') > 0 AS has_trade_date,
    MAX(CASE WHEN r.field_name = 'settlement_date' THEN s.settlement_date END) AS settlement_date,
    COUNT_IF(r.field_name = 'settlement_date') > 0 AS has_settlement_date,
    MAX(
      CASE
        WHEN r.field_name = 'original_acquisition_date'
        THEN s.original_acquisition_date
      END
    ) AS original_acquisition_date,
    COUNT_IF(r.field_name = 'original_acquisition_date') > 0 AS has_original_acquisition_date,
    MAX(CASE WHEN r.field_name = 'subtype' THEN s.subtype END) AS subtype,
    COUNT_IF(r.field_name = 'subtype') > 0 AS has_subtype,
    MAX(CASE WHEN r.field_name = 'quantity' THEN s.quantity END) AS quantity,
    COUNT_IF(r.field_name = 'quantity') > 0 AS has_quantity,
    MAX(CASE WHEN r.field_name = 'price' THEN s.price END) AS price,
    COUNT_IF(r.field_name = 'price') > 0 AS has_price,
    MAX(CASE WHEN r.field_name = 'amount' THEN s.amount END) AS amount,
    COUNT_IF(r.field_name = 'amount') > 0 AS has_amount,
    MAX(CASE WHEN r.field_name = 'fees' THEN s.fees END) AS fees,
    COUNT_IF(r.field_name = 'fees') > 0 AS has_fees,
    MAX(CASE WHEN r.field_name = 'trade_date' THEN s.trade_date_basis END) AS trade_date_basis
  FROM resolution_scopes AS c
  JOIN app.investment_match_field_resolutions AS r
    USING (resolution_id)
  JOIN scoped AS s
    ON s.projection_scope = c.projection_scope
    AND s.investment_transaction_id = r.investment_transaction_id
    AND s.source_type = r.source_type
    AND s.source_origin = r.source_origin
    AND s.native_reference = r.native_reference
    AND s.observation_version = r.observation_version
  GROUP BY
    c.projection_scope,
    r.investment_transaction_id
), defaults AS (
  SELECT
    projection_scope,
    investment_transaction_id,
    FIRST_VALUE(event_group_id) OVER (
      PARTITION BY projection_scope, investment_transaction_id
      ORDER BY source_type = 'manual', source_type, source_origin, native_reference
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS event_group_id,
    FIRST_VALUE(account_id) OVER (
      PARTITION BY projection_scope, investment_transaction_id
      ORDER BY source_type = 'manual', source_type, source_origin, native_reference
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS account_id,
    FIRST_VALUE(security_id) OVER (
      PARTITION BY projection_scope, investment_transaction_id
      ORDER BY source_type = 'manual', source_type, source_origin, native_reference
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS security_id,
    FIRST_VALUE(leg_role) OVER (
      PARTITION BY projection_scope, investment_transaction_id
      ORDER BY source_type = 'manual', source_type, source_origin, native_reference
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS leg_role,
    FIRST_VALUE(type) OVER (
      PARTITION BY projection_scope, investment_transaction_id
      ORDER BY source_type = 'manual', source_type, source_origin, native_reference
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS type,
    FIRST_VALUE(provider_type) OVER (
      PARTITION BY projection_scope, investment_transaction_id
      ORDER BY source_type = 'manual', source_type, source_origin, native_reference
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS provider_type,
    FIRST_VALUE(provider_subtype) OVER (
      PARTITION BY projection_scope, investment_transaction_id
      ORDER BY source_type = 'manual', source_type, source_origin, native_reference
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS provider_subtype,
    FIRST_VALUE(source_type) OVER (
      PARTITION BY projection_scope, investment_transaction_id
      ORDER BY source_type = 'manual', source_type, source_origin, native_reference
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS source_type,
    FIRST_VALUE(source_origin) OVER (
      PARTITION BY projection_scope, investment_transaction_id
      ORDER BY source_type = 'manual', source_type, source_origin, native_reference
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS source_origin,
    FIRST_VALUE(native_reference) OVER (
      PARTITION BY projection_scope, investment_transaction_id
      ORDER BY source_type = 'manual', source_type, source_origin, native_reference
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS native_reference,
    FIRST_VALUE(observation_version) OVER (
      PARTITION BY projection_scope, investment_transaction_id
      ORDER BY source_type = 'manual', source_type, source_origin, native_reference
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS observation_version,
    FIRST_VALUE(description) OVER (
      PARTITION BY projection_scope, investment_transaction_id
      ORDER BY source_type = 'manual', source_type, source_origin, native_reference
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS description,
    FIRST_VALUE(currency_code) OVER (
      PARTITION BY projection_scope, investment_transaction_id
      ORDER BY source_type = 'manual', source_type, source_origin, native_reference
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS currency_code,
    FIRST_VALUE(trade_date IGNORE NULLS) OVER (
      PARTITION BY projection_scope, investment_transaction_id
      ORDER BY trade_date_basis <> 'explicit', source_type = 'manual', source_type, source_origin, native_reference
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS trade_date,
    FIRST_VALUE(settlement_date IGNORE NULLS) OVER (
      PARTITION BY projection_scope, investment_transaction_id
      ORDER BY source_type = 'manual', source_type, source_origin, native_reference
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS settlement_date,
    FIRST_VALUE(original_acquisition_date IGNORE NULLS) OVER (
      PARTITION BY projection_scope, investment_transaction_id
      ORDER BY source_type = 'manual', source_type, source_origin, native_reference
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS original_acquisition_date,
    FIRST_VALUE(subtype IGNORE NULLS) OVER (
      PARTITION BY projection_scope, investment_transaction_id
      ORDER BY source_type = 'manual', source_type, source_origin, native_reference
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS subtype,
    FIRST_VALUE(quantity IGNORE NULLS) OVER (
      PARTITION BY projection_scope, investment_transaction_id
      ORDER BY source_type = 'manual', source_type, source_origin, native_reference
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS quantity,
    FIRST_VALUE(price IGNORE NULLS) OVER (
      PARTITION BY projection_scope, investment_transaction_id
      ORDER BY source_type = 'manual', source_type, source_origin, native_reference
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS price,
    FIRST_VALUE(amount IGNORE NULLS) OVER (
      PARTITION BY projection_scope, investment_transaction_id
      ORDER BY source_type = 'manual', source_type, source_origin, native_reference
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS amount,
    FIRST_VALUE(fees IGNORE NULLS) OVER (
      PARTITION BY projection_scope, investment_transaction_id
      ORDER BY source_type = 'manual', source_type, source_origin, native_reference
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS fees,
    FIRST_VALUE(trade_date_basis) OVER (
      PARTITION BY projection_scope, investment_transaction_id
      ORDER BY trade_date_basis <> 'explicit', source_type = 'manual', source_type, source_origin, native_reference
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS trade_date_basis,
    MIN(created_at) OVER (PARTITION BY projection_scope, investment_transaction_id) AS created_at,
    MAX(projection_changed_at) OVER (PARTITION BY projection_scope, investment_transaction_id) AS updated_at
  FROM scoped
  QUALIFY
    ROW_NUMBER() OVER (
      PARTITION BY projection_scope, investment_transaction_id
      ORDER BY source_type = 'manual', source_type, source_origin, native_reference
    ) = 1
)
SELECT
  d.projection_scope,
  d.investment_transaction_id,
  d.event_group_id,
  d.account_id,
  d.security_id,
  d.native_reference,
  d.observation_version,
  d.leg_role,
  d.type,
  CASE WHEN e.has_subtype THEN e.subtype ELSE d.subtype END AS subtype,
  d.provider_type,
  d.provider_subtype,
  d.source_type,
  d.source_origin,
  d.description,
  CASE WHEN e.has_trade_date THEN e.trade_date_basis ELSE d.trade_date_basis END AS trade_date_basis,
  d.currency_code,
  CASE WHEN e.has_quantity THEN e.quantity ELSE d.quantity END AS quantity,
  CASE WHEN e.has_price THEN e.price ELSE d.price END AS price,
  CASE WHEN e.has_amount THEN e.amount ELSE d.amount END AS amount,
  CASE WHEN e.has_fees THEN e.fees ELSE d.fees END AS fees,
  CASE WHEN e.has_trade_date THEN e.trade_date ELSE d.trade_date END AS trade_date,
  CASE WHEN e.has_settlement_date THEN e.settlement_date ELSE d.settlement_date END AS settlement_date,
  CASE
    WHEN e.has_original_acquisition_date
    THEN e.original_acquisition_date
    ELSE d.original_acquisition_date
  END AS original_acquisition_date,
  d.created_at,
  d.updated_at
FROM defaults AS d
LEFT JOIN explicit AS e
  USING (projection_scope, investment_transaction_id)
