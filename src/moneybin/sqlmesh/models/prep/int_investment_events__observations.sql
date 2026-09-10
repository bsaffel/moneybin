MODEL (
  name prep.int_investment_events__observations,
  dialect duckdb,
  kind VIEW
);

/* Hash source values before canonical routing; identity corrections change evidence,
   never the immutable observation version. Descriptions are projection context. */
WITH routing_audits AS (
  SELECT
    audit_id,
    target_table,
    COALESCE(after_value ->> '$.source_type', before_value ->> '$.source_type') AS source_type,
    COALESCE(after_value ->> '$.source_origin', before_value ->> '$.source_origin', '') AS source_origin,
    COALESCE(after_value ->> '$.ref_kind', before_value ->> '$.ref_kind') AS ref_kind,
    COALESCE(after_value ->> '$.ref_value', before_value ->> '$.ref_value') AS ref_value
  FROM app.audit_log
  WHERE
    target_schema = 'app' AND target_table IN ('account_links', 'security_links')
), routing_generations AS (
  SELECT
    target_table,
    source_type,
    source_origin,
    ref_kind,
    ref_value,
    SHA256(TO_JSON(ARRAY_AGG(audit_id ORDER BY audit_id))) AS identity_generation
  FROM routing_audits
  GROUP BY
    target_table,
    source_type,
    source_origin,
    ref_kind,
    ref_value
), plaid_account_routes AS (
  SELECT
    source_type,
    source_origin,
    ref_value,
    CASE WHEN COUNT(*) = 1 THEN MIN(account_id) END AS account_id
  FROM app.account_links
  WHERE
    status = 'accepted' AND ref_kind = 'source_native'
  GROUP BY
    source_type,
    source_origin,
    ref_value
), observations AS (
  SELECT
    m.source_transaction_id AS native_reference,
    m.source_type,
    m.source_origin,
    'manual_' || SUBSTRING(
      SHA256(
        TO_JSON(
          {'native_reference': t.source_transaction_id, 'source_type': t.source_type, 'source_origin': t.source_origin, 'account_id': t.account_id, 'security_id': t.security_id, 'security_ref': t.security_ref, 'type': t.type, 'subtype': t.subtype, 'event_group_id': t.event_group_id, 'trade_date': t.trade_date, 'settlement_date': t.settlement_date, 'original_acquisition_date': t.original_acquisition_date, 'quantity': t.quantity, 'price': t.price, 'amount': t.amount, 'fees': t.fees, 'currency_code': t.currency_code, 'description': t.description}
        )
      ),
      1,
      16
    ) AS observation_version,
    m.investment_transaction_id AS original_investment_transaction_id,
    m.account_id,
    m.security_id,
    i.account_identity_generation,
    i.security_identity_generation,
    m.event_group_id AS source_group_reference,
    LOWER(m.type) AS type,
    CASE
      WHEN LOWER(m.type) = 'reinvest'
      THEN COALESCE(LOWER(m.subtype), 'dividend')
      ELSE LOWER(m.subtype)
    END AS subtype,
    m.currency_code AS source_currency_code,
    m.description,
    'explicit' AS trade_date_basis,
    m.quantity,
    m.price,
    m.amount,
    m.fees,
    TRUE AS supports_split,
    FALSE AS is_unsupported_compound,
    NOT t.security_id IS NULL AS has_source_security,
    m.trade_date,
    m.settlement_date,
    m.original_acquisition_date
  FROM prep.stg_manual__investment_transactions AS m
  JOIN raw.manual_investment_transactions AS t
    ON t.source_transaction_id = m.source_transaction_id
  JOIN prep.int_manual__investment_identity AS i
    ON i.source_transaction_id = m.source_transaction_id
  UNION ALL
  SELECT DISTINCT
    p.investment_transaction_id,
    p.source_type,
    p.source_origin,
    p.observation_version,
    CASE WHEN p.ledger_include THEN p.investment_transaction_id END,
    ar.account_id,
    p.security_id,
    ag.identity_generation,
    sg.identity_generation,
    NULL::TEXT,
    p.type,
    p.subtype,
    p.currency_code,
    p.description,
    p.trade_date_basis,
    p.quantity,
    p.price,
    p.amount,
    p.fees,
    FALSE,
    LOWER(p.provider_subtype) IN ('merger', 'spin off', 'trade'),
    NOT p.source_security_key IS NULL,
    p.trade_date,
    p.settlement_date,
    p.original_acquisition_date
  FROM prep.stg_plaid__investment_transactions AS p
  LEFT JOIN plaid_account_routes AS ar
    ON ar.source_type = p.source_type
    AND ar.source_origin = p.source_origin
    AND ar.ref_value = p.source_account_key
  LEFT JOIN routing_generations AS ag
    ON ag.target_table = 'account_links'
    AND ag.source_type = p.source_type
    AND ag.source_origin = p.source_origin
    AND ag.ref_kind = 'source_native'
    AND ag.ref_value = p.source_account_key
  LEFT JOIN routing_generations AS sg
    ON sg.target_table = 'security_links'
    AND sg.source_type = p.source_type
    AND sg.ref_kind = 'plaid_security_id'
    AND sg.ref_value = p.source_security_key
)
SELECT
  o.native_reference,
  o.observation_version,
  o.original_investment_transaction_id,
  o.account_id,
  o.security_id,
  o.source_group_reference,
  o.source_type,
  o.source_origin,
  o.account_identity_generation,
  o.security_identity_generation,
  o.type,
  o.subtype,
  o.description,
  o.trade_date_basis,
  o.quantity,
  o.price,
  o.amount,
  o.fees,
  o.source_currency_code,
  a.currency_code AS account_currency_code,
  COALESCE(
    NULLIF(UPPER(TRIM(o.source_currency_code)), ''),
    NULLIF(UPPER(TRIM(a.currency_code)), '')
  ) AS currency_code,
  o.supports_split,
  o.is_unsupported_compound,
  o.has_source_security,
  NOT a.account_id IS NULL
  AND (
    NOT o.has_source_security OR NOT s.security_id IS NULL
  )
  AND (
    NOT o.type IN ('buy', 'sell', 'reinvest', 'transfer_in', 'transfer_out', 'split')
    OR NOT s.security_id IS NULL
  ) AS has_resolved_identity,
  FALSE AS supports_native_relationships,
  FALSE AS supports_corrections,
  FALSE AS supports_reversals,
  o.trade_date,
  o.settlement_date,
  o.original_acquisition_date
FROM observations AS o
LEFT JOIN core.dim_accounts AS a
  ON a.account_id = o.account_id
LEFT JOIN app.securities AS s
  ON s.security_id = o.security_id
