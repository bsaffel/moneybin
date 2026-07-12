MODEL (
  name prep.stg_manual__investment_transactions,
  kind VIEW
);

SELECT
  t.source_transaction_id,
  t.source_type,
  t.source_origin,
  t.import_id,
  t.account_id, /* Already canonical: manual entry resolves the account interactively at entry time (identifiers.md Guard 2) */
  t.security_id, /* Already canonical: resolved at entry; NULL for cash-only events */
  t.security_ref,
  t.type,
  t.subtype,
  t.event_group_id,
  t.trade_date::DATE AS trade_date,
  t.settlement_date::DATE AS settlement_date,
  t.original_acquisition_date::DATE AS original_acquisition_date,
  t.quantity::DECIMAL(28, 10) AS quantity,
  t.price::DECIMAL(28, 10) AS price,
  t.amount::DECIMAL(18, 2) AS amount,
  t.fees::DECIMAL(18, 2) AS fees,
  COALESCE(t.currency_code, 'USD') AS currency_code,
  t.description,
  t.created_at,
  t.created_by,
  t.investment_transaction_id
FROM raw.manual_investment_transactions AS t
