MODEL (
  name prep.stg_manual__investment_transactions,
  kind VIEW
);

SELECT
  t.source_transaction_id,
  t.source_type,
  t.source_origin,
  t.import_id,
  i.account_id,
  i.security_id,
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
  t.currency_code, /* Verbatim: NULL means the user named none, and core.fct_investment_transactions inherits the account's (multi-currency.md Requirement 3) */
  t.description,
  t.created_at,
  t.created_by,
  t.investment_transaction_id
FROM raw.manual_investment_transactions AS t
LEFT JOIN prep.int_manual__investment_identity AS i
  ON i.source_transaction_id = t.source_transaction_id
