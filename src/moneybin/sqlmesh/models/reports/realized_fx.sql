/* Realized FX gain/loss per disposal and lot allocation; unmatched inventory has a null lot. */
MODEL (
  name reports.realized_fx,
  kind VIEW
);

SELECT
  g.realized_fx_gain_id, /* Stable realized FX gain identifier */
  g.conversion_id, /* Disposal conversion identifier */
  g.currency_lot_id, /* Consumed Currency lot; NULL for unmatched-inventory placeholders */
  g.account_id, /* Holding Account identifier */
  c.transfer_pair_id, /* Accepted Transfer Decision linking the disposal legs */
  c.from_transaction_id, /* Canonical sent-leg Transaction identifier */
  c.to_transaction_id, /* Canonical received-leg Transaction identifier */
  c.from_source_transaction_id, /* Native reference supplying the sent leg */
  c.to_source_transaction_id, /* Native reference supplying the received leg */
  l.source_conversion_id, /* Conversion that originally acquired the consumed lot */
  l.source_investment_transaction_id, /* Foreign Security sale that originally acquired the lot */
  l.source_transfer_id, /* Accepted same-Currency Transfer that last moved the lot */
  a.display_name AS account_name, /* Account display name */
  g.currency_code, /* ISO 4217 Currency disposed */
  g.home_currency, /* ISO 4217 Home currency for accounting amounts */
  c.from_currency, /* ISO 4217 Currency sent in the conversion */
  c.to_currency, /* ISO 4217 Currency received in the conversion */
  c.source_shape, /* linked_two_row or single_row */
  l.acquisition_type, /* conversion, security_sale, or transfer */
  g.cost_basis_method, /* FIFO or average cost basis */
  g.valuation_source_type, /* Actual, override, or Provider valuation source */
  c.from_source_type, /* Sent-leg Source type */
  c.from_source_origin, /* Sent-leg Source origin */
  c.to_source_type, /* Received-leg Source type */
  c.to_source_origin, /* Received-leg Source origin */
  g.coverage_status, /* complete or incomplete accounting coverage */
  g.coverage_reason, /* Closed reason when accounting coverage is incomplete */
  g.acquisition_date, /* Date the consumed Currency was acquired */
  g.disposal_date, /* Date the Currency was disposed */
  g.valuation_rate_date, /* Date of the actual terms or stored valuation rate */
  c.executed_rate, /* Actual received units per sent unit */
  g.valuation_rate, /* Rate used for Home-currency proceeds */
  GREATEST(g.updated_at, l.updated_at, c.updated_at) AS updated_at, /* Latest contributing input timestamp */
  c.from_amount, /* Positive magnitude actually sent in from_currency */
  c.to_amount, /* Positive magnitude actually received in to_currency */
  g.disposed_amount, /* Positive amount of Currency disposed */
  g.proceeds, /* Home-currency disposal proceeds */
  g.cost_basis, /* Home-currency basis of the disposed Currency */
  g.fee_amount, /* Home-currency fee allocated to the disposal */
  g.gain_loss /* Home-currency proceeds less basis; positive is a gain */
FROM core.fct_realized_fx_gains AS g
LEFT JOIN core.fct_currency_lots AS l
  ON g.currency_lot_id = l.currency_lot_id
LEFT JOIN core.bridge_currency_conversions AS c
  ON g.conversion_id = c.conversion_id
LEFT JOIN core.dim_accounts AS a
  ON g.account_id = a.account_id
