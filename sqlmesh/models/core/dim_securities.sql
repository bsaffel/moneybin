/* Canonical securities dimension; v1 projects the manually-maintained catalog,
   structured as a union so importers add resolved securities from their own
   staging models later (same extension pattern as core.fct_asset_valuations). */
MODEL (
  name core.dim_securities,
  kind VIEW
);

SELECT
  security_id, /* Stable surrogate key */
  name, /* Display name */
  security_type, /* equity | etf | mutual_fund | bond | crypto | cash | other */
  ticker, /* Display/lookup ticker (carry the ID per identifiers.md Guard 1) */
  exchange, /* Listing exchange */
  cusip, /* Licensed identifier; present only if user-supplied */
  isin, /* International identifier */
  figi, /* OpenFIGI mapping */
  coingecko_id, /* Crypto price-lookup slug (Pillar C) */
  is_cash_equivalent, /* Treat-like-cash flag (money-market/sweep) */
  currency_code /* Denominating currency */
FROM app.securities /* Future: UNION ALL resolved securities from prep.stg_plaid__securities, etc. */
