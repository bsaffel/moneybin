MODEL (
  name prep.stg_plaid__securities,
  kind VIEW
);

/* Canonical security_id resolves only through app.security_links (status = 'accepted');
   unlike accounts, there is no source-native fallback here — a provider id
   masquerading as canonical would corrupt downstream lot resolution, so an
   unresolved security surfaces as NULL. The resolver binds on every rung before
   this view runs, so NULL here is a transient failure path that self-heals on the
   next sync (sync-plaid-investments.md). */
SELECT
  links.security_id AS security_id,
  s.security_id AS source_security_key,
  s.institution_security_id,
  s.institution_id,
  s.ticker_symbol AS ticker,
  s.market_identifier_code AS exchange,
  TRIM(s.security_name) AS security_name,
  CASE LOWER(COALESCE(s.security_type, ''))
    WHEN 'equity'
    THEN 'equity'
    WHEN 'etf'
    THEN 'etf'
    WHEN 'mutual fund'
    THEN 'mutual_fund'
    WHEN 'fixed income'
    THEN 'bond'
    WHEN 'cash'
    THEN 'cash'
    WHEN 'cryptocurrency'
    THEN 'crypto'
    ELSE 'other'
  END AS security_type,
  s.close_price,
  s.close_price_as_of,
  COALESCE(s.iso_currency_code, s.unofficial_currency_code) AS currency_code,
  s.cusip,
  s.isin,
  s.is_cash_equivalent,
  s.source_file,
  s.source_type,
  s.source_origin,
  s.extracted_at,
  s.loaded_at
FROM raw.plaid_securities AS s
LEFT JOIN app.security_links AS links
  ON links.status = 'accepted'
  AND links.ref_kind = 'plaid_security_id'
  AND links.source_type = s.source_type
  AND links.ref_value = s.security_id
