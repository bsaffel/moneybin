MODEL (
  name prep.int_investment_events__headers,
  dialect duckdb,
  kind VIEW
);

SELECT
  source_event_key,
  LIST_SORT(ARRAY_AGG(DISTINCT account_id)) AS account_ids,
  COALESCE(
    LIST_SORT(ARRAY_AGG(DISTINCT security_id) FILTER(WHERE
      NOT security_id IS NULL)),
    []::TEXT[]
  ) AS security_ids,
  source_type,
  source_origin,
  event_type,
  source_type || '_' || SUBSTRING(
    SHA256(
      TO_JSON(
        ARRAY_AGG(
          {'native_reference': native_reference, 'observation_version': observation_version, 'account_id': account_id, 'security_id': security_id, 'account_identity_generation': account_identity_generation, 'security_identity_generation': security_identity_generation, 'currency_code': currency_code, 'account_currency_code': account_currency_code, 'leg_role': leg_role} ORDER BY native_reference
        )
      )
    ),
    1,
    16
  ) AS event_fingerprint,
  COUNT(*) AS member_count,
  BOOL_AND(is_match_eligible::BOOLEAN) AS is_match_eligible,
  BOOL_AND(supports_split::BOOLEAN) AS supports_split,
  BOOL_AND(supports_native_relationships::BOOLEAN) AS supports_native_relationships,
  BOOL_AND(supports_corrections::BOOLEAN) AS supports_corrections,
  BOOL_AND(supports_reversals::BOOLEAN) AS supports_reversals,
  MIN(trade_date) AS date_start,
  MAX(GREATEST(settlement_date, trade_date)) AS date_end
FROM prep.int_investment_events__legs
GROUP BY
  source_event_key,
  source_type,
  source_origin,
  event_type
