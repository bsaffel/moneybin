MODEL (
  name prep.int_investment_events__evidence,
  dialect duckdb,
  kind VIEW
);

/* Pairwise observed evidence, not assignment or acceptance. Descriptions and
   projection-only acquisition dates never establish candidate eligibility. */
WITH legs AS (
  SELECT
    *,
    CASE
      WHEN event_type IN ('buy', 'sell', 'reinvest')
      THEN 5
      WHEN event_type IN ('transfer_in', 'transfer_out', 'deposit', 'withdrawal')
      THEN 7
      WHEN event_type = 'split'
      THEN 0
      ELSE 3
    END AS date_threshold_days
  FROM prep.int_investment_events__legs
), compared AS (
  SELECT
    l.source_event_key AS left_source_event_key,
    r.source_event_key AS right_source_event_key,
    l.native_reference AS left_native_reference,
    r.native_reference AS right_native_reference,
    l.observation_version AS left_observation_version,
    r.observation_version AS right_observation_version,
    l.account_identity_generation AS left_account_identity_generation,
    r.account_identity_generation AS right_account_identity_generation,
    l.security_identity_generation AS left_security_identity_generation,
    r.security_identity_generation AS right_security_identity_generation,
    l.source_type AS left_source_type,
    r.source_type AS right_source_type,
    l.source_origin AS left_source_origin,
    r.source_origin AS right_source_origin,
    l.event_type,
    l.leg_role,
    l.type,
    lh.member_count AS left_member_count,
    rh.member_count AS right_member_count,
    l.trade_date AS left_trade_date,
    r.trade_date AS right_trade_date,
    l.trade_date_basis AS left_trade_date_basis,
    r.trade_date_basis AS right_trade_date_basis,
    l.original_acquisition_date AS left_original_acquisition_date,
    r.original_acquisition_date AS right_original_acquisition_date,
    l.date_threshold_days,
    CASE
      WHEN l.event_type = 'split'
      THEN ABS(DATE_DIFF('DAY', l.trade_date, r.trade_date))
      ELSE LEAST(
        ABS(DATE_DIFF('DAY', l.trade_date, r.trade_date)),
        ABS(DATE_DIFF('DAY', l.trade_date, r.settlement_date)),
        ABS(DATE_DIFF('DAY', l.settlement_date, r.trade_date)),
        ABS(DATE_DIFF('DAY', l.settlement_date, r.settlement_date))
      )
    END AS date_distance_days,
    lh.is_match_eligible
    AND rh.is_match_eligible
    AND l.type = r.type
    AND lh.member_count = rh.member_count
    AND l.account_id = r.account_id
    AND l.security_id IS NOT DISTINCT FROM r.security_id
    AND l.currency_code = r.currency_code AS structure_agrees,
    CASE
      WHEN l.event_type = 'split'
      THEN l.quantity = r.quantity
      ELSE l.quantity IS NULL
      OR r.quantity IS NULL
      OR ABS(l.quantity - r.quantity) <= GREATEST(0.000001, GREATEST(ABS(l.quantity), ABS(r.quantity)) * 0.00000001)
    END AS quantity_within_tolerance,
    l.amount IS NULL OR r.amount IS NULL OR ABS(l.amount - r.amount) <= 0.01 AS amount_within_tolerance,
    ABS(COALESCE(l.fees, 0) - COALESCE(r.fees, 0)) <= 0.01 AS fees_within_tolerance,
    l.price IS NULL
    OR r.price IS NULL
    OR ABS(l.price - r.price) <= GREATEST(0.01, GREATEST(ABS(l.price), ABS(r.price)) * 0.0001)
    OR (
      ABS(l.quantity * l.price + l.amount + COALESCE(l.fees, 0)) <= 0.01
      AND ABS(r.quantity * r.price + r.amount + COALESCE(r.fees, 0)) <= 0.01
    ) AS price_within_tolerance,
    CASE
      WHEN l.type IN ('buy', 'sell', 'reinvest')
      THEN NOT l.quantity IS NULL
      AND NOT r.quantity IS NULL
      AND NOT l.amount IS NULL
      AND NOT r.amount IS NULL
      WHEN l.type IN ('transfer_in', 'transfer_out', 'split')
      THEN NOT l.quantity IS NULL AND NOT r.quantity IS NULL
      ELSE NOT l.amount IS NULL AND NOT r.amount IS NULL
    END AS has_required_economics,
    l.trade_date = r.trade_date
    AND l.quantity IS NOT DISTINCT FROM r.quantity
    AND l.amount IS NOT DISTINCT FROM r.amount
    AND COALESCE(l.fees, 0) = COALESCE(r.fees, 0)
    AND l.price IS NOT DISTINCT FROM r.price AS is_exact_economic_identity,
    COALESCE(
      l.type = 'dividend'
      AND l.subtype IN ('qualified', 'non_qualified')
      AND r.subtype IN ('qualified', 'non_qualified')
      AND l.subtype <> r.subtype
      OR l.type = 'capital_gain_distribution'
      AND l.subtype IN ('short_term', 'long_term')
      AND r.subtype IN ('short_term', 'long_term')
      AND l.subtype <> r.subtype,
      FALSE
    ) AS subtype_conflict
  FROM legs AS l
  JOIN legs AS r
    ON l.event_type = r.event_type
    AND l.leg_role = r.leg_role
    AND l.account_id = r.account_id
    AND l.security_id IS NOT DISTINCT FROM r.security_id
    AND l.currency_code = r.currency_code
    AND CASE
      WHEN l.event_type = 'split'
      THEN ABS(DATE_DIFF('DAY', l.trade_date, r.trade_date))
      ELSE LEAST(
        ABS(DATE_DIFF('DAY', l.trade_date, r.trade_date)),
        ABS(DATE_DIFF('DAY', l.trade_date, r.settlement_date)),
        ABS(DATE_DIFF('DAY', l.settlement_date, r.trade_date)),
        ABS(DATE_DIFF('DAY', l.settlement_date, r.settlement_date))
      )
    END <= l.date_threshold_days
    AND (
      l.source_type < r.source_type
      OR (
        l.source_type = r.source_type AND l.source_origin < r.source_origin
      )
    )
  JOIN prep.int_investment_events__headers AS lh
    ON lh.source_event_key = l.source_event_key
  JOIN prep.int_investment_events__headers AS rh
    ON rh.source_event_key = r.source_event_key
), measured AS (
  SELECT
    *,
    date_distance_days <= date_threshold_days AS date_within_tolerance,
    COALESCE(
      (
        left_trade_date_basis = 'explicit'
        AND right_trade_date_basis = 'explicit'
        AND left_trade_date <> right_trade_date
      )
      OR ABS(DATE_DIFF('DAY', left_trade_date, right_trade_date)) > date_threshold_days,
      FALSE
    ) AS trade_date_conflict,
    COALESCE(left_original_acquisition_date <> right_original_acquisition_date, FALSE) AS original_acquisition_date_conflict,
    CASE
      WHEN left_trade_date_basis = 'explicit'
      AND right_trade_date_basis = 'posting_fallback'
      THEN left_trade_date
      WHEN right_trade_date_basis = 'explicit'
      AND left_trade_date_basis = 'posting_fallback'
      THEN right_trade_date
      WHEN right_source_type = 'plaid'
      THEN right_trade_date
      ELSE left_trade_date
    END AS preferred_trade_date
  FROM compared
)
SELECT
  left_source_event_key,
  right_source_event_key,
  left_native_reference,
  right_native_reference,
  left_observation_version,
  right_observation_version,
  left_source_type,
  right_source_type,
  left_source_origin,
  right_source_origin,
  left_account_identity_generation,
  right_account_identity_generation,
  left_security_identity_generation,
  right_security_identity_generation,
  event_type,
  leg_role,
  type,
  left_trade_date_basis,
  right_trade_date_basis,
  date_threshold_days,
  date_distance_days,
  structure_agrees,
  has_required_economics,
  quantity_within_tolerance,
  amount_within_tolerance,
  fees_within_tolerance,
  price_within_tolerance,
  date_within_tolerance,
  is_exact_economic_identity,
  subtype_conflict,
  trade_date_conflict,
  original_acquisition_date_conflict,
  FALSE AS has_validated_native_relationship,
  COUNT(*) OVER (PARTITION BY left_source_event_key, right_source_event_key) = left_member_count
  AND COUNT(*) OVER (PARTITION BY left_source_event_key, right_source_event_key) = right_member_count
  AND BOOL_AND(
    COALESCE(
      structure_agrees
      AND has_required_economics
      AND date_within_tolerance
      AND quantity_within_tolerance
      AND amount_within_tolerance
      AND fees_within_tolerance
      AND price_within_tolerance,
      FALSE
    )::BOOLEAN
  ) OVER (PARTITION BY left_source_event_key, right_source_event_key) AS is_candidate,
  left_trade_date,
  right_trade_date,
  preferred_trade_date,
  left_original_acquisition_date,
  right_original_acquisition_date
FROM measured
