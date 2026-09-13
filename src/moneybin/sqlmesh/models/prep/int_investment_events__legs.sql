MODEL (
  name prep.int_investment_events__legs,
  dialect duckdb,
  kind VIEW
);

/* Complete source events only: neither side of an ambiguous reinvest pair wins
   by ordering. Legacy multi-row hints cannot become ordinary singleton matches. */
WITH counted AS (
  SELECT
    *,
    CASE
      WHEN source_group_reference IS NULL
      THEN 1
      ELSE COUNT(*) OVER (PARTITION BY source_type, source_origin, source_group_reference)
    END AS source_group_size
  FROM prep.int_investment_events__observations
), possible_pairs AS (
  SELECT
    a.source_type,
    a.source_origin,
    a.native_reference AS acquisition_reference,
    i.native_reference AS income_reference,
    COUNT(*) OVER (PARTITION BY a.source_type, a.source_origin, a.native_reference) AS acquisition_options,
    COUNT(*) OVER (PARTITION BY i.source_type, i.source_origin, i.native_reference) AS income_options
  FROM counted AS a
  JOIN counted AS i
    ON a.source_type = i.source_type
    AND a.source_origin = i.source_origin
    AND a.account_id = i.account_id
    AND a.security_id = i.security_id
    AND a.currency_code = i.currency_code
    AND a.type = 'reinvest'
    AND i.type = CASE a.subtype
      WHEN 'dividend'
      THEN 'dividend'
      WHEN 'interest'
      THEN 'interest'
      WHEN 'capital_gain'
      THEN 'capital_gain_distribution'
    END
    AND a.has_resolved_identity
    AND i.has_resolved_identity
    AND ABS(DATE_DIFF('DAY', a.trade_date, i.trade_date)) <= 3
    AND a.quantity > 0
    AND i.quantity IS NULL
    AND a.amount < 0
    AND i.amount > 0
    AND ABS(i.amount + a.amount + COALESCE(a.fees, 0)) <= 0.01
    AND ABS(COALESCE(i.fees, 0)) <= 0.01
    AND (
      a.source_type = 'plaid'
      OR (
        a.source_group_reference = i.source_group_reference
        AND a.source_group_size = 2
        AND i.source_group_size = 2
        AND a.trade_date = i.trade_date
      )
    )
), pairs AS (
  SELECT
    *
  FROM possible_pairs
  WHERE
    acquisition_options = 1 AND income_options = 1
), grouped AS (
  SELECT
    o.*,
    NOT p.acquisition_reference IS NULL AS is_paired_reinvest,
    CASE
      WHEN NOT p.acquisition_reference IS NULL
      THEN LIST_SORT([p.acquisition_reference, p.income_reference])
      ELSE [o.native_reference]
    END AS member_references
  FROM counted AS o
  LEFT JOIN pairs AS p
    ON p.source_type = o.source_type
    AND p.source_origin = o.source_origin
    AND o.native_reference IN (p.acquisition_reference, p.income_reference)
)
SELECT
  source_type || '_' || SUBSTRING(
    SHA256(
      TO_JSON(
        {'source_type': source_type, 'source_origin': source_origin, 'native_references': member_references}
      )
    ),
    1,
    16
  ) AS source_event_key,
  native_reference,
  observation_version,
  original_investment_transaction_id,
  account_id,
  security_id,
  source_group_reference,
  source_type,
  source_origin,
  account_identity_generation,
  security_identity_generation,
  type,
  subtype,
  CASE WHEN is_paired_reinvest THEN 'reinvest' ELSE type END AS event_type,
  CASE
    WHEN type = 'reinvest'
    THEN 'acquisition'
    WHEN is_paired_reinvest
    THEN 'income'
    WHEN type IN ('buy', 'transfer_in')
    THEN 'acquisition'
    WHEN type IN ('sell', 'transfer_out')
    THEN 'disposal'
    WHEN type IN ('dividend', 'interest', 'capital_gain_distribution')
    THEN 'income'
    WHEN type = 'return_of_capital'
    THEN 'basis_reduction'
    WHEN type = 'split'
    THEN 'split'
    ELSE 'cash'
  END AS leg_role,
  description,
  trade_date_basis,
  quantity,
  price,
  amount,
  fees,
  source_currency_code,
  account_currency_code,
  currency_code,
  source_group_size,
  supports_split,
  supports_native_relationships,
  supports_corrections,
  supports_reversals,
  is_unsupported_compound,
  has_source_security,
  has_resolved_identity,
  is_paired_reinvest,
  COALESCE(
    has_resolved_identity
    AND NOT currency_code IS NULL
    AND NOT is_unsupported_compound
    AND (
      source_group_size = 1 OR is_paired_reinvest
    )
    AND (
      is_paired_reinvest
      OR type IN (
        'buy',
        'sell',
        'dividend',
        'interest',
        'capital_gain_distribution',
        'fee',
        'return_of_capital',
        'transfer_in',
        'transfer_out',
        'deposit',
        'withdrawal'
      )
      OR (
        type = 'split' AND supports_split
      )
    ),
    FALSE
  ) AS is_match_eligible,
  trade_date,
  settlement_date,
  original_acquisition_date
FROM grouped
