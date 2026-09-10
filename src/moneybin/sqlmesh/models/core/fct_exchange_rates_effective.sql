/* Same grain as core.fct_exchange_rates_daily — (from_currency, to_currency,
   effective_date) — and the model the three net-worth rungs actually join.
   Applies override precedence at READ time to the observation each daily row
   carried forward from, not only to the calendar day the override is filed
   under: it densifies the winning observation rather than overlaying
   corrections at their recorded dates.

   That distinction is the whole correctness of the model.
   CurrencyService.resolve_rate consults _stored_rate twice — once for the
   exact day, once for _last_publication_day of it — and _stored_rate is
   override-first both times. A user who corrects Friday's quote is therefore
   already pricing Saturday today. An overlay matched on effective_date alone
   would leave Saturday carrying the provider's Friday rate, so the SQL
   reports would ignore the correction on exactly the days carry-forward
   exists to cover.

   Three rules, in this precedence, reproduce _stored_rate:

   1. An override on the effective_date itself wins — _stored_rate's exact-day
      check. published_date becomes that day and days_since_published is 0:
      the user priced the day itself.
   2. Otherwise an override on the row's published_date wins — the same check
      reached through the carry-forward. A corrected Friday prices the
      Saturday and Sunday carrying from it, and a corrected quote prices
      every interior non-publication day carrying from it. published_date and
      days_since_published KEEP the hop core.fct_exchange_rates_daily already
      recorded — only the rate is replaced.
   3. An override on a pair and date the daily spine does not cover
      contributes its own row — _stored_rate answers from the override table
      whether or not a provider ever priced that day, so a correction is
      never invisible because the provider was silent. Such a row gets the
      same bounded weekend hop a provider observation would (Friday carries
      to Saturday/Sunday, nothing further) — the one piece of "carries
      forward under the same rules as an observation" that is a cheap,
      non-recursive computation rather than a second densification engine.
      It does NOT interior-fill between two disconnected uncovered override
      dates for the same pair; Requirement 5 (never manufacture a rate) is
      the controlling invariant when that is ambiguous, so an uncovered gap
      between two standalone overrides stays unpriced rather than guessed.

   Every row an override wins reads rate_source = 'override'.

   The split exists because the two halves have opposite freshness
   requirements. A materialized override would regress against the path this
   spec replaces: _stored_rate checks the override table FIRST, so a
   kind FULL model carrying overrides would serve the pre-override rate until
   the next sqlmesh run. Dense carry-forward is expensive and safely
   cacheable; override precedence is a cheap join and must be live — the seam
   goes between them. */
MODEL (
  name core.fct_exchange_rates_effective,
  kind VIEW,
  grain (from_currency, to_currency, effective_date)
);

WITH daily_with_overrides AS (
  SELECT
    d.from_currency,
    d.to_currency,
    d.effective_date,
    CASE WHEN NOT oe.rate IS NULL THEN d.effective_date ELSE d.published_date END AS published_date,
    CASE WHEN NOT oe.rate IS NULL THEN 0 ELSE d.days_since_published END AS days_since_published,
    COALESCE(oe.rate, op.rate, d.rate) AS rate,
    CASE
      WHEN NOT oe.rate IS NULL OR NOT op.rate IS NULL
      THEN 'override'
      ELSE d.rate_source
    END AS rate_source
  FROM core.fct_exchange_rates_daily AS d
  LEFT JOIN app.exchange_rate_overrides AS oe
    ON oe.from_currency = d.from_currency
    AND oe.to_currency = d.to_currency
    AND oe.rate_date = d.effective_date
  LEFT JOIN app.exchange_rate_overrides AS op
    ON op.from_currency = d.from_currency
    AND op.to_currency = d.to_currency
    AND op.rate_date = d.published_date
), uncovered_overrides AS (
  /* An override the daily spine does not already reach on either its
     effective_date or its published_date arm — rule 3. */
  SELECT
    o.from_currency,
    o.to_currency,
    o.rate_date,
    o.rate
  FROM app.exchange_rate_overrides AS o
  WHERE
    NOT EXISTS(
      SELECT
        1
      FROM core.fct_exchange_rates_daily AS d
      WHERE
        d.from_currency = o.from_currency
        AND d.to_currency = o.to_currency
        AND (
          d.effective_date = o.rate_date OR d.published_date = o.rate_date
        )
    )
), uncovered_spine AS (
  SELECT
    u.from_currency,
    u.to_currency,
    u.rate_date AS published_date,
    u.rate,
    s.effective_date::DATE AS effective_date
  FROM uncovered_overrides AS u, GENERATE_SERIES(
    u.rate_date,
    CASE
      WHEN ISODOW(u.rate_date) = 5
      THEN u.rate_date + INTERVAL '2' DAY
      ELSE u.rate_date
    END,
    INTERVAL '1' DAY
  ) AS s(effective_date)
), uncovered_rows AS (
  SELECT
    s.from_currency,
    s.to_currency,
    s.effective_date,
    s.published_date,
    CAST(s.effective_date - s.published_date AS INT) AS days_since_published,
    s.rate,
    'override' AS rate_source
  FROM uncovered_spine AS s
  /* Grain guard: an uncovered override's own weekend hop can only reach a
     date the daily spine already covers for the same pair when a provider
     has (unusually) published on a weekend, so this is defensive rather
     than expected to fire — but a real one covers this row from
     daily_with_overrides already, so it must not also appear here. */
  WHERE
    NOT EXISTS(
      SELECT
        1
      FROM core.fct_exchange_rates_daily AS d
      WHERE
        d.from_currency = s.from_currency
        AND d.to_currency = s.to_currency
        AND d.effective_date = s.effective_date
    )
  QUALIFY
    ROW_NUMBER() OVER (
      PARTITION BY s.from_currency, s.to_currency, s.effective_date
      ORDER BY (
        s.effective_date = s.published_date
      ) DESC, s.published_date DESC
    ) = 1
), unioned AS (
  SELECT
    from_currency,
    to_currency,
    effective_date,
    published_date,
    days_since_published,
    rate,
    rate_source
  FROM daily_with_overrides
  UNION ALL
  SELECT
    from_currency,
    to_currency,
    effective_date,
    published_date,
    days_since_published,
    rate,
    rate_source
  FROM uncovered_rows
)
SELECT
  u.from_currency, /* ISO 4217, upper (grain) */
  u.to_currency, /* ISO 4217, upper (grain) */
  u.effective_date, /* The calendar day this rate is applied ON (grain) */
  u.published_date, /* The day this rate was actually priced — an override's own date under rule 1 or 3, else the carried observation's date */
  u.rate, /* Multiply a from_currency amount by this — the override when one won, else the daily spine's provider or identity rate */
  u.rate_source, /* override / provider / identity */
  u.days_since_published /* effective_date - published_date; 0 on a publication day or a same-day override */
FROM unioned AS u
