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

   Four rules, in this precedence, reproduce _stored_rate:

   1. An override on the effective_date itself wins — _stored_rate's exact-day
      check. published_date becomes that day and days_since_published is 0:
      the user priced the day itself.
   2. Otherwise, on a Saturday or Sunday, an override on the calendar Friday
      immediately before it wins — _last_publication_day's weekend hop, which
      is a function of the calendar date asked about, not of what
      core.fct_exchange_rates_daily happens to record as this row's
      published_date. _stored_rate(Friday) checks the override table before
      ever touching the daily spine's own notion of what Friday carries from,
      so a Friday override applies to the weekend it hops to even when Friday
      itself was never a provider publication day — a gap the daily spine
      carries straight through from the prior observation. published_date
      becomes that Friday and days_since_published counts from it (1 for
      Saturday, 2 for Sunday). This is distinct from rule 3 below: it fires
      only for the two weekend days, and only against the exact calendar
      Friday, never an earlier weekday.
   3. Otherwise an override on the row's published_date wins — the same
      exact-day check reached through the ordinary carry-forward. A corrected
      publication prices every day carrying from it, weekend or interior
      weekday alike. published_date and days_since_published KEEP the hop
      core.fct_exchange_rates_daily already recorded — only the rate is
      replaced. (An override on an interior non-publication day that is
      itself NOT the calendar Friday of a weekend it precedes does not gain
      this cascade — seen only in rule 1, on its own day. Extending a
      same-pair, non-publication correction past the single day it was filed
      under would assume a claim about neighboring days the user never made;
      Requirement 5 governs the ambiguity the same way rule 4 states it for
      an uncovered override. This mirrors currency_service.py:197-205's
      open weekday-holiday gap for `resolve_rate` itself: neither surface
      claims to close it.)
   4. An override on a pair and date the daily spine does not cover
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

   Every row an override wins reads rate_source = 'override' and rate_vendor =
   NULL — an override is user-authored, not sourced from a named feed, and a
   row an override does NOT win carries whatever core.fct_exchange_rates_daily
   already resolved (a named feed for 'provider', NULL for 'identity'). See
   core.fct_exchange_rates for why rate_source/rate_vendor is two columns
   rather than one, shared across all three rate models.

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
    CASE
      WHEN NOT oe.rate IS NULL
      THEN d.effective_date
      WHEN NOT ow.rate IS NULL
      THEN ow.rate_date
      ELSE d.published_date
    END AS published_date,
    CASE
      WHEN NOT oe.rate IS NULL
      THEN 0
      WHEN NOT ow.rate IS NULL
      THEN CAST(d.effective_date - ow.rate_date AS INT)
      ELSE d.days_since_published
    END AS days_since_published,
    COALESCE(oe.rate, ow.rate, op.rate, d.rate) AS rate,
    CASE
      WHEN NOT oe.rate IS NULL OR NOT ow.rate IS NULL OR NOT op.rate IS NULL
      THEN 'override'
      ELSE d.rate_source
    END AS rate_source,
    CASE
      WHEN NOT oe.rate IS NULL OR NOT ow.rate IS NULL OR NOT op.rate IS NULL
      THEN NULL
      ELSE d.rate_vendor
    END AS rate_vendor
  FROM core.fct_exchange_rates_daily AS d
  LEFT JOIN app.exchange_rate_overrides AS oe
    ON oe.from_currency = d.from_currency
    AND oe.to_currency = d.to_currency
    AND oe.rate_date = d.effective_date
  /* The weekend-hop arm (rule 2): the calendar Friday immediately before a
     Saturday/Sunday row, independent of whatever core.fct_exchange_rates_daily
     recorded as this row's own published_date. ISODOW 6/7 = Saturday/Sunday;
     every other day never matches (rate_date is never NULL, but the CASE
     forces NULL on a non-weekend day so it cannot coincide with one). */
  LEFT JOIN app.exchange_rate_overrides AS ow
    ON ow.from_currency = d.from_currency
    AND ow.to_currency = d.to_currency
    AND ow.rate_date = CASE ISODOW(d.effective_date)
      WHEN 6
      THEN d.effective_date - INTERVAL '1' DAY
      WHEN 7
      THEN d.effective_date - INTERVAL '2' DAY
    END
  LEFT JOIN app.exchange_rate_overrides AS op
    ON op.from_currency = d.from_currency
    AND op.to_currency = d.to_currency
    AND op.rate_date = d.published_date
), uncovered_overrides AS (
  /* An override the daily spine does not already reach on either its
     effective_date or its published_date arm — rule 4. (An override
     reached only through the weekend-hop arm, rule 2, is never uncovered:
     that arm requires a d row to already exist for the Saturday/Sunday
     effective_date it joins from.) */
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
    'override' AS rate_source,
    NULL::TEXT AS rate_vendor
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
    rate_source,
    rate_vendor
  FROM daily_with_overrides
  UNION ALL
  SELECT
    from_currency,
    to_currency,
    effective_date,
    published_date,
    days_since_published,
    rate,
    rate_source,
    rate_vendor
  FROM uncovered_rows
)
SELECT
  u.from_currency, /* ISO 4217, upper (grain) */
  u.to_currency, /* ISO 4217, upper (grain) */
  u.rate_source, /* provider / identity / override */
  u.rate_vendor, /* The named feed behind a provider row (e.g. 'frankfurter'); NULL when rate_source is identity or override */
  u.rate, /* Multiply a from_currency amount by this — the override when one won, else the daily spine's provider or identity rate */
  u.days_since_published, /* effective_date - published_date; 0 on a publication day or a same-day override */
  u.effective_date, /* The calendar day this rate is applied ON (grain) */
  u.published_date /* The day this rate was actually priced — an override's own date under rule 1, the weekend-hop Friday under rule 2, or its own date again under rule 4, else the carried observation's date */
FROM unioned AS u
