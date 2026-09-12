/* Grain (from_currency, to_currency, effective_date). Mirrors
   core.fct_balances_daily: a dense daily spine over an observation model.
   Densifies the provider observations in prep.stg_exchange_rates — published
   rates and identity rows only. User overrides are applied above it, at read
   time, by core.fct_exchange_rates_effective; see that model for why.
   rate_source here is therefore 'provider' or 'identity', never 'override'.
   core.fct_exchange_rates keeps resolving precedence for its own consumers;
   this one takes the half that is safe to materialize.

   IT DENSIFIES THE STAGED ROWS RATHER THAN core.fct_exchange_rates, AND THE
   CHOICE OF UPSTREAM IS LOAD-BEARING. core.fct_exchange_rates resolves
   precedence at observation grain, so on a date carrying both a provider
   quote and an override it emits the override alone: the provider row is
   gone, and no provider arm survives on that date to densify. A spine built
   from it would carry an older quote across the day the override corrected.
   Nothing looks wrong while the override stands, because
   core.fct_exchange_rates_effective wins that day at read time regardless.
   Delete the override, though, and the effective view falls back to THIS
   spine — and if this spine had densified from fct_exchange_rates, its
   rate/published_date for that day would never have been the provider's, and
   it would stay wrong until the next sqlmesh run rebuilds this table. An
   override applies the moment it is written; its deletion has to take effect
   just as immediately, and only a spine built from the unresolved provider
   rows does that. See docs/specs/reports-net-worth-sql-surface.md §Rate
   models for the argument in full.

   WINDOW-BOUNDED FILL, WEEKEND HOP AT THE TRAILING EDGE, AND NOTHING FURTHER.
   For a real pair, rows start at that pair's first observation and normally
   end at its last — before the first quote or after the last, there is no
   row at all, so a downstream join misses visibly rather than matching a
   manufactured rate (Requirement 5). The one exception is the weekend
   CurrencyService.resolve_rate already carries a Friday quote across, because
   no reference rate is ever published on a weekend: a pair whose last
   observation falls on a Friday gets two more spine rows (Saturday, Sunday)
   carrying that quote forward, and nothing else. This mirrors
   `_last_publication_day` exactly and stops exactly where it stops —
   `MAX_BACKWARD_RESOLUTION_DAYS` (currency_service.py) bounds a fetch
   response's own distance from the day asked about, not a carry-forward
   window, and borrowing it here would price an ordinary weekday from a
   quote up to 14 days old, which is the substitution Requirement 5 forbids
   by name.

   Multiple providers quoting the same pair and date is resolved by the same
   tie-break core.fct_exchange_rates uses for provider rows (freshest write,
   then provider name) before the spine is built, so at most one provider
   observation per (from_currency, to_currency, rate_date) ever reaches the
   densification below.

   RATE_VENDOR CARRIES FORWARD ALONGSIDE RATE AND PUBLISHED_DATE. A day carrying
   from an earlier observation names the same named feed that priced it —
   the forward-fill is one fact (a provider's quote persisting across
   non-publication days), not three independent ones, so the three columns
   move together through the same window function. See
   core.fct_exchange_rates for why rate_source/rate_vendor is two columns
   rather than one: the split is shared across all three rate models so a
   caller reads one vocabulary regardless of which one it joins.

   IDENTITY ROWS. For every currency appearing in core.dim_accounts, an X → X
   row at 1.0 with rate_source = 'identity', rate_vendor = NULL (an identity
   price is definitional, not sourced from a feed), and
   days_since_published = 0, spanning the date domain of
   core.fct_balances_daily (its global MIN/MAX(balance_date), not scoped per
   account) — Requirement 11: one join path, no branch, and a
   single-currency profile never sees a NULL converted column. A currency
   that also carries real provider quotes for its own X→X pair (never
   observed in practice) keeps the provider arm rather than colliding with
   the identity one.

   KIND FULL, recomputed every sqlmesh run. A retroactively corrected provider
   rate is picked up by the next run with no incremental bookkeeping and no
   staleness marker — matching core.fct_balances_daily and
   core.fct_security_prices. A user override is deliberately NOT part of that
   recompute; see the header note above. */
MODEL (
  name core.fct_exchange_rates_daily,
  kind FULL,
  grain (from_currency, to_currency, effective_date)
);

WITH provider_obs AS (
  SELECT
    from_currency,
    to_currency,
    rate_date,
    rate,
    source_type AS provider_name,
    loaded_at
  FROM prep.stg_exchange_rates
  QUALIFY
    ROW_NUMBER() OVER (
      PARTITION BY from_currency, to_currency, rate_date
      ORDER BY loaded_at DESC, source_type
    ) = 1
), pair_bounds AS (
  SELECT
    from_currency,
    to_currency,
    MIN(rate_date) AS first_date,
    MAX(rate_date) AS last_date
  FROM provider_obs
  GROUP BY
    from_currency,
    to_currency
), pair_spine AS (
  SELECT
    b.from_currency,
    b.to_currency,
    d.effective_date::DATE AS effective_date
  FROM pair_bounds AS b, GENERATE_SERIES(
    b.first_date,
    CASE
      WHEN ISODOW(b.last_date) = 5
      THEN b.last_date + INTERVAL '2' DAY
      ELSE b.last_date
    END /* The weekend hop: two extra days past a Friday last observation, none
       past any other. */,
    INTERVAL '1' DAY
  ) AS d(effective_date)
), provider_filled AS (
  SELECT
    s.from_currency,
    s.to_currency,
    s.effective_date,
    LAST_VALUE(o.rate_date IGNORE NULLS) OVER pair_order AS published_date,
    LAST_VALUE(o.rate IGNORE NULLS) OVER pair_order AS rate,
    LAST_VALUE(o.provider_name IGNORE NULLS) OVER pair_order AS rate_vendor,
    'provider' AS rate_source
  FROM pair_spine AS s
  LEFT JOIN provider_obs AS o
    ON o.from_currency = s.from_currency
    AND o.to_currency = s.to_currency
    AND o.rate_date = s.effective_date
  WINDOW pair_order AS (
    PARTITION BY s.from_currency, s.to_currency
    ORDER BY s.effective_date
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  )
), identity_currencies AS (
  SELECT DISTINCT
    a.currency_code
  FROM core.dim_accounts AS a
  WHERE
    NOT a.currency_code IS NULL
    AND NOT EXISTS(
      SELECT
        1
      FROM pair_bounds AS b
      WHERE
        b.from_currency = a.currency_code AND b.to_currency = a.currency_code
    )
), balances_domain AS (
  SELECT
    MIN(balance_date) AS first_date,
    MAX(balance_date) AS last_date
  FROM core.fct_balances_daily
), identity_rows AS (
  SELECT
    c.currency_code AS from_currency,
    c.currency_code AS to_currency,
    d.effective_date::DATE AS effective_date,
    d.effective_date::DATE AS published_date,
    1::DECIMAL(18, 8) AS rate,
    NULL::TEXT AS rate_vendor,
    'identity' AS rate_source
  FROM identity_currencies AS c, balances_domain AS b, GENERATE_SERIES(b.first_date, b.last_date, INTERVAL '1' DAY) AS d(effective_date)
), unioned AS (
  SELECT
    from_currency,
    to_currency,
    effective_date,
    published_date,
    rate,
    rate_vendor,
    rate_source
  FROM provider_filled
  UNION ALL
  SELECT
    from_currency,
    to_currency,
    effective_date,
    published_date,
    rate,
    rate_vendor,
    rate_source
  FROM identity_rows
)
SELECT
  u.from_currency, /* ISO 4217, upper (grain) */
  u.to_currency, /* ISO 4217, upper (grain) */
  u.rate_source, /* provider / identity — never override; see the header note */
  u.rate_vendor, /* The named feed behind a provider row (e.g. 'frankfurter'), carried forward with the rate it priced; NULL when rate_source = 'identity' */
  u.rate, /* Multiply a from_currency amount by this */
  CAST(u.effective_date - u.published_date AS INT) AS days_since_published, /* effective_date - published_date; 0 on a publication day */
  u.effective_date, /* The calendar day this rate is applied ON (grain) */
  u.published_date /* The day the provider priced it (= fct_exchange_rates.rate_date) */
FROM unioned AS u
