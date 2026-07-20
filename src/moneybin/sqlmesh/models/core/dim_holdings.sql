/* Current positions: the sum of open lots per (account, security). The "now"
   snapshot with no date dimension, rebuilt on every run. Carries cost basis and,
   since Pillar C, market value and unrealized gain against the most recent close at
   or before today. Uses cost_basis_remaining (not cost_basis_total) because under
   average cost the pooled remaining basis is the meaningful figure and can
   exceed a lot's own total.

   Market value is WITHHELD (status 'withheld') whenever the share COUNT is known wrong
   — a broker snapshot contradicting the ledger, an unreconciled split, or a fresh
   snapshot omitting a position the ledger still carries. A withheld row publishes NO
   pricing at all: market_value, unrealized_gain, price_date, price_source, and
   days_since_observed are ALL NULL, even when a fresh close did resolve. Leaving the
   three price columns populated let a withheld row advertise a zero-day-old price
   beside blanked figures, which reads as "the pricing is current and something
   unrelated is missing" rather than "the share count is disputed". The resolved close
   is not destroyed by this — it stays queryable in core.fct_security_prices, so the
   diagnostic survives one model over while this row stops making a claim it cannot
   stand behind.

   The withhold predicate is quantity-specific by design: market value is quantity x
   price and does not touch cost basis, so the broader investment_holdings_divergence
   and investment_staging_rejects doctor checks would each withhold a correct number
   for reasons that cannot affect it.

   The provider_reported_* columns are STORE-DON'T-TRUST: the broker's CLAIM
   about the same position, joined from its newest holdings snapshot and never
   blended into the ledger-derived figures above them. They exist to be
   reconciled against (system doctor warns on divergence), not to be read as
   MoneyBin's position. A position MoneyBin holds but the broker's newest
   snapshot omits shows NULL — that NULL is itself the signal. The converse is
   NOT covered: a position the broker reports but MoneyBin has no open lot
   for (unbound security, a declined bootstrap, or a holdings snapshot that
   landed before its transactions) produces no row here at all — a doctor
   check for that direction must scan prep.stg_plaid__investment_holdings
   directly, not this view. */
MODEL (
  name core.dim_holdings,
  kind VIEW,
  grain (account_id, security_id)
);

WITH positions AS (
  SELECT
    l.account_id,
    l.security_id,
    SUM(l.remaining_quantity)::DECIMAL(28, 10) AS quantity,
    SUM(l.cost_basis_remaining)::DECIMAL(18, 2) AS cost_basis,
    (
      SUM(l.cost_basis_remaining) / NULLIF(SUM(l.remaining_quantity), 0)
    )::DECIMAL(28, 10) AS average_cost,
    MAX(l.currency_code) AS currency_code,
    MAX(l.updated_at) AS updated_at
  FROM core.fct_investment_lots AS l
  WHERE
    l.is_open
  GROUP BY
    l.account_id,
    l.security_id
), newest_snapshot AS (
  /* ONE WHOLE SNAPSHOT per item — the source_file with the latest extracted_at,
     never "the latest row per position" and never "the latest holdings_date"
     (holdings_date is extracted_at::DATE, so two pulls on one UTC day tie on it).
     Scoping to a whole snapshot is what makes an omitted position read as NULL
     below instead of as a stale survivor from an earlier pull.

     Read from the snapshot RECEIPTS, never from the holdings rows themselves.
     Plaid returns no holding entries for an item that holds nothing, so a pull
     where every account is liquidated writes ZERO holdings rows — and a
     row-derived newest snapshot cannot see that pull at all, silently keeping
     the last NON-EMPTY one. The provider claim below would then come back as
     the STALE quantity the broker no longer reports, on the position most
     overstated (all of it). The receipt exists for exactly that pull; joining
     the holdings rows to it leaves an item that reported nothing with an EMPTY
     newest snapshot (claim NULL — correct) and an item that never reported with
     NO newest snapshot (no rows to join — also correct). */
  SELECT
    source_origin,
    source_file
  FROM (
    SELECT
      source_origin,
      source_file,
      ROW_NUMBER() OVER (PARTITION BY source_origin ORDER BY extracted_at DESC, source_file DESC) AS snapshot_rank
    FROM prep.stg_plaid__investment_holdings_snapshots
  )
  WHERE
    snapshot_rank = 1
), provider_reported AS (
  /* Aggregated to the position grain (account, security): a security merge can
     bind two provider security ids in one account onto one canonical id, and a
     canonical security can be held at several institutions. Summing here — not
     joining row-per-row — is what keeps the LEFT JOIN below from fanning the
     position out. provider_reported_as_of takes MIN, not MAX: when one item's
     connection breaks and its snapshot goes stale, the summed quantity/cost
     basis above still carries that stale contributor at full weight, so the
     honest freshness is "as fresh as the stalest contributor," not the newest
     one — MAX would let a healthy item's fresh timestamp mask a broken one. */
  SELECT
    h.account_id,
    h.security_id,
    SUM(h.quantity)::DECIMAL(28, 10) AS provider_reported_quantity,
    SUM(h.cost_basis)::DECIMAL(18, 2) AS provider_reported_cost_basis,
    SUM(h.institution_value)::DECIMAL(18, 2) AS provider_reported_value,
    MIN(h.extracted_at) AS provider_reported_as_of
  FROM prep.stg_plaid__investment_holdings AS h
  JOIN newest_snapshot AS ns
    ON ns.source_file = h.source_file AND ns.source_origin = h.source_origin
  WHERE
    NOT h.security_id IS NULL
  GROUP BY
    h.account_id,
    h.security_id
), latest_price AS (
  /* As-of, not equal: the most recent close on or before today. Equality would leave a
     hole on every weekend, holiday, and provider outage; unbounded lookahead would value
     today with a price observed later. Partitioned by currency as well as security so a
     dual-quoted security keeps its two series separate — the join below then requires
     the position's own currency rather than valuing it at a close denominated
     differently from its cost basis. */
  SELECT
    security_id,
    quote_currency,
    close,
    price_date,
    source
  FROM core.fct_security_prices
  WHERE
    price_date <= CURRENT_DATE
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY security_id, quote_currency ORDER BY price_date DESC) = 1
), split_reject_securities AS (
  /* A Plaid-reported split is routed to review as split_underivable and held out of
     core.fct_investment_transactions, because a derived multiplier that is wrong
     corrupts the basis of the whole position. Until it lands, the position still
     reports the PRE-split quantity, and quantity x price is wrong by the split factor
     while every other signal reads healthy. Detected per SECURITY: a split is a
     corporate action, so a reject arriving through one account implicates every
     position in that security.

     Bounded to BOUND securities: split_underivable is set on the mapped subtype whether
     or not the security resolved, but an unbound reject has no canonical security_id to
     implicate a position with. A user tracking that same security manually under a
     canonical id therefore keeps publishing a pre-split quantity. Known and accepted —
     an unbound Plaid security is already surfaced for binding, and inventing a fallback
     key here would implicate positions on a match this model cannot actually prove.

     match_window_days is the tolerance the clearing match below allows between the two
     dates, and exists because the reject and the ledger split come from INDEPENDENT
     suppliers that date the same corporate action differently: Plaid's reject carries
     whatever its feed reported (commonly the settlement date), while a hand-entered or
     third-party split is normally recorded on the ex-date. Requiring the two to be
     equal means a user who reconciles a reject dated 2026-03-16 by entering the split
     on its ex-date, 2026-03-15, restates the quantity correctly and still withholds
     forever — the design carries no resolved-flag to clear, so an exact-match miss is
     permanent, not merely delayed. Observed ex-date/settlement skew is 1-3 calendar
     days; 5 covers it with margin while staying far below the interval between two
     splits of one security. Defined once here and referenced once below; fixed by
     design, never configurable — a tunable would let a user widen it until unrelated
     splits started clearing each other. */
  SELECT DISTINCT
    security_id,
    trade_date,
    5 AS match_window_days
  FROM prep.stg_plaid__investment_transactions
  WHERE
    review_reason = 'split_underivable' AND NOT security_id IS NULL
), position_split_events AS (
  /* Resolved per POSITION: a ledger that already carries a split within the reject's
     match window has been restated correctly, whoever supplied it. This is also what
     makes the withhold self-clearing — when the Plaid split behaviour is settled and
     the events reach the ledger, positions stop withholding with no resolved-flag to
     maintain. Self-clearing is only true because the match is windowed: under exact
     date equality it clears solely when both suppliers happen to pick the same date. */
  SELECT DISTINCT
    account_id,
    security_id,
    trade_date
  FROM core.fct_investment_transactions
  WHERE
    type = 'split'
), broker_covered_accounts AS (
  /* An account is broker-covered when it is known to a Plaid item — through EITHER
     investment staging view — and that item has a current snapshot receipt. Without
     this scope the phantom clause below would fire on every manual-only account, whose
     provider claim is NULL simply because no broker reports it — silently unvaluing
     every manually-tracked position in the database.

     The UNION with the transactions view is load-bearing, and mirrors the
     investment_phantom_holdings doctor check: a LIQUIDATED account writes no holdings
     rows at all, so a holdings-only scope drops the one account whose every position is
     a phantom — the 100%-overstated account is exactly the one the narrower scope
     filters out. The account survives in the transactions view, which is what supplies
     its coverage. This is the same blind spot newest_snapshot avoids by reading
     receipts rather than rows, one level down.

     Joined on source_origin alone, NOT on source_file: coverage is a property of the
     ITEM, and requiring the account to appear in the newest snapshot itself would make
     the phantom clause unreachable — a position dropped from that snapshot is exactly
     the case being detected. */
  SELECT DISTINCT
    ai.account_id
  FROM (
    SELECT DISTINCT
      account_id,
      source_origin
    FROM prep.stg_plaid__investment_holdings
    UNION
    SELECT DISTINCT
      account_id,
      source_origin
    FROM prep.stg_plaid__investment_transactions
  ) AS ai
  JOIN newest_snapshot AS ns
    ON ns.source_origin = ai.source_origin
), withheld AS (
  /* Three clauses, none redundant — each guards a failure the others miss, and all
     three are quantity-specific: market value is quantity x price and does not depend
     on cost basis at all, so gating on investment_holdings_divergence (which also
     fails on a pure cost-basis mismatch) or on investment_staging_rejects (which fires
     on unmapped_subtype and transfer_direction_underivable too) would withhold a
     correct number for unrelated reasons.

     The third is not covered by the first: when a fresh snapshot omits a position the
     ledger still carries, provider_reported_quantity is NULL, so
     `quantity <> provider_reported_quantity` evaluates to UNKNOWN rather than true and
     the position would slip through — publishing a market value for shares the broker
     says are gone and overstating net worth by exactly that amount. */
  SELECT
    pos.account_id,
    pos.security_id,
    (
      NOT pr.provider_reported_quantity IS NULL
      AND pos.quantity <> pr.provider_reported_quantity
    )
    OR EXISTS(
      SELECT
        1
      FROM split_reject_securities AS sr
      WHERE
        sr.security_id = pos.security_id
        AND NOT EXISTS(
          SELECT
            1
          FROM position_split_events AS pse
          WHERE
            pse.account_id = pos.account_id
            AND pse.security_id = pos.security_id
            AND ABS(CAST(pse.trade_date - sr.trade_date AS INT)) <= sr.match_window_days
        )
    )
    OR (
      pos.account_id IN (
        SELECT
          account_id
        FROM broker_covered_accounts
      )
      AND pr.provider_reported_quantity IS NULL
    ) AS is_withheld
  FROM positions AS pos
  LEFT JOIN provider_reported AS pr
    ON pr.account_id = pos.account_id AND pr.security_id = pos.security_id
)
SELECT
  p.account_id, /* FK to core.dim_accounts (grain) */
  p.security_id, /* FK to core.dim_securities (grain) */
  p.quantity, /* Total open units (Σ remaining_quantity); cast back to (28,10) — SUM widens to (38,10) */
  p.cost_basis, /* Total open basis (Σ cost_basis_remaining); cast back to (18,2) — SUM widens to (38,2) */
  p.average_cost, /* cost_basis / quantity; cast wraps the WHOLE division so the result is DECIMAL(28,10), not DOUBLE (DuckDB decimal / promotes to DOUBLE); (28,10) for crypto fractional-unit precision; NULL when quantity is 0 */
  p.currency_code, /* Denominating currency (one per position) */
  CASE
    WHEN wh.is_withheld
    THEN NULL
    ELSE (
      p.quantity * lp.close
    )::DECIMAL(18, 2)
  END AS market_value, /* quantity × the resolved close. NULL — never zero — when no usable price applies or the quantity is known wrong: a zero is indistinguishable from a worthless position and silently understates every aggregate that sums it */
  CASE
    WHEN wh.is_withheld
    THEN NULL
    ELSE (
      (
        p.quantity * lp.close
      )::DECIMAL(18, 2) - p.cost_basis
    )::DECIMAL(18, 2)
  END AS unrealized_gain, /* market_value less cost basis; NULL whenever market_value is NULL. Realized gain is ledger-derived and lives in core.fct_realized_gains */
  CASE WHEN wh.is_withheld THEN NULL ELSE lp.price_date END AS price_date, /* The date of the close used, which may be earlier than today. NULL whenever market_value is NULL — both when no close resolved ('unpriced') and when one did but the quantity is known wrong ('withheld'): a withheld row publishing today's date beside blanked figures reads as "pricing is current, something else is missing", which is the opposite of the truth. The close itself is not lost — it stays queryable in core.fct_security_prices, which is where a support path should look */
  CASE WHEN wh.is_withheld THEN NULL ELSE lp.source END AS price_source, /* Which source supplied the close (see core.fct_security_prices); NULL exactly when price_date is NULL, on both 'unpriced' and 'withheld' */
  CASE
    WHEN wh.is_withheld
    THEN NULL
    ELSE CAST(CURRENT_DATE - lp.price_date AS INT)
  END AS days_since_observed, /* Calendar days between the price used and today (uncategorized_queue.age_days precedent for this CAST-subtraction form). DATE_DIFF('day', ...) here fails every one of this model's valuation tests with a SQLMesh PlanError — measured to come from SQLMesh's render path losing the duckdb dialect for this node, not from sqlglot mishandling DATE_DIFF outright. 0 on a same-day close; a Monday reading 3 on an equity is an ordinary weekend, not a fault. NULL exactly when price_date is NULL, on both 'unpriced' and 'withheld' */
  CASE
    WHEN wh.is_withheld
    THEN 'withheld'
    WHEN lp.close IS NULL
    THEN 'unpriced'
    WHEN lp.price_date = CURRENT_DATE
    THEN 'valued'
    ELSE 'carried_forward'
  END AS valuation_status, /* valued | carried_forward | unpriced | withheld. Every status either carries a number the reader can rely on or carries none at all — no status publishes a qualified figure. The non-valued statuses stay distinct because each has a different remedy: unpriced wants a price feed; withheld wants the share count reconciled — an unreconciled split recorded, a broker divergence resolved, or a position the broker no longer reports closed out */
  pr.provider_reported_quantity, /* NON-AUTHORITATIVE: the broker's claimed open units in its newest snapshot. Reconciliation reference only — `quantity` above is MoneyBin's figure. NULL = the broker's newest snapshot does not report this position */
  pr.provider_reported_cost_basis, /* NON-AUTHORITATIVE: the broker's claimed cost basis. Never overwrites or feeds `cost_basis` above; system doctor warns when the two diverge */
  pr.provider_reported_value, /* NON-AUTHORITATIVE: the broker's claimed market value. MoneyBin computes `market_value` above independently, as quantity × its own resolved close, and never blends this claim into it — no doctor check reconciles the two yet */
  pr.provider_reported_as_of, /* Oldest extracted_at among the snapshots summed into the three columns above (MIN, not MAX) — a canonical position spanning multiple broker connections is only as fresh as its stalest contributor; NULL when the broker no longer reports this position */
  p.updated_at /* Latest of all per-row input timestamps contributing to this row's current values (MAX over the position's open lots). Provider-reported columns do not advance it — they are a reference, not an input. Does not advance on idempotent SQLMesh re-applies. See docs/specs/core-updated-at-convention.md. */
FROM positions AS p
LEFT JOIN provider_reported AS pr
  ON pr.account_id = p.account_id AND pr.security_id = p.security_id
/* Both sides of the currency predicate are UPPER()ed because they arrive from
   different provider objects with no shared casing guarantee: the price's
   quote_currency comes from the security object, the lot's currency_code from the
   transaction object (COALESCE(iso_currency_code, unofficial_currency_code), stored
   verbatim). unofficial_currency_code — crypto and other non-ISO instruments —
   promises no casing at all. A case-sensitive match here would report a position as
   'unpriced' while the resolved close for it sits in core.fct_security_prices.
   prep.stg_security_prices normalizes its own side because fct_security_prices'
   grain depends on it; the lot side is left as stored, so the fold happens here. */
LEFT JOIN latest_price AS lp
  ON lp.security_id = p.security_id AND lp.quote_currency = UPPER(p.currency_code)
LEFT JOIN withheld AS wh
  ON wh.account_id = p.account_id AND wh.security_id = p.security_id
