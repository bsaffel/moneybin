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

   One further state blanks the same five columns and is deliberately NOT spelled
   'withheld': status 'source_overlap', for a position whose ACCOUNT carries an
   investment ledger from more than one source at once. That is a different fault with
   a different remedy — the four withhold clauses each say one position's share count
   is wrong and want it reconciled, while this says the account has two ledgers
   interleaved and wants one of the two feeds removed (moneybin system doctor's
   investment_source_overlap check fails on it and names the remedy). Overloading one
   status would leave the reader unable to tell which repair applies. It takes
   precedence over 'withheld': a quantity mismatch measured against a double-counted
   ledger is a symptom, not the fault to report.

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
    COUNT(DISTINCT l.currency_code) AS currency_count,
    COUNT(*) FILTER(WHERE
      l.currency_code IS NULL) AS unknown_currency_lots,
    MIN(l.acquisition_date) AS earliest_acquisition_date,
    BOOL_OR(l.basis_incomplete::BOOLEAN) AS basis_incomplete,
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
    source_file,
    extracted_at
  FROM (
    SELECT
      source_origin,
      source_file,
      extracted_at,
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
    source_type,
    updated_at
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
), ever_reported_positions AS (
  /* Every (account, security) the broker has EVER carried in a holdings snapshot — read
     across ALL snapshots, not only the newest. The phantom clause requires this: "the
     broker stopped reporting this position" is only meaningful for a position the broker
     once reported. A position that never appears in any snapshot is a manual holding, and
     withholding it would tell the user their share count is wrong when it is not.

     security_id here is the CANONICAL id (prep.stg_plaid__investment_holdings resolves the
     provider key through the accepted 'plaid_security_id' link), so it joins to
     positions.security_id below. A holdings row exists only for a broker-covered account,
     so this position-level gate strictly implies account coverage and replaces the older
     account-level scope entirely — one precise check rather than two overlapping ones.

     Known limitation: a Plaid security id that churned across a corporate action, whose
     OLD id was bound and reported, still resolves to the same canonical id and so still
     withholds here. That case wants the new id bound, not a "share count wrong" claim; it
     is a named limitation this gate does not fix. */
  SELECT DISTINCT
    account_id,
    security_id
  FROM prep.stg_plaid__investment_holdings
  WHERE
    NOT security_id IS NULL
), position_snapshot_freshness AS (
  /* The newest snapshot receipt time for each (account, security) the broker has EVER
     reported, read across ALL snapshots and joined to the newest snapshot of the
     source_origin that carried it. This is the watermark input the per-position provider
     claim cannot supply: when a fresh pull OMITS a position, provider_reported emits no
     row (NULL as-of), yet that same pull is what flips the row to 'withheld' — a real
     input change. Keyed here on the position (not the account) so a purely manual holding
     in a broker-covered account, whose valuation never depends on a snapshot, is absent
     and its watermark is not spuriously advanced by an unrelated pull. */
  SELECT
    h.account_id,
    h.security_id,
    MAX(ns.extracted_at) AS newest_snapshot_at
  FROM prep.stg_plaid__investment_holdings AS h
  JOIN newest_snapshot AS ns
    ON ns.source_origin = h.source_origin
  WHERE
    NOT h.security_id IS NULL
  GROUP BY
    h.account_id,
    h.security_id
), source_overlap_accounts AS (
  /* Accounts whose investment ledger arrives from more than one source at once — a
     broker file import beside a connector sync, say. MoneyBin does not dedup investment
     events across sources yet (transactions have prep.int_transactions__matched;
     investments have no equivalent), so the two ledgers interleave rather than merge:
     every event exists twice, lots double-count, and cost basis mixes two accountings.
     Nothing derived from that account can be trusted, so every position in it publishes
     no figure at all and carries valuation_status 'source_overlap'.

     Counted over source_type in the LEDGER, not over the raw tables the doctor's
     investment_source_overlap check reads. The two answer different questions on
     purpose: the check must fire before a transform has ever run, while the withhold
     must key on exactly the rows that fed these positions — a Plaid row routed to
     review never reached the ledger and never double-counted anything.

     The opening-lot bootstrap is EXCLUDED, and that exclusion is load-bearing rather
     than a refinement. A subtype 'opening_bootstrap' row is MoneyBin's own
     reconstruction of a pre-window position, synthesized from a broker snapshot
     precisely BECAUSE no transaction covers it (prep.int_plaid__opening_positions
     synthesizes only the gap the in-window transactions leave). It is not a second
     observation of an event the other source also reported, so it double-counts
     nothing. Counting it would make every broker-covered account holding any manual
     entry read as overlapping — and, worse, would put this model at odds with the
     doctor check that REPORTS the state: investment_source_overlap joins
     raw.plaid_investment_transactions to raw.manual_investment_transactions, so a
     holdings snapshot alone is not an overlap there. A user would then hold a
     withheld portfolio with a passing check and no remedy named anywhere, which is
     strictly worse than the double-count this withhold exists to contain. The
     subtype is not user-authorable, so it cannot be spoofed into hiding a real
     overlap.

     Distinct source_type rather than a manual/plaid pair: two importers of the same
     kind are one accounting, and the failure is generic to any second source.

     Joined into the withheld CTE below as its own flag rather than added as a fifth
     clause: those four all describe one position's own quantity and this describes the
     account's ledger, and their remedies have nothing in common — those want a share
     count reconciled, this wants a whole feed removed.

     It carries a timestamp beside the flag because the final updated_at has to fold
     this input too. The flag is ACCOUNT-scoped, so a second-source event recorded
     against security B moves security A from 'valued' to 'source_overlap' while none
     of A's own lots, price, or snapshot timestamps change; a watermark built only
     from position-scoped inputs would report A as unchanged and an incremental
     consumer reading it would keep serving the pre-flip figure. The MAX is taken over
     exactly the rows the HAVING counts, so the timestamp and the flag can never
     describe different sets. */
  SELECT
    account_id,
    MAX(updated_at) AS overlap_observed_at /* Freshness of the account-level overlap input; folded into every affected row's updated_at below */
  FROM core.fct_investment_transactions
  WHERE
    COALESCE(subtype, '') <> 'opening_bootstrap'
  GROUP BY
    account_id
  HAVING
    COUNT(DISTINCT source_type) > 1
), withheld AS (
  /* Four clauses, none redundant — each guards a failure the others miss. The first
     three are quantity-specific: market value is quantity x price and does not depend
     on cost basis at all, so gating on investment_holdings_divergence (which also
     fails on a pure cost-basis mismatch) or on investment_staging_rejects (which fires
     on unmapped_subtype and transfer_direction_underivable too) would withhold a
     correct number for unrelated reasons.

     The second is scoped to positions HELD ACROSS the split
     (earliest_acquisition_date <= sr.trade_date): a corporate action only misstates a
     quantity that existed at the split. A position whose earliest lot opened AFTER the
     reject's date has a correct quantity from inception and carries no split event of
     its own, so without this bound it would withhold forever with nothing that could
     ever clear it.

     The third is not covered by the first: when the newest snapshot omits a position the
     ledger still carries, provider_reported_quantity is NULL, so
     `quantity <> provider_reported_quantity` evaluates to UNKNOWN rather than true and
     the position would slip through — publishing a market value for shares the broker
     says are gone. It fires ONLY when the broker once reported this position
     (ever_reported_positions): a position never in any snapshot is a manual holding, not
     a phantom, and withholding it would falsely claim the share count is wrong.

     The fourth guards a value that cannot be computed rather than one known wrong: open
     lots recorded in more than one currency (the manual event API takes --currency per
     event) have no single close to value the combined quantity against, so quantity x
     price would multiply a mixed-unit sum by one currency's price. Withheld until the
     lots agree; the arbitrary MAX(currency_code) picked above never reaches a figure.

     Its second arm catches the disagreement the count cannot see. COUNT(DISTINCT) ignores
     NULL, so a lot with no currency beside a known one counts as a single currency and
     would value the combined quantity at that currency's close, folding an amount with no
     unit into one that has one. An unknown unit does not agree with a known unit. Lots
     that are ALL unknown are deliberately not withheld: they disagree with nothing, carry
     no resolvable close either way (the price join matches on currency, which no NULL
     satisfies), and land 'unpriced' — the honest label for a unit nobody has stated. */
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
        AND pos.earliest_acquisition_date <= sr.trade_date
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
      pr.provider_reported_quantity IS NULL
      AND EXISTS(
        SELECT
          1
        FROM ever_reported_positions AS erp
        WHERE
          erp.account_id = pos.account_id AND erp.security_id = pos.security_id
      )
    )
    OR pos.currency_count > 1
    OR (
      pos.currency_count = 1 AND pos.unknown_currency_lots > 0
    ) AS is_withheld,
    NOT so.account_id IS NULL AS is_source_overlap,
    so.overlap_observed_at AS source_overlap_at
  FROM positions AS pos
  LEFT JOIN provider_reported AS pr
    ON pr.account_id = pos.account_id AND pr.security_id = pos.security_id
  LEFT JOIN source_overlap_accounts AS so
    ON so.account_id = pos.account_id
), usable_price AS (
  /* The latest close for each position, dropped — so the LEFT JOIN below reads NULL and
     the position falls back to 'unpriced' — when the ledger carries a recorded split
     (core.fct_investment_transactions type='split', via position_split_events) NEWER
     than that close. positions.quantity is already post-split, so multiplying it by a
     pre-split close would misvalue by the split factor and publish it as
     'carried_forward' — reading "a bit old" rather than "wrong by 2x". This is the
     recorded-split-but-stale-price gap the split-reject withhold does not cover (that
     clause fires on an UNrecorded split whose quantity is still pre-split).

     The currency fold that used to sit on the final price join lives here now: both
     sides UPPER()ed because the price's quote_currency (from the security object) and
     the lot's currency_code (from the transaction object, stored verbatim; unofficial
     crypto codes uncased) share no casing guarantee — a case-sensitive match would
     report a position 'unpriced' while the resolved close sits in
     core.fct_security_prices. */
  SELECT
    p.account_id,
    p.security_id,
    lp.close,
    lp.price_date,
    lp.source_type,
    lp.updated_at AS price_updated_at
  FROM positions AS p
  JOIN latest_price AS lp
    ON lp.security_id = p.security_id AND lp.quote_currency = UPPER(p.currency_code)
  WHERE
    NOT EXISTS(
      SELECT
        1
      FROM position_split_events AS pse
      WHERE
        pse.account_id = p.account_id
        AND pse.security_id = p.security_id
        AND pse.trade_date > lp.price_date
    )
)
SELECT
  p.account_id, /* FK to core.dim_accounts (grain) */
  p.security_id, /* FK to core.dim_securities (grain) */
  p.quantity, /* Total open units (Σ remaining_quantity); cast back to (28,10) — SUM widens to (38,10) */
  p.cost_basis, /* Total open basis (Σ cost_basis_remaining); cast back to (18,2) — SUM widens to (38,2) */
  p.average_cost, /* cost_basis / quantity; cast wraps the WHOLE division so the result is DECIMAL(28,10), not DOUBLE (DuckDB decimal / promotes to DOUBLE); (28,10) for crypto fractional-unit precision; NULL when quantity is 0 */
  p.currency_code, /* Denominating currency (one per position) */
  CASE
    WHEN wh.is_source_overlap OR wh.is_withheld
    THEN NULL
    ELSE (
      p.quantity * lp.close
    )::DECIMAL(18, 2)
  END AS market_value, /* quantity × the resolved close. NULL — never zero — when no usable price applies, the quantity is known wrong, or the account's ledger mixes two sources: a zero is indistinguishable from a worthless position and silently understates every aggregate that sums it */
  CASE
    WHEN wh.is_source_overlap OR wh.is_withheld OR p.basis_incomplete
    THEN NULL
    ELSE (
      (
        p.quantity * lp.close
      )::DECIMAL(18, 2) - p.cost_basis
    )::DECIMAL(18, 2)
  END AS unrealized_gain, /* market_value less cost basis. NULL whenever market_value is NULL, AND — even on a valued row — whenever any contributing open lot has basis_incomplete: an ACATS-style transfer_in with unknown basis stores a 0.00 cost that is not a real zero, so the subtraction would overstate the gain by the missing basis. market_value stays published (quantity × close is unaffected); only the gain is unknowable. Realized gain is ledger-derived and lives in core.fct_realized_gains */
  CASE WHEN wh.is_source_overlap OR wh.is_withheld THEN NULL ELSE lp.price_date END AS price_date, /* The date of the close used, which may be earlier than today. NULL whenever market_value is NULL — when no close resolved ('unpriced') and when one did but the figure cannot be trusted ('withheld', 'source_overlap'): a held-back row publishing today's date beside blanked figures reads as "pricing is current, something else is missing", which is the opposite of the truth. The close itself is not lost — it stays queryable in core.fct_security_prices, which is where a support path should look */
  CASE WHEN wh.is_source_overlap OR wh.is_withheld THEN NULL ELSE lp.source_type END AS price_source, /* Which source_type supplied the close (see core.fct_security_prices); NULL exactly when price_date is NULL — on 'unpriced', 'withheld', and 'source_overlap' */
  CASE
    WHEN wh.is_source_overlap OR wh.is_withheld
    THEN NULL
    ELSE CAST(CURRENT_DATE - lp.price_date AS INT)
  END AS days_since_observed, /* Calendar days between the price used and today (uncategorized_queue.age_days precedent for this CAST-subtraction form). DATE_DIFF('day', ...) here fails every one of this model's valuation tests with a SQLMesh PlanError — measured to come from SQLMesh's render path losing the duckdb dialect for this node, not from sqlglot mishandling DATE_DIFF outright. 0 on a same-day close; a Monday reading 3 on an equity is an ordinary weekend, not a fault. NULL exactly when price_date is NULL — on 'unpriced', 'withheld', and 'source_overlap' */
  CASE
    WHEN wh.is_source_overlap
    THEN 'source_overlap'
    WHEN wh.is_withheld
    THEN 'withheld'
    WHEN lp.close IS NULL
    THEN 'unpriced'
    WHEN lp.price_date = CURRENT_DATE
    THEN 'valued'
    ELSE 'carried_forward'
  END AS valuation_status, /* valued | carried_forward | unpriced | withheld | source_overlap. Every status either carries a number the reader can rely on or carries none at all — no status publishes a qualified figure. The non-valued statuses stay distinct because each has a different remedy: unpriced wants a price feed; withheld wants the share count reconciled — an unreconciled split recorded, a broker divergence resolved, or a position the broker no longer reports closed out; source_overlap wants one of the account's two source ledgers removed, and is checked first because a share count measured against a double-counted ledger is a symptom rather than the fault */
  pr.provider_reported_quantity, /* NON-AUTHORITATIVE: the broker's claimed open units in its newest snapshot. Reconciliation reference only — `quantity` above is MoneyBin's figure. NULL = the broker's newest snapshot does not report this position */
  pr.provider_reported_cost_basis, /* NON-AUTHORITATIVE: the broker's claimed cost basis. Never overwrites or feeds `cost_basis` above; system doctor warns when the two diverge */
  pr.provider_reported_value, /* NON-AUTHORITATIVE: the broker's claimed market value. MoneyBin computes `market_value` above independently, as quantity × its own resolved close, and never blends this claim into it — no doctor check reconciles the two yet */
  pr.provider_reported_as_of, /* Oldest extracted_at among the snapshots summed into the three columns above (MIN, not MAX) — a canonical position spanning multiple broker connections is only as fresh as its stalest contributor; NULL when the broker no longer reports this position */
  GREATEST(
    p.updated_at,
    COALESCE(lp.price_updated_at, p.updated_at),
    COALESCE(psf.newest_snapshot_at, p.updated_at),
    COALESCE(wh.source_overlap_at, p.updated_at)
  ) AS updated_at /* Latest of all per-row input timestamps: the MAX over the position's open lots, the resolved close's freshness (market_value advances when a newer close lands), for a broker-reported position the newest snapshot receipt's time (valuation_status flips to 'withheld' when a fresh pull contradicts or omits the position), and for a position in a mixed-source account the newest ledger row behind that overlap (valuation_status flips to 'source_overlap'). The snapshot term keys on the position, not the account, so a purely manual holding in a covered account is not advanced by an unrelated pull; and it uses the snapshot RECEIPT (not the per-position claim), so a pull that DROPS a position still advances its watermark. The overlap term is the one deliberately ACCOUNT-scoped input: the flag it tracks is account-scoped too, so a second-source event on security B flips security A's status without touching any of A's position-scoped timestamps, and folding only those would report A unchanged. It reaches only overlapping accounts — source_overlap_accounts holds no others, so a single-source position joins to NULL and keeps its own watermark. COALESCE folds the LEFT-JOINed price, snapshot and overlap timestamps only when present, so an unpriced, manual, single-source position falls back to the lot timestamp. Does not advance on idempotent SQLMesh re-applies. See docs/specs/core-updated-at-convention.md. */
FROM positions AS p
LEFT JOIN provider_reported AS pr
  ON pr.account_id = p.account_id AND pr.security_id = p.security_id
LEFT JOIN position_snapshot_freshness AS psf
  ON psf.account_id = p.account_id AND psf.security_id = p.security_id
/* usable_price is per-position — currency-matched and split-staleness-excluded in the
   CTE above — so join on the position grain. A missing row means no usable close (none
   resolved, or the only one predates a recorded split), which reads as 'unpriced'. */
LEFT JOIN usable_price AS lp
  ON lp.account_id = p.account_id AND lp.security_id = p.security_id
LEFT JOIN withheld AS wh
  ON wh.account_id = p.account_id AND wh.security_id = p.security_id
