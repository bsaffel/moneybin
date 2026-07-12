MODEL (
  name prep.stg_plaid__investment_transactions,
  kind VIEW
);

/* Maps Plaid's investment taxonomy (6 types x ~48 subtypes) onto MoneyBin's
   closed 14-value ledger vocabulary, flips the amount sign, and resolves dates.
   Lifecycle rows are dropped entirely; rows that must not become ledger events
   carry ledger_include = FALSE + a review_reason (core unions WHERE
   ledger_include; system doctor reads review_reason IS NOT NULL).

   THE SIGN FLIP LIVES HERE AND NOWHERE ELSE. Plaid's amount is positive =
   cash OUT (a buy); the ledger is negative = cash out. The loader stores Plaid's
   value verbatim and core does not touch it, so this view is the single point of
   inversion — flipping in a second place turns every buy into income. quantity is
   NEVER flipped at any layer: Plaid already signs it per the ledger convention.

   security_id is NULL-passthrough (no COALESCE to the provider id) — the
   stg_plaid__securities / __investment_holdings precedent. A provider id in the
   canonical column sails past cost_basis.py's `if security_id is None: continue`
   guard and silently corrupts basis; NULL is honest and detectable. account_id
   keeps the source-native fallback (the accounts precedent, app_account_links.sql).

   Three behaviors are GOLDEN-GATED — the Plaid Sandbox goldens that would settle
   them do not exist yet, so v1 ships the conservative default and marks the branch
   point (sync-plaid-investments.md, Open Questions). All three live in the
   `reviewed` CTE. */
WITH classified AS (
  SELECT
    t.*,
    LOWER(COALESCE(t.investment_transaction_type, '')) AS ptype,
    LOWER(COALESCE(t.investment_transaction_subtype, '')) AS psub
  FROM raw.plaid_investment_transactions AS t
), mapped AS (
  /* is_lifecycle marks rows captured in raw for audit that are never ledger
     events; the next CTE filters them out entirely. (cancel_transaction_id is a
     deprecated dead field -- never build on it.)

     The taxonomy CASE is a closed function: every branch emits one of the 14
     ledger types, and an unrecognized subtype falls to the `other` fallback --
     a raw Plaid string can never reach the `type`/`subtype` columns (it survives
     only in provider_type / provider_subtype). Branch order is load-bearing:
     the option legs (assignment/exercise/expire) and the short legs (buy to
     cover / sell short) are matched on subtype BEFORE the ptype-gated buy/sell
     branches, so they route to `other` rather than opening a spurious long lot.

     transfer/{transfer, send, merger, spin off, trade} splits on security
     presence. A cash-only leg (no security_id) is a cash movement, not a share
     movement, so it maps to deposit/withdrawal with direction from the amount
     sign alone -- quantity is never consulted on that arm. A security-bearing
     leg takes its direction from the quantity SIGN and nothing else: the amount
     sign is NOT a proxy for it, since cash coming in accompanies shares going
     OUT. A security-bearing leg carrying no share delta (Plaid sends quantity 0,
     not NULL) therefore has no derivable direction at all, and is routed to
     review via is_underivable_transfer rather than guessed -- guessing it
     transfer_in makes it an _ACQUISITION_TYPE (cost_basis.py), which opens a
     phantom zero-share lot carrying a merger's cash-in-lieu as its basis while
     the proceeds never realize. */
  SELECT
    *,
    (
      ptype = 'cancel'
      OR (
        ptype = 'cash' AND psub IN ('pending credit', 'pending debit')
      )
      OR (
        ptype = 'transfer' AND psub = 'request'
      )
    ) AS is_lifecycle,
    (
      ptype = 'transfer'
      AND psub IN ('transfer', 'send', 'merger', 'spin off', 'trade')
      AND NOT security_id IS NULL
      AND COALESCE(quantity, 0) = 0
    ) AS is_underivable_transfer,
    CASE
      WHEN ptype = 'buy' AND psub IN ('buy', 'contribution')
      THEN 'buy'
      WHEN psub IN (
        'dividend reinvestment',
        'interest reinvestment',
        'long-term capital gain reinvestment',
        'short-term capital gain reinvestment'
      )
      THEN 'reinvest'
      WHEN psub IN ('assignment', 'exercise', 'expire')
      THEN 'other'
      WHEN ptype = 'sell' AND psub = 'sell'
      THEN 'sell'
      WHEN psub IN ('buy to cover', 'sell short')
      THEN 'other'
      WHEN ptype = 'sell' AND psub = 'distribution'
      THEN 'transfer_out'
      WHEN psub = 'stock distribution'
      THEN 'transfer_in'
      WHEN psub IN (
        'account fee',
        'legal fee',
        'management fee',
        'transfer fee',
        'trust fee',
        'fund fee',
        'miscellaneous fee',
        'margin expense',
        'tax',
        'tax withheld',
        'non-resident tax'
      )
      THEN 'fee'
      WHEN psub IN ('dividend', 'qualified dividend', 'non-qualified dividend')
      THEN 'dividend'
      WHEN psub IN ('interest', 'interest receivable')
      THEN 'interest'
      WHEN psub IN ('long-term capital gain', 'short-term capital gain', 'unqualified gain')
      THEN 'capital_gain_distribution'
      WHEN psub = 'return of principal'
      THEN 'return_of_capital'
      WHEN ptype = 'cash' AND psub IN ('contribution', 'deposit')
      THEN 'deposit'
      WHEN ptype = 'cash' AND psub = 'withdrawal'
      THEN 'withdrawal'
      WHEN psub = 'split'
      THEN 'split'
      WHEN ptype = 'transfer'
      AND psub IN ('transfer', 'send', 'merger', 'spin off', 'trade')
      THEN CASE
        WHEN security_id IS NULL
        THEN (
          CASE WHEN amount > 0 THEN 'withdrawal' ELSE 'deposit' END
        )
        WHEN quantity > 0
        THEN 'transfer_in'
        WHEN quantity < 0
        THEN 'transfer_out'
        ELSE 'other'
      END
      WHEN psub IN ('adjustment', 'loan payment', 'rebalance')
      THEN 'other'
      ELSE NULL
    END AS mapped_type,
    CASE
      WHEN psub = 'dividend reinvestment'
      THEN 'dividend'
      WHEN psub = 'interest reinvestment'
      THEN 'interest'
      WHEN psub IN ('long-term capital gain reinvestment', 'short-term capital gain reinvestment')
      THEN 'capital_gain'
      WHEN psub IN ('tax', 'tax withheld', 'non-resident tax')
      THEN 'tax_withheld'
      WHEN psub = 'qualified dividend'
      THEN 'qualified'
      WHEN psub = 'non-qualified dividend'
      THEN 'non_qualified'
      WHEN psub = 'long-term capital gain'
      THEN 'long_term'
      WHEN psub = 'short-term capital gain'
      THEN 'short_term'
      ELSE NULL
    END AS mapped_subtype
  FROM classified
), reviewed AS (
  /* GOLDEN-GATED (1 of 3) -- split routing. Plaid reports a share DELTA;
     cost_basis.py's _apply_split reads a MULTIPLIER and scales every open lot by
     it, so a raw passthrough destroys the basis of the whole position. Whether M
     is derivable from the row at all is exactly what the Sandbox goldens must
     settle, so v1 derives nothing and routes EVERY split to review -- a wrong
     multiplier is worse than a surfaced gap. Swap-in when the goldens land:
     M = (pre_split_qty + delta) / pre_split_qty over strictly-earlier ledger rows
     for the same (source_account_key, source_security_key, source_origin), still
     routing to review when pre_split_qty <= 0 or the position predates the window.

     GOLDEN-GATED (2 of 3) -- event_group_id stays NULL. Pairing the two legs of a
     reinvest / corporate action needs a key the goldens have not validated. NULL
     loses no correctness, only linkage: the acquisition still opens its lot and
     income still counts once. Swap-in: a content hash over the validated key,
     applied to reinvest / corporate-action rows only.

     GOLDEN-GATED (3 of 3) -- fee inclusion in ledger_amount. The plain flip
     assumes Plaid's amount is fee-INCLUSIVE (what the ledger contract requires);
     Plaid does not document it either way. The loader's drift guard surfaces rows
     that reconcile under neither convention. Swap-in if the goldens show
     fee-EXCLUSIVE: -(amount + COALESCE(fees, 0)) for fee-bearing rows.

     Also here: basis-unknown in-kind movements (transfer with amount = 0) map to
     a NULL ledger amount, never a literal 0 -- cost_basis.py flags an acquisition
     `basis_incomplete` on exactly `event.amount is None`, so a false zero-basis lot
     would look complete and a later sale would realize a fully-taxed phantom gain.

     Also here: ledger_quantity is NULL on every cash-only / basis-only type --
     the closed `_QTY_NULL` set the ledger's own writer enforces
     (investment_service.py) and core.fct_investment_transactions.quantity
     contracts ("Signed units: + acquire, - dispose, NULL cash-only"). Plaid
     sends 0, not NULL, on those legs, and a literal 0 makes every dividend and
     fee read as a share-moving event to any consumer keying on
     `quantity IS NULL` (prep.int_plaid__opening_positions does). The same NULL
     covers `other` (option legs, short legs, adjustment/loan payment/rebalance):
     MoneyBin models no short-position or options book, so the raw share count on
     those legs is not lot-affecting and must not masquerade as one. A row that
     instead falls to `other` via the unmapped-subtype review path below keeps
     its raw quantity -- mapped_type is NULL there, not 'other' -- since that
     value is exactly what routes it to review. */
  SELECT
    *,
    CASE
      WHEN mapped_type = 'split'
      THEN 'split_underivable'
      WHEN is_underivable_transfer
      THEN 'transfer_direction_underivable'
      WHEN mapped_type IS NULL AND NOT security_id IS NULL AND COALESCE(quantity, 0) <> 0
      THEN 'unmapped_subtype'
      ELSE NULL
    END AS review_reason,
    NULL::TEXT AS ledger_event_group_id,
    CASE
      WHEN COALESCE(mapped_type, '') IN ('transfer_in', 'transfer_out') AND amount = 0
      THEN NULL::DECIMAL(18, 2)
      ELSE (
        -1 * amount
      )::DECIMAL(18, 2)
    END AS ledger_amount,
    CASE
      WHEN mapped_type IN (
        'dividend',
        'interest',
        'capital_gain_distribution',
        'deposit',
        'withdrawal',
        'fee',
        'return_of_capital',
        'other'
      )
      THEN NULL::DECIMAL(28, 10)
      ELSE quantity
    END AS ledger_quantity
  FROM mapped
  WHERE
    NOT is_lifecycle
)
SELECT
  r.investment_transaction_id,
  COALESCE(al.account_id, r.account_id) AS account_id,
  r.account_id AS source_account_key,
  sl.security_id AS security_id,
  r.security_id AS source_security_key,
  COALESCE(r.transaction_datetime::DATE, r.transaction_date) AS trade_date,
  r.transaction_date AS settlement_date,
  NULL::DATE AS original_acquisition_date,
  COALESCE(r.mapped_type, 'other') AS type,
  r.mapped_subtype AS subtype,
  r.ledger_event_group_id AS event_group_id,
  r.ledger_quantity AS quantity,
  r.price,
  r.ledger_amount AS amount,
  r.fees,
  COALESCE(r.iso_currency_code, r.unofficial_currency_code) AS currency_code,
  r.investment_transaction_type AS provider_type,
  r.investment_transaction_subtype AS provider_subtype,
  TRIM(r.transaction_name) AS description,
  r.review_reason IS NULL AS ledger_include,
  r.review_reason,
  r.source_file,
  r.source_type,
  r.source_origin,
  r.extracted_at,
  r.loaded_at AS created_at
FROM reviewed AS r
LEFT JOIN app.account_links AS al
  ON al.status = 'accepted'
  AND al.ref_kind = 'source_native'
  AND al.source_type = r.source_type
  AND al.source_origin = r.source_origin
  AND al.ref_value = r.account_id
LEFT JOIN app.security_links AS sl
  ON sl.status = 'accepted'
  AND sl.ref_kind = 'plaid_security_id'
  AND sl.source_type = r.source_type
  AND sl.ref_value = r.security_id
