# Cross-Source Auto-Merge on Description Agreement

> Last updated: 2026-08-05
> Status: implemented
> Filename note: the file is still `matching-exact-key-dedup.md` because
> CHANGELOG history and five specs link to it. The exact-key rule it was named
> for was replaced on 2026-08-02 — see [§Decision](#decision-2026-08-02-supersedes-2026-06-13).
> Address: M1B (matching engine refinement)
> Parent: [`matching-same-record-dedup.md`](matching-same-record-dedup.md) (cross-source dedup, pillar A)
> Refines: [`matching-nway-dedup.md`](matching-nway-dedup.md) — adds a cardinality guard to its Requirement 2 edge-add rule, and supersedes its "No change to scoring" out-of-scope note for the cross-source tier
> Inert on real cross-source data until: [`account-identity-resolution.md`](account-identity-resolution.md) (M1S) — see [§Prerequisite](#prerequisite-shared-account_id-m1s)

## Goal

The same transaction imported from two file formats of one account (e.g. Wells
Fargo `.qfx` and `.csv`) must collapse into **one** `core.fct_transactions` row
with `source_count = 2`, instead of doubling.

## The bug

Cross-source dedup already blocks candidates correctly (same `account_id` +
**exact `amount`** + date within `date_window_days`). The failure was purely in
scoring/acceptance:

```
confidence = 0.40·date_score + 0.60·description_similarity
auto-merge requires confidence ≥ high_confidence_threshold (0.95)
```

(Those are the weights as of the bug. They are now 0.30/0.70 — see
[§The cross-source asymmetry](#the-cross-source-asymmetry).)

For an exact duplicate (`date_distance = 0` → `date_score = 1.0`), auto-merge
needed description similarity `S ≥ 0.92`. OFX truncates/splits descriptions
differently from CSV (OFX `description` is truncated; the rest lands in the
unscored `memo`), so cross-format `S` is well below 0.92 — exact duplicates
**never auto-merged**. Verified live: importing 5 WF `.csv` files (twins of 5
already-loaded `.qfx`) produced **558** core rows instead of **279**.

## Prerequisite: shared `account_id` (M1S)

This fix is **correct but inert on real cross-source data** until account
identity unifies. The blocking self-join requires `a.account_id = b.account_id`
(`scoring.py`), but today each source mints its own `account_id`, so a real
account imported as both `.qfx` and `.csv` carries **two** `account_id`s and the
join produces **zero** candidate pairs — scoring (this fix) is never reached.
Verified live 2026-06-13: the 5-WF `.qfx`+`.csv` case yielded 10 `account_id`s
for 5 accounts → 558 rows, all `source_count = 1`. The unit/scenario fixtures
here pass because they construct both sides with the **same** `account_id`;
production data does not. [`account-identity-resolution.md`](account-identity-resolution.md)
(M1S) makes `account_id` canonical across sources, at which point this auto-merge
fires as designed (279 @ `source_count = 2`).

## Decision (2026-08-02, supersedes 2026-06-13)

**Auto-merge on description agreement.** A pair auto-merges when one description
**contains** the other, after normalization (case-fold, reduce punctuation to a
space, collapse internal whitespace runs, trim), with both sides non-empty and
the **contained** side carrying at least one token that is not transaction-type
boilerplate — unless the two normalize to the same string, which is agreement on
its own. The date gap no longer gates anything — it only modulates the score
below the floor.

Agreement is required at the decision, not merely rewarded in the score:
`TransactionMatcher._classify_pair` accepts only a pair that both clears
`high_confidence_threshold` **and** agrees, for **every** tier. The score cannot
carry that gate alone. Its weighted branch is
`_WEIGHT_DATE * date_score + _WEIGHT_DESCRIPTION * similarity`, and `date_score`
peaks at `date_distance_days = 0`, so at the shipped weights any similarity above
~0.93 clears 0.95 with no agreement at all — `SHELL 1234` against `SHELL 1235`,
same account, same amount, same day, scores 0.972. The **agreement floor** stays
cross-source-only; the **agreement requirement** applies everywhere.

Containment is the literal mechanism: sources carry a shared merchant string,
truncate it at different lengths, and wrap it in their own preamble and trailing
detail, so the common text is not always at the front. A prefix-only rule was
implemented first and rejected — it missed real Wells Fargo pairs where the CSV
prepends a transaction-type preamble (`RECURRING PAYMENT AUTHORIZED ON 01/25
TASKAPP …` against an OFX `TASKAPP`). The relation is structural, so it needs no
similarity cutoff to tune.

Every cross-source pair that survives 1:1 assignment and does *not* agree goes
to **review** — never silently dropped. Blocking has already required the same
account, an exact amount, and a date inside the window; that is enough evidence
to ask about, and dropping instead would be its own silent action, leaving a
duplicate in the ledger nobody is told about.

Tier 2b has no review queue, so a within-source pair that does not agree is
simply not merged: both rows stay in the ledger. That is a double-count —
visible in any total, and correctable — where the alternative is deleting one of
two genuinely distinct transactions with nothing recorded anywhere. It is also
the premise the agreement floor is withheld from Tier 2b for: inside one source
the rendering is consistent, so two rows written differently are two
transactions.

### Why the 2026-06-13 exact-key rule was replaced

That decision held that `date_distance = 0` made a pair "near-certain" and that
no description threshold cleanly separates cross-format pairs. Measured against
live data, both halves turned out to be wrong:

- The same-day band held the **weakest** evidence in the set, not the
  strongest. On a real card, 22 of the 27 pairs whose descriptions shared
  nothing sat at `date_distance = 0` — including amount collisions between
  genuinely different merchants, which the old rule merged silently.
- Review volume was anti-correlated with real uncertainty: 265 of 266 queued
  pairs had descriptions already agreeing at 0.90+, while the 22 ambiguous ones
  bypassed review entirely.
- Containment *does* separate the population cleanly, and needs no threshold.
  On the card above it split 349 candidate pairs into **346 auto-merge / 3
  review**, where the 346 are exactly the genuine duplicates and the 3 are
  exactly the false amount-collisions (two different merchants that happened to
  charge the same amount within the window). **These counts measure containment
  alone**, before the merchant-token requirement below was added; that guard can
  only move pairs from auto-merge to review, so 346 is an upper bound and the
  split has not been re-measured. Confirmed on a second account with
  a different source pair (csv↔ofx rather than pdf↔ofx): 168 auto-merge, 12
  review, with 191 of 201 previously-scored pairs byte-identical.

Whitespace collapse is load-bearing, not cosmetic. Two of the five pairs
containment initially missed differed *only* by internal spacing
(`UBER   *TRIP` against `UBER *TRIP HELP.UBER.COM CA`) — sources pad
descriptions to fixed column widths.

Normalizing descriptions first (`normalize_description`) was measured and
rejected: it moved average similarity by +0.011, left 270 of 349 pairs unequal,
and produced one *more* sub-0.80 pair than raw text. Its trailing-location
regex requires a plain capitalized word before the state code, so statement
layouts like `NETFLIX.COM NETFLIX.COM CA` pass through untouched.

### The cross-source asymmetry

Only the *floor* is asymmetric; the *agreement requirement* is not. The floor is
Tier 3 only: across sources each side lists a transaction once, so two agreeing
rows are one transaction rendered twice, and lifting them to auto-merge is safe.
Inside one source the rendering is consistent, so two rows written *differently*
are two transactions — granting the floor there would silently delete one.

Tier 2b gets the requirement anyway. `_classify_pair` demands
`descriptions_agree` before any silent merge on either tier, because the
sentence that withholds the floor from Tier 2b ("two rows written differently
are two transactions") is the same sentence that forbids merging them. Without
it the weighted score is the only gate on Tier 2b, and that score peaks at
`date_distance_days = 0` where its date term is 1.0 — so a same-day pair reaches
0.95 at `description_similarity ≥ 0.929`, which two distinct charges at one
merchant differing only in a trailing reference number clear
(`SHELL 1234` / `SHELL 1235` score 0.972).

Tier 2b's score also moved, as a consequence of the reweight below rather than a
retune of this tier: at 0.40/0.60 a same-day pair needed 0.917 similarity, at
0.30/0.70 it needs 0.929. That narrows what Tier 2b merges silently, which is
the safe direction for a tier with no review queue, so the weights were not
re-tuned to restore the old boundary — `_WEIGHT_DESCRIPTION ≥ review_threshold`
(0.70) is load-bearing for the window-edge dead zone, and 0.60 does not satisfy
it.

## Mechanisms

Two changes to the cross-source tier and shared assignment, plus one gate that
applies to every dedup tier. Transfer matching and blocking are unchanged.

### 1. Description-agreement confidence floor (`scoring.py`)

The blocking query computes `desc_agree` — one normalized description containing
the other, **with both sides required non-empty**. That non-empty requirement is
load-bearing, not defensive: `contains(x, '')` is `TRUE`, so a source that omits
descriptions would otherwise agree with every row it met and merge an entire
account silently.

**The contained side must also carry a merchant token** (`_BOILERPLATE_TOKENS`,
`_carries_a_merchant_token()`). Containment asks whether one string sits inside
another, never whether the shared text means anything, and the empty description
is only the extreme case of that: `DEBIT` sits inside most card descriptions, so
a source rendering a row as bare boilerplate would agree with an unrelated row
that merely collided on amount inside the window. The contained side *is* the
shared evidence, so it is the side tested. One non-boilerplate token is enough —
sources routinely prepend their own transaction-type words to a real merchant
string (`POS AMAZON` inside `POS AMAZON MKTPL*… SEATTLE WA` is a genuine
agreement), so rejecting any description merely *containing* boilerplate would
throw away the matches this gate exists to find.

A qualifying token must carry a letter. Without that the rule counts a card or
reference number as merchant evidence while it names nobody: `POS 1234` is
boilerplate plus a number, and it sits inside every longer description from the
other source that prints the same digits — so two distinct charges on one card,
equal in amount and inside the window, would auto-merge and one would vanish with
no review entry. The requirement is a letter *somewhere in the token*, not the
absence of digits; real merchant strings are full of them (`7ELEVEN`).

**Containment must begin at a token boundary.** A raw substring test reads `ARCO`
as agreeing with `MARCOS PIZZA` — a real merchant name buried inside an unrelated
one, so it is neither boilerplate nor digits and no other guard refuses it. A
truncating source cuts the tail and a wrapping source leaves a space in front, so
both still begin where a token begins; a fragment beginning inside a word cannot
arise from either. Implemented as a leading space on both sides of the
`contains()` call rather than a token-set test, because a token-set test would
refuse the truncation case — `WALMART STO` is not a token of `WALMART STORE`.

**A match ending inside a token must carry a whole token before it.** Constraining
only the start leaves `SHELL` agreeing with `SHELLYS CAFE`. That is the same shape
a truncating source produces (`STARBUCK` from `STARBUCKS STORE 1234`), and nothing
in the strings separates the two — a prefix that is a whole merchant on its own is
as likely a coincidence as a truncation. What differs is how much matched *whole*:
a fixed-width truncation usually cuts several tokens in, and those complete tokens
are evidence a bare one-token prefix does not have. So the match must end on a
token boundary too, *or* the contained side must hold at least one interior space.
`STARBUCKS STO` still relates to the full string; `SHELL` no longer does, and
reaches Tier 3 review instead of merging silently. Normalization collapses
whitespace runs and trims, so an interior space is a reliable "more than one
token". The cost is the single-token truncation, which now costs a review click;
the asymmetry pays for it, since the merge it prevents deletes a real charge with
no review entry.

**Exact equality is exempt from the merchant-token requirement on the same day.**
The rule guards against a short *fragment* hiding inside a longer string; two
identical descriptions have no longer string to hide inside. A bank writing
`DEPOSIT` in both its OFX and CSV export of one account cannot name a merchant,
and demanding one refuses the plainest duplicate there is — caught by
`account-identity-cross-source`, where a $5,000 `Deposit` present in both sources
stopped collapsing.

The date bound is what stops that exemption from becoming a hole once the window
widened to five days. Two *different* charges of one amount, three days apart on
one account, are both `DEBIT` as well, and no other field separates them — so an
unbounded carve-out silently merges them and deletes one. The pair the exemption
exists for shares a posting date (the `Deposit` above is same-day in both
sources), and before the window widened no cross-source pair could merge at any
other gap, so same-day is exactly the condition under which the carve-out was
validated. A generic pair at a gap keeps its Tier 3 review entry; nothing is
dropped, the question is only who answers it.

The vocabulary is hand-maintained and pinned by **set equality**, not membership:
every token added to it removes evidence from the gate and widens what merges
without review, so the change has to land as a deliberate edit in one reviewable
diff. A length floor was considered and rejected on the numbers — `PURCHASE` is
8 characters and `TASKAPP` is 7, so the boilerplate is *longer* than the real
merchant token and length cannot separate them.

Known limit: a boilerplate word MoneyBin has not seen reopens the hole quietly.
The durable form is a selectivity floor — reject a fragment contained in many
other rows on the same account, since boilerplate matches dozens and a merchant
string matches one or two — which needs measurement against real data before it
can replace the list.

Normalization is `_normalized_description()` in `scoring.py` — case-fold, reduce
punctuation to a space, collapse internal whitespace runs, trim. Deliberately
*only* canonicalization: running the categorization normalizer
(`normalize_description`) here was measured and rejected (see above).

Punctuation is **reduced to a space, not deleted**. That is the difference
between `WAL-MART` matching `WAL MART` (it does) and matching `WALMART` (it does
not): deleting separators would merge pairs whose word boundaries genuinely
differ, while reducing them only absorbs the characters one source prints and
another omits. Once agreement became the sole route to auto-merge, a single `#`
was enough to sink an otherwise identical pair — `STARBUCKS #1234` against
`STARBUCKS 1234` is the ordinary cross-source rendering difference this feature
exists to absorb, and it was costing a human decision every time. A digit is not
punctuation, so `SHELL 1234` and `SHELL 1235` still disagree.

`compute_confidence(..., agreement_floor=high_confidence_threshold,
descriptions_agree=True)` then lifts the pair into `[floor, 1.0]` via
`floor + (1 − floor)·description_similarity`:

- The minimum (`description_similarity = 0`) equals `high_confidence_threshold`,
  so every agreeing pair clears auto-merge at any date gap inside the window.
- The result is **monotonic** in `description_similarity`, so `assign_components`
  still orders true twins ahead of bridge pairs (the tiebreaker).
- `match_signals` records `descriptions_agree` alongside the raw similarity.
  Auto-merge is the one path with no human in it, so the decision record is the
  only place its reasoning survives.

The floor is threaded from `MatchingSettings.high_confidence_threshold` (not
hard-coded) so the two stay coupled if the threshold ever changes. Tier 2b
passes no floor.

`_classify_pair` accepts a pair only when it is at/above the threshold **and**
`descriptions_agree`, and routes **every** other surviving Tier 3 pair to
`pending`. `review_threshold` no longer applies to Tier 3; `assign_components`
bounds the queue, so it cannot run away.

### 2. Agreement required for every silent merge (`engine.py::_classify_pair`)

The score alone cannot gate an auto-merge, on any tier. `compute_confidence`
peaks at `date_distance_days = 0`, where the date term is 1.0 and the pair
scores `0.30 + 0.70 · description_similarity` — so `similarity ≥ 0.929` clears
0.95 without the floor and without agreeing. Two distinct charges at one
merchant differing only in a trailing reference number sit there. Requiring
`descriptions_agree` at the classification site closes that route on Tier 3 and
Tier 2b alike; the floor (mechanism 1) then decides only *which* agreeing
cross-source pairs also clear the threshold.

Both are needed. The floor without the gate leaves the day-0 hole open on Tier
2b; the gate without the floor sends agreeing but low-similarity cross-source
pairs to review, which is the queue-volume problem this spec set out to fix.

### 3. Cardinality guard (`assignment.py::assign_components`)

`assign_components` is a union-find spanning forest. With description no longer
gating, multiple exact-key pairs in one `(account, amount, date)` bucket would
chain transitively and **over-collapse** N genuinely-distinct transactions into
one (`source_count = N`), silently deleting real money.

The guard rejects any edge that would place two rows from the **same physical
source** — keyed on `(source_type, source_origin, source_file)` — into one
component. Within a single import file every row is a distinct transaction
(distinct FITID / content hash; within-source dedup only ever pairs across
*different* files), so two same-source rows can never be duplicates of each
other. Keying on the full triple (not the file string alone) keeps two
*different* sources distinct even when their file strings collide (a `march.csv`
vs a `march.ofx`). Nodes with an unknown file (None — seed-only nodes, unit
fixtures) impose no constraint.

This guard is universally correct for all dedup tiers (it only ever blocks a
would-be over-collapse) and a no-op for correct N-way collapse, where each
member comes from a distinct source.

## Precision

The 2026-06-13 design accepted a known false-merge: a **lone** cross-source pair
on the same day that is actually two *different* merchants — a $5 coffee in csv
and a $5 donut in ofx — merged silently, because the cardinality guard cannot
help when there is only one row per source.

Description agreement closes that case. Two different merchants do not contain
each other's descriptions, so the pair no longer clears the floor; it lands in
review, where a human sees both and decides. On live data the queue now holds
exactly the pairs that survive assignment but share no description — the
confirmed amount collisions.

The cardinality guard remains and still protects the full-dual-import case: when
both transactions appear in **both** formats (4 rows), they pair 1:1 and stay two
records.

### Two residual costs, both accepted

**A genuine duplicate rendered with no shared text is reviewed, not merged** — an
OFX row reading `ACH DEBIT` against a CSV row naming the merchant. Intended: the
pair is surfaced, not lost, and a wrong silent merge is the harder error to
notice and undo.

**A lone same-merchant, same-amount pair inside the window merges silently.** Two
separate $28.10 Trader Joe's trips on consecutive days, one captured by each of
two sources, are indistinguishable from one transaction posted a day after
purchase — same account, same amount, agreeing descriptions, one day apart. The
scorer has no signal that separates them.

Brandon accepted this on 2026-08-02, weighing that posting lag is the far more
common shape (266 such pairs on live data were all genuine duplicates) against
the rarer case where two sources each miss a different transaction. This
**widens** rather than creates the tradeoff: the 2026-06-13 rule already accepted
it for a lone same-day pair; it now extends across the window.

A coverage-overlap check was considered as a discriminator — "the other source
covers this date and has no agreeing row on it, so this is a distinct
transaction" — and rejected on inspection. That condition is precisely what
posting lag produces, so it cannot separate the two cases; it would break the
duplicate merges it was meant to preserve. The only remaining idea is
account-level corroboration (trust gap-merges more on accounts whose sources
already twin nearly every row); unimplemented, not required by this change.

`dedup-negative-fixture` pins both directions: the Trader Joe's pair merges by
design, and a SHELL/COSTCO pair (same account, same amount, one day apart,
descriptions sharing nothing) must not.

## Testing

- **Unit** (`tests/moneybin/matching/`): the agreement floor lifts
  low-similarity pairs ≥ threshold at every date gap the window admits and
  preserves description ordering; disagreeing pairs stay below the floor even on
  the same day; a blank description is not agreement; a boilerplate-only
  contained side is not agreement, while one merchant token beside boilerplate
  still is, and identical descriptions agree even when the shared text is pure
  boilerplate — but only on the same day, since identical boilerplate across a
  date gap is not agreement; the boilerplate vocabulary is pinned by set equality; a digit-only
  token is not merchant evidence while a merchant token carrying digits still is;
  a fragment starting mid-token is not agreement (`ARCO` inside `MARCOS PIZZA`)
  while a truncated tail still is; punctuation
  alone does not defeat agreement while a differing reference number still does;
  the floor does not reach
  Tier 2b (same fixture, opposite outcome); an auto-merge records
  `descriptions_agree`; the cardinality guard pairs N duplicates 1:1 (including
  with equal scores) and still collapses distinct-file N-way groups; a pair that
  cannot be persisted is filtered before assignment, so a higher-scoring
  disagreeing edge cannot claim the slot and strand an agreeing one.
  `_classify_pair` refuses a disagreeing pair at the weighted formula's true
  peak — reviewed on Tier 3, unmerged on Tier 2b — and the peak itself is
  asserted to clear `high_confidence_threshold`, so the day the formula alone
  becomes safe, the test that says otherwise fails loudly rather than rotting.
- **Scenario** (`tests/scenarios/`):
  - `dedup-cross-format-truncation` (positive) — 4 real deidentified WF
    OFX↔CSV pairs with low description similarity collapse to 4 gold records,
    each `source_count = 2`.
  - `dedup-overmerge-guard` (negative/precision) — two distinct $5 txns, each in
    both formats (4 rows), stay two records (`source_count = 2` each), never one
    (`source_count = 4`).
  - `dedup-negative-fixture` reconciled (2026-08-02): the TRADER JOES pair
    (same merchant/amount, one day apart) now auto-merges by design, with the
    justification recorded inline in the YAML; a SHELL/COSTCO pair — same
    account, same amount, one day apart, descriptions sharing nothing — was
    added in the same change so the scenario keeps a real negative on the
    agreement gate rather than only on the amount predicate.

## Out of scope

- The OFX `&` double-HTML-encoding (`AT&T` → `AT&amp;amp;T`) is handled upstream
  at extraction by `_decode_text_field` (OFX extractor, since #194), so the
  decoded description is what reaches dedup. This change adds the previously
  missing regression test for it; no further fix is needed (stale rows imported
  before #194 are cleaned by re-import, not retroactively).
- No change to transfer matching, blocking, or the prep fold. Within-source
  (Tier 2b) acceptance **is** in scope — it gains the agreement requirement and
  inherits the reweight — but gains no floor and no review queue.
