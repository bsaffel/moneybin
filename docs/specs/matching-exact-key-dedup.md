# Exact-Key Cross-Source Auto-Merge

> Last updated: 2026-06-13
> Status: implemented
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

## Decision (2026-06-13)

**Auto-merge on exact key.** For the **cross-source tier only**, a pair that is
same `account_id` + exact `amount` + `date_distance = 0` is a near-certain
duplicate — accept it regardless of description similarity. Description becomes a
**tiebreaker** (which rows pair), not a **gate** (whether they merge). Chosen
over "route to review" and "tighter multi-signal guard": across formats the
description is unreliable in both directions (same merchant looks different;
different merchants look similar), so no description threshold cleanly separates
them.

`date_distance > 0` keeps the weighted formula — when dates differ, description
still matters.

## Mechanisms

Two changes, both scoped to the cross-source tier / shared assignment; transfer
matching and within-source (Tier 2b) acceptance are unchanged.

### 1. Exact-key confidence floor (`scoring.py`)

`compute_confidence(..., exact_key_floor=high_confidence_threshold)` lifts an
exact-key cross-source pair into `[floor, 1.0]` via
`floor + (1 − floor)·description_similarity`:

- The minimum (`description_similarity = 0`) equals `high_confidence_threshold`,
  so every exact-key pair clears auto-merge — no `_classify_pair` change needed.
- The result is **monotonic** in `description_similarity`, so `assign_components`
  still orders true twins ahead of bridge pairs (the tiebreaker).
- The **persisted** `match_confidence` honestly reflects exact-key certainty
  (≥ 0.95), not the low jaro score; the raw similarity is still recorded in
  `match_signals.description_similarity` for audit.

The floor is threaded from `MatchingSettings.high_confidence_threshold` (not
hard-coded) so the two stay coupled if the threshold ever changes. Tier 2b
passes no floor.

### 2. Cardinality guard (`assignment.py::assign_components`)

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

## Precision tradeoff (accepted)

A **lone** exact-key cross-source pair (one csv + one ofx, no competing rows)
that is actually two *different* merchants — e.g. a $5 coffee in csv and a $5
donut in ofx on the same day — is indistinguishable from a true cross-format
duplicate and **will merge**. The cardinality guard cannot help (only one row
per source). This is the accepted cost of the decision.

The guard *does* protect the realistic full-dual-import case: when both
transactions appear in **both** formats (4 rows), they pair 1:1 and stay two
records. In a real account dual-imported as csv + ofx, every transaction appears
in both formats, so this is the common shape; the lone asymmetric pair is the
rare exception.

## Testing

- **Unit** (`tests/moneybin/matching/`): exact-key floor lifts low-similarity
  pairs ≥ threshold and preserves description ordering; `date_distance > 0`
  still uses the weighted formula; the cardinality guard pairs N duplicates 1:1
  (including with equal scores) and still collapses distinct-file N-way groups.
- **Scenario** (`tests/scenarios/`):
  - `dedup-cross-format-truncation` (positive) — 4 real deidentified WF
    OFX↔CSV pairs with low description similarity collapse to 4 gold records,
    each `source_count = 2`.
  - `dedup-overmerge-guard` (negative/precision) — two distinct $5 txns, each in
    both formats (4 rows), stay two records (`source_count = 2` each), never one
    (`source_count = 4`).
  - `dedup-negative-fixture` reconciled: its former lone exact-key
    different-merchant case now auto-merges by design and was removed; the
    realistic precision concern moved to `dedup-overmerge-guard`.

## Out of scope

- The OFX `&` double-HTML-encoding (`AT&T` → `AT&amp;amp;T`) is handled upstream
  at extraction by `_decode_text_field` (OFX extractor, since #194), so the
  decoded description is what reaches dedup. This change adds the previously
  missing regression test for it; no further fix is needed (stale rows imported
  before #194 are cleaned by re-import, not retroactively).
- No change to transfer matching, within-source (Tier 2b) acceptance, blocking,
  or the prep fold.
