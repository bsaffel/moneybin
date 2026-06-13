# dedup-overmerge fixture (precision guard)

The hard part of exact-key auto-merge: two **genuinely-distinct** transactions
that share the same `(account, amount, date)` key, each exported by **both**
formats. Naive all-pairs + transitive closure would collapse all four rows into
one transaction (`source_count=4`), silently deleting a real $5 charge. Correct
result: **two** transactions, each `source_count=2`.

| txn | amount | date | CSV description | OFX description (payee) |
|---|---|---|---|---|
| coffee | -5.00 | 2026-03-10 | COFFEE ROASTERS DOWNTOWN MARKET | COFFEE ROAS |
| bookshop | -5.00 | 2026-03-10 | CITY BOOKSHOP UPTOWN | CITY BOOK |

Both $5 on the same day in the same account → exact-key blocking pairs all four
cross combinations. The `assign_components` cardinality guard keys on
`(source_type, source_origin, source_file)`: the two CSV rows share one file and
the two OFX rows share one file, so neither cross "bridge" edge can be added —
the engine pairs coffee↔coffee and bookshop↔bookshop 1:1.

## Hand-derived expectation

4 fixture rows (2 CSV + 2 OFX) → **2** gold records in `core.fct_transactions`,
each `source_count = 2`. Counted by hand from the two labeled txns above — not
from program output (see `.claude/rules/testing.md`).

Driven by `tests/scenarios/test_dedup_overmerge_guard.py`.
