# dedup-negative fixture

Two pairs of (CSV, OFX) rows hand-authored to look superficially similar
but represent genuinely distinct transactions. The dedup engine MUST NOT
collapse any of them — see `tests/scenarios/test_dedup_negative_fixture.py`.

## Why each pair must not collapse

| CSV row | OFX row | Why distinct |
|---|---|---|
| WHOLE FOODS 2024-04-10 -$32.45 | AMAZON.COM 2024-04-10 -$32.99 | different merchants, different amounts (blocked by exact-amount) |
| TRADER JOES 2024-04-10 -$28.10 | TRADER JOES 2024-04-11 -$28.10 | same merchant, different days (date_distance>0 → weighted, not auto-merged) |

Expected `core.fct_transactions` count: 4 (no collapse).

## Removed: lone exact-key different-merchant pair

A former third pair (STARBUCKS csv vs DUNKIN ofx — different merchants but
same account, amount, and day) was removed when exact-key auto-merge landed
(2026-06-13). A **lone** cross-source pair sharing
`(account, exact amount, same day)` is indistinguishable from a true
cross-format duplicate, and now auto-merges by design — the accepted precision
tradeoff. The realistic form of that precision concern (both merchants present
in **both** formats, so the cardinality guard can pair them 1:1) lives in the
`dedup-overmerge-guard` scenario.
