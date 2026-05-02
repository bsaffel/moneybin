# dedup-negative fixture

Three pairs of (CSV, OFX) rows hand-authored to look superficially similar
but represent genuinely distinct transactions. The dedup engine MUST NOT
collapse any of them — see `tests/scenarios/test_dedup_negative_fixture.py`.

## Why each pair must not collapse

| CSV row | OFX row | Why distinct |
|---|---|---|
| WHOLE FOODS 2024-04-10 -$32.45 | AMAZON.COM 2024-04-10 -$32.99 | different merchants, different amounts |
| TRADER JOES 2024-04-10 -$28.10 | TRADER JOES 2024-04-11 -$28.10 | same merchant, different days |
| STARBUCKS 2024-04-15 -$5.75 | DUNKIN 2024-04-15 -$5.75 | different merchants on same day |

Expected `core.fct_transactions` count: 6 (no collapse).
