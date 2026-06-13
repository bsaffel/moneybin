# dedup-cross-format fixture

Four real-world WF OFX↔CSV pairs (deidentified) that failed to dedup before
exact-key auto-merge. Each pair is the **same** `(account, amount, date)` on
both sides; only the description text diverges because OFX truncates/splits it
differently from CSV. The matcher scores the CSV `description` against the OFX
`description` (sourced from the fixture `payee` column — the OFX `memo` is not
scored), so similarity lands well below the 0.95 auto-merge threshold. The
exact key must override that and auto-merge each pair.

| amount | date | CSV description | OFX description (payee) | why similarity is low |
|---|---|---|---|---|
| -150.00 | 2026-04-15 | RECURRING TRANSFER TO ACME HOLDINGS LLC BUSINESS MARKET RATE SAVINGS REF #… | TO ACME HOLDINGS LLC BUSINESS MA | "MARKET" cut mid-word; prefix reordered into the (unscored) memo |
| -2868.61 | 2026-04-07 | BILL PAY Megabank - Rewards Card ON-LINE xxxxxxxxxxxx1234 ON 04-07 | BILL PAY Megabank - Rewards | OFX name truncated; account tail lives in the memo |
| -29.00 | 2026-01-26 | RECURRING PAYMENT AUTHORIZED ON 01/25 TASKAPP TASKAPP.COM DE … CARD 1199 | TASKAPP | OFX description is just the merchant token |
| -105.00 | 2026-04-07 | BILL PAY Telco & Cable Internet ON-LINE xxxxx0062 ON 04-07 | BILL PAY Telco & Cable | OFX name truncated to the bill-pay prefix |

## Hand-derived expectation

8 fixture rows (4 CSV + 4 OFX) → **4** gold records in `core.fct_transactions`,
each `source_count = 2`. Counted by hand from the four labeled pairs above —
not from program output (see `.claude/rules/testing.md`). The `-2868.61` and
`-105.00` rows share a date but differ in amount, so exact-amount blocking keeps
them independent pairs.

Driven by `tests/scenarios/test_dedup_cross_format_truncation.py`.

Note: real OFX from these banks double-HTML-encodes `&` (`AT&T` → `AT&amp;amp;T`),
but the OFX extractor already decodes entities at import (`_decode_text_field`,
since #194), so the description reaching dedup is clean — the TELCO row's OFX
value above is the post-decode form a real import produces. The decode itself is
covered by `tests/moneybin/test_extractors/test_ofx_extractor.py`.
