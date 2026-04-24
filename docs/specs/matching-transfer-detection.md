# Transfer Detection

> Last updated: 2026-04-19
> Status: in-progress
> Parent: [`matching-overview.md`](matching-overview.md) (pillar B)
> Companions: [`matching-same-record-dedup.md`](matching-same-record-dedup.md) (sibling spec, pillars A+C), [`categorization-overview.md`](categorization-overview.md) (independent axis), `CLAUDE.md` "Architecture: Data Layers", `.claude/rules/database.md` (column naming, model prefixes)

## Goal

When two transactions from different accounts describe opposite sides of a money movement between accounts the user owns, link them as a transfer pair. Confirmed transfers are flagged on `core.fct_transactions` for query ergonomics and excluded from spending/income totals by default. Every match is logged, reviewable, and reversible.

## Background

Without transfer detection, a $500 move from checking to savings counts as both a $500 expense and $500 income. Spending totals, budget actuals, and category breakdowns are all wrong. This spec fixes that by extending the matching engine (shared with same-record dedup) with a transfer detection mode.

This spec covers pillar B from the [transaction-matching umbrella](matching-overview.md). It builds on the matching infrastructure defined in [`matching-same-record-dedup.md`](matching-same-record-dedup.md) — shared engine, shared `app.match_decisions` table, shared review CLI.

### Relevant prior art

- [matching-overview.md](matching-overview.md) — umbrella vision, scope, build order
- [matching-same-record-dedup.md](matching-same-record-dedup.md) — pillars A+C, defines the matching engine, `app.match_decisions`, review CLI
- [fct_transactions.sql](../../sqlmesh/models/core/fct_transactions.sql) — current core model (VIEW, no transfer detection)
- [categorization-overview.md](categorization-overview.md) — independent axis; matching does not gate categorization

## Design Principles

1. **Separate and composable.** `is_transfer` and categorization are independent metadata axes. A transaction can be both transfer-flagged and categorized. Neither gates the other.
2. **Always-review in v1.** Every transfer pair proposal goes through the review queue. No auto-confirmation. This prevents cascading mis-matches from recurring transfers (the QuickBooks problem).
3. **Shared engine, different mode.** Transfer detection reuses the same blocking → scoring → assignment → persistence pipeline as same-record dedup, with `match_type = 'transfer'` as a discriminator.
4. **Reversibility is first-class.** Confirmed pairs can be un-matched at any time, restoring both transactions to independent status. Re-running the matcher re-proposes (does not re-apply) the same pair.

## Requirements

### Detection

1. Transactions from different accounts with opposite signs and the same absolute amount, within a configurable date window, are candidates for transfer pairing.
2. Candidates are scored using four signals: date distance, description keyword presence, amount roundness, and account pair frequency within the batch.
3. All scored pairs above `transfer_review_threshold` go to the review queue. No auto-confirmation in v1.
4. Pairs below `transfer_review_threshold` are not surfaced (logged at DEBUG level only).
5. Each transaction participates in at most one transfer pair (1:1 assignment).
6. All match decisions are persisted in `app.match_decisions` with `match_type = 'transfer'`, confidence score, per-signal scores in `match_signals` JSON, and timestamp.
7. Rejected pairs are not re-proposed by the matcher.
8. Any confirmed pair can be reversed. After reversal, re-running the matcher re-proposes (does not re-apply) the same pair.

### Interaction with other systems

9. Transfer detection runs after same-record dedup (Tier 4). Dedup resolves duplicates first so transfer detection sees one clean row per real-world transaction.
10. Confirmed transfers are excluded from spending/income totals, budget actuals, and category breakdowns by default.
11. The categorization pipeline runs on all transactions regardless of `is_transfer`. Matching does not gate categorization.
12. If a user categorizes a transfer-flagged transaction, that categorization is honored (`categorized_by = 'user'` outranks everything). Whether it counts in spending totals remains a consumer-level decision.

### Scope

13. v1 supports 1:1 pairs only. Many-to-one and one-to-many transfers are a future enhancement.
14. v1 supports same-currency matching only. Cross-currency transfer detection is deferred to the multi-currency initiative.

## Data Model

### `core.bridge_transfers` (new, SQLMesh model)

```sql
/* Confirmed transfer pairs linking two fct_transactions rows; rebuilt from app.match_decisions on every SQLMesh run */
CREATE TABLE core.bridge_transfers (
    transfer_id VARCHAR NOT NULL,        -- UUID for this transfer pair
    debit_transaction_id VARCHAR NOT NULL, -- FK to fct_transactions; the outgoing side (negative amount)
    credit_transaction_id VARCHAR NOT NULL, -- FK to fct_transactions; the incoming side (positive amount)
    match_id VARCHAR NOT NULL,            -- FK to app.match_decisions; the decision that created this pair
    date_offset_days INTEGER,             -- Days between the two post dates (0 = same day)
    amount DECIMAL(18, 2),                -- Absolute transfer amount
    PRIMARY KEY (transfer_id)
);
```

Derived from `app.match_decisions` where `match_type = 'transfer'` and `match_status = 'accepted'`. Rebuilt on every SQLMesh run — no mutable state in core.

### `core.fct_transactions` (modified)

Two new columns for query ergonomics, JOIN-derived from `bridge_transfers`:

| Column | Type | Description |
|---|---|---|
| `is_transfer` | BOOLEAN | TRUE if this transaction is part of a confirmed transfer pair |
| `transfer_pair_id` | VARCHAR | FK to `core.bridge_transfers.transfer_id`; NULL if not a transfer |

These are derived via LEFT JOIN in the existing `fct_transactions` model, not stored in `app.*`.

### `app.match_decisions` (shared, columns added by this spec)

Transfer detection reuses the existing table from same-record dedup. Two columns are added to support transfers:

- `match_type` — `'dedup'` (default) or `'transfer'`. Added to the DDL in `matching-same-record-dedup.md` to support both modes from day one.
- `account_id_b` — second account for transfers (dedup matches have the same account on both sides, so this is NULL for dedup rows).

Transfer-specific values in existing columns:

- `match_type = 'transfer'`
- `match_tier = NULL` (tiers are dedup-specific)
- `match_signals` JSON carries transfer-specific signal scores: `{"date_distance": 0.8, "keyword_score": 0.6, "amount_roundness": 1.0, "pair_frequency": 0.7}`

## Matching Algorithm

### Shared engine architecture

Transfer detection is a mode of the same Python matching engine defined in `matching-same-record-dedup.md`. The engine dispatches based on `match_type`:

- **Dedup mode** (Tiers 2b, 3): same amount, same sign, different sources
- **Transfer mode** (Tier 4): same amount, opposite signs, different accounts

Shared infrastructure: blocking query generation, 1:1 greedy assignment, `app.match_decisions` persistence, review CLI/MCP. Mode-specific: blocking criteria, scoring signals, automation posture.

### Execution order

```
1. Python loader         -> writes to raw.*
2. Python matcher        -> reads prep.stg_* views
                         -> Tier 2b: within-source overlap (dedup)
                         -> Tier 3: cross-source dedup
                         -> Tier 4: transfer detection    <- THIS SPEC
                         -> writes to app.match_decisions
3. sqlmesh run           -> int_*__unioned -> __matched -> __merged
                         -> core.fct_transactions (with is_transfer, transfer_pair_id)
                         -> core.bridge_transfers
                         -> meta.fct_transaction_provenance
```

Transfer detection runs after dedup so that duplicates are resolved first. This prevents a cross-source duplicate from creating a phantom transfer pair.

### Candidate blocking (SQL)

Query against prep views. A candidate pair must satisfy ALL of:

1. **Different accounts** — `account_id_a != account_id_b`
2. **Opposite signs** — `amount_a * amount_b < 0` (one negative, one positive)
3. **Exact amount match** — `ABS(amount_a) = ABS(amount_b)` (same-currency only)
4. **Date within window** — `ABS(transaction_date_a - transaction_date_b) <= date_window_days` (default 3)
5. **Not already matched** — neither transaction is part of an accepted dedup match or transfer pair

This produces a narrow candidate set. No fuzzy logic at this stage.

### Scoring

For each candidate pair, four signals combined into a confidence score:

| Signal | Range | Logic |
|---|---|---|
| **Date distance** | 0.0-1.0 | `1.0 - (days_apart / date_window_days)`. Same-day = 1.0, 3 days apart = 0.0. Primary discriminator for recurring transfers. |
| **Keyword presence** | 0.0-1.0 | Scan both descriptions for transfer-indicating terms: TRANSFER, XFER, ACH, DIRECT DEP, WIRE, account number fragments. Score based on how many indicators found in either description. |
| **Amount roundness** | 0.0-1.0 | Round numbers score higher. 1.0 if divisible by 100, 0.7 if divisible by 10, 0.5 if whole dollar, 0.3 otherwise. |
| **Account pair frequency** | 0.0-1.0 | How often this (account_a, account_b) pair appears among all candidates in this batch. Accounts that frequently transfer between each other score higher within the current run. |

Exact signal weights are an implementation detail, tuned against real data. Default starting point: `{date_distance: 0.4, keyword: 0.3, roundness: 0.15, pair_frequency: 0.15}`. All four signal scores are persisted in `match_signals` JSON for auditability and tuning diagnosis.

### 1:1 assignment

Same greedy best-score-first algorithm as dedup:

1. Sort all scored pairs by confidence descending
2. Iterate: if neither transaction is already claimed, accept the pair, mark both as claimed
3. Remaining unclaimed candidates are discarded

**Recurring transfer safety:** For monthly same-amount transfers (e.g., $500 checking->savings on the 1st every month), date distance is the primary discriminator. Same-day pairs score 1.0 on date distance; pairs from different months that happen to fall within the date window score much lower. Greedy assignment picks same-day matches first, preventing cross-month contamination. And since v1 is always-review, the user catches anything the algorithm gets wrong.

### All pairs -> review queue

Every pair above `transfer_review_threshold` (default 0.70) is written to `app.match_decisions`:

- `match_status = 'pending'`
- `match_type = 'transfer'`
- `decided_by = 'auto'`

No auto-confirmation in v1.

## Tuning & Feedback Loop

Transfer detection is tunable through a concrete observe-diagnose-adjust cycle.

### Observe: what tells you something is off?

Two signals from the review queue:

1. **Noise ratio** — you're rejecting a lot of proposed pairs. Scoring is too permissive or threshold too low.
2. **Missed pairs** — you manually identify transfers that the matcher didn't propose. Blocking criteria too strict or threshold too high.

These surface naturally during `moneybin matches review`.

### Diagnose: where is the problem?

`moneybin matches log --type transfer` shows recent decisions with `match_signals` JSON breakdown:

```
Transfer: Checking -> Savings  $500.00  (2026-03-15 / 2026-03-15)
  Confidence: 0.88
  Signals: date_distance=1.0, keyword=0.6, roundness=1.0, pair_freq=0.8
  Status: rejected (user, 2026-03-16)
```

The signal breakdown shows what's driving false positives or misses.

### Adjust: three levers

| Lever | Setting | Effect | When to use |
|---|---|---|---|
| **Review threshold** | `matching.transfer_review_threshold` | Raise to see fewer, higher-quality proposals; lower to catch more | Too much noise -> raise. Missing pairs -> lower. |
| **Date window** | `matching.date_window_days` | Narrow to reduce cross-month false matches; widen for slow ACH | Recurring transfers matching wrong months -> narrow to 2. International wires -> widen. |
| **Signal weights** | `matching.transfer_signal_weights` | Dict of per-signal weights | When a specific signal consistently drives false positives |

All three are Pydantic settings with env var overrides. Changes take effect on the next `moneybin matches run`.

### The recipe

**"I'm rejecting too many transfer proposals":**

1. `moneybin matches log --type transfer --status rejected` — examine signal breakdowns
2. Identify which signal is consistently high on rejected pairs
3. Lower that signal's weight, or raise the review threshold
4. `moneybin matches run` — re-score with new settings
5. Check if the review queue improved

**"The matcher missed an obvious transfer":**

1. Check blocking criteria (different accounts, opposite signs, exact amount, within date window)
2. If blocking failed: widen `date_window_days`, or check if an earlier dedup match claimed one of the transactions
3. If blocked but scored too low: `moneybin matches log --debug` shows below-threshold pairs. Adjust weights or lower threshold.

### v2 connection

This manual tuning cycle is what v2 learned promotions automate. After the user confirms the same (account_a, account_b, similar_amount) pattern N times, the system offers to auto-confirm that pattern. The earned-trust graduation path.

## Interaction with Reports and Budgets

### Default behavior

Consumers that compute spending/income totals filter transfers out by default:

- **Budget actuals** — `WHERE NOT is_transfer`. A $500 checking->savings transfer doesn't count as a $500 expense.
- **Category breakdowns** — same filter. Transfers excluded unless the user explicitly categorized one side.
- **Monthly summaries** — income and expense totals exclude transfers. A separate "Transfers" line item shows net internal movement.
- **Net worth** — unaffected. Transfers move money between accounts but don't change total net worth.

### User override

If a user categorizes a transfer-flagged transaction (e.g., marks a brokerage transfer as "Investment"), that categorization is honored — `categorized_by = 'user'` outranks everything. Whether that transaction counts in spending totals is a consumer-level decision. Reports could offer `--include-categorized-transfers` for users who want categorized transfers in category breakdowns.

### Independence guarantee

- The categorization pipeline runs on all transactions regardless of `is_transfer`
- Transfer detection runs regardless of categorization status
- A transaction can be both `is_transfer = TRUE` and have a category — these are orthogonal properties

## CLI Interface

### Import integration

After import, the matcher runs in transfer mode alongside dedup:

```
Imported 142 transactions from chase_checking_2026-03.csv
  Matching: 8 dedup auto-merged, 5 potential transfers found
  Run 'moneybin matches review' when ready
```

### Match commands (extended)

Transfer detection extends the commands defined in `matching-same-record-dedup.md` — no new subcommands:

| Command | Transfer behavior |
|---|---|
| `moneybin matches review` | Shows dedup and transfer proposals. Transfer pairs display both sides. `--type transfer` to filter. |
| `moneybin matches log` | `--type transfer` filter. `--status rejected` for tuning diagnosis. Signal breakdown per entry. |
| `moneybin matches undo <match_id>` | Un-matches a confirmed transfer pair, restores both transactions to independent status. |
| `moneybin matches run` | Runs dedup + transfers. `--type transfer` for transfers only. |
| `moneybin matches backfill` | Scans existing transactions for transfer pairs (runs after dedup backfill). |

### Transfer review UX

Transfer review shows both sides for full context:

```
Transfer pair (confidence: 0.88)
  DEBIT:  Chase Checking  -$500.00  2026-03-15  "ONLINE TRANSFER TO SAV"
  CREDIT: Chase Savings   +$500.00  2026-03-15  "TRANSFER FROM CHK"
  Signals: date=1.0  keyword=0.6  roundness=1.0  pair_freq=0.8

  [a]ccept / [r]eject / [s]kip / [q]uit
```

### Non-interactive parity

| Interactive | Flag equivalent |
|---|---|
| Review one-by-one | `moneybin matches review --accept <match_id> --accept <match_id>` |
| Accept all pending | `moneybin matches review --accept-all --type transfer` |
| Reject specific | `moneybin matches review --reject <match_id>` |

## MCP Interface

Designed alongside CLI. Implementation may be sequenced after CLI, but the data model and `app.match_decisions` schema support MCP from day one.

### Tools

Transfer detection reuses the same MCP tools as same-record dedup with `match_type` filtering. No new tools needed:

| Tool | Transfer usage |
|---|---|
| `list_pending_matches` | `match_type='transfer'` filter. Returns both sides of each pair with signal breakdown. |
| `confirm_match` | Accepts a `match_id` regardless of type. |
| `reject_match` | Rejects a `match_id` regardless of type. |
| `undo_match` | Reverses a previously accepted match. |
| `get_match_log` | `match_type='transfer'` filter. Signal breakdown per entry. |

### Prompt

| Prompt | Purpose |
|---|---|
| `review_matches` | "Help me review pending transaction matches. Show dedup and transfer proposals, explain why each was proposed, and let me accept or reject them." |

The AI can walk the user through the review queue conversationally — showing both sides of transfer pairs, explaining signal scores, and calling `confirm_match`/`reject_match` as the user decides.

## Configuration

```python
class MatchingSettings(BaseModel):
    # ... existing dedup settings from matching-same-record-dedup.md ...

    # Transfer-specific settings
    transfer_review_threshold: float = 0.70
    date_window_days: int = 3  # shared with dedup
    transfer_signal_weights: dict[str, float] = {
        "date_distance": 0.4,
        "keyword": 0.3,
        "roundness": 0.15,
        "pair_frequency": 0.15,
    }
```

Env var overrides:

- `MONEYBIN_MATCHING__TRANSFER_REVIEW_THRESHOLD=0.80`
- `MONEYBIN_MATCHING__DATE_WINDOW_DAYS=5`
- `MONEYBIN_MATCHING__TRANSFER_SIGNAL_WEIGHTS='{"date_distance": 0.5, "keyword": 0.25, "roundness": 0.15, "pair_frequency": 0.1}'`

### What is not configurable

| Invariant | Rationale |
|---|---|
| Different accounts required | Transfers are by definition cross-account |
| Opposite signs required | One debit, one credit |
| Exact amount match | Amount is the strongest identity signal; FX tolerance deferred to multi-currency |
| 1:1 assignment | Each transaction participates in at most one transfer pair |
| Always-review in v1 | Prevents cascading mis-matches from recurring transfers |

## Testing Strategy

### Unit tests

- **Blocking query**: given transactions with known amounts/dates/accounts/signs, verify candidate pairs are correctly identified (opposite signs, same amount, different accounts, within window)
- **Scoring function**: given a candidate pair, verify each signal component (date distance, keyword, roundness, pair frequency) and the combined score
- **1:1 assignment with recurring transfers**: 3 months of $500 checking->savings, verify greedy assignment pairs same-day matches correctly without cross-month contamination
- **Bridge table derivation**: given accepted match decisions, verify `bridge_transfers` output (debit/credit sides, date offset, amount)

### Integration tests

- **End-to-end**: load two files from different accounts with overlapping transfers, run matcher, run SQLMesh, verify `is_transfer` and `bridge_transfers` populated correctly
- **Dedup-then-transfer sequencing**: load OFX + CSV with duplicates AND transfers. Verify dedup resolves first, then transfer detection pairs the deduplicated gold records.
- **Review flow**: propose a transfer, confirm via test harness, verify `bridge_transfers` populated after SQLMesh run
- **Undo flow**: confirm a transfer, undo it, verify `is_transfer` cleared and both transactions restored to independent status
- **Report filtering**: confirmed transfers excluded from spending totals; categorized transfers still excluded by default
- **Backfill**: run backfill on existing data, verify transfer pairs found alongside dedup matches

### What to mock

- No mocking of DuckDB — real instances with fixture data (same pattern as dedup)
- Mock external file I/O for loader tests

## Synthetic Data Contract

Requirements for the synthetic data generator (`testing-synthetic-data.md`) to support transfer detection testing.

### Happy path scenarios

- **Same-day, same-institution transfers** — checking->savings, exact amount, transfer keywords in both descriptions
- **Cross-institution ACH** — 1-3 day date offset, completely different descriptions between the two sides
- **Multiple account pairs** — checking<->savings, checking<->brokerage, savings<->brokerage exercised simultaneously

### Recurring transfer scenarios

- **Monthly same-amount transfers** — $500 checking->savings on the 1st of every month for 12 months. Tests greedy assignment doesn't cross months.
- **Recurring with date drift** — same $500 transfer but post date drifts +/-1-2 days across months. Tests date scoring resilience.
- **Recurring with occasional skip** — 10 months of transfers with months 4 and 8 missing. Tests that the matcher doesn't stretch to fill gaps.

### False positive scenarios

- **Coincidental same-amount** — $100 electric bill debit and $100 refund credit on the same day across different accounts. Same amount, opposite signs, not a transfer.
- **Round-number noise** — multiple $50 and $100 transactions across accounts in the same week
- **Same-amount different purpose** — $500 rent payment and $500 paycheck in the same week

### Edge cases

- **Near-boundary dates** — transfer pair where one side posts exactly `date_window_days` apart
- **Multiple candidates for one transaction** — $200 debit with two possible $200 credits (same-day savings, next-day brokerage). Tests 1:1 assignment picks best match.
- **One-sided transfer** — only one account imported, so only the debit side exists. Matcher should propose nothing.
- **Already-deduped transactions** — same transfer visible in OFX and CSV. Dedup resolves first, then the gold record is available for transfer matching.

### Data shape requirements

- **Ground truth manifest** — each synthetic dataset includes which transactions are genuine transfer pairs, so tests assert precision and recall
- **Source diversity** — transfers across OFX, CSV, and (eventually) Plaid sources with realistic description patterns per source type
- **Controllable parameters** — date offset range, number of recurring patterns, noise-to-signal ratio

## Dependencies

- Same-record dedup spec (`matching-same-record-dedup.md`) — provides the shared matching engine, `app.match_decisions` table, review CLI, 1:1 assignment algorithm
- DuckDB `jaro_winkler_similarity()` (available since DuckDB 0.8.0) — not used for transfer scoring directly, but available in the shared engine
- Database migration system (`database-migration.md`) — for `core.bridge_transfers` table creation and `fct_transactions` column additions
- Existing prep staging views and raw schema

## Out of Scope

- **Cross-currency transfer detection** — deferred to multi-currency initiative. v1 requires exact amount match in same currency.
- **Many-to-one / one-to-many transfers** — e.g., paycheck + bonus -> one deposit. v1 is 1:1 pairs only. Future enhancement.
- **Auto-confirmation (v2 learned promotions)** — after user confirms N matches of the same pattern, offer to auto-confirm. Deferred per umbrella spec. v1 is always-review.
- **Same-record dedup** — pillar A+C, sibling spec. Different semantics: collapses records rather than linking them.
- **Investment transaction transfers** — depends on investment-tracking spec.

## Future Enhancements

### v2: Learned promotions

After the user confirms the same (account_a, account_b, similar_amount) pattern N times, the system offers to auto-confirm that pattern going forward. This is the earned-trust graduation path from always-review to selective auto-confirmation. Design details deferred to a v2 spec.

### v2: Amount-aware filtering

Transfers that always occur at the same amount or within a narrow range (e.g., $500 +/- $0) could be distinguished from coincidental same-amount transactions more effectively. Combined with learned promotions, this enables reliable auto-confirmation for recurring transfers.

## Open Questions

### Resolved

- **Backfill ordering.** Dedup completes fully first, then transfer detection runs. Transfers are cross-account by definition, so all accounts must be deduplicated before transfer pairing can produce correct results. Same order as import-time matching (Tiers 2b → 3 → 4).

### Deferred to implementation tuning milestone

1. **Confidence score formula.** Exact combination of the four signals. Starting weights provided (`date_distance: 0.4, keyword: 0.3, roundness: 0.15, pair_frequency: 0.15`) but should be tuned against real data. See MVP roadmap Level 1 tuning milestone.
2. **Keyword list.** The initial set of transfer-indicating terms (TRANSFER, XFER, ACH, DIRECT DEP, WIRE, etc.) and how to handle institution-specific variations. Start with a reasonable default set; tune alongside confidence scores with real data.
