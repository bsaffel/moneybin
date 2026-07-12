# Auto-Rule Generation

> Last updated: 2026-07-12
> Status: Implemented
> Parent: [`categorization-overview.md`](categorization-overview.md) (pillar E)
> Companions: [`categorization-matching-mechanics.md`](categorization-matching-mechanics.md) (algorithm contract: source-precedence enforcement on write, snowball auto-apply, `match_text` and exemplar accumulator that auto-rule writes interoperate with), [`archived/transaction-categorization.md`](archived/transaction-categorization.md) (existing rule engine this builds on), [`moneybin-mcp.md`](moneybin-mcp.md) (tool signatures), `CLAUDE.md` "Architecture: Data Layers"

## Goal

When a user categorizes a transaction, identify the pattern and propose a rule so future matching transactions are categorized automatically. Proposals are staged — never silently activated. The user reviews and approves them in batch. The system gets smarter with every import.

## Background

Today's categorization pipeline has user-defined rules and merchant mappings, but the user must create rules manually. If a user categorizes 50 Starbucks transactions as "Coffee Shops" over three months, there's no mechanism to learn that pattern and apply it to the 51st automatically.

This spec adds auto-rule generation — pillar E from the [categorization umbrella](categorization-overview.md). It hooks into the existing categorization code paths, proposes rules from observed patterns, and promotes approved proposals to active rules in `app.categorization_rules` with `created_by = 'auto_rule'`.

### Relevant prior art

- [categorization-overview.md](categorization-overview.md) — umbrella vision, priority hierarchy, pipeline, build order
- [archived/transaction-categorization.md](archived/transaction-categorization.md) — existing rule engine and merchant normalization
- [app_categorization_rules.sql](../../src/moneybin/sql/schema/app_categorization_rules.sql) — existing rule schema with `created_by` column
- [app_transaction_categories.sql](../../src/moneybin/sql/schema/app_transaction_categories.sql) — categorization output table
- [app_merchants.sql](../../src/moneybin/sql/schema/app_merchants.sql) — merchant normalization mappings

## Design Principles

1. **Capture user intent immediately.** A manual categorization is a signal. Propose a rule on the first occurrence (configurable). The review queue is the safety net, not a high threshold.
2. **Merchant-first pattern extraction.** When a merchant match exists, use the canonical merchant name. Fall back to cleaned description only when no merchant is matched. Don't re-derive what merchant normalization already knows.
3. **Proposals, not silent activation.** Nothing activates without user approval. The system proposes; the user decides.
4. **Fit into the existing pipeline.** Auto-rules use the same `app.categorization_rules` table and rule engine as user-created rules. No parallel categorization system.

## Requirements

### Proposal generation

1. After any categorization event (`transactions_categorize_commit` MCP tool or `moneybin transactions categorize commit` CLI), the system checks each categorized transaction for a potential auto-rule proposal.
2. A proposal is generated when no active rule or merchant mapping already covers the transaction's pattern AND no pending proposal for the same pattern exists (if a pending proposal exists, its `trigger_count` is incremented instead).
3. The proposal threshold is configurable (`categorization.auto_rule_proposal_threshold`, default 1). A value of 1 means propose on first categorization; a value of 3 means propose after three matching categorizations.
4. Pattern extraction uses the merchant-first strategy: canonical merchant name when a `merchant_id` exists, cleaned description otherwise.
5. Proposals are stored in `app.proposed_rules` with `status = 'pending'`.
6. **Specificity floor on invented patterns.** A `contains` pattern the description-fallback path invents (not a user-authored merchant pattern) is only proposed as `contains` when it meets a minimum length (`auto_rule_min_contains_length`, default 4); shorter patterns are proposed as `exact` instead. See "Specificity floor" under Pattern Extraction.

### Proposal lifecycle

7. Proposals have four states: `pending`, `approved`, `rejected`, `superseded`.
8. Approved proposals are promoted to active rules in `app.categorization_rules` with `created_by = 'auto_rule'` and `priority = 200` — **unless** the proposal is flagged broad (see "Blast-radius review" below), in which case promotion is refused without an explicit `allow_broad` override.
9. On promotion, the new rule is immediately run against existing uncategorized transactions so approval has instant effect.
10. Rejected proposals are not re-proposed for the same pattern unless the user later categorizes a transaction with that pattern differently (which creates a new proposal).
11. When the same pattern is categorized differently, the existing proposal is marked `superseded` and a new proposal is created with the new category.

### Correction handling

12. A single user override of an auto-rule-categorized transaction does not affect the rule. `categorized_by = 'user'` outranks the rule per the priority hierarchy.
13. After `auto_rule_override_threshold` (default 2) user overrides of the same auto-rule, the system deactivates the rule, marks the proposal `superseded`, and creates a new proposal with the most common correction category.
14. Override counting is query-based: count transactions where `categorized_by = 'user'` AND the transaction matches the auto-rule's pattern and has a different category. No stored counter needed.

### Priority hierarchy integration

15. Auto-rules sit at priority level 3 in the categorization hierarchy (user > user-rules > auto-rules > ML > provider_native > ai). They use `categorized_by = 'auto_rule'`.
16. Auto-rules are never evaluated for transactions already categorized by a higher-priority source.
17. Auto-rules at `priority = 200` are evaluated after user-created rules at default `priority = 100`. Two auto-rules are ordered by their own priority values (first-created wins at equal priority).
18. Auto-rule writes route through the `write_categorization` helper, which enforces the source-priority ladder defined in [`categorization-matching-mechanics.md`](categorization-matching-mechanics.md) §Source precedence. A user manual categorization (`'user'`) or user-authored rule (`'rule'`) categorization is never overwritten by an `'auto_rule'` write — the write is skipped at the SQL level, not after the fact.

## Data Model

### `app.proposed_rules` (new)

```sql
/* Auto-rule proposals generated from user categorization patterns; staged for review before activation */
CREATE TABLE IF NOT EXISTS app.proposed_rules (
    proposed_rule_id VARCHAR PRIMARY KEY,  -- 12-char truncated UUID4 hex
    merchant_pattern VARCHAR NOT NULL,     -- Pattern to match: canonical merchant name or cleaned description
    match_type VARCHAR DEFAULT 'contains', -- How the pattern is matched: contains, exact, or regex
    category VARCHAR NOT NULL,             -- DEPRECATED in V014 (Phase 1 dual-write): display snapshot; category_id is canonical
    subcategory VARCHAR,                   -- DEPRECATED in V014 (Phase 1 dual-write): display snapshot; category_id is canonical
    category_id VARCHAR,                   -- FK to core.dim_categories.category_id (added in PR #174); NULL only for orphaned legacy rows
    status VARCHAR DEFAULT 'pending',      -- Lifecycle: tracking, pending, approved, rejected, superseded
    trigger_count INTEGER DEFAULT 1,       -- Number of categorizations that triggered or reinforced this proposal
    source VARCHAR DEFAULT 'pattern_detection', -- How the proposal was generated: pattern_detection or ml
    sample_txn_ids VARCHAR[],              -- Up to 5 transaction_ids that triggered this proposal
    proposed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- When the proposal was created
    decided_at TIMESTAMP,                  -- When the user approved or rejected; NULL if pending
    decided_by VARCHAR                     -- Who decided: 'user' or NULL if still pending
);
```

> **PR #174 migration note.** The category text columns are dual-write display snapshots; `category_id` is the canonical reference. Phase 2 (post-launch) drops the text columns.

### `app.categorization_rules` (existing, no schema changes)

Auto-rules are inserted with:

- `created_by = 'auto_rule'` (existing column, new value)
- `priority = 200` (below user rules at default 100)
- `is_active = true` (set to `false` when correction threshold triggers deactivation)

No schema changes needed.

### `app.transaction_categories` (existing, no schema changes)

Auto-rule categorizations are written with:

- `categorized_by = 'auto_rule'` (new value alongside existing `'rule'`, `'ai'`, `'user'`)

No schema changes needed — `categorized_by` is VARCHAR.

## Pattern Extraction

### Merchant-first strategy

```
Transaction categorized
  |
  v
Has merchant_id in app.transaction_categories?
  |
  +-- YES --> Use core.dim_merchants.canonical_name as pattern
  |           match_type = 'contains'
  |
  +-- NO  --> Clean raw description:
              1. Strip trailing transaction IDs (#1234, *AB1CD2)
              2. Strip location suffixes (SEATTLE WA, CA 94103)
              3. Strip payment processor prefixes (SQ *, TST *)
              4. Strip trailing whitespace
              Use cleaned string as pattern
              match_type = 'contains'
```

The cleaning regex list is a simple ordered set of strip rules — not a general NLP pipeline. Good enough for v1; real-world data informs whether it needs to be smarter.

### Specificity floor on invented patterns

`normalize_description` can reduce a description to a 1–2 character token — a truncated "TRANSFER TO ..." becomes "TO". As a `contains` rule, that token matches STORE, AUTO, TOTAL; one accepted proposal then relabels those rows on the next categorize run, and a Transfer label drops them out of spend reports entirely.

To guard against this, the description/memo fallback path checks the invented pattern's length before choosing `match_type`:

- **Pattern length ≥ `auto_rule_min_contains_length`** (default 4): proposed as `contains`, as before.
- **Pattern length < `auto_rule_min_contains_length`**: proposed as `exact` instead. The user's evidence (the trigger transaction) is kept, but the rule can only fire on a description that IS the token — it no longer silently sweeps up unrelated merchants.

This floor applies **only to machine-invented patterns** from the description/memo fallback. A user-authored `app.user_merchants.raw_pattern` is never touched — the guard protects the inference, not the human's own judgment.

The floor is implemented once (`_shared.is_unselective_contains`) and shared by two call sites: this pattern-extraction step (`AutoRuleService._invented_match_type`) and direct rule creation via `transactions_categorize_rules_create` / `moneybin transactions categorize rules create` (`MatchApplier.create_rules_core`) — an agent or user creating a rule directly is refused the same short `contains` pattern unless it passes `allow_broad=True`. See [`moneybin-mcp.md`](moneybin-mcp.md) §`transactions_categorize_rules_create`.

### Deduplication

| Scenario | Behavior |
|---|---|
| Same `merchant_pattern` + same `category` | Increment `trigger_count`, append to `sample_txn_ids` (capped at 5). No duplicate proposal. |
| Same `merchant_pattern` + different `category` | Mark existing proposal `superseded`. Create new proposal with the new category. User sees both in review history. |
| Overlapping patterns (e.g., "STARBUCKS" and "STARBUCKS RESERVE") | Both proposals survive independently. User decides during review. |

## Blast-Radius Review

`trigger_count` (how many times a proposal was reinforced) is evidence of user intent, not a bound on the rule's actual reach — a pattern extracted from one transaction can still match hundreds of others already in the ledger. The review and promotion surfaces close that gap:

- **`estimated_match_count`.** Every proposal returned by `transactions_categorize_auto_review` (MCP) / `moneybin transactions categorize auto review` (CLI) carries `estimated_match_count` — how many transactions the proposed pattern would actually match today, computed with the live matcher's own predicate (not an approximation).
- **`is_broad`.** `True` when the blast radius is disproportionate to the evidence behind it: `estimated_match_count` exceeds both an absolute floor (`auto_rule_broad_match_min`, default 20) and `auto_rule_broad_match_factor` (default 10) times `trigger_count`. A proposal with thin evidence (`trigger_count = 1`) that would already match 25 transactions is flagged; a proposal reinforced 30 times that matches 25 transactions is not — the guard only fires when the reach outruns what the user has actually confirmed.
- **`allow_broad` override.** A proposal flagged `is_broad` cannot be promoted via `transactions_categorize_auto_accept` / `moneybin transactions categorize auto accept` without an explicit `allow_broad=True`. Without it, the proposal is skipped (counted in the response's `skipped`, not `accepted`) rather than silently promoted. The CLI review table marks broad proposals with a `⚠️  BROAD, requires --allow-broad to accept` warning line.

`allow_broad` is a deliberate opt-in per "Magic stays visible" (`.claude/rules/design-principles.md`): the system surfaces its own uncertainty about a proposal's reach instead of promoting it silently, and the human (or an agent acting on explicit instruction) has to say so before a wide-reaching rule goes active.

## Integration Hook

The auto-rule engine hooks into the categorization service layer, which is shared by MCP and CLI:

| Hook point | Trigger |
|---|---|
| `CategorizationService.categorize_items()` (in `src/moneybin/services/categorization/`) | Batch categorization via `transactions_categorize_commit` MCP tool or `moneybin transactions categorize commit` CLI |

**CLI parity:** CLI commands use the same service layer as MCP tools. Same code path, not a separate implementation.

**Snowball trigger.** `categorize_pending()` (which invokes `apply_rules()` for auto-rule fan-out) is called automatically by `categorize_items()` after every commit. Newly-promoted auto-rules apply to remaining uncategorized rows in the same batch without waiting for the next import or an explicit `rules apply` invocation. See [`categorization-matching-mechanics.md`](categorization-matching-mechanics.md) §Apply order.

### Hook logic (synchronous)

After the categorization is written to `app.transaction_categories`:

1. Extract pattern from the transaction (merchant-first strategy)
2. Check `app.categorization_rules` — does an active rule already cover this pattern? -> skip
3. Check `core.dim_merchants` — does a merchant mapping already produce this category for this pattern? -> skip
4. Check `app.proposed_rules` — does a pending proposal for this pattern + category exist? -> increment `trigger_count`
5. Otherwise -> create new proposal

The hook is lightweight: one SELECT each against rules, merchants, and proposals, then at most one INSERT/UPDATE. No perceptible latency on the categorization call.

**Manual-entry exemption.** Transactions with `source_type = 'manual'` are excluded from training-data extraction — they never seed proposals or contribute to `trigger_count`. Manual rows are typed under user attention and reflect deliberate choices ("I categorized this $40 line as Groceries because that's what it was"); using them to project recurring auto-rules over future *imported* rows would amplify one-off intent into a heuristic the user never asked for. The auto-rule engine's pattern source is imported transactions only. See [`transaction-curation.md`](transaction-curation.md) §Manual Entry for the broader rationale.

## CLI Interface

| Command | Description |
|---|---|
| `moneybin transactions categorize auto review` | Table of pending proposals with sample transactions, trigger counts, pattern details, and blast-radius (`estimated_match_count`, flagged `⚠️  BROAD` when `is_broad`) |
| `moneybin transactions categorize auto accept --accept <id> [<id>...] --reject <id> [<id>...]` | Batch accept/reject proposals (renamed from `confirm` / `--approve` in PR #171) |
| `moneybin transactions categorize auto accept --accept-all` | Accept all pending proposals |
| `moneybin transactions categorize auto accept --reject-all` | Reject all pending proposals |
| `moneybin transactions categorize auto accept ... --allow-broad` | Required to promote a proposal flagged `is_broad`; see Blast-Radius Review |
| `moneybin transactions categorize auto stats` | Auto-rule health: active count, proposal count, override rate, top-performing rules |
| `moneybin transactions categorize auto rules` | List active auto-rules (equivalent to `list-rules --created-by auto_rule`) |

### Import-time output

Proposals accumulate during categorization. The import summary includes:

```
Imported 120 transactions from chase_checking.csv
  85 auto-categorized:
    42 by rules
    10 by auto-rules
    25 by merchant mappings
     8 by ML (high confidence)
  35 uncategorized
  4 new rules proposed
  Run 'moneybin transactions categorize auto review' to review proposed rules
```

### Non-interactive parity

| Interactive | Flag equivalent |
|---|---|
| Review table | `moneybin transactions categorize auto review --output json` |
| Accept specific | `moneybin transactions categorize auto accept --accept ar_001 ar_002` |
| Reject specific | `moneybin transactions categorize auto accept --reject ar_003` |
| Accept all | `moneybin transactions categorize auto accept --accept-all` |
| Reject all | `moneybin transactions categorize auto accept --reject-all` |

## MCP Interface

### Tools

| Tool | Type | Description |
|---|---|---|
| `transactions_categorize_auto_review` | Read | List pending proposals with sample transactions, trigger counts, pattern details, and blast-radius (`estimated_match_count`, `is_broad`) |
| `transactions_categorize_auto_accept` | Write | Batch accept/reject proposals by ID. Accepted proposals are promoted to active rules. `allow_broad=True` is required to promote a proposal flagged `is_broad` — see Blast-Radius Review. (Renamed from `_auto_confirm` in PR #171.) |
| `transactions_categorize_auto_stats` | Read | Auto-rule health: active count, proposal count, override rate, top-performing rules by match count |

### Prompt

| Prompt | Purpose |
|---|---|
| `review_auto_rules` | "Help me review proposed auto-categorization rules. Show pending proposals with sample transactions, explain the pattern, and let me approve or reject them." |

### MCP flow

```mermaid
sequenceDiagram
    actor User
    participant AI as AI (MCP)
    participant Sys as MoneyBin

    User->>AI: Show me proposed rules
    AI->>Sys: transactions_categorize_auto_review()
    Sys-->>AI: 8 pending rules with samples
    AI->>User: 8 patterns found:<br/>1. STARBUCKS -> Coffee (3 matches)<br/>2. AMAZON -> Shopping (5 matches)...
    User->>AI: Accept all except #4,<br/>that should be Groceries
    AI->>Sys: transactions_categorize_auto_accept(<br/>accept x7, reject x1)
    AI->>Sys: transactions_categorize_rules_create(<br/>corrected #4 as Groceries)
    Sys-->>AI: 7 accepted, 1 rejected, 1 created
    AI->>User: 7 rules activated,<br/>1 corrected to Groceries.<br/>Next import will auto-categorize<br/>these patterns.
```

## Configuration

```python
class CategorizationSettings(BaseModel):
    # Auto-rule proposal settings
    auto_rule_proposal_threshold: int = 1  # Propose after N matching categorizations
    auto_rule_override_threshold: int = 2  # Deactivate rule after N user overrides
    auto_rule_default_priority: int = 200  # Priority for promoted auto-rules
    auto_rule_min_contains_length: int = 4  # Shorter invented pattern -> 'exact'
    auto_rule_broad_match_min: int = 20  # Below this, is_broad never fires
    auto_rule_broad_match_factor: int = 10  # Broad when matches > factor x triggers
```

Env var overrides:

- `MONEYBIN_CATEGORIZATION__AUTO_RULE_PROPOSAL_THRESHOLD=3`
- `MONEYBIN_CATEGORIZATION__AUTO_RULE_OVERRIDE_THRESHOLD=5`
- `MONEYBIN_CATEGORIZATION__AUTO_RULE_MIN_CONTAINS_LENGTH=3`
- `MONEYBIN_CATEGORIZATION__AUTO_RULE_BROAD_MATCH_MIN=50`
- `MONEYBIN_CATEGORIZATION__AUTO_RULE_BROAD_MATCH_FACTOR=5`

## Testing Strategy

### Unit tests

- **Pattern extraction (merchant path)**: given a transaction with `merchant_id`, verify pattern uses `canonical_name` from `core.dim_merchants`
- **Pattern extraction (description fallback)**: given a transaction without merchant match, verify description cleaning strips IDs, locations, prefixes
- **Dedup — same pattern same category**: categorize same merchant twice same category -> one proposal with `trigger_count = 2`
- **Dedup — same pattern different category**: categorize same merchant two different categories -> first proposal `superseded`, second created
- **Promotion**: approve proposal -> verify rule in `app.categorization_rules` with `created_by = 'auto_rule'`, `priority = 200`
- **Correction threshold**: override auto-rule N times -> verify rule deactivated, proposal superseded, new proposal created
- **Priority ordering**: user rule at 100 and auto-rule at 200 on overlapping pattern -> user rule wins

### Integration tests

- **End-to-end**: import -> categorize run -> verify proposals created -> accept -> re-import -> verify new transactions auto-categorized by the promoted rule
- **Hook fires on all paths**: `transactions_categorize_commit` MCP tool and `moneybin transactions categorize commit` CLI both trigger proposal generation (same service layer)
- **Immediate effect**: accept a proposal, verify existing uncategorized transactions matching the pattern are categorized immediately
- **Priority hierarchy**: transaction categorized by user rule -> auto-rule hook does not propose (pattern already covered)
- **Hook idempotency**: categorize same transaction twice -> no duplicate proposal

### Synthetic data contract

- Datasets with repeated merchants across months (Starbucks 3x/week, Amazon 2x/month) to verify trigger counts accumulate correctly
- Merchants with description variation (STARBUCKS #1234 vs STARBUCKS #5678) to verify merchant-first extraction handles normalization
- Mixed categorization sources (user, AI, rule) to verify hooks fire for the correct source types
- Conflict scenarios: same merchant categorized differently by amount (Starbucks $5 = Coffee, Starbucks $25 = Food & Drink) to test conflict detection

## Dependencies

- Existing rule engine (`app.categorization_rules`, rule evaluation logic)
- Existing merchant normalization (`core.dim_merchants`, canonical name resolution)
- Existing categorization service layer (`CategorizationService.categorize_items()` in `src/moneybin/services/categorization/`, backing `transactions_categorize_commit` MCP tool — service was split into a facade + collaborators in PR #155)
- Database migration system (`database-migration.md`) for `app.proposed_rules` table creation

## Out of Scope

- **ML-powered categorization** — pillar D, separate spec (`categorization-ml.md`). Auto-rules are deterministic; ML is statistical. ML proposals will feed into the same `app.proposed_rules` table with `source = 'ml'`.
- **Amount/account-aware rule proposals** — v1 generates simple `merchant_pattern -> category` proposals. When the same merchant is categorized two ways depending on amount range, the conflict is surfaced in review and the user manually creates filtered rules. See Future Enhancements.
- **Overlapping pattern merging** — when proposals exist for both "STARBUCKS" and "STARBUCKS RESERVE", both survive. The user decides during review.
- **Auto-rule expiry** — rules don't expire in v1. Deactivation only happens via correction threshold or explicit user action.
- **Community-contributed rules** — deferred per categorization umbrella spec.

## Future Enhancements

### Amount/account-aware proposals

When the same merchant is consistently categorized differently depending on amount range (e.g., Starbucks $5 = Coffee, Starbucks $25 = Food & Drink), the proposal engine could detect this pattern and propose two filtered rules with `min_amount`/`max_amount` bounds. v1 surfaces the conflict; v2 resolves it intelligently.

### Overlapping pattern resolution

When proposals exist for both a broad pattern ("STARBUCKS") and a narrow pattern ("STARBUCKS RESERVE"), the system could suggest merging into the broader pattern or keeping both with different categories. Deferred to real-world experience with proposal volume.

### Auto-rule expiry

Rules that haven't matched any transaction in N months could be flagged for review or auto-deactivated. Useful for merchants the user no longer patronizes. Deferred until rule volume is high enough to warrant cleanup.

## Resolved Questions

Decisions made during spec review, preserved for context.

1. **Description cleaning regex list.** The description fallback path reuses the existing `normalize_description()` helper from the categorization service package (`src/moneybin/services/categorization/`), which already handles POS prefixes, trailing location info, trailing store IDs, and whitespace normalization. The regex approach is conservative (prefers false negatives over false positives) and is best-effort for the long tail — the merchant-first path handles the majority case, and the review queue catches what regex misses. The exact regex list is an implementation detail, extended based on real-world data. Note: merchant entity resolution (`merchant-entity-resolution.md`, planned) will improve the merchant-first path over time, reducing reliance on regex cleaning.
2. **Promotion timing.** Synchronous. Approved rules are immediately evaluated against existing uncategorized transactions. Instant feedback ("3 uncategorized transactions now categorized by your new rule") outweighs the marginal latency. The operation is fast at personal-finance scale.
3. **`sample_txn_ids` cap.** 5 is sufficient for v1. Provides enough context for the user to confirm the pattern during review without bloating the proposal table. Trivially adjustable during implementation if review experience suggests otherwise.
