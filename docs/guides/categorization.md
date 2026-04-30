# Categorization

MoneyBin uses a rule engine to categorize transactions. Rules, merchant mappings, and manual categorizations work together in a priority hierarchy.

## Rule Engine

Categorization rules match transactions by description pattern and optional filters. Rules are applied in priority order during import and when you run `categorize apply-rules`.

### Match Types

| Type | Behavior | Example |
|------|----------|---------|
| `exact` | Full string match | "NETFLIX.COM" matches only "NETFLIX.COM" |
| `contains` | Substring match (default) | "STARBUCKS" matches "STARBUCKS #1234 SEATTLE WA" |
| `regex` | Regular expression | `UBER\s*(EATS\|TRIP)` matches Uber Eats and Uber Trip |

### Rule Filters

Rules can be further scoped with:
- **Amount range** (`min_amount`, `max_amount`) — only match transactions within a dollar range
- **Account filter** (`account_id`) — only match transactions from a specific account
- **Priority** — lower numbers take precedence (priority 50 beats priority 100)

### CLI Commands

```bash
# Apply all active rules to uncategorized transactions
moneybin categorize apply-rules

# List all active rules
moneybin categorize list-rules

# View categorization coverage statistics
moneybin categorize stats
```

## Merchant Normalization

Merchant mappings clean up messy bank descriptions and associate merchants with default categories. When you categorize a transaction (via CLI or MCP), a merchant mapping is automatically created so future transactions with similar descriptions are categorized without manual intervention.

**Examples:**

| Raw description | Canonical name | Default category |
|----------------|---------------|-----------------|
| `SQ *STARBUCKS #1234 SEATTLE WA` | Starbucks | Food & Drink > Coffee Shops |
| `AMZN MKTP US*2K4F91R03` | Amazon | Shopping > Online |
| `UBER *EATS 3X7F2` | Uber Eats | Food & Drink > Delivery |
| `WHOLEFDS MKT 10142` | Whole Foods | Food & Drink > Groceries |

Each merchant mapping specifies:
- **Raw pattern** — what to match in transaction descriptions
- **Canonical name** — clean display name
- **Match type** — exact, contains, or regex
- **Default category/subcategory** — auto-assigned to matching transactions

## Bulk Operations

All categorization operations support batch mode for efficient processing. These are designed for AI assistants that review and categorize many transactions in a single interaction turn.

**Via MCP tools** (all batch-capable — single or many records per call):
- `categorize.bulk` — categorize one or many transactions (auto-creates merchant mappings)
- `categorize.create_rules` — create one or many categorization rules
- `categorize.create_merchants` — create one or many merchant mappings
- `categorize.delete_rule` — remove a rule

**Via CLI** — the `categorize.bulk` tool has a CLI equivalent that accepts the same JSON shape from a file or stdin:

```bash
moneybin categorize bulk --input cats.json
cat cats.json | moneybin categorize bulk -
```

Both surfaces share the same response envelope; pass `--output json` to get the structured result.

## Category Taxonomy

MoneyBin ships with the Plaid Personal Finance Category v2 (PFCv2) taxonomy — approximately 100 default categories organized into top-level categories and subcategories.

```bash
# Initialize default categories (safe to run multiple times)
moneybin categorize seed
```

**Top-level categories include:** Food & Drink, Shopping, Travel, Transportation, Entertainment, Bills & Utilities, Health & Fitness, Personal Care, Education, Income, Transfer, and more.

You can also:
- **Create custom categories** via the `categorize.create_category` MCP tool
- **Toggle categories on/off** — disabled categories are hidden from the taxonomy but existing categorizations are preserved

## Auto-Rules

MoneyBin learns categorization patterns from how you (or your AI assistant) categorize transactions and proposes rules you can approve. Once approved, those rules categorize future transactions automatically — and roll themselves back if you start correcting their output.

### How learning works

Every time `categorize.bulk` writes a categorization (CLI, MCP, or AI agent), MoneyBin records the `(pattern, category)` pair. After enough independent transactions categorize the same way, the proposal moves from `tracking` to `pending` and shows up in `auto-review`. You decide whether to promote it to a real rule.

| Term | Meaning |
|---|---|
| Proposal | A `(pattern, category)` pair MoneyBin is considering. Lives in `app.proposed_rules`. |
| `tracking` | Below proposal threshold. Not shown in `auto-review`. |
| `pending` | Reached threshold; waiting for your approve/reject. |
| `approved` | Promoted to an active rule (`categorization_rules.created_by='auto_rule'`). |
| Override | A user/AI categorization that disagrees with what the auto-rule would assign. |

### CLI Commands

```bash
# List pending auto-rule proposals (table or JSON)
moneybin categorize auto-review
moneybin categorize auto-review --output json

# Approve / reject specific proposals
moneybin categorize auto-confirm --approve abc123 --approve def456
moneybin categorize auto-confirm --reject abc123

# Approve all pending — except the ones you reject explicitly
moneybin categorize auto-confirm --approve-all --reject abc123

# Reject everything pending
moneybin categorize auto-confirm --reject-all

# List active auto-rules
moneybin categorize auto-rules

# Show health: active auto-rules, pending proposals, transactions auto-ruled
moneybin categorize auto-stats
```

### Tunables

These live under `categorization.*` in your profile config (see [profiles guide](profiles.md)):

| Setting | Default | What it does |
|---|---|---|
| `auto_rule_proposal_threshold` | 3 | Distinct transactions needed before a proposal becomes `pending` |
| `auto_rule_override_threshold` | 3 | User corrections needed before an active rule is deactivated |
| `auto_rule_default_priority` | 200 | Priority of new auto-rules (lower number wins; user rules typically use 50–100) |
| `auto_rule_sample_txn_cap` | 5 | Sample transaction IDs shown in `auto-review` per proposal |
| `auto_rule_backfill_scan_cap` | 50,000 | Max uncategorized transactions scanned when an approval back-fills history |

The constraint `proposal_threshold <= override_threshold` is enforced at config load — if proposal were higher, an override-driven re-proposal could land in `tracking` and never resurface.

### Self-healing: override-driven deactivation

If you approve an auto-rule and then start correcting its output (assigning a different category to transactions it would have caught), MoneyBin counts those as overrides. Once override count reaches `auto_rule_override_threshold`:

1. The rule is deactivated (`is_active=false`).
2. Its source proposal is marked `superseded`.
3. A new proposal is created with the **most common** category among your corrections — already promoted to `pending` if you've corrected at least `auto_rule_proposal_threshold` transactions to that category.
4. An audit row is written to `app.rule_deactivations` with the override count and the new category.

You'll see the new proposal in `auto-review`. Approve it to install the corrected rule. There's no manual cleanup step.

### What patterns get proposed

The proposal pattern comes from the merchant resolution that already happens during `categorize.bulk`:

- **If the transaction matched an existing merchant** — the merchant's `raw_pattern` and `match_type` are used (e.g., `AMZN` / `exact`). This is the precise substring that matches statement descriptions, not the canonical display name.
- **If no merchant matched** — the cleaned-up description with `match_type='contains'` is used as a fallback.

A proposal is suppressed when an active rule or merchant mapping already produces the same category for the transaction, so you don't see redundant proposals for patterns already covered.

## Typical Workflow

1. **Import data** — `moneybin import file transactions.csv`
2. **Seed categories** — `moneybin categorize seed` (first time only)
3. **Apply existing rules** — `moneybin categorize apply-rules`
4. **Review uncategorized** — ask your AI assistant: *"Help me categorize my uncategorized transactions"*
5. **Review auto-rule proposals** — `moneybin categorize auto-review`, then approve the ones that look right
6. **Rules build up** — each categorization creates merchant mappings and feeds auto-rule learning, so the next import has fewer uncategorized transactions

Over time, the rule engine, merchant mappings, and auto-rules handle most categorization automatically. Each import requires less manual work.

For the architecture and lifecycle internals (state diagrams, sequence diagrams, atomicity guarantees), see [auto-rule pipeline tech brief](../tech/auto-rule-pipeline.md).
