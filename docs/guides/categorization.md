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

**Via MCP tools:**
- `bulk_categorize` — categorize many transactions in one call
- `bulk_create_categorization_rules` — define multiple rules at once
- `bulk_create_merchant_mappings` — set up merchant recognition in batch

**Via individual MCP tools:**
- `categorize_transaction` — assign a category (auto-creates merchant mapping)
- `create_categorization_rule` — create a single rule
- `create_merchant_mapping` — create a single merchant mapping

## Category Taxonomy

MoneyBin ships with the Plaid Personal Finance Category v2 (PFCv2) taxonomy — approximately 100 default categories organized into top-level categories and subcategories.

```bash
# Initialize default categories (safe to run multiple times)
moneybin categorize seed
```

**Top-level categories include:** Food & Drink, Shopping, Travel, Transportation, Entertainment, Bills & Utilities, Health & Fitness, Personal Care, Education, Income, Transfer, and more.

You can also:
- **Create custom categories** via the `create_category` MCP tool
- **Toggle categories on/off** — disabled categories are hidden from the taxonomy but existing categorizations are preserved

## Typical Workflow

1. **Import data** — `moneybin import file transactions.csv`
2. **Seed categories** — `moneybin categorize seed` (first time only)
3. **Apply existing rules** — `moneybin categorize apply-rules`
4. **Review uncategorized** — ask your AI assistant: *"Help me categorize my uncategorized transactions"*
5. **Rules build up** — each categorization creates merchant mappings, so the next import has fewer uncategorized transactions

Over time, the rule engine and merchant mappings handle most categorization automatically. Each import requires less manual work.
