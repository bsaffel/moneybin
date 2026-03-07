# Feature: Transaction Categorization

## Status
draft

## Goal
Provide a complete category system for transactions with manual assignment, rule-based auto-categorization, and bulk operations, enabling spending analysis by category.

## Background
- [MCP Write Tools](implemented/mcp-write-tools.md) -- `categorize_transaction` and `get_uncategorized_transactions` already implemented
- User schema: `src/moneybin/sql/schema/user_schema.sql`
- Source: `src/moneybin/mcp/write_tools.py`

## Requirements

1. Support a hierarchical category system: category > subcategory (e.g., Food > Groceries, Food > Restaurants).
2. Provide a default category list covering common personal finance categories.
3. Allow users to define custom categories.
4. Support rule-based auto-categorization (payee pattern -> category mapping).
5. Support bulk categorization (apply category to all matching transactions).
6. Track who categorized each transaction (`ai` vs `user`) and when.
7. Rules are persisted and applied to new transactions automatically.
8. Categories are case-insensitive for matching, title-case for display.

## Data Model

### Existing tables (already created)

```sql
-- user.transaction_categories (exists)
-- PK: transaction_id, columns: category, subcategory, categorized_at, categorized_by
```

### New tables

```sql
-- Category definitions with hierarchy
CREATE TABLE IF NOT EXISTS "user".categories (
    category_id VARCHAR PRIMARY KEY,
    category VARCHAR NOT NULL,
    subcategory VARCHAR,
    description VARCHAR,
    is_default BOOLEAN DEFAULT false,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (category, subcategory)
);

-- Auto-categorization rules
CREATE TABLE IF NOT EXISTS "user".categorization_rules (
    rule_id VARCHAR PRIMARY KEY,
    payee_pattern VARCHAR NOT NULL,
    category VARCHAR NOT NULL,
    subcategory VARCHAR,
    match_type VARCHAR DEFAULT 'contains',  -- contains, exact, regex
    priority INTEGER DEFAULT 0,
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR DEFAULT 'user'
);
```

### Default categories

| Category | Subcategories |
|----------|--------------|
| Housing | Rent, Mortgage, Insurance, Maintenance, Utilities |
| Food | Groceries, Restaurants, Coffee, Delivery |
| Transportation | Gas, Parking, Public Transit, Rideshare, Auto Insurance, Maintenance |
| Healthcare | Insurance, Doctor, Dental, Pharmacy, Vision |
| Entertainment | Streaming, Events, Hobbies, Games |
| Shopping | Clothing, Electronics, Home Goods, Online |
| Utilities | Electric, Gas, Water, Internet, Phone |
| Insurance | Health, Auto, Home, Life |
| Income | Salary, Freelance, Interest, Dividends, Refund |
| Transfer | Internal, Payment, ATM |
| Other | Uncategorized |

## Implementation Plan

### Files to create
- `src/moneybin/sql/schema/user_categories.sql` -- Categories DDL
- `src/moneybin/sql/schema/user_categorization_rules.sql` -- Rules DDL
- `src/moneybin/services/categorization_service.py` -- Rule engine and bulk operations
- `tests/moneybin/test_services/test_categorization_service.py`

### Files to modify
- `src/moneybin/mcp/write_tools.py` -- Add rule management and bulk categorization tools
- `src/moneybin/sql/schema/user_schema.sql` -- Add new tables
- `tests/moneybin/test_mcp/test_tools.py` -- Add tool tests

### Key decisions

- **Rule engine in Python**: Rules are simple pattern matching (LIKE/ILIKE), not a full rules engine. Applied in priority order.
- **Bulk apply**: `apply_categorization_rules` tool runs all active rules against uncategorized transactions.
- **AI-suggested rules**: When the AI categorizes transactions, it can also suggest creating a rule for the pattern.
- **No ML initially**: Start with pattern matching. LLM-based categorization can be added later via MCP sampling.

## MCP Interface

### New tools

| Tool | Description |
|------|-------------|
| `categories.list` | List all categories and subcategories |
| `categories.create` | Create a custom category/subcategory |
| `rules.create` | Create an auto-categorization rule |
| `rules.list` | List all categorization rules |
| `rules.apply` | Run all rules against uncategorized transactions |
| `transactions.bulk_categorize` | Categorize all transactions matching a pattern |

### Existing tools (already implemented)

- `categorize_transaction` -- Single transaction categorization
- `get_uncategorized_transactions` -- Find uncategorized transactions
- `get_spending_by_category` -- Spending breakdown by category

## Testing Strategy

- Test default category seeding
- Test rule creation and pattern matching (contains, exact, regex)
- Test rule priority ordering
- Test bulk categorization with multiple matching rules
- Test idempotency (re-running rules doesn't duplicate)
- Test category hierarchy queries
- Test AI vs user attribution

## Dependencies

- No new packages required
- Prerequisite: MCP write tools (implemented)

## Out of Scope

- Machine learning-based categorization
- Merchant name normalization (clean payee names)
- Category budgets (see [Budget Tracking](budget-tracking.md))
- Cross-account category rollup
