# Feature: Transaction Categorization

## Status

Fully implemented

## Goal
Provide a layered categorization system that works from day one without training data, improves over time via merchant mappings and rules, and supports bulk LLM-assisted categorization in a single tool call.

## Categorization Priority

| Priority | Source | `categorized_by` | Overwritten by |
|----------|--------|-------------------|----------------|
| 1 | User manual | `'user'` | Nothing |
| 2 | User-defined rules | `'rule'` | User only |
| 3 | Plaid-provided categories | `'plaid'` | User, rules |
| 4 | LLM via `bulk_categorize` | `'ai'` | All above |

## Data Model

### New tables

**`app.categories`** — Category taxonomy (seeded with Plaid PFCv2)
- `category_id` (PK), `category`, `subcategory`, `description`, `is_default`, `is_active`, `plaid_detailed`, `created_at`
- 16 primary categories, ~100 subcategories
- Users can add custom categories and hide defaults

**`app.merchants`** — Merchant normalization + category cache
- `merchant_id` (PK), `raw_pattern`, `match_type` (exact/contains/regex), `canonical_name`, `category`, `subcategory`, `created_by`, timestamps
- Dual purpose: normalizes messy descriptions AND caches merchant-to-category mappings

**`app.categorization_rules`** — Pattern-based auto-categorization rules
- `rule_id` (PK), `name`, `merchant_pattern`, `match_type`, `min_amount`, `max_amount`, `account_id`, `category`, `subcategory`, `priority`, `is_active`, `created_by`, timestamps
- Amount filtering uses sign convention (negative = expense, positive = income)

### Modified tables

**`app.transaction_categories`** — Added columns:
- `merchant_id` — links to merchant that matched
- `confidence` (DECIMAL 0-1) — how certain the categorization is
- `rule_id` — links to rule that matched

### TableRef constants
`CATEGORIES`, `MERCHANTS`, `CATEGORIZATION_RULES` in `tables.py`.

## Merchant Name Normalization

Deterministic cleanup in `normalize_description()` before matching:
1. Strip POS prefixes (`SQ *`, `TST*`, `PP*`, `PAYPAL *`, `VENMO *`, `ZELLE *`)
2. Strip trailing location info (city/state/zip patterns)
3. Strip trailing store IDs / reference numbers (3+ digits)
4. Normalize whitespace

When any source categorizes a transaction, a merchant mapping is auto-created if one doesn't exist.

## Categorization Triggers

### A. On import (automatic, deterministic)
After SQLMesh transforms complete, `import_service.py` calls `apply_deterministic_categorization()`:
1. Rule engine — evaluate active rules in priority order (rules take precedence so specific filters like amount ranges or account IDs are honoured first)
2. Merchant lookups — match remaining uncategorized descriptions against `app.merchants` (fallback for anything rules didn't cover)

Fast, no LLM dependency, works from CLI and MCP.

### B. LLM classification (explicit, user-initiated)
The LLM calls `get_uncategorized_transactions`, classifies the returned list in its own reasoning, then submits all results in a single `bulk_categorize` call. Two tool calls total regardless of transaction count.

Over time, as merchants and rules accumulate from LLM results and user corrections, the deterministic layer handles an increasing share.

## Bulk Categorization Flow

```text
get_uncategorized_transactions(limit=100)
  → LLM reviews descriptions and decides categories
  → bulk_categorize([{transaction_id, category, subcategory, merchant_name}, ...])
      → INSERT OR REPLACE into app.transaction_categories (categorized_by='ai')
      → for each item with merchant_name: normalize description, create merchant
          mapping if one doesn't exist
      → return "Categorized N transactions, created M merchant mappings."
```

This avoids MCP sampling (not supported by Claude Code or Claude Desktop) and avoids per-transaction tool calls (hits turn limits ~30).

## Default Taxonomy

SQLMesh seed model (`sqlmesh/models/seeds/categories.sql` + `.csv`) with Plaid PFCv2:

**16 primary categories:** Income, Transfer, Loan Payments, Bank Fees, Entertainment, Food & Drink, Shopping, Home Improvement, Healthcare, Personal Care, Services, Government & Nonprofit, Transportation, Travel, Housing & Utilities, Other

Each with detailed subcategories (~100 total). `plaid_detailed` column enables direct mapping when Plaid data arrives.

## MCP Tools

### Read tools (`tools.py`)
| Tool | Description |
|------|-------------|
| `list_categories` | Active categories |
| `list_categorization_rules` | Active rules |
| `list_merchants` | Known merchants |
| `get_categorization_stats` | Categorized vs uncategorized summary |
| `get_uncategorized_transactions` | Fetch transactions without a category |

### Write tools (`write_tools.py`)
| Tool | Description |
|------|-------------|
| `bulk_categorize` | Apply LLM-decided categories to a list of transactions in one call |
| `categorize_transaction` | Categorize a single transaction (user-initiated corrections) |
| `create_categorization_rule` | Create pattern-based rule |
| `delete_categorization_rule` | Remove rule |
| `create_merchant_mapping` | Add merchant mapping |
| `seed_categories` | Copy PFCv2 defaults from seed table |
| `toggle_category` | Enable/disable category |
| `create_category` | Add custom category |

### Prompts (`prompts.py`)
- `categorize_transactions` — Updated to reference new tools
- `auto_categorize_transactions` — Guides LLM through bulk categorization workflow

## CLI Commands

Under `data categorize`:
| Command | Description |
|---------|-------------|
| `apply-rules` | Run rules + merchants against uncategorized transactions |
| `seed` | Initialize default categories |
| `stats` | Categorization coverage statistics |
| `list-rules` | Display active rules |

## Files Created
- `src/moneybin/sql/schema/app_categories.sql`
- `src/moneybin/sql/schema/app_merchants.sql`
- `src/moneybin/sql/schema/app_categorization_rules.sql`
- `sqlmesh/models/seeds/categories.sql`
- `sqlmesh/models/seeds/categories.csv`
- `src/moneybin/services/categorization_service.py`
- `src/moneybin/cli/commands/categorize.py`
- `tests/moneybin/test_services/test_categorization_service.py`
- `tests/moneybin/test_mcp/test_categorization_tools.py`

## Files Modified
- `src/moneybin/sql/schema/app_schema.sql` — New columns on transaction_categories
- `src/moneybin/tables.py` — CATEGORIES, MERCHANTS, CATEGORIZATION_RULES
- `src/moneybin/schema.py` — Register new DDL files
- `src/moneybin/services/import_service.py` — Call deterministic categorization after transforms
- `src/moneybin/mcp/tools.py` — 4 new read tools
- `src/moneybin/mcp/write_tools.py` — 8 write tools (bulk_categorize replaces auto_categorize)
- `src/moneybin/mcp/prompts.py` — Updated + new prompt
- `src/moneybin/cli/commands/data.py` — Registered categorize subgroup

## Testing

54 tests covering:
- **Normalization:** POS prefix stripping, trailing location/numbers, whitespace
- **Pattern matching:** exact, contains, regex, case insensitive, priority ordering
- **Rule engine:** basic rules, priority ordering, amount filters, account filters, idempotency, inactive rules
- **Merchant matching:** category application, skip without category, combined pipeline
- **Stats:** uncategorized counts, source breakdown
- **Seed idempotency:** duplicate runs don't create duplicates
- **MCP tools:** all CRUD operations, argument parsing, DB reads/writes, bulk categorization

## Key Design Decisions
- **Merchants separate from rules** — merchants normalize names + cache categories; rules express policies with conditions
- **`bulk_categorize` instead of MCP sampling** — MCP sampling (`sampling/createMessage`) is not supported by Claude Code or Claude Desktop; bulk tool is simpler, faster, and avoids per-transaction tool-call overhead
- **Seed data via SQLMesh** — taxonomy in version-controlled CSV, leverages existing infrastructure
