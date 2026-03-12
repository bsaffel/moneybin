# Feature: Transaction Categorization

## Status
**Phase 1-3 implemented** | Phase 4 in progress

## Goal
Provide a layered categorization system that works from day one without training data, works with any LLM (not just Claude), and improves over time. Rules first, then LLM for the remainder, with user corrections feeding back into rules.

## Categorization Priority

| Priority | Source | `categorized_by` | Overwritten by |
|----------|--------|-------------------|----------------|
| 1 | User manual | `'user'` | Nothing |
| 2 | User-defined rules | `'rule'` | User only |
| 3 | Plaid-provided categories | `'plaid'` | User, rules |
| 4 | MCP sampling / LLM | `'ai'` | All above |

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
1. Merchant lookups — match descriptions against `app.merchants`
2. Rule engine — evaluate active rules in priority order

Fast, no LLM dependency, works from CLI and MCP.

### B. LLM classification (explicit, user-initiated)
The `auto_categorize` MCP tool is invoked separately. Uses MCP sampling to delegate classification to whatever LLM the user has connected — no vendor lock-in.

Over time, as merchants and rules accumulate from LLM results and user corrections, the deterministic layer handles an increasing share.

## MCP Sampling Auto-Categorization Flow

The `auto_categorize` tool (async):
1. Fetch uncategorized transactions
2. Group by normalized description (deduplicate)
3. Build prompt with active taxonomy + batch of descriptions (~25 per call)
4. Call MCP sampling via `ctx.session.create_message()`
5. Parse JSON response: `[{description, category, subcategory, confidence, merchant_name}]`
6. Create merchant mappings for high-confidence results (>= 0.7)
7. Apply categories, return summary

Prompt is terse, LLM-agnostic, requests JSON output (not tool use).

## Default Taxonomy

SQLMesh seed model (`sqlmesh/models/seeds/seed_categories.sql` + `.csv`) with Plaid PFCv2:

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

### Write tools (`write_tools.py`)
| Tool | Description |
|------|-------------|
| `auto_categorize` | Full LLM pipeline (async) |
| `create_categorization_rule` | Create pattern-based rule |
| `delete_categorization_rule` | Remove rule |
| `create_merchant_mapping` | Add merchant mapping |
| `seed_categories` | Copy PFCv2 defaults from seed table |
| `toggle_category` | Enable/disable category |
| `create_category` | Add custom category |
| `categorize_transaction` | Updated: accepts `categorized_by`, auto-creates merchant mapping |

### Prompts (`prompts.py`)
- `categorize_transactions` — Updated to reference new tools
- `auto_categorize_transactions` — New: guides LLM through auto-categorization workflow

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
- `sqlmesh/models/seeds/seed_categories.sql`
- `sqlmesh/models/seeds/seed_categories.csv`
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
- `src/moneybin/mcp/write_tools.py` — 7 new write tools, updated categorize_transaction
- `src/moneybin/mcp/prompts.py` — Updated + new prompt
- `src/moneybin/cli/commands/data.py` — Registered categorize subgroup

## Testing

56 new tests covering:
- **Normalization:** POS prefix stripping, trailing location/numbers, whitespace
- **Pattern matching:** exact, contains, regex, case insensitive, priority ordering
- **Rule engine:** basic rules, priority ordering, amount filters, account filters, idempotency, inactive rules
- **Merchant matching:** category application, skip without category, combined pipeline
- **Stats:** uncategorized counts, source breakdown
- **Prompt construction:** taxonomy inclusion, description inclusion, JSON format
- **Response parsing:** valid JSON, markdown fences, extra text, invalid JSON, missing fields, default confidence
- **Seed idempotency:** duplicate runs don't create duplicates
- **MCP tools:** all CRUD operations, argument parsing, DB reads/writes

## Key Design Decisions
- **Merchants separate from rules** — merchants normalize names + cache categories; rules express policies with conditions
- **Async `auto_categorize`** — MCP sampling is async; first async tool in the codebase
- **JSON output from LLM** — simpler, more portable across providers than tool use
- **Low-confidence results still applied** — better than uncategorized; flagged via confidence score
- **Seed data via SQLMesh** — taxonomy in version-controlled CSV, leverages existing infrastructure
