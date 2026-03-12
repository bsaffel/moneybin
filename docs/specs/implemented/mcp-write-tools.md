# Feature: MCP Write Tools

## Status
implemented

## Goal
Allow AI assistants to modify data through the MCP server: importing files, categorizing transactions, managing budgets, and running analytical queries that depend on user-created data.

## Background
- [ADR-003: MCP Primary Interface](../../architecture/003-mcp-primary-interface.md)
- Source: `src/moneybin/mcp/write_tools.py`
- App schema: `src/moneybin/sql/schema/app_schema.sql`

## Requirements

1. Write tools are opt-in (enabled via `--allow-writes` flag or config).
2. Write tools operate on the `app` schema (categories, budgets, notes), not raw/core schemas.
3. The `import_file` tool delegates to the service layer for extraction and loading.
4. All write operations are logged for auditability.
5. Analytical tools that depend on user data (categories, budgets) are included in this module.

## Implemented Tools (9)

### Import

| Tool | Description |
|------|-------------|
| `import_file` | Import a financial data file (OFX/QFX or W-2 PDF) via the service layer |

### Transaction categorization

| Tool | Description |
|------|-------------|
| `categorize_transaction` | Assign category/subcategory to a transaction |
| `get_uncategorized_transactions` | Find transactions without categories (limit 1000) |

### Budget management

| Tool | Description |
|------|-------------|
| `set_budget` | Create or update monthly budget for a category |
| `get_budget_status` | Budget vs actual spending comparison for a month |

### Analytical tools (depend on user data)

| Tool | Description |
|------|-------------|
| `get_monthly_summary` | Income vs expenses summary by month |
| `get_spending_by_category` | Spending breakdown by category for a month |
| `find_recurring_transactions` | Identify subscriptions and recurring charges |

## Data Model

### App schema tables

```sql
CREATE SCHEMA IF NOT EXISTS app;

-- Transaction categories assigned by user or AI
CREATE TABLE IF NOT EXISTS app.transaction_categories (
    transaction_id VARCHAR PRIMARY KEY,
    category VARCHAR NOT NULL,
    subcategory VARCHAR,
    categorized_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    categorized_by VARCHAR DEFAULT 'ai'
);

-- Monthly budget targets by category
CREATE TABLE IF NOT EXISTS app.budgets (
    budget_id VARCHAR PRIMARY KEY,
    category VARCHAR NOT NULL,
    monthly_amount DECIMAL(18, 2) NOT NULL,
    start_month VARCHAR NOT NULL,
    end_month VARCHAR,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Free-form notes on transactions
CREATE TABLE IF NOT EXISTS app.transaction_notes (
    transaction_id VARCHAR PRIMARY KEY,
    note VARCHAR NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

## Implementation Plan

### Files created
- `src/moneybin/mcp/write_tools.py` -- Write tool implementations
- `src/moneybin/sql/schema/app_schema.sql` -- App schema DDL
- `src/moneybin/services/import_service.py` -- Import business logic

### Key decisions

- **`app` schema isolation**: Write tools only modify `app.*` tables, keeping raw and core schemas immutable from the MCP server.
- **`INSERT OR REPLACE`**: Categories and budgets use upsert semantics for idempotency.
- **`categorized_by` column**: Tracks whether a category was assigned by `'ai'` or `'user'` for auditability.
- **Budget status uses JOINs**: `get_budget_status` joins `app.budgets` with `core.fct_transactions` via `app.transaction_categories` to calculate spending vs budget.
- **Service layer for imports**: `import_file` delegates to `import_service.py` rather than implementing import logic directly in the tool.

## CLI Interface

```bash
# Start MCP server with write tools enabled
moneybin mcp serve --allow-writes
```

## Testing Strategy

- Test categorization with mock DuckDB (INSERT OR REPLACE behavior)
- Test budget creation and update paths
- Test budget status calculation with sample data
- Test import_file delegation to service layer
- Test analytical tools with and without categorized data
- Test uncategorized transaction query (LEFT JOIN filtering)

## Dependencies

- Prerequisite: MCP read tools (implemented)
- Prerequisite: Import service layer (`src/moneybin/services/`)

## Out of Scope

- Category rules engine (auto-categorization) -- see [Transaction Categorization](../transaction-categorization.md)
- Budget rollover and alerts -- see [Budget Tracking](../budget-tracking.md)
- Transaction notes tool (schema exists, tool not yet implemented)
