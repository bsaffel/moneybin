# Feature: Budget Tracking

## Status
draft

## Goal
Enable users to define monthly budgets by category and track spending against those budgets, with rollover support and status alerts.

## Background
- [MCP Write Tools](implemented/mcp-write-tools.md) -- `set_budget` and `get_budget_status` already implemented
- [Transaction Categorization](transaction-categorization.md) -- Prerequisite for meaningful budgets
- User schema: `src/moneybin/sql/schema/user_schema.sql`

## Requirements

1. Create monthly budgets by category with dollar amounts.
2. Track spending vs budget with status indicators (OK, WARNING at 90%, OVER).
3. Support budget periods with start/end months.
4. Support rollover: unspent budget carries forward to next month.
5. Provide historical budget performance (trend over months).
6. Support budget templates (copy last month's budgets to new month).
7. Calculate total budgeted vs total spent across all categories.

## Data Model

### Existing table (already created)

```sql
-- user.budgets (exists)
-- PK: budget_id, columns: category, monthly_amount, start_month, end_month, created_at, updated_at
```

### New/modified tables

```sql
-- Budget rollover tracking
CREATE TABLE IF NOT EXISTS "user".budget_rollovers (
    rollover_id VARCHAR PRIMARY KEY,
    budget_id VARCHAR NOT NULL REFERENCES "user".budgets(budget_id),
    month VARCHAR NOT NULL,          -- YYYY-MM
    rollover_amount DECIMAL(18, 2),  -- positive = unspent, negative = overspent
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

## Implementation Plan

### Files to create
- `src/moneybin/sql/schema/user_budget_rollovers.sql` -- Rollover DDL
- `src/moneybin/services/budget_service.py` -- Budget calculations, rollover logic
- `tests/moneybin/test_services/test_budget_service.py`

### Files to modify
- `src/moneybin/mcp/write_tools.py` -- Add budget tools
- `tests/moneybin/test_mcp/test_tools.py` -- Add tool tests

### Key decisions

- **Monthly granularity**: Budgets are always monthly. Weekly/quarterly can be derived.
- **Rollover is opt-in**: Default behavior is no rollover (each month starts fresh). Enable per-budget.
- **Budget status thresholds**: OK (< 90%), WARNING (90-100%), OVER (> 100%). Configurable per budget.
- **No automatic budget creation**: User explicitly creates budgets. AI can suggest based on spending patterns.

## MCP Interface

### Existing tools (already implemented)

- `set_budget` -- Create/update budget
- `get_budget_status` -- Budget vs actual for a month

### New tools

| Tool | Description |
|------|-------------|
| `budget.list` | List all active budgets |
| `budget.history` | Budget performance trend over N months |
| `budget.delete` | Remove a budget |
| `budget.copy_month` | Copy budgets from one month to another |
| `budget.rollover` | Calculate and apply rollover for a month |
| `budget.summary` | Total budgeted vs total spent across all categories |

## Testing Strategy

- Test budget CRUD operations
- Test budget status calculation (OK, WARNING, OVER)
- Test rollover calculation and application
- Test budget history aggregation
- Test with no categorized transactions (graceful handling)
- Test month boundary handling

## Dependencies

- No new packages required
- Prerequisite: Transaction categorization (for meaningful budget vs actual)

## Out of Scope

- Weekly or quarterly budget periods
- Budget alerts/notifications (push notifications)
- Savings goals (separate from spending budgets)
- Budget sharing between profiles
