# Feature: MCP Read Tools

## Status
implemented

## Goal
Give AI assistants secure, read-only access to local financial data through MCP tools organized by financial domain.

## Background
- [ADR-003: MCP Primary Interface](../../decisions/003-mcp-primary-interface.md)
- Source: `src/moneybin/mcp/tools.py`

## Requirements

1. All read tools open DuckDB in read-only mode.
2. SQL queries validated to reject write operations.
3. Results limited by configurable `MAX_ROWS` (1000) and `MAX_CHARS` (50000).
4. Optional table allowlist restricts accessible tables.
5. Tools read from core schema only (via `TableRef` constants).
6. Tools return JSON-formatted results.
7. Graceful error messages without exposing PII or internal details.

## Implemented Tools (8)

### `schema.*` -- Database discovery

| Tool | Description |
|------|-------------|
| `list_tables` | List all tables and views with schema, type, and column count |
| `describe_table` | Show columns, data types, and row count for a specific table |

### `accounts.*` -- Account management

| Tool | Description |
|------|-------------|
| `list_accounts` | List all financial accounts with type and institution |
| `get_account_balances` | Get the most recent balance for each account |

### `transactions.*` -- Transaction queries

| Tool | Description |
|------|-------------|
| `query_transactions` | Search with date, amount, payee, and account filters |

### `tax.*` -- Tax information

| Tool | Description |
|------|-------------|
| `get_w2_summary` | W-2 tax form data including wages, taxes, and employer info |

### `institutions.*` -- Institution info

| Tool | Description |
|------|-------------|
| `list_institutions` | List all connected financial institutions from OFX data |

### `sql.*` -- Power user

| Tool | Description |
|------|-------------|
| `run_read_query` | Execute arbitrary read-only SQL (SELECT, WITH, DESCRIBE, SHOW, PRAGMA, EXPLAIN only) |

## Implementation Plan

### Files created
- `src/moneybin/mcp/server.py` -- FastMCP server, DuckDB lifecycle, TableRef constants
- `src/moneybin/mcp/tools.py` -- Read tool implementations
- `src/moneybin/mcp/resources.py` -- Resource endpoints (5 live)
- `src/moneybin/mcp/prompts.py` -- Prompt templates (5 live)
- `src/moneybin/mcp/privacy.py` -- Security controls (query validation, result limits, allowlist)
- `src/moneybin/mcp/__init__.py` -- Package init

### Key decisions

- **`_query_to_json` helper**: Centralized query execution with result truncation and error handling.
- **`TableRef` constants**: Type-safe table references (`DIM_ACCOUNTS`, `FCT_TRANSACTIONS`, etc.) instead of hardcoded strings.
- **`table_exists` guard**: Each tool checks if required tables exist before querying, returning helpful messages if not.
- **Parameterized queries**: All user-supplied filters use parameterized SQL to prevent injection.

## Resources (5 live)

| URI | Description |
|-----|-------------|
| `moneybin://schema/tables` | All tables with schema and type |
| `moneybin://schema/{table_name}` | Column definitions for a table |
| `moneybin://accounts/summary` | Account listing with latest balances |
| `moneybin://transactions/recent` | Last 30 days of transactions |
| `moneybin://w2/{tax_year}` | W-2 data for a specific year |

## Prompts (5 live)

| Prompt | Description |
|--------|-------------|
| `analyze_spending` | Analyze spending patterns for a period |
| `find_anomalies` | Look for unusual transactions |
| `tax_preparation` | Gather tax-related information for a year |
| `account_overview` | Comprehensive overview of accounts and balances |
| `transaction_search` | Help find transactions matching a description |

## Testing Strategy

- Mock DuckDB connection with in-memory database
- Test each tool with and without data present
- Test privacy controls: read-only validation, result truncation, table allowlist
- Test error handling for missing tables, invalid queries
- Test resources and prompts

## Dependencies

- `mcp[cli]` -- FastMCP server framework
- `duckdb` -- Database engine

## Out of Scope

- Write tools (see [MCP Write Tools](mcp-write-tools.md))
- Tier 1 analytical tools (see [MCP Tier 1 Tools](../mcp-tier1-tools.md))
- MCP elicitation and sampling features
