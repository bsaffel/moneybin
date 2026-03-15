---
paths: ["src/moneybin/mcp/**"]
---

# MCP Server

## Data Access

- Read from **core schema only** (`dim_accounts`, `fct_transactions`, etc.)
- Use `TableRef` constants from `moneybin.tables` for table references, never hardcoded strings.
- Never query raw or staging tables from the MCP server.

## Connection Model

- **Read tools** use `get_db()` — a long-lived read-only connection opened at startup.
- **Write tools** use `get_write_db()` — a context manager that temporarily closes the read connection, opens a read-write connection, then restores the read connection after the write completes.
- DuckDB does not allow mixed read-only and read-write connections to the same file in the same process, so the swap is necessary.
- This design allows other processes (CLI, notebooks) to read the database concurrently.

## Privacy Tiers

The MCP server exposes financial data through privacy tiers:
- Aggregated data (summaries, totals) is lowest sensitivity.
- Individual transaction details require higher trust.
- Account identifiers and credentials are highest sensitivity.

## Bulk operations

- Prefer tools that accept lists over tools that operate on one item at a time. Single-item tools hit turn tool-call limits for bulk workflows.
- The standard pattern: one read tool to fetch candidates → LLM reasons about all of them → one write tool submits the full list.
- Example: `get_uncategorized_transactions` → Claude classifies → `bulk_categorize([...])`.

## Principles

- **Local storage only**: All data stays on user's machine.
- **Minimize data in errors**: Don't expose account numbers, balances, or PII in error messages.
- **Read-only by default**: Write operations require explicit user confirmation.
- Treat external APIs (Plaid, etc.) as data sources, not storage.
