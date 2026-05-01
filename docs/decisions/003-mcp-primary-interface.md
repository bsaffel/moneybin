# ADR-003: MCP Server as Primary Interface

## Status
accepted

## Context

MoneyBin needs a primary interface for users to interact with their financial data. Options considered:

1. **CLI only** -- Direct SQL queries and CLI commands
2. **Web dashboard** -- Streamlit or similar
3. **MCP server** -- AI assistant integration via Model Context Protocol
4. **All of the above** -- Multiple parallel interfaces

The Model Context Protocol (MCP) allows AI assistants (Claude, Cursor, etc.) to securely access local data through a standardized tool interface. This enables natural language interaction with financial data while keeping everything local.

## Decision

Make the MCP server the **primary** interface, with the data toolkit (DuckDB SQL, SQLMesh, Jupyter) as a parallel secondary interface for power users.

### MCP server characteristics

- Runs locally via stdio transport (not a remote service)
- Uses a **read-only DuckDB connection by default** for all queries
- Write operations (imports, categorization, budgets) acquire a short-lived read-write connection via `get_write_db()` context manager, then restore the read-only connection
- This allows other processes (CLI, notebooks, other MCP instances) to read the database concurrently
- Reads from **core schema only** (`dim_accounts`, `fct_transactions`, etc.)
- Uses `TableRef` constants for table references, never hardcoded strings
- Uses dot-separated namespaces per MCP SEP-986 (e.g., `spending_monthly_summary`)

### Privacy controls

- SQL queries validated to reject write operations
- Configurable result size limits (`MAX_ROWS`, `MAX_CHARS`)
- Optional table allowlist
- No credential or PII exposure in error messages
- Profile isolation (each user profile has its own database)

### Data toolkit (secondary interface)

The same DuckDB database is directly accessible via:

- `moneybin db shell` -- Interactive SQL
- `moneybin db ui` -- Web UI
- SQLMesh models -- Transformation logic
- Jupyter notebooks -- Ad-hoc analysis

## Consequences

- Natural language access to financial data via AI assistants.
- Low barrier to entry: users ask questions instead of writing SQL.
- MCP tools serve as a well-documented API surface for the data model.
- Data toolkit remains available for power users and custom analysis.
- MCP server design must prioritize security (read-only, result limits, allowlists).
- Tool naming and organization affects discoverability for AI assistants.

## References

- [MCP Read Tools Spec](../specs/archived/mcp-read-tools.md) -- Implemented read tools
- [MCP Write Tools Spec](../specs/archived/mcp-write-tools.md) -- Implemented write tools
- [MCP Tier 1 Tools Spec](../specs/mcp-tier1-tools.md) -- Next tools to implement
