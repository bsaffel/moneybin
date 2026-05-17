# MCP Server (AI Integration)

The MCP ([Model Context Protocol](https://modelcontextprotocol.io)) server exposes your financial data to AI assistants like Claude, Cursor, and any MCP-compatible client. It's the primary programmatic interface — not an afterthought.

## Connecting to AI Clients

```bash
# Print the config snippet for a specific client (no write)
moneybin mcp install --client claude-desktop --print

# Install directly into the client's config file
moneybin mcp install --client claude-desktop
```

Supported clients: Claude Desktop, Claude Code, Cursor, Windsurf, VS Code, Gemini CLI, Codex (CLI / Desktop / IDE), and ChatGPT Desktop. See [Configuring MCP Clients](mcp-clients.md) for install paths, the concurrency model, and Claude Code's per-session opt-in.

After connecting, you can ask your AI assistant things like:
- *"What's my spending by category this month?"*
- *"Find all my recurring subscriptions and their annual cost"*
- *"Help me categorize my uncategorized transactions"*
- *"Import this CSV file and set up categorization rules"*
- *"How much did I pay in taxes last year?"*
- *"Show me my monthly income vs expenses trend"*

## Tool Catalog

Tools are organized into domain namespaces. Names are stable — AI clients call `<domain>.<action>`.

Tool names follow the v2 path-prefix-verb-suffix convention (e.g., `accounts_balance_list`, `transactions_categorize_apply`).

### system / orientation

| Tool | Description |
|------|-------------|
| `system_status` | Data inventory (accounts, transactions, freshness) + pending review queue counts |
| `transactions_review` | Lighter orientation tool: just the queue counts |

### accounts

| Tool | Description |
|------|-------------|
| `accounts_list` | All financial accounts with type and institution |
| `accounts_balance_list` | Most recent balance for each account |

### transactions

| Tool | Description |
|------|-------------|
| `transactions_search` | Search with date, amount, payee, account, and category filters |
| `transactions_categorize_pending_list` | Find transactions needing categorization |
| `transactions_categorize_apply` | Categorize many transactions in one call (auto-creates merchant mapping) |
| `transactions_categorize_stats` | Categorization coverage statistics |
| `transactions_categorize_rules_list` / `_create` / `_rule_delete` | Manage categorization rules |
| `transactions_categorize_auto_review` / `_auto_confirm` / `_auto_stats` | Auto-rule learning workflow |

### categories / merchants

| Tool | Description |
|------|-------------|
| `categories_list` | List the category taxonomy (use `include_inactive=True` to see disabled) |
| `categories_create` | Create a custom category or subcategory |
| `categories_set` | Update a category's settings (currently only `is_active`) |
| `merchants_list` | List merchant name mappings |
| `merchants_create` | Create one or many merchant mappings |

### reports

| Tool | Description |
|------|-------------|
| `reports_spending_summary` | Income vs expenses summary by month |
| `reports_spending_by_category` | Spending breakdown by category for a period |
| `reports_budget` | Budget vs actual spending comparison |
| `reports_recurring` | Detect recurring subscriptions and regular charges with confidence scores |

### import

| Tool | Description |
|------|-------------|
| `import_file` | Import a financial data file (auto-detects format) |
| `import_preview` | Preview structure and column mapping (dry run) |
| `import_formats_list` | Available tabular import formats and saved profiles |
| `import_status` | Summary of all imported data |

### refresh

| Tool | Description |
|------|-------------|
| `refresh_run` | Run the post-load pipeline: matching, SQLMesh apply, categorization. Optional `steps` parameter scopes the cascade (e.g., `steps=["transform"]` for SQLMesh apply alone — the granular form formerly exposed as `transform_apply`). |

### budget, tax (de-registered — re-register when backing spec lands)

`budget_set` and `tax_w2` were de-registered 2026-05-17. Both functional implementations remain in the codebase (`src/moneybin/mcp/tools/budget.py`, `tools/tax.py`) and the CLI counterparts (`moneybin budget set`, `moneybin tax w2`, `moneybin tax deductions`) still work — only the MCP surface is gated until `budget-tracking.md` reaches `in-progress` and a tax spec lands per the stub-gating rule in `.claude/rules/mcp-server.md`.

### sync, transform (taxonomy stubs)

`sync_*` (9 tools: login/logout/connect/disconnect/pull/status/schedule_set/show/remove) and `transform_*` (4 tools: status/plan/validate/audit) are registered but currently return `not_implemented` envelopes pointing at their owning specs (`sync-overview.md`, `moneybin-mcp.md` §transform_*). They become real once the corresponding service layers land. `transform_apply` was folded into `refresh_run(steps=["transform"])` on 2026-05-17.

### sql

| Tool | Description |
|------|-------------|
| `sql_query` | Execute arbitrary read-only SQL (power user) |

```bash
# See all registered tools with full descriptions and schemas
moneybin mcp list-tools
```

## Prompt Templates

Prompt templates guide AI assistants through multi-step financial workflows. Each provides structured instructions and context so the AI knows which tools to call and in what order.

| Prompt | Description |
|--------|-------------|
| `account_overview` | Comprehensive overview of all accounts |
| `analyze_spending` | Spending pattern analysis for a time period |
| `categorize_transactions` | Guided categorization of uncategorized transactions |
| `find_anomalies` | Detect unusual or suspicious transactions |
| `import_data` | Guided file import workflow |
| `monthly_review` | Comprehensive monthly financial review |
| `setup_budget` | Budget setup by category |
| `tax_preparation` | Tax-related information gathering for a year |
| `transaction_search` | Find transactions matching a description |

```bash
moneybin mcp list-prompts
```

## Response Envelope

Every tool returns the standard response envelope: `summary` (row counts, truncation, sensitivity tier, currency), `data` (the payload — list of records or a single result dict), `actions` (contextual next-step hints), and an optional `error` (populated when the tool fails with a classified `UserError`; `data` is empty in that case). The `--output json` flag on equivalent CLI commands produces the same envelope, so MCP and CLI consumers can share parsing logic. See [`mcp-architecture`](../specs/mcp-architecture.md) §4 for the full schema.

## Server Management

```bash
# Start the MCP server (used by AI clients, not typically run manually)
moneybin mcp serve

# Install MoneyBin into an MCP client's config (or print the snippet with --print)
moneybin mcp install --help
```

The MCP server uses stdio transport by default — the AI client starts and communicates with it through stdin/stdout. You don't normally need to run `mcp serve` directly; the client handles this based on the generated config.
