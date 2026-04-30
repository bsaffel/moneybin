# MCP Server (AI Integration)

The MCP ([Model Context Protocol](https://modelcontextprotocol.io)) server exposes your financial data to AI assistants like Claude, Cursor, and any MCP-compatible client. It's the primary programmatic interface — not an afterthought.

## Connecting to AI Clients

```bash
# Generate config for a specific client (prints JSON)
moneybin mcp config generate --client claude-desktop

# Install directly into the client's config file
moneybin mcp config generate --client claude-desktop --install
```

Supported clients: **Claude Desktop**, **Cursor**, **Windsurf**. Works with any MCP-compatible client that accepts stdio transport config.

After connecting, you can ask your AI assistant things like:
- *"What's my spending by category this month?"*
- *"Find all my recurring subscriptions and their annual cost"*
- *"Help me categorize my uncategorized transactions"*
- *"Import this CSV file and set up categorization rules"*
- *"How much did I pay in taxes last year?"*
- *"Show me my monthly income vs expenses trend"*

## Tool Catalog

Tools are organized into domain namespaces. Names are stable — AI clients call `<domain>.<action>`.

### accounts

| Tool | Description |
|------|-------------|
| `accounts.list` | All financial accounts with type and institution |
| `accounts.balances` | Most recent balance for each account |

### transactions

| Tool | Description |
|------|-------------|
| `transactions.search` | Search with date, amount, payee, account, and category filters |
| `transactions.recurring` | Detect subscriptions and regular charges |

### spending

| Tool | Description |
|------|-------------|
| `spending.summary` | Income vs expenses summary by month |
| `spending.by_category` | Spending breakdown by category for a period |

### categorize

| Tool | Description |
|------|-------------|
| `categorize.categories` | List active category taxonomy |
| `categorize.rules` | List active categorization rules |
| `categorize.merchants` | List merchant name mappings |
| `categorize.stats` | Categorization coverage statistics |
| `categorize.uncategorized` | Find transactions needing categorization |
| `categorize.bulk` | Categorize many transactions in one call (auto-creates merchant mapping) |
| `categorize.create_rules` | Create one or many categorization rules |
| `categorize.delete_rule` | Remove a rule |
| `categorize.create_merchants` | Create one or many merchant mappings |
| `categorize.create_category` | Create a custom category or subcategory |
| `categorize.toggle_category` | Enable or disable a category |
| `categorize.seed` | Initialize default category taxonomy (Plaid PFCv2) |
| `categorize.auto_review` | List pending auto-rule proposals |
| `categorize.auto_confirm` | Approve or reject auto-rule proposals |
| `categorize.auto_stats` | Auto-rule health (active, pending, transactions covered) |

### import

| Tool | Description |
|------|-------------|
| `import.file` | Import a financial data file (auto-detects format) |
| `import.csv_preview` | Preview structure and column mapping (dry run) |
| `import.list_formats` | Available tabular import formats and saved profiles |
| `import.status` | Summary of all imported data |

### budget

| Tool | Description |
|------|-------------|
| `budget.set` | Create or update a monthly budget for a category |
| `budget.status` | Budget vs actual spending comparison |

### tax

| Tool | Description |
|------|-------------|
| `tax.w2` | W-2 summary (wages, withholding, employer info) for a year |

### sql

| Tool | Description |
|------|-------------|
| `sql.query` | Execute arbitrary read-only SQL (power user) |

### moneybin

| Tool | Description |
|------|-------------|
| `moneybin.discover` | Tool catalog and capability discovery for AI clients |

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

# Generate or inspect MCP config
moneybin mcp config generate --help
```

The MCP server uses stdio transport by default — the AI client starts and communicates with it through stdin/stdout. You don't normally need to run `mcp serve` directly; the client handles this based on the generated config.
