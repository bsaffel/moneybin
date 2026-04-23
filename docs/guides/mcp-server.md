# MCP Server (AI Integration)

The MCP ([Model Context Protocol](https://modelcontextprotocol.io)) server exposes your financial data to AI assistants like Claude, ChatGPT, Cursor, and any MCP-compatible client. It's the primary programmatic interface — not an afterthought.

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
- *"Look for unusual or suspicious transactions in the last month"*

## Available Tools

The MCP server exposes 30+ tools across these domains:

### Query & Analysis

| Tool | Description |
|------|-------------|
| `query_transactions` | Search transactions with date, amount, payee, and account filters |
| `get_monthly_summary` | Income vs expenses summary by month |
| `get_spending_by_category` | Spending breakdown by category for a month |
| `get_account_balances` | Most recent balance for each account |
| `get_budget_status` | Budget vs actual spending comparison |
| `find_recurring_transactions` | Detect subscriptions and regular charges |
| `get_w2_summary` | W-2 tax data (wages, withholding, employer info) |
| `get_categorization_stats` | Categorization coverage statistics |
| `list_accounts` | All financial accounts with type and institution |
| `list_institutions` | Connected financial institutions |
| `list_categories` | Active category taxonomy |
| `list_categorization_rules` | Active auto-categorization rules |
| `list_merchants` | Merchant name mappings |

### Import

| Tool | Description |
|------|-------------|
| `import_file` | Import a financial data file (OFX, CSV, Excel, etc.) |
| `import_preview` | Preview file structure and column mapping (dry run) |
| `import_history` | List past imports with batch details |
| `import_revert` | Undo an import batch |
| `list_formats` | Available tabular import formats |

### Categorization

| Tool | Description |
|------|-------------|
| `get_uncategorized_transactions` | Find transactions needing categorization |
| `categorize_transaction` | Assign a category (auto-creates merchant mapping) |
| `bulk_categorize` | Categorize many transactions in one call |
| `create_categorization_rule` | Create an auto-categorization rule |
| `bulk_create_categorization_rules` | Create multiple rules at once |
| `create_merchant_mapping` | Map raw description to clean merchant name |
| `bulk_create_merchant_mappings` | Create multiple merchant mappings at once |
| `create_category` | Create a custom category or subcategory |
| `toggle_category` | Enable or disable a category |
| `seed_categories` | Initialize default category taxonomy |
| `delete_categorization_rule` | Remove a categorization rule |

### Budget

| Tool | Description |
|------|-------------|
| `set_budget` | Create or update a monthly budget for a category |

### Database

| Tool | Description |
|------|-------------|
| `list_tables` | List all tables and views in the database |
| `describe_table` | Show columns, types, and row count for a table |
| `run_read_query` | Execute arbitrary read-only SQL (power user) |

```bash
# See all registered tools with full descriptions
moneybin mcp list-tools
```

## Prompt Templates

Prompt templates guide AI assistants through multi-step financial workflows. Each template provides structured instructions and context so the AI knows which tools to call and in what order.

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
# See all registered prompts with descriptions
moneybin mcp list-prompts
```

## Server Management

```bash
# Start the MCP server (used by AI clients, not typically run manually)
moneybin mcp serve

# Show current MCP config
moneybin mcp config
```

The MCP server uses stdio transport by default — the AI client starts and communicates with it through stdin/stdout. You don't normally need to run `mcp serve` directly; the client handles this based on the generated config.
