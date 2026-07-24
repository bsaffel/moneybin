<!-- Last reviewed: 2026-07-18 -->
# MCP Prompts

MCP prompts are optional conversation starters registered by the local
MoneyBin server. Client support varies; use `uv run moneybin mcp list-prompts`
to see the live catalog for your installed version.

| Prompt | Purpose |
|---|---|
| `monthly_review` | Review spending, cash flow, balances, and recurring charges. |
| `categorization_organize` | Work through uncategorized transactions and propose rules. |
| `review_auto_rules` | Review pending auto-categorization rules before accepting them. |
| `onboarding` | Import initial data, verify accounts, and inspect categorization coverage. |
| `curate_recent_transactions` | Add useful tags and notes to recent transactions. |
| `review_curation_history` | Summarize recent curation activity from the audit log. |
| `sync_review` | Review sync health and suggest next steps. |

The server defines these prompts in
[`src/moneybin/mcp/prompts.py`](../../../src/moneybin/mcp/prompts.py) and
[`src/moneybin/mcp/tools/sync.py`](../../../src/moneybin/mcp/tools/sync.py)
(`sync_review`). For the
tool contract and client setup, see the [MCP server guide](../../guides/mcp-server.md)
and [MCP clients guide](../../guides/mcp-clients.md).
