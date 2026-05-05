---
description: "MCP server: tool taxonomy, response envelope, sensitivity tiers, service layer architecture"
paths: ["src/moneybin/mcp/**", "src/moneybin/services/**"]
---

# MCP Server

**Authoritative design:** [`docs/specs/mcp-architecture.md`](../../docs/specs/mcp-architecture.md)

## Architecture

MCP tools are thin wrappers around a shared service layer. They contain no business logic, no SQL, and no privacy enforcement — all of that lives below them.

```
MCP Tools / CLI  →  Privacy Middleware  →  Service Layer  →  DuckDB
```

- **MCP/CLI layer** — parameter validation, input/output formatting only.
- **Privacy middleware** — sensitivity gates, consent checks, audit logging, response filtering. Tools are unaware of their own privacy enforcement.
- **Service layer** — business logic, parameterized SQL, returns typed Python objects (dataclasses or Pydantic models).

## Design Philosophy

1. **Import-first, not ledger-first.** No general-purpose `add_transaction` tool. Transactions come from sources (files, connectors). Corrections and annotations are metadata on source-imported records, not counter-entries.
2. **Privacy by architecture.** Every tool declares a sensitivity tier (`low`, `medium`, `high`). The middleware enforces consent and redaction automatically.
3. **Batch-first, composable.** Each tool is called once per turn with a complete result. Collection operations accept lists, not single items.
4. **AI-ergonomic.** Tool names, descriptions, and parameter schemas are designed for LLM tool selection.
5. **CLI symmetry.** Every MCP tool has a CLI equivalent via the same service layer. `--output json` on any CLI command returns the same response envelope. **The CLI is a first-class agent surface, not just a human surface** — Claude Code, Codex CLI, Gemini CLI, and similar agents drive CLI commands directly as a peer pathway to MCP. When designing data-flow primitives (JSON I/O, stdin/stdout, redaction contracts), assume both human and agent consumers; agents pipe and chain commands the way humans use shells. See `cli.md` for stdout/stderr conventions, scripting flags, and `--output json`.

## Tool Taxonomy

Tools use underscore-joined names: `domain_action_or_view`. The MCP spec / SEP-986 permits dots, but Anthropic and OpenAI clients enforce `^[a-zA-Z0-9_-]{1,64}$`, so we use the portable subset.

| Namespace | Purpose |
|---|---|
| `spending.*` | Expense analysis, trends, category breakdowns |
| `cashflow.*` | Income vs outflows, income sources |
| `accounts.*` | Account listing, balances, net worth |
| `transactions.*` | Search, corrections, annotations, recurring |
| `import.*` | File import, status |
| `categorize.*` | Rules, merchant mappings, bulk categorization |
| `budget.*` | Targets, status, rollovers |
| `tax.*` | W-2 data, future capital gains |
| `privacy.*` | Consent status, grants, revocations, audit log |
| `overview.*` | Cross-domain summaries, system info |

Naming: **noun = query** (`spending_summary`), **verb = action** (`categorize_bulk`). No CRUD naming.

**Progressive disclosure:** Per-session, tag-based visibility. All tools are registered at boot; extended-namespace tools carry `tags={domain}` and are hidden by `Visibility(False, tags={domain})` transforms. Core namespaces (~19 tools) are visible at connect; extended namespaces (`categorize`, `budget`, `tax`, `privacy`, `transactions_matches`) are revealed for the calling session only via the `moneybin_discover` meta-tool. Each tool stays single-purpose — no consolidation into action-parameter tools. See `mcp-architecture.md` §3.

## Response Envelope

Every tool returns this shape:

```json
{
  "summary": {"total_count": 247, "returned_count": 50, "has_more": true, "sensitivity": "medium", "display_currency": "USD"},
  "data": [ ... ],
  "actions": ["Use spending_by_category for breakdown"]
}
```

- **`summary`** — metadata for the AI: counts, truncation, sensitivity, currency.
- **`data`** — structured objects, never pre-formatted strings.
- **`actions`** — contextual next-step hints for composability.

Currency lives in `summary.display_currency`, not per-row. Per-row `currency` only when returning mixed unconverted currencies.

## Sensitivity Tiers

| Tier | Data | Consent |
|---|---|---|
| `low` | Aggregates, counts, category labels | None |
| `medium` | Row-level: descriptions, amounts, dates | `mcp-data-sharing` (persistent) |
| `high` | Critical PII fields (account numbers) | `mcp-data-sharing` + always masked for cloud |

Tools without consent return **degraded responses** (aggregates instead of row-level data) using the same envelope with `summary.degraded: true`. Never fail — always return something useful.

The `detail` parameter (`summary`, `standard`, `full`) lets the AI self-select verbosity. `detail=summary` always returns aggregates without triggering consent.

## When CLI-only is justified

Default: every operation is MCP-exposed. CLI-only status requires a justified exception. Two acceptable justifications:

1. **Secret material through the LLM context window.** Tools that accept passphrases or encryption keys (`db_unlock`, `db_rotate_key`, `sync_rotate_key`, `db_init`). Routing those through an LLM-mediated channel is a security model violation, not a capability gap.
2. **Hands-on operator territory.** Bootstrapping, troubleshooting, and developer-tooling operations that require physical operator presence (`db_init/lock/ps/kill/migrate/shell/ui`, `mcp_serve`, `mcp_config_*`, `profile_*`, `transform_restate`). The MCP server can't even start when the database is locked, so exposing recovery/lifecycle tools to MCP would be meaningless.

What is NOT a valid CLI-only justification:
- "Long-running" — MCP supports progress notifications.
- "Needs OAuth / browser" — tools can return redirect URLs; clients open them.
- "Destructive" — use a `confirm` parameter or elicitation; the AI must obtain explicit user agreement.
- "Interactive" — split into a list-tool (read) and act-tools (write); the AI orchestrates the loop.
- "Writes to scheduler / filesystem" — server has filesystem access; routine.

When adding a new operation, the default is "expose to MCP." Apply this filter at design time, not after the fact.

## Server Instructions Field

The `FastMCP(instructions=...)` argument in `src/moneybin/mcp/server.py` is the canonical onboarding text injected into the LLM's system prompt at session start. Treat it as a load-bearing surface, not a comment.

- **Keep in sync with taxonomy.** Any rename, new top-level group, or change to orientation tools (`system_status`, `reports_health`) must update the instructions text in the same change.
- **Required content:** one-line product description, top-level group enumeration, naming convention with examples, orientation pointers, response envelope shape, bulk-tool preference, sensitivity tiers / degraded-response behavior.
- **Length budget:** ~150–300 tokens. Loaded once per session, but competes with conversation and tool descriptions for working memory.
- **Style:** triple-quoted string via `textwrap.dedent(...)` — not concatenated string literals.
- See [`docs/specs/mcp-tool-surface.md`](../../docs/specs/mcp-tool-surface.md) §1 for required-content rationale.

## Connection Model

All tools use `get_database()` from `src/moneybin/database.py` — a single long-lived read-write connection per process. The `Database` class handles encryption, schema init, and migrations transparently. See [`privacy-data-protection.md`](../../docs/specs/privacy-data-protection.md).

## Data Access

- Read from **core schema** via `TableRef` constants for analytics queries.
- Write to **app schema** for user-authored state (categories, budgets, consent, annotations).
- Write to **raw schema** for import operations only.
- Use parameterized SQL with `?` placeholders for all values.

## Error Messages

- **Minimize data in errors** — no account numbers, balances, or PII in error messages. Privacy enforcement (consent, redaction, audit) is handled by the middleware, not tool code.
