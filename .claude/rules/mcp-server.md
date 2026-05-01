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
5. **CLI symmetry.** Every MCP tool has a CLI equivalent via the same service layer. `--output json` on any CLI command returns the same response envelope.

## Tool Taxonomy

Tools use dot-separated namespaces (MCP SEP-986): `domain.action_or_view`.

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

## Connection Model

All tools use `get_database()` from `src/moneybin/database.py` — a single long-lived read-write connection per process. The `Database` class handles encryption, schema init, and migrations transparently. See [`privacy-data-protection.md`](../../docs/specs/privacy-data-protection.md).

## Data Access

- Read from **core schema** via `TableRef` constants for analytics queries.
- Write to **app schema** for user-authored state (categories, budgets, consent, annotations).
- Write to **raw schema** for import operations only.
- Use parameterized SQL with `?` placeholders for all values.

## Error Messages

- **Minimize data in errors** — no account numbers, balances, or PII in error messages. Privacy enforcement (consent, redaction, audit) is handled by the middleware, not tool code.
