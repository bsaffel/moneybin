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
| `categorize.*` | Rules, merchant mappings, categorization |
| `budget.*` | Targets, status, rollovers |
| `tax.*` | W-2 data, future capital gains |
| `privacy.*` | Consent status, grants, revocations, audit log |
| `overview.*` | Cross-domain summaries, system info |

Naming: **noun = query** (`spending_summary`), **verb = action** (`categorize_apply`). No CRUD naming.

**Progressive disclosure** is a desired future state, not the current operational reality. The mechanism (tag-based visibility + `moneybin_discover` meta-tool + `tools/list_changed` notification) is wired and tested, but **`MoneyBinSettings.mcp.progressive_disclosure` defaults `False`** because client support for `tools/list_changed` is unreliable in practice (Claude Desktop has spotty support; only Claude Code reliably honors it). In default deployment **every registered tool is visible at connect** — the `tags={domain}` markers on extended-namespace tools are dormant metadata until the flag is flipped on.

**Design implication:** When adding a new MCP tool, assume the **full tool surface is always visible** to the agent. Do not rely on progressive disclosure to keep a tool out of the context window. The agent-attention budget for tool descriptions and schemas is set by the total registered surface, not by any "core vs. extended" split. Each tool's description, parameter schema, and namespace placement must justify itself against the full-surface bar.

See `mcp-architecture.md` §3 for the design rationale and `MoneyBinSettings.mcp.progressive_disclosure` field description for the current flag state.

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

1. **Secret material through the LLM context window.** Tools that accept or display passphrases, encryption keys, or key-derivation material (`db_init`, `db_unlock`, `db_key_rotate`, `db_key_show`, `db_key_export`, `db_key_import`, `db_key_verify`, `sync_key_rotate`). Routing those through an LLM-mediated channel is a security model violation, not a capability gap.
2. **Hands-on operator territory.** Bootstrapping, recovery, and developer-tooling operations that require physical operator presence. The MCP server cannot even start when the database is locked, so exposing lifecycle tools to MCP would be meaningless. Covers:
   - **Database lifecycle:** `db_init`, `db_lock`, `db_ps`, `db_kill`, `db_shell`, `db_ui`, `db_migrate_apply`, `db_migrate_status`, `db_backup`, `db_restore`, `db_info`, `db_query` (raw SQL access; agent path is `sql_query`).
   - **Server lifecycle:** `mcp_serve`, `mcp_install`, `mcp_config_path`, `mcp_list_tools`, `mcp_list_prompts` (operator introspection of the local MCP surface).
   - **Profile + identity:** `profile_*`.
   - **Developer tooling:** `logs`, `stats`, `synthetic_generate`, `synthetic_reset`, `transform_seed`, `transform_restate`.

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
- **Required content:** one-line product description, top-level group enumeration, naming convention with examples, orientation pointers, response envelope shape, collection-cap convention (list-typed parameters are capped per-call), sensitivity tiers / degraded-response behavior.
- **Length budget:** ~150–300 tokens. Loaded once per session, but competes with conversation and tool descriptions for working memory.
- **Style:** triple-quoted string via `textwrap.dedent(...)` — not concatenated string literals.
- See [`docs/specs/moneybin-mcp.md`](../../docs/specs/moneybin-mcp.md) §1 for required-content rationale.

## Connection Model

All tools use `get_database()` from `src/moneybin/database.py`. Each call returns a **fresh, short-lived connection** that the caller must close via the context manager (`with get_database(...) as db:`). Read-only tools pass `read_only=True` so they attach DuckDB in shared-read mode and do not hold the exclusive write lock. Write tools use the default `read_only=False`. See [`database-writer-coordination.md`](../../docs/specs/database-writer-coordination.md) and [`privacy-data-protection.md`](../../docs/specs/privacy-data-protection.md).

## Data Access

- Read from **core schema** via `TableRef` constants for analytics queries.
- Write to **app schema** for user-authored state (categories, budgets, consent, annotations).
- Write to **raw schema** for import operations only.
- Use parameterized SQL with `?` placeholders for all values.

## Error Messages

- **Minimize data in errors** — no account numbers, balances, or PII in error messages. Privacy enforcement (consent, redaction, audit) is handled by the middleware, not tool code.

## Surface change discipline

Any PR that adds, renames, or removes a tool (MCP) or command (CLI) MUST update **two** specs in the same change:

1. The **surface-specific spec** — [`docs/specs/moneybin-mcp.md`](../../docs/specs/moneybin-mcp.md) for MCP changes, [`docs/specs/moneybin-cli.md`](../../docs/specs/moneybin-cli.md) for CLI changes. Per-surface implementation detail (parameter schemas, sensitivity tiers, envelope shape, flag conventions) lives here.
2. The **cross-surface capability map** — [`docs/specs/moneybin-capabilities.md`](../../docs/specs/moneybin-capabilities.md). Add a new row (new capability) or update the existing row's cell (rename, removed, exempt change).

Reviewers verify both updates AND that the capability's user-language description matches what the surface actually does.

- Reviewers grep for `@mcp_tool` diffs and Typer command registrations and verify each touches both specs.
- Removed tools/commands require both spec updates AND a CHANGELOG.md `Removed` entry under `Unreleased`.
- Renamed tools/commands require updating every reference in the surface-specific spec, updating the relevant row in the capabilities map, plus tests, plus a `Changed` entry in the CHANGELOG.
- Exempting a surface (e.g., CLI-only by secret-material policy) requires citing the category by number from "When CLI-only is justified" above; the citation must match the exemption-category index in the capabilities map.

This rule replaces the proposed automated drift test (see `moneybin-mcp.md` §18). Automated enforcement was rejected because a fixture-based test detects code-vs-fixture drift but not code-vs-spec drift, and a spec-parsing test is fragile to spec restructuring. Revisit this decision if PR review proves insufficient.

## Description requirements

The MCP description string passed to `register(mcp, fn, name, description)` is the only schema-attached prose the agent sees at tool-selection time. Tool descriptions MUST state, when applicable:

- **Sign convention** — for tools accepting or returning amount-shaped data: "Amounts use the accounting convention: negative = expense, positive = income; transfers exempt." Tools that intentionally flip the convention (e.g., presentation aggregations that show expenses as positive values) MUST state the override explicitly.
- **Currency** — for tools returning currency-bearing data: amounts are in the currency named by `summary.display_currency`, never inferred from context (per `architecture-shared-primitives.md` Invariant 7).
- **Mutation surface** — for tools with `read_only=False`: the `app.*` table written and the revert path (audit log reference, paired undo tool, or "permanent — no revert").

Reviewer responsibility on every PR adding or modifying an `@mcp_tool` decoration. The `.claude/rules/database.md` and `AGENTS.md` files document these invariants for human contributors, but the agent never sees those — invariants the agent must apply correctly belong in the tool description itself.

## Agent-experience reports

Any session that touches MoneyBin's MCP server — testing, real use, or
incidental lookup — must produce an agent-experience report per
[`agent-experience.md`](agent-experience.md). PRs shipping MCP changes
include the report (or a link) in the description; reviewers verify it
exists and that prior-report blockers are either addressed here or have an
open follow-up.
