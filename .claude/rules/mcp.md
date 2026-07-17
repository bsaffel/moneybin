---
description: "MCP server: tool taxonomy, response envelope, sensitivity tiers, service layer architecture"
paths: ["src/moneybin/mcp/**", "src/moneybin/services/**"]
---

# MCP Server

**Authoritative design:** [`docs/specs/mcp-architecture.md`](../../docs/specs/mcp-architecture.md)

**Surface-shape rules:** [`surface-design.md`](surface-design.md) — operation-shape taxonomy, verb vocabulary, audience layering. Cross-surface (governs MCP, CLI, and future REST endpoints). Consult before adding, renaming, or restructuring a tool.

## Architecture

MCP tools are thin wrappers around a shared service layer. They contain no business logic, no SQL, and no privacy enforcement — all of that lives below them.

```
MCP Tools / CLI  →  Privacy Middleware  →  Service Layer  →  DuckDB
```

- **MCP/CLI layer** — parameter validation, input/output formatting only.
- **Privacy middleware** — sensitivity gates, consent checks, audit logging, response filtering. Tools are unaware of their own privacy enforcement.
- **Service layer** — business logic, parameterized SQL, returns typed Python objects (dataclasses or Pydantic models).

**Enforcement:** `tests/moneybin/test_architecture/test_adapter_layering.py` fails CI when adapters in `src/moneybin/mcp/tools/` or `src/moneybin/cli/commands/` import write-callable symbols from `moneybin.loaders`, `moneybin.extractors`, or `moneybin.matching`. Pure constants, pure read helpers, DI targets, and type/format descriptors are allowlisted explicitly. If you hit the guardrail, the cleanest fix is a new service method; only add an allowlist entry for a genuine exception with a `# why` comment.

## Design Philosophy

1. **Import-first, not ledger-first.** No general-purpose `add_transaction` tool. Transactions come from sources (files, connectors). Corrections and annotations are metadata on source-imported records, not counter-entries.
2. **Privacy by architecture.** Every tool declares a sensitivity tier (`low`, `medium`, `high`). The middleware enforces consent and redaction automatically.
3. **Batch-first, composable.** Each tool is called once per turn with a complete result. Collection operations accept lists, not single items.
4. **AI-ergonomic.** Tool names, descriptions, and parameter schemas are designed for LLM tool selection.
5. **CLI capability symmetry.** MCP and CLI map to the same capability IDs,
   service operations, and observable outcomes. They do not require 1:1 method
   or name equality. Granular CLI operator controls may sit behind one MCP
   workflow umbrella; `--output json` still returns the same response envelope.
   See `cli.md`.

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

Naming: **noun = query** (`accounts_summary`), **verb = action** (`transactions_categorize_commit`). No CRUD naming.

**Tool disclosure: one bounded standard registry.** Generic clients receive
every registered standard tool at connect. Capable hosts may defer schemas from
that same registry, but availability, names, annotations, allowlists,
approvals, and audit identity remain unchanged. Do not add packs, profiles,
reconnect modes, or a runtime discovery tool. Each tool must justify its
serialized metadata and carrying weight. See
[`mcp-tool-surface-scaling.md`](../../docs/specs/mcp-tool-surface-scaling.md).

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

Every public tool MUST return canonical JSON text and equivalent
`structuredContent`. Do not advertise `outputSchema` by default. A schema is an
opt-in public contract and requires a named consuming client, evidence that
structured content alone is insufficient, exact byte/context cost,
representative client tests, and persisted benefit evidence. If admitted, it
MUST match runtime `structuredContent` exactly.

## Sensitivity Tiers

| Tier | Data | Consent |
|---|---|---|
| `low` | Aggregates, counts, category labels | None |
| `medium` | Row-level: descriptions, amounts, dates | `mcp-data-sharing` (persistent) |
| `high` | Critical PII fields (account numbers) | `mcp-data-sharing` + always masked for cloud |

Tools without consent return **degraded responses** (aggregates instead of row-level data) using the same envelope with `summary.degraded: true`. Never fail — always return something useful.

The `detail` parameter (`summary`, `standard`, `full`) lets the AI self-select verbosity. `detail=summary` always returns aggregates without triggering consent.

## When CLI-only is justified

Default: every non-exempt user capability is covered by MCP. CLI-only
capability status requires a justified exception; granular CLI commands may
still sit behind an MCP umbrella. Two acceptable justifications:

1. **Secret material through the LLM context window.** Tools that accept or display passphrases, encryption keys, or key-derivation material (`db_init`, `db_unlock`, `db_key_rotate`, `db_key_show`, `db_key_export`, `db_key_import`, `db_key_verify`, `sync_key_rotate`). Routing those through an LLM-mediated channel is a security model violation, not a capability gap.
2. **Hands-on operator territory.** Bootstrapping, recovery, and developer-tooling operations that require physical operator presence. The MCP server cannot even start when the database is locked, so exposing lifecycle tools to MCP would be meaningless. Covers:
   - **Database lifecycle:** `db_init`, `db_lock`, `db_ps`, `db_kill`, `db_shell`, `db_ui`, `db_migrate_apply`, `db_migrate_status`, `db_backup`, `db_restore`, `db_info`, `db_query` (raw SQL access; agent path is `sql_query`). Note: `db_query`, `db_shell`, and `db_ui` emit an operator-bypass banner (stderr) and include it in their `--help` text warning that no privacy middleware applies — account numbers and other CRITICAL-tier fields are NOT masked. The MCP `sql_query` tool is the privacy-safe agent path for ad-hoc SQL.
   - **Server lifecycle:** `mcp_serve`, `mcp_install`, `mcp_config_path`, `mcp_list_tools`, `mcp_list_prompts` (operator introspection of the local MCP surface).
   - **Profile + identity:** `profile_*`.
   - **Developer tooling:** `logs`, `stats`, `synthetic_generate`, `synthetic_reset`, `transform_seed`, `transform_restate`.
   - **Bootstrapping:** `demo` — CLI-only for the same reason as `profile_*` (it creates and activates a profile, so it must run *before* an MCP session can exist), but its audience is the external evaluator, not the developer.

What is NOT a valid CLI-only justification:
- "Long-running" — MCP supports progress notifications.
- "Needs OAuth / browser" — tools can return redirect URLs; clients open them.
- "Destructive" — use payload-bound confirmation through elicitation or an
  opaque fallback token; the AI must obtain explicit user agreement.
- "Interactive" — preserve the propose/review/confirm trust boundary using the
  smallest coherent read/write contracts; the AI orchestrates the loop.
- "Writes to scheduler / filesystem" — server has filesystem access; routine.

When adding a new capability, the default is "cover it in MCP." First try an
existing filter, projection, method, batch input, target-state mutation,
registered report, or workflow umbrella. A new CLI command or service method
does not imply a new MCP tool. Apply this filter at design time.

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

## Entity resolution

All coarse operations use one shared reference-resolution contract:

1. explicit stable ID;
2. exact alias or name;
3. unambiguous normalized match;
4. structured `not_found` or `ambiguous` result with candidate IDs.

Writes never select the first fuzzy match. Resolution belongs in the service
layer, not individual tool wrappers.

## Destructive confirmation

A destructive preview binds approval to canonical validated arguments, resolved
stable IDs, actor/profile and authorization context, operation kind, and a
blast-radius summary. Approval is short-lived and single-use. Recompute the
canonical representation immediately before commit and distinguish expired,
replayed, mismatched, and nothing-to-do results.

Use MCP elicitation when the client supports it. Degraded clients receive an
opaque confirmation token carrying the same binding. A bare boolean `confirm`
is not sufficient for new operations. Compatible same-intent writes may share
one conservatively annotated tool; classify validated arguments and confirm
only the destructive branch. Do not combine reads with writes or operations
whose authorization, sensitivity, audit, or recovery contracts differ.

## Surface change discipline

**Stub gate.** Register an `@mcp_tool` only when its backing feature spec in `docs/specs/INDEX.md` is `in-progress` or `implemented`. No stubs on the public surface — tools whose dependency is `draft`, `ready`, or unwritten stay unregistered (the implementation file may remain in `src/moneybin/mcp/tools/` as a dormant building block; only the `register_*_tools(mcp)` call is gated). Phantom prefixes — those with no registered tools — never appear in the orientation surfaces (FastMCP `instructions` field, `moneybin://tools` resource, `mcp-architecture.md` §3 namespace table). One narrow carve-out applies to *promotion* (not registration): a namespace whose tools register but whose top-level domain is deliberately omitted from the orientation surfaces by design must be documented in `moneybin-mcp.md` §15 with the trigger that would promote it. Active carve-outs are tracked in `moneybin-mcp.md` §17 "Dependency tracker".

**Registry budget.** Target 30–40 tools. Crossing 40 requires a carrying-weight
review of every registered tool; 50 is a hard maximum unless ADR-016 is
superseded. Advertise zero deprecated aliases. A hidden compatibility alias
must name its removal release. A report registers behind the single read-only
`reports` catalog/runner and never adds an MCP tool.

**Admission sequence.** Before proposing a tool, try an existing projection,
method, batch, declarative state, report entry, or workflow umbrella. The PR
must name:

1. the capability ID and user intent;
2. the closest existing tool and why it cannot carry the behavior;
3. the material intent, safety, authorization, sensitivity, confirmation,
   output, audit, or recovery boundary;
4. tool-count and actual serialized `tools/list` byte deltas;
5. persisted evaluation cases covering selection, arguments, workflow, safety,
   and client-schema compatibility.

A consolidation is accepted only when metadata bytes decrease and evaluation
results are no worse. Count reduction alone is insufficient.

**Output-schema admission.** The initial standard registry advertises zero
output schemas. A PR adding one must include the consuming client/integration,
the concrete hydration or validation failure without it, exact per-tool and
registry-wide byte deltas, representative compatibility tests, and a persisted
evaluation showing material benefit. Do not add schema resources, profiles, or
configuration switches speculatively.

Any PR that adds, renames, or removes a tool (MCP) or command (CLI) MUST update **two** specs in the same change:

1. The **surface-specific spec** — [`docs/specs/moneybin-mcp.md`](../../docs/specs/moneybin-mcp.md) for MCP changes, [`docs/specs/moneybin-cli.md`](../../docs/specs/moneybin-cli.md) for CLI changes. Per-surface implementation detail (parameter schemas, sensitivity tiers, envelope shape, flag conventions) lives here.
2. The **cross-surface capability map** — [`docs/specs/moneybin-capabilities.md`](../../docs/specs/moneybin-capabilities.md). Add a new row (new capability) or update the existing row's cell (rename, removed, exempt change).

Reviewers verify both updates AND that the capability's user-language description matches what the surface actually does.

- Reviewers grep for `@mcp_tool` diffs and Typer command registrations and verify each touches both specs.
- Removed tools/commands require both spec updates AND a CHANGELOG.md `Removed` entry under `Unreleased`.
- Renamed tools/commands require updating every reference in the surface-specific spec, updating the relevant row in the capabilities map, plus tests, plus a `Changed` entry in the CHANGELOG.
- Exempting a surface (e.g., CLI-only by secret-material policy) requires citing the category by number from "When CLI-only is justified" above; the citation must match the exemption-category index in the capabilities map.

## Description requirements

The MCP description string passed to `register(mcp, fn, name, description)` is the only schema-attached prose the agent sees at tool-selection time. Tool descriptions MUST state, when applicable:

- **Sign convention** — for tools accepting or returning amount-shaped data: "Amounts use the accounting convention: negative = expense, positive = income; transfers exempt." Tools that intentionally flip the convention (e.g., presentation aggregations that show expenses as positive values) MUST state the override explicitly.
- **Currency** — for tools returning currency-bearing data: amounts are in the currency named by `summary.display_currency`, never inferred from context (per `architecture-shared-primitives.md` Invariant 7).
- **Mutation surface** — for tools with `read_only=False`: the `app.*` table written and the revert path (audit log reference, paired undo tool, or "permanent — no revert").
- **Presentation hint** — for tools returning series or breakdown data: the natural chart shape and display-sign handling (e.g., "suitable for a stacked monthly bar; for display charts flip sign — negative = expense"). The driving agent is the renderer in every shipping host; conventions in [`agent-visualization.md`](../../docs/specs/agent-visualization.md) (draft, M3K.1).

Reviewer responsibility on every PR adding or modifying an `@mcp_tool` decoration. The `.claude/rules/database.md` and `AGENTS.md` files document these invariants for human contributors, but the agent never sees those — invariants the agent must apply correctly belong in the tool description itself.

Descriptions begin with a distinct, intent-specific opening. CI compares at
least the first 60 characters, targets no more than 120 characters for the
first sentence and 900 characters total, and rejects copied cross-cutting
prose. Shared trust, privacy, and sign-convention guidance belongs in server
instructions when it can be loaded once.

Tests inspect the actual `tools/list` response, not only Python/Pydantic types.
For coarse or discriminated contracts they prove that all variants render,
variant-specific required fields survive, JSON text equals
`structuredContent`, decimal values remain numeric, and commonly stringified
client inputs fail or coerce safely. Any selectively admitted `outputSchema`
must also match runtime structured content.
Do not add a global coercion layer without supported-client evidence.

## Agent-experience reports

Any session in which the agent **invokes MoneyBin's MCP tools as a
first-person consumer** — real use, smoke probing, or incidental lookup —
must produce an agent-experience report per
[`agent-experience.md`](agent-experience.md). Running the project test
suite or editing MCP code/tests does not trigger a report; the signal is
what it felt like to use the surface. Reports are session-internal: present
to the developer in chat, never paste into PRs, commits, CHANGELOG, or ADRs. See
`agent-experience.md` for the full trigger list and reporting workflow.
