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
- **Privacy middleware** — sensitivity classification, critical-field masking,
  and response filtering. Global consent enforcement is deferred, so tools
  must not rely on an automatic consent gate or degraded response.
- **Service layer** — business logic, parameterized SQL, returns typed Python objects (dataclasses or Pydantic models).

**Enforcement:** `tests/moneybin/test_architecture/test_adapter_layering.py` fails CI when adapters in `src/moneybin/mcp/tools/` or `src/moneybin/cli/commands/` import write-callable symbols from `moneybin.loaders`, `moneybin.extractors`, or `moneybin.matching`. Pure constants, pure read helpers, DI targets, and type/format descriptors are allowlisted explicitly. If you hit the guardrail, the cleanest fix is a new service method; only add an allowlist entry for a genuine exception with a `# why` comment.

## Design Philosophy

1. **Import-first, not ledger-first.** No general-purpose `add_transaction` tool. Transactions come from sources (files, connectors). Corrections and annotations are metadata on source-imported records, not counter-entries.
2. **Privacy by architecture.** Every tool declares a sensitivity tier (`low`,
   `medium`, `high`). Classification and critical-field masking are wired;
   **global consent enforcement is deferred**. Do not claim or depend on an
   automatic consent gate or degraded response until that gate ships.
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
| `system_*` | Orientation and audit |
| `reports` | Registered analytical catalog and execution |
| `accounts_*`, `investments_*` | Financial entities and holdings |
| `transactions_*`, `reviews*` | Transaction and decision workflows |
| `taxonomy_*` | Taxonomy projection and target state |
| `import_*` | File ingestion and reversal |
| `sync_*`, `gsheet_*` | External connection workflows |
| `privacy_*` | Consent and privacy |
| `refresh_*`, `sql_*` | Platform workflow and operator SQL |

Naming: **noun = query** (`reports`, `accounts`, `transactions`), **verb =
action** (`transactions_categorize_commit`, `refresh_run`). No CRUD naming.

**Tool disclosure: one bounded standard registry.** Generic clients receive
every registered standard tool at connect. Capable hosts may optionally defer
schemas from that same registry, but availability, names, annotations,
allowlists, approvals, and audit identity remain unchanged. Do not add packs,
profiles, reconnect modes, or a runtime discovery tool. Each tool must justify
its serialized metadata and carrying weight. See
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

### Pagination

Every standard **resumable collection read** uses the shared versioned keyset
cursor in `moneybin.mcp.pagination`; do not add an offset cursor or a second
opaque envelope. Bind the cursor to the exact public view and canonicalized
public filters, order by immutable keys with an explicit unique tie-breaker,
validate decoded key types before reference resolution or data access, and push
snapshot/continuation predicates into the service query when practical. The
cursor carries the exact initial eligible-row total so continuations keep one
coherent `summary.total_count`. Tests must cover a removal and a row that
prepends ahead of the first-page boundary, malformed typed keys, and
cross-view/filter reuse.

This is a stateless, weakly consistent contract: removals and prepends do not
skip or duplicate the continuation, but an arbitrary concurrent insert whose
immutable sort key falls inside the unserved range may appear. A fully frozen
membership snapshot requires either a monotonic creation key in that domain or
a stateful snapshot store; do not claim that stronger guarantee without one.

`reports` and `sql_query` are bounded caller-shaped executions, not resumable
collections: neither has a declared immutable unique ordering key that MoneyBin
can safely continue. They may signal truncation with `has_more` and a
lower-bound total, never an offset cursor; callers refine the query or rerun
with a higher limit. Bounded summary, detail, catalog, and status views also do
not acquire cursors merely because they accept a limit elsewhere in the same
coarse tool.

Ranked entity resolution is likewise a bounded search, not a resumable
collection. Relevance or confidence depends on mutable names and metadata, so
it cannot be a stateless keyset key without skip/duplicate behavior. Return the
best `limit` candidates in rank order, report truncation without a cursor, and
tell callers to refine the query or rerun with a larger limit.

## Sensitivity Tiers

| Tier | Data | Consent |
|---|---|---|
| `low` | Aggregates, counts, category labels | None |
| `medium` | Row-level: descriptions, amounts, dates | Consent ledger exists; enforcement deferred |
| `high` | Critical PII fields (account numbers) | Critical masking wired; consent enforcement deferred |

The consent ledger is not yet a global runtime gate. Tools cannot assume that a
missing consent grant automatically degrades or blocks data. A future gate may
use degraded envelopes, but only after the corresponding enforcement ships.

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

- **Keep in sync with taxonomy.** Any rename, new top-level group, or change to orientation tools (`system_status`, `reports`) must update the instructions text in the same change.
- **Required content:** one-line product description, top-level group enumeration, naming convention with examples, orientation pointers, response envelope shape, collection-cap convention (list-typed parameters are capped per-call), sensitivity tiers, critical masking, and the deferred consent-enforcement status.
- **Length budget:** ~150–300 tokens. Loaded once per session, but competes with conversation and tool descriptions for working memory.
- **Style:** triple-quoted string via `textwrap.dedent(...)` — not concatenated string literals.
- See [`docs/specs/moneybin-mcp.md`](../../docs/specs/moneybin-mcp.md) for the
  current concrete contract.

## Connection Model

All tools use `get_database()` from `src/moneybin/database.py`. Each call returns a **fresh, short-lived connection** that the caller must close via the context manager (`with get_database(...) as db:`). Read-only tools pass `read_only=True` so they attach DuckDB in shared-read mode and do not hold the exclusive write lock. Write tools use the default `read_only=False`. See [`database-writer-coordination.md`](../../docs/specs/database-writer-coordination.md) and [`privacy-data-protection.md`](../../docs/specs/privacy-data-protection.md).

## Data Access

- Read from **core schema** via `TableRef` constants for analytics queries.
- Write to **app schema** for user-authored state (categories, budgets, consent, annotations).
- Write to **raw schema** for import operations only.
- Use parameterized SQL with `?` placeholders for all values.

## Error Messages

- **Minimize data in errors** — no account numbers, balances, or PII in error messages. Classification and critical-field masking are middleware concerns; global consent enforcement remains deferred and must not be assumed by tool code.

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

Use MCP elicitation when the client supports it. Request an explicit boolean
and confirm only an accepted `true`, then compare a freshly recomputed binding.
Do not use FastMCP's deprecated empty-schema elicitation. This elicited boolean
is not a bare `confirm` tool argument: degraded clients receive an opaque token
carrying the same binding, and no operation proceeds without exact binding
verification. Treat approval as an immutable digest grant; verify that grant
against live state inside the same write transaction, immediately before the
first mutation. Never verify and then open a separate mutation transaction.
Compatible same-intent writes may share one conservatively annotated tool;
classify validated arguments and confirm only the destructive branch. Do not
combine reads with writes or operations whose authorization, sensitivity,
audit, or recovery contracts differ.

## Surface change discipline

**Stub gate.** Register an `@mcp_tool` only when its backing feature spec in
`docs/specs/INDEX.md` is `in-progress` or `implemented`. No stubs or hidden
compatibility aliases belong on the public surface. The current exact registry
and its verification are defined by `STANDARD_TOOL_NAMES` and the scaling spec;
the orientation text may position those same tools but must not create a second
discovery or profile surface.

**Registry budget.** Target 30–40 tools. Crossing 40 requires a carrying-weight
review of every registered tool; 50 is a hard maximum unless ADR-016 is
superseded. Advertise zero deprecated aliases. A hidden compatibility alias
must name its removal release. A report registers behind the single read-only
`reports` catalog/runner and never adds an MCP tool.

**Current registry.** The 45-tool standard registry is operating. Generic
clients receive every tool; capable hosts may optionally defer schemas from
that same registry without reconnect, packs, or profiles. Reports never
consume tool slots. The deterministic comparison passed, but promotion remains
unready until context-budget and host-native-deferral evidence is observed.
Future MCP capabilities remain unnamed until admission through the bounded
registry.

**Admission sequence.** Before proposing a tool, try an existing projection,
method, batch, declarative state, report entry, or workflow umbrella. The PR
must answer the **seven-question admission record**:

1. Which capability ID and user intent does it serve?
2. What is the closest existing tool?
3. Why can it not be an existing filter, projection, method, batch input,
   declarative state, report entry, or workflow umbrella?
4. Which safety, authorization, sensitivity, confirmation, output, audit, or
   recovery difference requires another identity?
5. What serialized count and byte delta does it add?
6. Which evaluation tasks prove the new surface is better?
7. Does the resulting standard registry remain within budget and workflow
   closure?

A consolidation is accepted only when metadata bytes decrease and evaluation
results are no worse. Count reduction alone is insufficient.

All seven answers are required for a future tool; reports enter the catalog
instead of consuming a tool slot.

**Output-schema admission.** The current standard registry advertises zero
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
