# MCP Tool Surface Scaling

> Companions: [`mcp-architecture.md`](mcp-architecture.md) (tool and envelope
> conventions), [`moneybin-mcp.md`](moneybin-mcp.md) (concrete tool catalog),
> [`moneybin-capabilities.md`](moneybin-capabilities.md) (capability parity),
> [`extension-contracts.md`](extension-contracts.md) (report and package
> registration), and
> [ADR-016](../decisions/016-bounded-mcp-tool-registry.md)

## Status

- **Type:** Architecture
- **Status:** in-progress — the 45-tool registry is operating, but promotion is
  blocked on observed context-budget and host-native-deferral evidence
- **Address:** M3K.2 — second work item under M3K (CLI / MCP UX standards)
- **Origin:** July 2026 MCP surface review. ADR-016 records the measured
  pre-cutover registry and the rationale for the bounded replacement.

## Goal

Expose one capability-complete, safety-aware MCP surface that remains small
enough for generic clients to load in full and precise enough for agents to use
reliably.

The target is **the most capability with the least permanent schema**, not the
smallest possible tool count and not a marketing-friendly headline.

## Decision summary

MoneyBin operates one bounded 45-tool standard registry.

- Every standard client receives the complete registered surface. There are no
  core capability packs, hidden expert tools, or reconnect-only modes.
- Hosts with native tool search or deferred loading may index the same registry
  and inject only task-relevant schemas. Deferral changes prompt cost, never
  capability availability or tool identity.
- The standard registry targets 30–40 tools, requires a carrying-weight review
  above 40, and may not exceed 50 without revising this architecture.
- CLI parity means capability and service-outcome parity across the surface,
  not one MCP method per CLI command.
- Compatible reads become explicit projections of one domain query.
  Compatible writes become batch or declarative target-state operations.
  Complete user workflows become deterministic umbrellas.
- Separate tools remain where intent, safety, authorization, sensitivity,
  confirmation, output, audit, or recovery contracts materially differ.
- All registered reports execute through one read-only `reports` catalog and
  runner. Adding a report does not add an MCP tool.
- Every public tool returns canonical JSON text and equivalent
  `structuredContent`. The current standard registry advertises no
  `outputSchema`; a schema is admitted only for a named consumer that needs
  protocol-level result validation or hydration.
- Every surface change is measured in serialized metadata bytes and evaluated
  for selection, arguments, workflow completion, and safety.

ADR-016 records why a connection-profile model was rejected. The operating
contract has one registry and one generic report runner.

The 45-tool standard registry is now the operating reality. Promotion remains
open for observed context-budget and host-native-deferral evidence.

## Why now

The operating registry contains 45 tools, stays below Windsurf's
100-active-tool global cap, and leaves 55 tool slots for other connected
servers. Its rendered metadata, tool identities, and zero advertised output
schemas are frozen in the standard snapshot below.

ADR-016, the archived MCP catalog, the changelog, and frozen fixtures retain
the measured pre-cutover registry and rejected-schema experiment. They are the
historical record; this active spec governs the current registry.

Count is only a proxy. Similar or overlapping tools create more selection
ambiguity than an equal number of distinct tools, while a large discriminated
union can cost more schema bytes than several narrow tools. MoneyBin therefore
measures count, descriptions, input schemas, output schemas, annotations, and
total serialized metadata.

## Design invariants

### 1. One bounded standard registry

Every implemented core MCP tool is part of the standard surface. Generic
clients receive the full list. Capable hosts may defer schemas, but they index
the same tools and preserve original names, annotations, allowlists, approvals,
and audit identity.

MoneyBin does not depend on `notifications/tools/list_changed` for ordinary
operation. The registered list changes only when software or installed
extensions change, not as an in-session discovery mechanism.

### 2. Start from capability, not method

Every surface proposal begins with a stable capability ID from
`moneybin-capabilities.md`. The designer must first try to express it through:

1. an existing filter or projection;
2. an existing method with compatible input/output;
3. a batch input;
4. a declarative target-state mutation;
5. a complete workflow umbrella.

A new tool is the final option, not the default translation of a new CLI
command or service method.

### 3. A tool is an intent and safety boundary

Operations remain separate when any of these materially differ:

- primary user intent;
- input or output contract;
- read versus write behavior;
- sensitivity or consent behavior;
- authorization realm;
- `readOnlyHint`, `destructiveHint`, `idempotentHint`, or `openWorldHint`;
- confirmation behavior;
- audit and recovery contract.

Read and write operations do not share one tool. A staged
propose → review → confirm/commit workflow remains staged because the pause is a
trust boundary, not accidental granularity.

### 4. Compatible writes may share conservative risk

Compatible same-intent writes may share a tool even when only some validated
arguments are destructive, provided they have:

- one coherent input and output contract;
- one authorization and sensitivity realm;
- normalized audit and recovery semantics;
- deterministic branch classification after validation.

The tool advertises the maximum possible static risk. MoneyBin evaluates the
validated arguments and asks for confirmation only for the destructive branch.
This permits target-state operations such as active/inactive/absent without
creating a second delete tool merely to preserve a narrower annotation.

It does not permit mixed reads/writes, unrelated CRUD verbs, secret-handling
differences, open-world/local-world differences, or incompatible recovery.

### 5. Destructive confirmation binds the exact operation

A destructive preview produces a canonical representation of:

- validated arguments;
- resolved stable IDs;
- actor/profile and authorization context;
- operation kind;
- blast-radius summary.

The subsequent confirmation is bound to that representation, short-lived, and
single-use. Confirmation yields an immutable digest grant. Inside the same
write transaction that performs the mutation, MoneyBin recomputes the canonical
representation immediately before the first write and refuses expired,
replayed, or mismatched approval. No approval check may complete before opening
a separate mutation transaction.

Capable clients use elicitation over this contract. The elicitation requests an
explicit boolean and confirms only an accepted `true`, followed by a fresh
binding comparison. This avoids FastMCP's deprecated empty-schema elicitation,
which can render a non-functional form in supported clients. The boolean is
elicitation response data, not a bare `confirm` tool argument. Degraded clients
use an opaque confirmation token carrying the same binding. Confirmation
prevents the wrong mutation; `system_audit_undo` remains the recovery path when
intent later changes. Issuance and consumption evict abandoned expired tokens
from the active registry under the broker lock. A hard-capped tombstone
registry preserves `expired` classification for recent evictions without
allowing process-local confirmation state to grow indefinitely; tokens
displaced from that bounded history report `unknown or already used`.

### 6. Entity references fail loud

Every coarse operation follows one resolution contract:

1. explicit stable ID wins;
2. exact alias or name;
3. unambiguous normalized match;
4. structured `not_found` or `ambiguous` result with candidate IDs.

No write silently chooses the first fuzzy result. Resolution lives in shared
services, not per-tool wrappers.

### 7. Granular pipeline stages are operator controls

When an umbrella produces the same observable outcome with the same audit and
recovery contract, granular execution stays available in the CLI for testing
and debugging but does not consume an MCP slot.

`refresh_run` is the exemplar. Its canonical full cascade becomes:

1. connector pull where requested;
2. transaction matching;
3. transform;
4. categorization;
5. identity backfill.

`steps=["match"]` remains the surgical matching path.
`steps=["identity"]` runs the account- and merchant-link backfills. The
granular CLI commands remain operator controls without additional MCP
identities.

### 8. Reports are registered data products, not tools

All core, package, and standalone-extension reports register a `ReportSpec`.
MCP exposes one read-only `reports` tool:

```text
reports(
  report_id: string | null = null,
  parameters: object | null = null,
  limit: integer | null = null
)
```

- Without `report_id`, it returns the catalog.
- With `report_id`, it validates the supplied parameters against the selected
  `ReportSpec`, runs the report through the shared executor, and returns a
  generic tabular result.
- It never accepts raw SQL. `sql_query` remains the arbitrary read-only SQL
  path.
- CLI continues to generate one ergonomic command per report.

Report IDs are stable and namespaced for extensions. Short aliases are accepted
only when unambiguous.

The catalog/result output is a tagged union. Catalog entries include:

- report ID, description, parameter schema, and examples;
- output columns and privacy classes;
- unit, currency, and sign convention;
- position/flow and valuation basis;
- FX-conversion basis;
- `as_of`/period semantics, denominator, comparison window, and material
  exclusions;
- SQL/view provenance.

Execution results repeat the relevant metric semantics and include parameters,
columns, rows, period, provenance, sensitivity, count, and truncation.

`reports` is not a generic meta-execution gateway. Every member is read-only,
registered, parameter-validated, privacy-classified, provenance-bearing, and
executed through one bounded report contract.

### 9. CLI parity is capability parity

The union of MCP operations covers every non-exempt user capability in
`moneybin-capabilities.md`. It does not need one method per CLI command.

Parity tests bind both surfaces to capability IDs, service calls, and observable
outcomes. Granular CLI operator commands and MCP-protocol-specific features may
retain documented exemptions.

### 10. Structured output does not require universal output schemas

Every public tool returns canonical JSON in both a text content block and
`structuredContent`. Internal Pydantic payloads, privacy classification,
`ResponseEnvelope`, exact decimal normalization, error envelopes, and
transport-conformance tests remain mandatory.

The [MCP tool
specification](https://modelcontextprotocol.io/specification/2025-06-18/server/tools)
makes `outputSchema` optional, and [FastMCP supports structured results without
one](https://gofastmcp.com/v2/servers/tools). MoneyBin therefore does not
advertise output schemas in the current standard registry. A future schema is
an opt-in public contract, admitted only when:

1. a named client or integration consumes it;
2. `structuredContent` alone is demonstrably insufficient;
3. exact serialized-byte and context-budget costs are recorded;
4. representative compatibility tests pass; and
5. persisted evaluation evidence shows that the benefit warrants the cost.

When admitted, the schema must match runtime `structuredContent` exactly.
MoneyBin does not add a schema resource, profile, or configuration switch in
anticipation of a consumer.

### 11. Documentation is not an operational tool

Resources and prompts carry schema guidance, workflows, sign conventions, and
examples. They do not replace operational capabilities. Critical orientation
also appears in server instructions or tool responses because resource support
is uneven.

### 12. Shared prose is loaded once

Server instructions carry cross-cutting trust and sign-convention guidance.
Tool descriptions begin with a short, distinct statement of intent, then carry
only operation-specific selection, safety, mutation, and recovery information.

CI rejects duplicate description openings and enforces description budgets.

## Standard registry

The 45-tool registry below is the live public contract. Generic clients receive
all 45 tools; capable hosts may optionally defer schemas from that same
registry without reconnect, packs, or profiles. The registry selection has
passed its deterministic contract comparison, but promotion remains open until
the observed-evidence gates below close.

| Domain | Standard tools | Responsibilities |
|---|---|---|
| System | `system_status`, `system_audit`, `system_audit_undo` | Sectioned overview/health/statistics; unified audit list/detail/history; operation-level recovery |
| Reports | `reports` | Catalog and execute every registered report |
| Accounts | `accounts`, `accounts_set`, `accounts_balances`, `accounts_balance_assert` | Collection/detail/resolve; settings; latest/history; assertion set/remove |
| Investments | `investments`, `investments_record`, `investments_securities_set`, `investments_lots_select` | Read views; ledger events; security catalog; specific-lot overrides |
| Transactions | `transactions`, `transactions_create`, `transactions_annotate`, `transactions_categorize_assist`, `transactions_categorize_commit`, `transactions_categorize_run`, `transactions_categorize_rules`, `transactions_categorize_rules_set` | Query; manual entries; notes/tags/splits; assisted and deterministic categorization; rule lifecycle |
| Reviews | `reviews`, `reviews_decide`, `identity_links_decide` | Normalized queue/history, including auto-rule proposals; ordinary and auto-rule decisions; destructive identity merges |
| Taxonomy | `taxonomy`, `taxonomy_set` | Category and merchant reads; category target-state and merchant mappings |
| Import | `import_files`, `import_preview`, `import_confirm`, `import_status`, `import_revert`, `import_inbox_sync`, `import_labels_set` | Batch/staged ingest, status/formats/inbox, recovery, labels |
| Sync | `sync_link`, `sync_status`, `sync_pull`, `sync_disconnect`, `gsheet`, `gsheet_connect`, `gsheet_pull`, `gsheet_disconnect` | Mediated bank sync and user-controlled Google Sheets flows |
| Privacy | `privacy`, `privacy_consent_set` | Consent/audit status and declarative consent |
| Platform | `refresh_run`, `sql_query`, `sql_schema` | Derived-state refresh and the two core SQL capabilities |

### Review decision persistence

`reviews_decide` uses each domain's canonical proposal store. Categorization
proposal attempts live in `app.categorization_decisions`. Attempt 1 uses the
deterministic `cat_<sha256(transaction_id)[:16]>` decision ID; later attempts
append `_aN` and are unique by transaction and attempt number. Each attempt
advances once from `pending` to `accepted`, `rejected`, or `superseded`.
Terminal attempts are immutable history snapshots: accepted attempts retain
the canonical category and optional merchant target plus attribution;
rejected and superseded attempts retain no inferred targets.

The category repository's audit revision is the single re-review trigger.
When a granular edit, clear, automated categorization, or undo changes a
transaction after its latest attempt, the normalized pending read projects the
next deterministic attempt. Materialization supersedes a stale pending attempt
before inserting that next version; accepted and rejected rows are never
reopened. Undo marks the terminal attempt reversed without erasing its outcome,
so its audit history remains intact and the next attempt becomes reviewable.
Pending projection batches category state and audit revisions rather than
performing a per-transaction decision lookup.

Auto-generated categorization-rule proposals remain a distinct
`reviews(kind='auto_rules')` queue backed by `app.proposed_rules`; they are not
the uncategorized-transaction queue above. Pending rows preserve
the current `estimated_match_count` and `is_broad`; terminal history preserves
the proposal ID, terminal status, and promoted `rule_id`. Decision-time
blast-radius evidence was not persisted by the prior granular surface and this
consolidation does not invent historical evidence. `reviews_decide` accepts
pending proposals through the `kind='auto_rule'` decision variant and returns
the prior aggregate approval/rejection/skipped/backfill impact alongside
ordered per-proposal outcomes. Its `allow_broad` flag lives on that exact
proposal decision rather than at batch scope, so approving one known broad
proposal cannot waive the safety gate for its neighbors. Auto-rule decisions
and transaction/match decisions use separate atomic batches because they have
different canonical proposal stores.

Existing transaction categorizations backfill once as system-decided accepted
attempts with immutable snapshots and paired migration audit events. Legacy
rows missing `category_id` or `categorized_at` stop V036 with an actionable
error and leave the proposal table unchanged; the migration does not invent
canonical identities or timestamps. Re-running V036 over a non-empty proposal
table is idempotent. Match accept/reject state remains in the existing
match-decision store.

An ordinary batch materializes any deterministic categorization proposal and
applies every terminal decision inside one operation and one transaction.
Already-terminal IDs are structured constraint violations, including repeated
rejects; a later category-state revision receives a new attempt ID rather than
mutating the terminal row. The normalized review read projects the same
decision IDs and immutable terminal history without introducing a parallel
review-state table.

`identity_links_decide` requires confirmation for every material ordered batch
that contains an accept, even when that accept is already satisfied and another
item supplies the material change. Confirmation blast radii count distinct
logical IDs actually affected by changed accepts; provider-link acceptance does
not claim unrelated already-categorized transactions, and manual investment
events already represented in core are counted once.

### Cross-surface outcome map

The executable capability map is authoritative. Representative CLI outcomes
reach the standard registry through these coarse operations and selectors:

| CLI outcome | Standard MCP contract |
|---|---|
| Account list/detail/summary/resolution | `accounts(view=..., reference=..., query=...)` |
| Latest balances/history/assertions/reconciliation | `accounts_balances(view=..., reference=...)` |
| Named analytical output | `reports(report_id=..., parameters=...)` |
| Investment events/holdings/lots/gains/securities | `investments(view=..., ...)` |
| Notes, tags, splits, and tag rename | `transactions_annotate(requests=[...])` |
| Pending and historical review queues | `reviews(kind=..., status=...)` |
| Match stage | `refresh_run(steps=["match"])` |
| Identity-link stage | `refresh_run(steps=["identity"])` |
| Category or merchant taxonomy | `taxonomy(view=...)` |
| Import status, inbox, or format discovery | `import_status(sections=...)` |

Future MCP capabilities remain unnamed until admission through the bounded
registry. A CLI command or service method does not reserve a future tool name.

## Surface budgets and admission

| Budget | Target | Gate |
|---|---:|---:|
| Standard registered tools | 30–40 | carrying-weight review above 40; 50 hard maximum |
| Deprecated aliases advertised | 0 | hard gate |
| Serialized tool metadata | <2% of smallest supported model context | block/re-evaluate at 5% |
| Description opening | distinct first 60 characters | hard gate |
| First sentence / full description | ≤120 / ≤900 characters unless justified | review gate |

CI records count and serialized bytes for:

- descriptions;
- input schemas;
- output schemas, when advertised;
- annotations and other metadata;
- the complete standard registry.

A consolidation is accepted only when:

1. its serialized metadata is lower than the definitions it replaces; and
2. selection, argument construction, workflow completion, and safety evals are
   no worse.

If either gate fails, MoneyBin spends the additional tool slot deliberately.

### Standard-registry carrying-weight evidence

The deterministic current
[`standard-45.json`](../../tests/fixtures/mcp_surface/standard-45.json) snapshot
contains 45 tools, 47,111 bytes of serialized metadata, zero advertised output
schemas, and registry SHA-256
`0146b0bd2ff044b989181f628c4c6547f3674eed688fc00fb4ef9112a7d2025d`.
The deterministic estimate is 11,778 metadata tokens; a percentage of context is
recorded only with observed host/model evidence because this contract does not
invent a context-window size.

### Historical evidence location

[ADR-016](../decisions/016-bounded-mcp-tool-registry.md) owns the measured
pre-cutover registry, replacement cohorts, metadata comparison, and rejected
alternatives. The archived MCP catalog, changelog, and frozen fixtures preserve
the underlying names and numbers. Active governance does not duplicate them.

### Tool admission record

A PR proposing a tool must answer:

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

Reports never pass this test as tools; they pass `ReportSpec` validation and
enter the report registry.

### Output-schema admission record

A PR proposing an `outputSchema` must additionally name:

1. the concrete consuming client or integration;
2. the result-hydration or validation failure without the schema;
3. the exact per-tool and registry-wide byte delta;
4. the representative client compatibility tests; and
5. the persisted evaluation demonstrating a material benefit.

The current standard registry has zero advertised output schemas. Advertising
one without this record is a contract failure.

### Documentation closure

Current MCP governance derives names and counts from `STANDARD_TOOL_NAMES` and
the executable capability map. Active specs, architecture documents, and
contributor rules describe the operating registry and its coarse selectors. A
retired callback name is replaced with the standard operation and the selector
that reaches the same outcome; a generic migration claim is not enough.

Historical counts and retired names remain only where removing them would
damage the record: changelog entries, ADRs, archived specs, and frozen
regression fixtures. These locations must identify the material as historical.
No other public document may present or repeat the retired surface.

Documentation changed with this contract uses the ledger-grade voice from
`.claude/rules/documentation.md`: exact numbers, present-tense claims, named
limitations, sentence-case headings, and no marketing or compliance cadence.
A documentation contract test scans current MCP governance and fails on
retired counts or tool names. Historical evidence remains path-separated in
the four record classes above.

## Evaluation contract

### Baselines

The frozen pre-cutover surface fixture lives at
[`tests/fixtures/mcp_surface/baseline-2026-07-17.json`](../../tests/fixtures/mcp_surface/baseline-2026-07-17.json).
ADR-016 owns its measured interpretation. The current registry fixture is
[`standard-45.json`](../../tests/fixtures/mcp_surface/standard-45.json).

The 50-tool maximum and 40-tool carrying-weight review threshold are durable
policy. Both hard-limit and description-budget enforcement apply to the
operating 45-tool registry.

The persisted comparison records `contract_passed: true` and
`promotion_ready: false`: context budget: not_observed; host-native deferral:
not_observed. Deterministic fixtures cannot establish either fact. These are
the remaining promotion blockers, not reasons to misstate the operating
45-tool registry.

Evaluate:

1. the exact 45-tool standard registry;
2. individual consolidation alternatives where schema size or accuracy is
   uncertain;
3. optional host-native deferred loading from that same registry.

### Corpus

Derive cases from `moneybin-capabilities.md` and representative workflows:

- first-contact orientation and financial pulse;
- account and transaction lookup;
- report discovery followed by valid execution;
- import preview → confirm → status;
- categorization, rule creation, and review;
- matching and identity resolution;
- investment recording, holdings, lots, and gains;
- privacy consent and degraded results;
- audit inspection and undo;
- SQL/schema escape-hatch use;
- invalid, ambiguous, destructive, and open-world requests.

### Metrics

Record:

- correct tool selection;
- valid argument construction;
- complete multi-step workflow;
- unnecessary calls and round trips;
- destructive/open-world false positives;
- confirmation binding and policy compliance;
- ambiguous-reference refusal;
- recovery from structured errors;
- privacy/consent behavior;
- input-schema bytes and any admitted output-schema bytes;
- latency and client compatibility failures.

The checked-in result artifact records:

- model and host;
- run date;
- standard-registry hash;
- serialized metadata bytes;
- cases and outcomes;
- comparison to the frozen baseline.

An eval harness without a persisted run result does not satisfy the gate.

### Actual rendered schema compatibility

Tests inspect the real `tools/list` response, not only Python/Pydantic types.
For every discriminated or coarse contract they prove:

- all input variants and variant-specific required fields render correctly;
- JSON text and `structuredContent` are identical;
- decimal values remain JSON numbers;
- success, error, privacy-degraded, dynamic SQL/report, and recovery envelopes
  retain their natural structured shapes;
- any selectively admitted output schema matches runtime structured content;
- native and commonly stringified client inputs behave safely;
- invalid values do not coerce to zero, false, or empty collections.

Compatibility fixes are schema-aware and conservative. MoneyBin does not add a
global coercion layer without evidence from supported clients.

## Remaining promotion work

- Capture observed model-context cost for the current standard registry.
- Capture optional host-native deferral using the same names, annotations,
  allowlists, approvals, and audit identities.
- Persist those results without changing what generic clients receive.

## Alternatives

### Retain the pre-cutover registry

Rejected. ADR-016 records the measured ceiling and growth-path rationale.

### Split core capabilities into standard and extended profiles

Rejected after design review. Profiles add setup and workflow-closure
complexity while changing tool availability and identity across clients.
MoneyBin maintains one bounded standard registry instead.

### Restore server-driven progressive disclosure

Rejected. Clients that ignore `tools/list_changed` can leave revealed tools
unreachable. Host-native deferral is allowed because the host owns schema
injection and preserves original identities.

### Consolidate CRUD into `manage_X(action=...)`

Rejected as a general pattern. It relocates complexity into large unions, mixes
read/write and safety contracts, can worsen approval behavior, and may not
reduce bytes. MoneyBin admits a coarse operation only when its rendered schema
stays valid across representative clients and the operation retains one
coherent safety contract.

### Replace domain tools with SQL and generic mutation

Rejected. It moves financial semantics, validation, privacy, audit,
confirmation, and recovery into model reasoning.

### Add a server-side search/execute gateway

Rejected for financial operations. A generic `call_tool` hides original
identity from client allowlists and approvals. The read-only `reports` registry
is a bounded domain runner, not an execution proxy.

## Promotion gates

This spec remains `in-progress` and must not move to `implemented` until:

- observed model-context evidence is persisted for the 45-tool registry;
- optional host-native deferral is observed against that same registry; and
- ADR-016 is accepted after those evidence gates close.

The deterministic contract records `contract_passed: true`. The remaining
promotion state is context budget: not_observed; host-native deferral:
not_observed; `promotion_ready: false`.

## Non-goals

- Marketing MoneyBin by tool count.
- Weakening confirmation, consent, audit, or undo to save schema.
- Depending on one model vendor's tool-search implementation.
- Changing service-layer capabilities solely to reach a number.
- Reintroducing unimplemented stubs.
- Hiding ordinary core workflows behind configuration.

## References

- [MCP client best practices: progressive tool discovery](https://modelcontextprotocol.io/docs/develop/clients/client-best-practices)
- [MCP tool schema and structured output](https://modelcontextprotocol.io/specification/2025-11-25/server/tools)
- [OpenAI Agents SDK: deferred loading and tool namespaces](https://openai.github.io/openai-agents-python/tools/)
