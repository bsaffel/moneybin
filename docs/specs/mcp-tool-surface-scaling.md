# MCP Tool Surface Scaling

> Companions: [`mcp-architecture.md`](mcp-architecture.md) (tool and envelope
> conventions), [`moneybin-mcp.md`](moneybin-mcp.md) (concrete tool catalog),
> [`moneybin-capabilities.md`](moneybin-capabilities.md) (capability parity),
> [`extension-contracts.md`](extension-contracts.md) (report and package
> registration), and
> [ADR-016](../decisions/016-bounded-mcp-tool-registry.md)

## Status

- **Type:** Architecture
- **Status:** in-progress — contract foundation is underway; fresh observed
  baseline and candidate evidence remains an external Plan 6 gate
- **Address:** M3K.2 — second work item under M3K (CLI / MCP UX standards)
- **Origin:** July 2026 deep MCP surface review: 105 tools are visible at every
  connection, exceed Windsurf's 100-tool global ceiling, and serialize to about
  90,600 characters before host-specific wrapping.

## Goal

Expose one capability-complete, safety-aware MCP surface that remains small
enough for generic clients to load in full and precise enough for agents to use
reliably.

The target is **the most capability with the least permanent schema**, not the
smallest possible tool count and not a marketing-friendly headline.

## Decision summary

MoneyBin will replace the current 105-tool surface with one bounded standard
registry of approximately 45 tools.

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
  `structuredContent`. The initial standard registry advertises no
  `outputSchema`; a schema is admitted only for a named consumer that needs
  protocol-level result validation or hydration.
- Every surface change is measured in serialized metadata bytes and evaluated
  for selection, arguments, workflow completion, and safety.

This design supersedes the proposed connection-profile model that appeared in
the first draft of this spec. The four-tool difference between a proposed
44-tool universal surface and 48-tool complete registry did not justify a
second user-visible concept. The generic report runner then reduced the target
to approximately 45 tools.

Until this spec is implemented, the current full 105-tool registry remains the
operating reality. Proposed text must not be described as shipped behavior.

## Why now

### Current measured surface

The July 2026 live registry contains:

| Measure | Current value |
|---|---:|
| Visible tools | 105 |
| Serialized tool metadata | 90,734 bytes |
| Rough token estimate | ~20,000–23,000 |
| Description characters | ~34,013 |
| Input-schema characters | ~35,270 |
| Parameters | 216 |
| Tools with advertised `outputSchema` | 0 |

The four largest groups — transactions, accounts, investments, and import —
contain 59 tools and about 62% of the serialized metadata.

The contract-foundation experiment advertised full payload schemas for all 105
tools. It increased serialized metadata to 861,301 bytes: 768,887 bytes of
output schemas alone. No current MoneyBin client depends on schema-based result
hydration or validation. The approximately 10× total increase therefore failed
the carrying-weight test and was rejected before the standard-registry
cutover.

Three entries are deprecated aliases rather than capabilities:

- `transactions_review`
- `sync_connect`
- `sync_connect_status`

Removing them after the existing pre-launch compatibility window reduces the
registry to 102 but does not solve the disclosure problem.

### The failure is no longer theoretical

Windsurf's Cascade accepts at most 100 active tools across all connected MCP
servers. MoneyBin alone advertises 105. The current design cannot work
completely in a documented client and leaves no budget for a second server in
clients with a similar ceiling.

Count is only a proxy. Similar or overlapping tools create more selection
ambiguity than an equal number of distinct tools, while a large discriminated
union can cost more schema bytes than several narrow tools. MoneyBin therefore
measures count, descriptions, input schemas, output schemas, annotations, and
total serialized metadata.

### Competitor evidence

FinLynq v4 consolidated 117 advertised HTTP tools to 75 and exposes 51 by
default. It folded CRUD families into thirteen `manage_*` discriminated unions
and hides 25 import/reconciliation tools behind a scope or persistent setting.
Its implementation validates several MoneyBin choices:

- tool-selection evals and deterministic `tools/list` snapshots are useful;
- payload-bound destructive confirmation is stronger than a boolean flag;
- shared server instructions and distinct description openings improve agent
  orientation;
- hidden, time-bounded aliases can support a breaking migration without
  consuming advertised slots.

It also demonstrates what MoneyBin should avoid:

- broad unions required custom `oneOf` rendering and client-specific coercion
  for stringified numbers, booleans, arrays, and objects;
- tools containing both list and delete modes carry conservative destructive
  annotations even for read calls;
- the default is not capability-complete;
- HTTP and stdio surfaces differ materially;
- reducing names does not prove serialized metadata decreased;
- its checked-in eval harness scores only first-tool choice and contains no
  persisted public run result.

Treeline's thirteen-tool surface reaches a lower count through raw SQL and
generic mutation. That transfers domain validation, privacy, audit,
confirmation, and recovery into model reasoning. MoneyBin keeps read-only SQL
as a core escape hatch but rejects generic write SQL.

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
intent later changes.

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
dedicated `transactions_matches_run`, `accounts_links_run`, and
`merchants_links_run` MCP tools retire while their CLI commands may remain.

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
advertise output schemas in the initial standard registry. A future schema is
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

## Proposed standard registry

The design target is 45 tools. Names are proposed public contracts and remain
subject to schema/evaluation gates before implementation.

| Domain | Standard tools | Responsibilities |
|---|---|---|
| System | `system_status`, `system_audit`, `system_audit_undo` | Sectioned overview/health/statistics; unified audit list/detail/history; operation-level recovery |
| Reports | `reports` | Catalog and execute every registered report |
| Accounts | `accounts`, `accounts_set`, `accounts_balances`, `accounts_balance_assert` | Collection/detail/resolve; settings; latest/history; assertion set/remove |
| Investments | `investments`, `investments_record`, `investments_securities_set`, `investments_lots_select` | Read views; ledger events; security catalog; specific-lot overrides |
| Transactions | `transactions`, `transactions_create`, `transactions_annotate`, `transactions_categorize_assist`, `transactions_categorize_commit`, `transactions_categorize_run`, `transactions_categorize_rules`, `transactions_categorize_rules_set` | Query; manual entries; notes/tags/splits; assisted and deterministic categorization; rule lifecycle |
| Reviews | `reviews`, `reviews_decide`, `identity_links_decide` | Normalized queue/history; ordinary decisions; destructive identity merges |
| Taxonomy | `taxonomy`, `taxonomy_set` | Category and merchant reads; category target-state and merchant mappings |
| Import | `import_files`, `import_preview`, `import_confirm`, `import_status`, `import_revert`, `import_inbox_sync`, `import_labels_set` | Batch/staged ingest, status/formats/inbox, recovery, labels |
| Sync | `sync_link`, `sync_status`, `sync_pull`, `sync_disconnect`, `gsheet`, `gsheet_connect`, `gsheet_pull`, `gsheet_disconnect` | Mediated bank sync and user-controlled Google Sheets flows |
| Privacy | `privacy`, `privacy_consent_set` | Consent/audit status and declarative consent |
| Platform | `refresh_run`, `sql_query`, `sql_schema` | Derived-state refresh and the two core SQL capabilities |

### Major consolidation map

| Existing/candidate tools | Proposed contract |
|---|---|
| `system_status`, `system_doctor`, categorization statistics | `system_status(sections=..., detail=...)` |
| `system_audit`, `system_audit_history`, `system_audit_get` | `system_audit(...)` |
| Every `reports_*` tool and future report extension | `reports(report_id=..., parameters=...)` |
| Account list/get/summary/resolve reads | `accounts(view=..., ...)` |
| Balance latest/history/drift reads | `accounts_balances(view=..., ...)` or a registered report where analytical |
| Net-worth snapshot/history and all other report outputs | Registered report entries |
| Investment events/holdings/lots/gains/securities reads | `investments(view=..., ...)` |
| Notes/tags/splits/tag rename | `transactions_annotate(...)` |
| Largest/anomalous transaction analysis | Registered reports, not transaction-domain tools |
| Categorization pending and all match/link pending/history tools | `reviews(kind=..., status=..., ...)` |
| Account/merchant/security merge decisions | `identity_links_decide(...)` |
| `transactions_matches_run` | `refresh_run(steps=["match"])` |
| `accounts_links_run`, `merchants_links_run` | `refresh_run(steps=["identity"])` |
| Category and merchant reads | `taxonomy(view=..., ...)` |
| Category create/set/delete and merchant create | `taxonomy_set(...)` |
| Import status, inbox pending, and format discovery | `import_status(sections=...)` |
| GSheet list/status | `gsheet(...)` |
| GSheet auth/connect/reconnect | `gsheet_connect(...)` |
| Sync link status | `sync_status(session_id=...)` |
| Privacy status/log | `privacy(...)` |
| Consent grant/revoke | `privacy_consent_set(...)` |

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

The initial standard registry has zero admitted output schemas. Advertising one
without this record is a contract failure.

## Evaluation contract

### Baselines

The contract foundation is active work. The frozen pre-cutover registry lives
at [`tests/fixtures/mcp_surface/baseline-2026-07-17.json`](../../tests/fixtures/mcp_surface/baseline-2026-07-17.json);
the matching evaluation capture lives at
[`tests/fixtures/mcp_eval/captures/baseline-105.json`](../../tests/fixtures/mcp_eval/captures/baseline-105.json).
The frozen snapshot preserves the actual 105-tool registry with no advertised
output schemas. It remains the byte baseline for the standard-registry
consolidation. The rejected full-schema experiment is recorded separately as
861,301 bytes and is not used to make the candidate comparison easier.

The 50-tool maximum and 40-tool carrying-weight review threshold are durable
policy. During the intentional 105-tool pre-cutover state, the contract runs
with both hard-limit and description-budget enforcement disabled while still
measuring the live inventory and legacy description debt. Plan 6 enables both
gates atomically when it cuts the standard registry to 45 tools.

`baseline-105.json` is a deterministic `contract_fixture`, not observed model
or host evidence. It proves the evaluation format and scoring path; fresh
observed baseline and candidate evidence remain required for promotion.

Evaluate:

1. the frozen current 105-tool surface;
2. the proposed approximately 45-tool registry;
3. individual consolidation alternatives where schema size or accuracy is
   uncertain;
4. host-native deferred loading where supported.

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

## Migration

### Phase 0 — freeze evidence

- Generate the current registry snapshot and byte inventory.
- Bind CLI/MCP parity to capability IDs and service outcomes.
- Commit the evaluation corpus and baseline results.
- Remove the three deprecated aliases from the advertised surface.

### Phase 1 — shared contracts

- Preserve canonical structured results without advertising output schemas.
- Add the consumer-driven output-schema admission gate.
- Add explicit capability, audience, safety, and workflow metadata.
- Implement shared entity resolution and payload-bound confirmation.
- Add count/byte, description, workflow-closure, and rendered-schema gates.

### Phase 2 — generic reports

- Change report MCP registration from one tool per report to one catalog.
- Extend `ReportSpec` with metric semantics and stable namespaced IDs.
- Migrate service-backed report exceptions into the registry.
- Preserve generated per-report CLI commands.

### Phase 3 — domain consolidation

- Land the approved domain contracts incrementally.
- Keep old names as hidden, callable aliases only when compatibility requires
  it, for one explicitly bounded pre-launch release.
- Re-run evaluation and byte gates after every cohort.

### Phase 4 — default cut

- Switch the advertised surface atomically after all 45 target tools are
  implemented and workflow-closed.
- Update instructions, resources, capability map, public catalog, and client
  compatibility docs in the same release.
- Remove expired aliases.

### Phase 5 — host-native deferral

- Enable host-native tool search/deferred loading where original tool identity,
  annotations, allowlists, and approvals are preserved.
- Keep the same complete registry for generic clients.
- Treat host support as compatibility data, not server-side branching.

## Alternatives

### Keep all 105 tools visible

Rejected. It already exceeds a documented client ceiling and has no credible
growth path.

### Split core capabilities into standard and extended profiles

Rejected after design review. A proposed 44-tool universal versus 48-tool
complete split added setup and workflow-closure complexity to hide only four
expert operations. The generic report runner reduced the complete target
further. MoneyBin will maintain one bounded standard registry instead.

### Restore server-driven progressive disclosure

Rejected. Clients that ignore `tools/list_changed` can leave revealed tools
unreachable. Host-native deferral is allowed because the host owns schema
injection and preserves original identities.

### Consolidate CRUD into `manage_X(action=...)`

Rejected as a general pattern. It relocates complexity into large unions, mixes
read/write and safety contracts, can worsen approval behavior, and may not
reduce bytes. FinLynq v4's custom schema rendering and coercion work are direct
evidence of the interoperability cost.

### Replace domain tools with SQL and generic mutation

Rejected. It moves financial semantics, validation, privacy, audit,
confirmation, and recovery into model reasoning.

### Add a server-side search/execute gateway

Rejected for financial operations. A generic `call_tool` hides original
identity from client allowlists and approvals. The read-only `reports` registry
is a bounded domain runner, not an execution proxy.

## Promotion gates

This spec remains `in-progress` and must not move to `implemented` until:

- the proposed names and contracts reconcile against live code;
- canonical FastMCP text/structured transport is proven;
- the standard registry advertises zero output schemas, or every exception has
  an approved consumer-driven admission record;
- the report catalog contract and metric metadata are concrete;
- payload-bound confirmation and entity-resolution contracts are concrete;
- the capability-parity test is enforceable;
- baseline and proposed surfaces have persisted eval results;
- the proposed registry satisfies count and metadata budgets;
- rendered schemas pass representative client compatibility tests;
- the atomic documentation/rule migration is complete;
- ADR-016 is accepted.

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
- [FinLynq v4 tool counts](https://github.com/finlynq/finlynq/blob/main/src/lib/mcp/tool-counts.ts)
- [FinLynq toolsets](https://github.com/finlynq/finlynq/blob/main/src/lib/mcp/toolsets.ts)
- [FinLynq consolidated schema tests](https://github.com/finlynq/finlynq/blob/main/tests/mcp/consolidated-schema-contract.test.ts)
- [FinLynq evaluation harness](https://github.com/finlynq/finlynq/blob/main/tests/mcp/eval/run-eval.ts)
- [Treeline MCP implementation](https://github.com/treeline-money/treeline/blob/main/cli/src/commands/mcp.rs)
