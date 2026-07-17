# ADR-016: MCP uses one bounded standard tool registry

**Status:** Proposed (governing spec
[`mcp-tool-surface-scaling.md`](../specs/mcp-tool-surface-scaling.md) is
`in-progress`; accept after implementation and evidence gates pass)

## Context

MoneyBin's May 2026 MCP architecture made every registered tool visible at
connection. It deliberately retired server-driven progressive disclosure
because major clients handled `notifications/tools/list_changed`
inconsistently. A tool revealed after the initial `tools/list` could remain
invisible for the session.

That choice was correct for the surface at the time. The operating conditions
have changed:

1. The registry contains 105 visible tools and serializes to roughly 90,600
   characters before host wrapping.
2. MoneyBin alone exceeds Windsurf Cascade's 100-tool global ceiling.
3. Planned domains and extensions would continue growing the surface.
4. Modern hosts increasingly support tool search or deferred schema loading
   without requiring server-side list mutation.
5. A carrying-weight review found that MoneyBin can preserve capability parity
   through approximately 45 intent-shaped operations.
6. A proposed 44-tool universal / 48-tool complete split hid too little to
   justify profiles, packs, reconnect behavior, and workflow-closure rules.
7. A generic read-only report registry prevents every new report from adding a
   tool.

This is pattern-establishing: every future core feature and extension will
inherit the admission rule, and the reason for separating registry availability
from host prompt loading is not recoverable from code.

## Decision

**MoneyBin will expose one bounded, capability-complete standard MCP registry.**

1. Every implemented core MCP tool is part of the standard surface. Generic
   clients receive the full registry.
2. The standard target is 30–40 tools. Crossing 40 requires a carrying-weight
   review; 50 is a hard maximum unless this ADR is superseded.
3. Hosts with native tool search or deferred loading may inject only relevant
   schemas, but they index the same registry and preserve original tool
   identity, annotations, approvals, allowlists, and audit behavior.
4. MoneyBin will not use core capability packs, hidden expert tiers, or
   reconnect-only modes merely to fit the budget.
5. `notifications/tools/list_changed` is not an ordinary discovery mechanism.
6. The union of MCP operations preserves capability parity with the CLI.
   Parity means capability and observable service outcomes, not name equality.
7. New capabilities first attempt an existing projection, method, batch,
   declarative target state, report entry, or workflow umbrella. A new tool
   requires documented carrying-weight evidence.
8. Separate tool identities remain where intent, safety, authorization,
   sensitivity, confirmation, output, audit, or recovery materially differ.
9. Compatible writes may share a conservatively annotated tool. The server
   classifies validated arguments and confirms only destructive branches.
10. Destructive confirmation binds canonical validated arguments, resolved IDs,
    actor/context, operation, and blast radius. It is short-lived and
    single-use.
11. All reports register behind one read-only `reports` catalog/runner. Reports
    retain per-entry validation, privacy, metric semantics, and provenance; raw
    SQL remains a separate tool.
12. Every public tool returns canonical JSON text and equivalent
    `structuredContent`. The initial standard registry advertises no
    `outputSchema`. A future schema requires a named consumer, evidence that
    structured content alone is insufficient, exact byte costs, compatibility
    tests, and persisted benefit evidence.
13. Count and serialized metadata are both gated. Consolidation must reduce
    bytes and pass selection, argument, workflow, safety, and compatibility
    evaluations.
14. The standard surface is transport-coherent. Transport-specific security or
    operator exemptions remain explicit in the capability map.

Exact target names and migration cohorts belong to the governing spec rather
than this ADR.

If accepted, this supersedes only the unbounded "full registered surface
visible at connect" portion of `mcp-architecture.md`. It does not supersede
ADR-003, the path-prefix taxonomy, response envelope, privacy middleware,
service-layer contract, or CLI-first operator exemptions.

## Consequences

- Generic clients receive every ordinary MoneyBin capability without setup.
- Capable hosts reduce prompt cost without creating a different product
  surface.
- Tool additions become budgeted public-contract decisions rather than
  reflexive wrappers around CLI commands.
- Reports and report extensions can grow without consuming tool slots.
- Consolidated contracts require stronger schema rendering and representative
  client compatibility tests.
- Payload-bound confirmation and shared entity resolution become cross-domain
  infrastructure.
- Tests must snapshot the actual `tools/list`, measure every advertised schema
  byte, prove canonical structured transport, capability parity, and workflow
  closure, and persist evaluation results.
- The registry has less numerical headroom than a profile-based design.
  Approaching 50 forces consolidation or an explicit architecture revision.
- Extension-owned operational tools remain subject to the same admission and
  total-budget rules; installation does not waive AX cost.

## Alternatives considered

### Keep the 105-tool surface

Rejected. It already fails a documented client ceiling and cannot absorb
planned growth.

### Stable standard/extended profiles

Rejected after design review. The proposed split exposed 44 universal tools and
hid only four expert operations. That did not justify configuration,
reconnection, prompt filtering, and cross-profile workflow closure.

### Server-driven dynamic progressive disclosure

Rejected. Clients that ignore `tools/list_changed` may never see revealed
tools. Host-native deferred loading is allowed because the host owns injection
and can verify it.

### Broad `manage_X(action=...)` consolidation

Rejected as a general strategy. It can mix read/write and safety contracts,
move rather than remove schema, degrade approval semantics, and require
client-specific union/coercion workarounds. Compatible target-state and method
contracts remain allowed.

### Generic SQL/query/mutate surface

Rejected. It transfers finance semantics, validation, privacy, audit,
confirmation, and recovery into model reasoning.

### Server-side search/execute meta-tool

Rejected for operations. It hides original tool identities from client
allowlists and approvals. The read-only `reports` registry is a bounded domain
runner, not an arbitrary tool proxy.

### Universal detailed output schemas

Rejected without a consumer. Full schemas for the existing 105 tools increased
serialized metadata from 90,734 to 861,301 bytes; output schemas alone accounted
for 768,887 bytes. MCP defines `outputSchema` as optional, and MoneyBin retains
canonical `structuredContent`, internal typed payloads, and transport tests
without advertising it. Selective future adoption remains available through
the explicit admission record.

## Acceptance trigger

Promote this ADR to **Accepted** when:

- the governing spec reaches `implemented`;
- the approximately 45-tool contract reconciles with live code;
- canonical structured transport and generic reports are proven;
- the initial standard registry advertises zero output schemas, or every
  exception has an approved consumer-driven admission record;
- payload-bound confirmation and entity resolution are concrete;
- capability parity and workflow closure are enforceable;
- persisted evaluations meet the approved gates;
- the registry meets count and byte budgets;
- representative clients render and call the coarse schemas safely;
- documentation and rule changes can land atomically with implementation.
