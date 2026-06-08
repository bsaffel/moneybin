# ADR-014: One shared UI core behind two surface shells

**Status:** Proposed (decision approved in design; flips to Accepted when [`ui-architecture.md`](../specs/ui-architecture.md) is promoted from `draft` → `ready`)

## Context

MoneyBin will ship two visual surfaces:

- an **MCP App** — interactive UI rendered inside an MCP host (Claude Desktop, ChatGPT, VS Code, …), now that MCP Apps is a ratified standard (`2026-01-26`, SEP-1865) with broad client support; and
- a **Web UI** — a browser app served locally by `moneybin ui` and by the hosted tier.

These could be built as two independent frontends. They share the same data, the same domain concepts, and (largely) the same components — a transaction table, a net-worth chart, a category picker look the same whether rendered in a chat iframe or a browser tab. Two independent frontends would duplicate that work and then drift, producing the "two patterns for the same job" rot that [`design-principles.md`](../../.claude/rules/design-principles.md) names as the single largest source of codebase decay.

The two surfaces genuinely differ in exactly one dimension: **how data crosses the wire.** The MCP App talks to its host over a postMessage / JSON-RPC bridge (`tools/call`); the Web UI talks to a FastAPI surface over HTTP. Everything above that boundary — components, view state, formatting, domain types — is identical.

This is a pattern-establishing decision: every future visual surface (an in-app agent panel, a mobile view, an extension-contributed dashboard) inherits whatever shape we choose here. It meets the ADR bar in [`design-principles.md`](../../.claude/rules/design-principles.md): it establishes a pattern others inherit; the "why" (dual-surface reuse via a transport-agnostic core) is not recoverable from reading the code; and a future contributor could reasonably propose undoing it ("why not just fetch in the components?").

## Decision

**Build one shared component-and-data core, with the transport injected, behind two thin surface shells.** Make the transport boundary *structural*, not conventional.

Concretely:

1. **Five pnpm-workspace packages:** `ui-core` (tokens, primitives, components, hooks, generated types, and the `MoneyBinClient` interface); `transport-mcp` and `transport-http` (each implements `MoneyBinClient`); `apps/mcp-app` and `apps/web` (the shells). `ui-core` depends on **neither** transport — a component physically cannot import transport-specific code. The wall is enforced by package boundaries first, `dependency-cruiser` second, ESLint third.

2. **React**, because the MCP Apps client ecosystem (`@modelcontextprotocol/ext-apps`, `@mcp-ui/client`) is React-first and the library catalog for financial tables/charts/forms is deepest there. The standard itself is framework-agnostic (postMessage + JSON-RPC), so this is a velocity/ecosystem choice, not a protocol constraint.

3. **shadcn/ui + Tailwind + Tremor** for components — copy-paste source owned in-repo (no version lock-in, directly agent-editable), Radix accessibility underneath, Tremor for analytics widgets.

4. **Typed `MoneyBinClient` interface + TanStack Query hooks** as the data layer. Components call `useReportSpending()`; the hook calls the injected client; the shell decides which transport implements it. The same component renders on both surfaces unchanged.

5. **Pydantic JSON Schema → committed TypeScript**, CI-gated against drift, so Python stays the single source of truth for wire types.

6. **Build once, embed in the Python package.** The React bundle is built at release time and shipped inside the wheel; users install via `brew`/`pip` and never see Node. The hosted server consumes the same bundle.

Full design, diagrams, and testing strategy: [`ui-architecture.md`](../specs/ui-architecture.md).

## Alternatives considered

- **Two independent frontends (one per surface).** Rejected: maximum duplication and guaranteed drift; contradicts the coherence principle.
- **Single isomorphic app, both transports in one bundle, conditional chrome.** Rejected: conflates two genuinely different runtime contexts (sandboxed iframe vs. full browser) and couples the shells; the conditional sprawl is its own rot.
- **Convention-only boundary (one package, ESLint rules).** Rejected: the failure mode we most need to prevent — transport code leaking into shared components — would be possible-but-discouraged rather than impossible. Package boundaries make it impossible by construction.
- **Non-React frameworks (Svelte, Vue, Solid).** Considered. Svelte/Vue offer smaller bundles and pleasant DX; they suit a single-surface desktop or browser app well. But our differentiator is the dual-surface requirement — the same components must render inside an MCP host *and* a browser — and the MCP Apps client SDKs plus the financial-UI library catalog (tables, charts, forms) are React-first. That alignment outweighs the bundle-size and DX edges here.

## Consequences

### What this enables

- A dashboard component written once renders in the MCP App and the Web UI — and later in any additional MCP host (e.g. an in-app agent panel) — with no rewrite. "Where else can this render?" becomes a configuration question, not a porting project.
- The transport contract test suite mechanically guarantees functional parity across surfaces, instead of relying on review.
- The MCP App's sandbox + CSP + self-contained bundle yield **zero external network calls**, making MoneyBin's "nothing leaves your machine" posture literally true at the UI layer.

### Costs accepted

- **Frontend toolchain in the repo.** A `ui/` workspace, Node 20+/pnpm for frontend contributors, and Node in the release/CI build. Python-only contributors are unaffected; users never see it.
- **A new thin FastAPI surface** over the existing service layer for the Web UI transport. No business logic — routes delegate to existing services.
- **Server-side MCP Apps support** must be verified: declaring `_meta.ui.resourceUri` on tools and serving `ui://` resources from the Python (FastMCP) server. Flagged as an open risk in the spec; the Web UI path does not depend on it.

### Boundaries deliberately left open

- **Extension-package UI slots** are stubbed (`<RegistrySlot>` + slot types) but not loaded — consistent with [`extension-contracts.md`](../specs/extension-contracts.md)'s "no arbitrary UI plugins in 1.0."
- **In-app AI** is a non-goal but explicitly not foreclosed: a future agent panel reuses `ui-core` through `MoneyBinClient`, and any agent loop MoneyBin runs will be BYOK / local-model only — never bundled keys — preserving the deterministic-compute / LLM-prose and no-PII-egress invariants.

## References

- [`ui-architecture.md`](../specs/ui-architecture.md) — the full architecture spec this ADR records the rationale for
- [MCP Apps overview](https://modelcontextprotocol.io/extensions/apps/overview) · [spec announcement (2026-01-26)](https://blog.modelcontextprotocol.io/posts/2026-01-26-mcp-apps/)
- [`design-principles.md`](../../.claude/rules/design-principles.md) — coherence ("one way to do each thing") and the ADR bar
- [ADR-011](011-docs-site-framework.md) — prior frontend-tooling decision (docs site); same "Python-native, minimize second-toolchain friction" instinct applied where it fits
