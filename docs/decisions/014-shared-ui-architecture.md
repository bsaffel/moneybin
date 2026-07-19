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

2. **React**, because the MCP Apps client ecosystem (`@modelcontextprotocol/ext-apps`, `@mcp-ui/client`) is React-first and the ecosystem for financial UI — tables, forms, and data-viz primitives (d3 / SVG-in-React) — is deepest there. The standard itself is framework-agnostic (postMessage + JSON-RPC), so this is a velocity/ecosystem choice, not a protocol constraint.

3. **shadcn/ui + Tailwind** for components — copy-paste source owned in-repo (no version lock-in, directly agent-editable), Radix accessibility underneath. **Charts are house-built SVG primitives on `d3-scale`/`d3-shape`, not a charting component library** — see the [Charting decision](#charting-decision) below. *(This supersedes the original "+ Tremor for analytics widgets".)*

4. **Typed `MoneyBinClient` interface + TanStack Query hooks** as the data layer. Components call `useReportSpending()`; the hook calls the injected client; the shell decides which transport implements it. The same component renders on both surfaces unchanged.

5. **Pydantic JSON Schema → committed TypeScript**, CI-gated against drift, so Python stays the single source of truth for wire types.

6. **Build once, embed in the Python package.** The React bundle is built at release time and shipped inside the wheel; users install via `brew`/`pip` and never see Node. The hosted server consumes the same bundle.

### Charting decision

Charts are **house-built SVG primitives on `d3-scale`/`d3-shape`**, wrapped in `ui-core` like `Amount`/`Icon` — not an off-the-shelf charting library (Tremor/Recharts) nor a Canvas engine (ECharts/uPlot). The design system's chart grammar (`design-system/charts.md`) is prescriptive down to the interaction (pin/release tooltips; a selection model that *forbids* the stroke/ring highlight libraries add by default), the form set is **closed** (ten forms by data shape), and the data is **small-n** — so a component library would be overridden almost entirely for negative value, and the grammar's per-mark CSS-token / DOM-tooltip / `aria` model requires SVG (ruling out Canvas). This is the Datadog / Treeline pattern (own the SVG components); rich interaction is hand-built in every serious tool regardless of library, and reference implementations to port already exist (`buildNw`, heatmap quantization, sankey collision, sparkline amplitude, the pin/release state machine). A competitor-form survey confirmed the ten forms cover today's domains, with three additions **reserved** for when their consuming domain UI is built — treemap, budget/goal progress, stacked-area — tracked as follow-up work. Full rationale, the PFM + BI/observability survey, and the cost model: [`ui-architecture.md` → Charting](../specs/ui-architecture.md#charting).

Full design, diagrams, and testing strategy: [`ui-architecture.md`](../specs/ui-architecture.md).

## Alternatives considered

- **Two independent frontends (one per surface).** Rejected: maximum duplication and guaranteed drift; contradicts the coherence principle.
- **Single isomorphic app, both transports in one bundle, conditional chrome.** Rejected: conflates two genuinely different runtime contexts (sandboxed iframe vs. full browser) and couples the shells; the conditional sprawl is its own rot.
- **Convention-only boundary (one package, ESLint rules).** Rejected: the failure mode we most need to prevent — transport code leaking into shared components — would be possible-but-discouraged rather than impossible. Package boundaries make it impossible by construction.
- **Non-React frameworks (Svelte, Vue, Solid).** Considered. Svelte/Vue offer smaller bundles and pleasant DX; they suit a single-surface desktop or browser app well. But our differentiator is the dual-surface requirement — the same components must render inside an MCP host *and* a browser — and the MCP Apps client SDKs plus the financial-UI ecosystem — tables, forms, and data-viz primitives (d3 / SVG-in-React) — are React-first. That alignment outweighs the bundle-size and DX edges here.
- **An off-the-shelf charting library (Tremor/Recharts, or a Canvas engine like ECharts/uPlot).** Rejected — see the [Charting decision](#charting-decision). In short: `design-system/charts.md` is prescriptive enough that a component library is overridden almost entirely and fought on interaction, over a closed ten-form set on small-n data; and the grammar's per-mark CSS-token / DOM-tooltip / `aria` model requires SVG, which Canvas engines can't express. *visx* (React SVG primitives) is the near-alternative but adds a multi-package dependency whose headline parts (axis / tooltip / legend) the grammar overrides. A survey of PFM and BI/observability tools found the whole best-in-class tier either rolls its own or uses a declarative engine — none use a Recharts-class component library.

## Consequences

### What this enables

- A dashboard component written once renders in the MCP App and the Web UI — and later in any additional MCP host (e.g. an in-app agent panel) — with no rewrite. "Where else can this render?" becomes a configuration question, not a porting project.
- The transport contract test suite mechanically guarantees functional parity across surfaces, instead of relying on review.
- The MCP App's sandbox + CSP + self-contained bundle yield **zero external network calls**, making MoneyBin's "nothing leaves your machine" posture literally true at the UI layer.

### Costs accepted

- **Frontend toolchain in the repo.** A `ui/` workspace, Node 20+/pnpm for frontend contributors, and Node in the release/CI build. Python-only contributors are unaffected; users never see it.
- **A new thin FastAPI surface** over the existing service layer for the Web UI transport. No business logic — routes delegate to existing services.
- **Server-side MCP Apps support** must be verified: declaring `_meta.ui.resourceUri` on tools and serving `ui://` resources from the Python (FastMCP) server. Flagged as an open risk in the spec; the Web UI path does not depend on it. *Update 2026-06-12: verified by a walking-skeleton spike — FastMCP 3.3.1 supports both first-class. The blocker moved host-side: shipping hosts don't render MCP Apps yet (open upstream bugs), so the mcp-app shell is paused and the web shell ships first. Verdict: [`ui-architecture.md`](../specs/ui-architecture.md) open question #1.*

### Boundaries deliberately left open

- **Extension-package UI slots** are stubbed (`<RegistrySlot>` + slot types) but not loaded — consistent with [`extension-contracts.md`](../specs/extension-contracts.md)'s "no arbitrary UI plugins in 1.0."
- **In-app AI** is a non-goal but explicitly not foreclosed: a future agent panel reuses `ui-core` through `MoneyBinClient`, and any agent loop MoneyBin runs will be BYOK / local-model only — never bundled keys — preserving the deterministic-compute / LLM-prose and no-PII-egress invariants.

## References

- [`ui-architecture.md`](../specs/ui-architecture.md) — the full architecture spec this ADR records the rationale for
- [MCP Apps overview](https://modelcontextprotocol.io/extensions/apps/overview) · [spec announcement (2026-01-26)](https://blog.modelcontextprotocol.io/posts/2026-01-26-mcp-apps/)
- [`design-principles.md`](../../.claude/rules/design-principles.md) — coherence ("one way to do each thing") and the ADR bar
- [ADR-011](011-docs-site-framework.md) — prior frontend-tooling decision (docs site); same "Python-native, minimize second-toolchain friction" instinct applied where it fits
