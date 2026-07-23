# Agent Visualization: Chart-Ready Responses & Presentation Hints

> Companions: [`mcp-architecture.md`](mcp-architecture.md) (envelope contract, design philosophy), [`moneybin-mcp.md`](moneybin-mcp.md) (concrete tool definitions), [`ui-architecture.md`](ui-architecture.md) (the same projections feed `ui-core` charts)

## Status

- **Type:** Feature
- **Status:** draft
- **Address:** M3K.1 — first work item under M3K (CLI / MCP UX standards); registered in [`docs/roadmap.md`](../roadmap.md). The broader `mcp-ux-standards.md` umbrella spec remains planned.
- **Origin:** 2026-06-12 MCP-App spike verdict ([`ui-architecture.md`](ui-architecture.md) open question #1). Shipping hosts don't render MCP Apps yet, which makes the driving agent MoneyBin's renderer for the foreseeable near term — so the rendering expertise belongs in the responses themselves.

## Goal

In every host MoneyBin runs in today — Claude Desktop / claude.ai (artifacts), ChatGPT (analysis charts), VS Code, CLI agents (terminal tables) — the **model is the renderer**: it turns tool responses into tables, charts, and artifacts. Encode MoneyBin's visualization expertise server-side, host-agnostically, so any competent agent presents financial data correctly and well on first contact.

Three pillars, all additive to the existing envelope contract:

1. **Chart-ready projections** in `data`
2. **Presentation hints** through existing levers (tool descriptions + `actions[]`)
3. **A served visualization guide** (tool-path reachable)

## Design

### 1. Chart-ready projections

Agents should never have to reshape MoneyBin data before plotting — reshaping is where sign flips, dropped months, and mis-bucketed categories creep in. Report-shaped tools return series already in plot order:

- **Time series in long form** — `[{"period": "YYYY-MM", "amount": …}, …]`, not wide pivots; periods contiguous with explicit zero-filled gaps so a line chart never silently interpolates across missing months.
- **Breakdowns offer top-N + `"other"`** — an explicit rollup row plus `share_pct` per row (display-ready percent of total), so "top 8 categories" charts don't require client-side math.
- **Display-sign guidance, never silent flips** — `data` keeps the accounting convention (negative = expense, positive = income; per [`architecture-shared-primitives.md`](architecture-shared-primitives.md)). Tools whose natural chart displays expenses as positive magnitudes say so in their hints; the data layer never flips.

Implementation is an additive audit of the `reports` catalog (and other
series/breakdown operations) against this checklist — new fields or parameters
only, no breaking changes.

### 2. Presentation hints (existing levers only)

No new envelope field in v1. Two existing levers carry rendering guidance:

- **Tool descriptions** state the natural visualization where one exists (e.g., *"suitable for a stacked monthly bar; for display charts flip sign — negative = expense"*), alongside the sign/currency statements already required by `.claude/rules/mcp.md` → Description requirements.
- **`actions[]`** may include one presentation hint when the data shape warrants it (e.g., *"Plot data as a line of period vs amount; gaps are zero-filled"*), following the existing next-step-hint conventions.

### 3. Served visualization guide

One orientation surface that teaches the driving agent how to visualize MoneyBin data well: sign convention and display flips, transfer exclusion, period semantics (`YYYY-MM`), which report pairs naturally with which chart shape, currency display (`summary.display_currency`). At plan time, choose between the FastMCP `instructions` field when the guide fits the ~150–300-token budget and a separately admitted tool or resource contract. This draft reserves no public identifier. Critical guidance must remain tool-reachable because MCP resources are not universally supported by clients.

## Non-Goals

- **Per-host capability matrices in server logic.** "Host X renders Y" knowledge churns monthly — the MCP-Apps render verdict is the cautionary tale (officially supported, broken in shipping clients). Host-specific observations live in living internal notes, not code. A future `clientInfo`-keyed hint tweak is a two-way door, openable on evidence that prose hints fall short for a specific host.
- **Pre-rendered HTML or images in responses.** The envelope returns structured data only; the host renders (`.claude/rules/security.md` → Output Encoding).
- **MCP App rendering.** Paused on the upstream host fix; owned by M3M / [`ui-architecture.md`](ui-architecture.md). The projections specified here are the same ones `ui-core` chart components will consume — this work feeds both streams.

## Open questions

1. Does a structured `summary.presentation` hint field earn its place, or do prose hints suffice? Start prose-only; revisit only on evidence agents misrender despite hints.
2. Guide surface placement — `instructions` section vs. a separately admitted tool or resource with a tool fallback (see §3).
3. Does CLI `--output json` need anything beyond automatic parity? (It inherits envelope changes through the shared service layer; expectation: no.)

## Relationship to existing specs

- [`mcp-architecture.md`](mcp-architecture.md) / [`moneybin-mcp.md`](moneybin-mcp.md) — the envelope contract and per-tool definitions stay canonical there; this spec adds presentation conventions on top.
- [`ui-architecture.md`](ui-architecture.md) — chart-ready projections double as `ui-core` chart inputs when the visual shells land.
- [`design-system/charts.md`](../../design-system/charts.md) — the binding chart grammar (ten canonical forms, category→hue map, "six max then Other" cutoff) that `ui-core` chart components render with once built; this spec's projections are the data-shape half of that contract. §2's presentation-hint vocabulary (chart form names, top-N sizing) should track that grammar's terms and cutoffs rather than drift independently; §3's served guide can reuse its "Chart-type per report" table instead of re-deriving report↔chart-shape pairings from scratch.
- `mcp-ux-standards.md` (planned, M3K) — the broader UX-standards umbrella; this spec is its first concrete work item and folds under it editorially when that spec is written.
