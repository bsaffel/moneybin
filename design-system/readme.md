# MoneyBin Design System

**MoneyBin** (by **PrestiDigital**) is a local-first, AI-native personal-finance platform: Python + DuckDB engine, all data in ONE encrypted file (AES-256-GCM) on the user's machine, everything queryable with SQL, first-party MCP server, open source (AGPL), no telemetry. Audience: engineers and data professionals (Cursor/Linear/Hex register) — consumer users accommodated, not centered.

**Design direction: "Ledger-grade."** Engraved print ledger (warm ink/paper neutrals, hairline rules, serif for brand voice) fused with terminal precision (all money in mono, exact values, provenance one click away). Personality: exact, calm, engraved, auditable.

**Signature element: trust as furniture.** Every number can show its work — brass `SQL` chips on every widget, dotted underlines on auditable figures, a persistent vault status bar in the app chrome. Trust is a UI affordance, not a marketing claim.

Sources: authored from scratch in this project (no external Figma/codebase). The full argued spec — with the hi-fi dashboard mockup and the design rationale — is `MoneyBin Brand Kit.dc.html` at the project root. A developer handoff copy lives in `design_handoff_moneybin/` (synced to the founder's repo at `~/Workspace/moneybin/design`).

## Content fundamentals
- Numbers first, verbs second: "Synced 4 min ago · 214 new transactions".
- No exclamation points, no "oops", no superlatives, no hype, never framed against competitors.
- Errors state the fact, then the next action: "Import failed on row 214: unparseable date. Fix the row or skip it."
- Scary words said plainly: encrypted, deleted, irreversible.
- Humor budget: empty states and CLI only — dry, one line, never near money/errors/security. ("No transactions yet. A blank ledger — enviable, briefly.")
- No emoji in product UI. Sentence case everywhere; overline labels are ALL-CAPS mono, tracked 0.12em.
- Surfaces sharing this language: web SPA (primary), marketing site, mobile app, MCP-app widgets, CLI.

## Visual foundations
- **Color:** warm ink/paper near-neutrals; ONE accent — brass (the coin), never blue. Dark theme leads (audience lives in dark editors); light is first-class (marketing/print lead). Income/expense pair survives color-blindness; sign is ALWAYS redundant (+/−, ▲/▼). Accounting convention: negative = expense.
- **Type:** serif speaks (Newsreader — headlines/wordmark only), sans works (Schibsted Grotesk — all UI; deliberately not Inter), mono counts (JetBrains Mono — EVERY amount, timestamp, axis label, SQL). All OFL 1.1; self-host in production (no font CDNs — no-telemetry promise).
- **Shape:** hairline 1px borders carry structure; NO shadows on resting surfaces; one ambient shadow on floating layers only. Radii small: 2/4/6/10. Spacing 4px grid: 8/12/16/24/28. Density: 32px compact rows (default), 40px cozy, 44px touch.
- **Backgrounds:** flat token surfaces only — no gradients, no textures, no imagery in app chrome.
- **Animation:** state transitions ≤150ms ease-out; no entrance animations; the pulsing vault dot is the only ambient motion; the vault-unlock (duck-key seats into keyhole) is the one sanctioned brand animation.
- **Hover:** surfaces step one level up (base→surface→raised); text steps secondary→primary; brass button → brass-strong; SQL chip border → brass. Press: no shrink effects.
- **Focus:** 2px solid var(--focus), 2px offset.
- **Charts (BI-grade, all binding):** no axis strokes, zero baseline only emphasized rule; horizontal hairlines max 5, never vertical; mono 11px labels, currency abbreviated on axes, exact in tooltips; LINEAR interpolation only (never splines), gaps never bridged; tooltips are ledger rows snapping to real points; single series brass, multi chart-1..8 max 6; area fills ≤8%; every chart carries its SQL chip; off-scale zero disclosed in the corner.

## Iconography
Custom-drawn line icons: 20×20 grid, 1.5px stroke, squared caps, no fills, one weight, literal metaphors (see `guidelines/icons-grammar.html` — copy these SVGs). The AI/ask surface is the terminal caret `▸_`, never ✨. Unicode used sparingly as glyphs (⌘K, ⇄ transfers, ▲▼ deltas, ● status). No icon font; inline SVG. No emoji ever. Banned metaphors: coins raining, sparkles, magic wands. If a stock icon is unavoidable, restroke to spec (nearest CDN match: Lucide at 1.5px, squared caps where possible — flag any substitution).

**Logo:** the "coin & slot" mark — solid coin poised over a slot cut clean through a rounded-square plate (`components/brand/Mark.jsx`). Wordmark: "MoneyBin" Newsreader semibold. **Duck-key** (`components/brand/DuckKey.jsx`): reusable glyph — the negative of the "bill-hole" keyhole; mono in app chrome, full-color eyed in docs/marketing; never rotated, bill always right. **Mascot ("Bill")** is docs/CLI only, never app chrome; NEVER a duck swimming/splashing in coins, no coin piles, no top hat/cane/spats (Disney IP adjacency).

## Index
- `styles.css` → `tokens/` (colors, typography, shape) — global CSS entry
- `guidelines/` — specimen cards (Colors ×5, Type ×3, Shape ×3, Brand ×2, Charts, Voice ×2, Iconography)
- `components/core/` — Button, Chip · `components/data/` — Amount, WidgetCard · `components/chrome/` — VaultStatusBar · `components/brand/` — Mark, DuckKey
- `ui_kits/web_app/` — dashboard home (static reference extraction of the brand kit §09)
- `MoneyBin Brand Kit.dc.html` — the full argued spec (sections 01–10 incl. rationale)
- `design_handoff_moneybin/` — developer handoff package (README, DESIGN_SYSTEM.md, tokens)

### Intentional additions
- `Amount` — not a visual primitive in a source kit, but the money-formatting hard rules (mono, tabular, redundant sign) need a single enforcement point.

### Caveats
- Fonts load from Google Fonts here for preview convenience; production must self-host the woff2s (no-telemetry promise). Font binaries are NOT bundled in this project yet.
- The dashboard UI kit is a static extraction of the brand-kit mockup (hover states and SQL toggle are not interactive there; see the DC file for the live version).
