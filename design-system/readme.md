# MoneyBin Design System

**MoneyBin** (by **PrestiDigital**) is a local-first, AI-native personal-finance platform: Python + DuckDB engine, all data in ONE encrypted file (AES-256-GCM) on the user's machine, everything queryable with SQL, first-party MCP server, open source (AGPL), no telemetry. Audience: engineers and data professionals (Cursor/Linear/Hex register) — consumer users accommodated, not centered.

**Design direction: "Ledger-grade."** Engraved print ledger (warm ink/paper neutrals, hairline rules, serif for brand voice) fused with terminal precision (all money in mono, exact values, provenance one click away). Personality: exact, calm, engraved, auditable.

**Signature element: trust as furniture.** Every number can show its work — brass `SQL` chips on every widget, dotted underlines on auditable figures, a persistent vault status bar in the app chrome. Trust is a UI affordance, not a marketing claim.

Sources: authored from scratch in this project (no external Figma/codebase). The full argued spec — with the hi-fi dashboard mockup and the design rationale — is `MoneyBin Brand Kit.dc.html` at the design-system root.

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
- **Charts (BI-grade):** `charts.md` is the binding grammar (it wins over any other chart section here), demonstrated by the 12 `guidelines/charts-*.html` specimens. In brief: no axis strokes, horizontal hairlines max 5, mono 11px labels; LINEAR interpolation only — gaps never bridged (stepped carry-forward is the honest form for balance data); single series brass, multi `chart-1..8` max 6, and a category keeps its hue in every view; area fills ≤8%; off-scale zero disclosed on the chart. Provenance is a **three-rung ladder**: SQL chip on every widget → a global **deep-audit strip** (one toggle adds a mono `AUDIT` line — n=, scale/clip, exclusions — to every widget) → **pinned tooltips** snapping to real data points. Signs print in the glyph on chart labels and legends too (sankey, donut, stacked bars), not only in `Amount`.

## Iconography
`components/core/Icon.jsx` is the single icon source — 19 custom-drawn glyphs (`guidelines/icons-grammar.html` is the specimen card). Grammar: 20×20 grid, 1.5px stroke, squared caps, no fills, one weight, literal metaphors, `currentColor`. Sizes: 16px in controls and table rows, 20px in nav rails. An icon never appears without a visible label except in a collapsed rail or an icon-only control, where `title` is mandatory. Brass only when the element itself is active/brass — never decoration. **New glyphs are a system change** — draw to the grammar and add them to `Icon.jsx`, never an inline one-off SVG in a screen; `Icon.jsx` also carries a reserve set drawn ahead of need, promoted into the typed vocabulary only when a shipping surface needs it. The AI/ask surface is the terminal caret `▸_`, never ✨. Unicode used sparingly as glyphs (⌘K, ⇄ transfers, ▲▼ deltas, ● status). No icon font. No emoji ever. Banned metaphors: coins raining, sparkles, magic wands. If a stock icon is unavoidable, restroke to spec (nearest CDN match: Lucide at 1.5px, squared caps where possible — flag any substitution).

**Logo:** the "coin & slot" mark — solid coin poised over a slot cut clean through a rounded-square plate (`components/brand/Mark.jsx`). Wordmark: "MoneyBin" Newsreader semibold — the `components/brand/Wordmark.jsx` lock-up composes the Mark, with the optical baseline nudge and brass on "Bin" baked in. **Duck-key** (`components/brand/DuckKey.jsx`): reusable glyph — the negative of the "bill-hole" keyhole; mono in app chrome, full-color eyed in docs/marketing; never rotated, bill always right. **Mascot ("Bill")** is docs/CLI only, never app chrome; NEVER a duck swimming/splashing in coins, no coin piles, no top hat/cane/spats (Disney IP adjacency).

## Index
- `styles.css` → `tokens/` (colors, typography, shape) — global CSS entry
- `guidelines/` — specimen cards (Colors ×5, Type ×3, Shape ×3, Brand ×3, Charts ×12, Voice ×2, Iconography)
- `charts.md` — binding chart grammar (prose companion to the 12 `guidelines/charts-*.html` specimens)
- `components/core/` — Button, Chip, Icon · `components/data/` — Amount, WidgetCard · `components/chrome/` — VaultStatusBar · `components/brand/` — Mark, DuckKey, Wordmark
- `ui_kits/web_app/` — dashboard home (static reference extraction of the brand kit §09)
- `MoneyBin Brand Kit.dc.html` — the full argued spec (sections 01–10 incl. rationale)

### Intentional additions
- `Amount` — not a visual primitive in a source kit, but the money-formatting hard rules (mono, tabular, redundant sign) need a single enforcement point.

### Caveats
- The dashboard UI kit is a static extraction of the brand-kit mockup (hover states and SQL toggle are not interactive there; see the DC file for the live version).

## Updating the design system

`design-system/` is the source of truth. Changes flow one way, always through the repo:

1. **Prototype / spec visually** in the claude.ai/design **Design Kit** project — the scratchpad for screens, studies, and spec docs; it renders against the current components.
2. **Promote into the repo** with `/design-import` — classify each asset (component vs specimen card vs screen-to-park), reconstruct it repo-native (tokens, card contract), and land it via a PR.
3. **Publish** the repo → the claude.ai **Design System** project with `/design-sync` (outbound), so the live design surface mirrors the repo.

The Design System project is a **generated mirror** — never hand-edit it as the authoritative copy; edits there drift from the repo and are overwritten on the next sync. Explore in claude.ai freely; it only becomes real once it lands in the repo via a PR.
