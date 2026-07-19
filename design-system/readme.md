# MoneyBin Design System

**MoneyBin** (by **PrestiDigital**) is a local-first, AI-native personal-finance platform: Python + DuckDB engine, all data in ONE encrypted file (AES-256-GCM) on the user's machine, everything queryable with SQL, first-party MCP server, open source (AGPL), no telemetry. Audience: engineers and data professionals (Cursor/Linear/Hex register) — consumer users accommodated, not centered.

**Design direction: "Ledger-grade."** Engraved print ledger (warm ink/paper neutrals, hairline rules, serif for brand voice) fused with terminal precision (all money in mono, exact values, provenance one click away). Personality: exact, calm, engraved, auditable.

**Signature element: trust as furniture.** Every number can show its work — brass `SQL` chips on every widget, dotted underlines on auditable figures, a persistent vault status bar in the app chrome. Trust is a UI affordance, not a marketing claim.

Sources: authored from scratch in this project (no external Figma/codebase). The argued spec behind these rules — the hi-fi dashboard mockup and the design rationale — lives in the claude.ai **Design Kit** project, where it renders against the design runtime. This tree carries the binding artifacts: tokens, components, specimen cards, and `charts.md`.

## Content fundamentals
- Numbers first, verbs second: "Synced 4 min ago · 214 new transactions".
- No exclamation points, no "oops", no superlatives, no hype, never framed against competitors.
- Errors state the fact, then the next action: "Import failed on row 214: unparseable date. Fix the row or skip it."
- Scary words said plainly: encrypted, deleted, irreversible.
- Humor budget: empty states and CLI only — dry, one line, never near money/errors/security. ("No transactions yet. A blank ledger — enviable, briefly.")
- No emoji in product UI. Sentence case everywhere; overline labels are ALL-CAPS mono, tracked 0.12em.
- Surfaces sharing this language: web SPA (primary), marketing site, mobile app, MCP-app widgets, CLI.

## Visual foundations
- **Color:** warm ink/paper near-neutrals; ONE metal accent in three tiers — brass speaks (provenance text), gilt fills (bars, dots, buttons), verdigris responds (selection, filters, links); never blue. Dark theme leads (audience lives in dark editors); light is first-class (marketing/print lead). Income/expense pair survives color-blindness; sign is ALWAYS redundant (+/−, ▲/▼). Accounting convention: negative = expense.
- **Type:** serif speaks (Newsreader — headlines, wordmark, and room/page titles only — 24px in app chrome, 34–46px in spec docs; never widgets, tables, controls, overlines, or any data surface), sans works (Schibsted Grotesk — all UI; deliberately not Inter), mono counts (JetBrains Mono — EVERY amount, timestamp, axis label, SQL). All OFL 1.1; self-host in production (no font CDNs — no-telemetry promise).
- **Shape:** hairline 1px borders carry structure; NO shadows on resting surfaces; one ambient shadow on floating layers only. Radii small: 2/4/6/10. Spacing 4px grid: 8/12/16/24/28. Density: 32px compact rows (default), 40px cozy, 44px touch.
- **Backgrounds:** flat token surfaces only — no gradients, no textures, no imagery in app chrome.
- **App chrome:** the rail shell is the chrome — a left rail (ROOMS + YOUR LIBRARY), a utility strip (Mark, omnisearch, `▸_ Ask`, MCP/sync chips), and the `VaultStatusBar` floor; the console is a room. Top-nav is deprecated. Nav vocabulary: the room is "Analytics", the library row "Saved reports" ("Reports" as a room name is dead). Load-bearing app patterns are specced in `patterns.md`.
- **Animation:** motion reports a state change and nothing else — 120ms flips / 180ms surfaces (`--motion-fast` / `--motion-slow`), no entrance or ambient motion, amounts never animate; the `sync` spinner is the only loop (while a sync is in flight) and the vault-unlock (duck-key seats into keyhole) the one ceremony. `motion.md` is binding.
- **Hover:** surfaces step one level up (base→surface→raised); text steps secondary→primary; gilt button → gilt-strong; SQL chip border → brass; selected/active filter → verdigris. Press: no shrink effects.
- **Focus:** 2px solid var(--focus), 2px offset.
- **Charts (BI-grade):** `charts.md` is the binding grammar (it wins over any other chart section here), demonstrated by the 12 `guidelines/charts-*.html` specimens. In brief: no axis strokes, horizontal hairlines max 5, mono 11px labels; LINEAR interpolation only — gaps never bridged (stepped carry-forward is the honest form for balance data); single-series value-over-time lines are brass and category bars are gilt fills, multi `chart-1..8` max 6, and a category keeps its hue in every view; area fills ≤8%; off-scale zero disclosed on the chart. Provenance is a **three-rung ladder**: SQL chip on every widget → a global **deep-audit strip** (one toggle adds a mono `AUDIT` line — n=, scale/clip, exclusions — to every widget) → **pinned tooltips** snapping to real data points. Signs print in the glyph on chart labels and legends too (sankey, donut, stacked bars), not only in `Amount`.

## Iconography
`components/core/Icon.jsx` is the single icon source — 19 custom-drawn glyphs (`guidelines/icons-grammar.html` is the specimen card). Grammar: 20×20 grid, 1.5px stroke, squared caps, no fills, one weight, literal metaphors, `currentColor`. Sizes: 16px in controls and table rows, 20px in nav rails. An icon never appears without a visible label except in a collapsed rail or an icon-only control, where `title` is mandatory. The glyph is `currentColor` and never gold; active *location* is a brass edge tick beside an ink glyph, a toggled-on control is verdigris. **New glyphs are a system change** — draw to the grammar and add them to `Icon.jsx`, never an inline one-off SVG in a screen; `Icon.jsx` also carries a reserve set drawn ahead of need, promoted into the typed vocabulary only when a shipping surface needs it. **"Shipping surface" means shipped app code, not a design spec** — a spec *earmarks* a glyph, it does not promote it, and a reserve glyph is fine in a mockup but must move to core before it reaches the product. Currently earmarked: `eye` / `eye-off`, by the vault passphrase reveal. The AI/ask surface is the terminal caret `▸_`, never ✨. Unicode used sparingly as glyphs (⌘K, ⇄ transfers, ▲▼ deltas, ● status). No icon font. No emoji ever. Banned metaphors: coins raining, sparkles, magic wands. If a stock icon is unavoidable, restroke to spec (nearest CDN match: Lucide at 1.5px, squared caps where possible — flag any substitution).

**Logo:** the "coin & slot" mark — solid coin poised over a slot cut clean through a rounded-square plate (`components/brand/Mark.jsx`). Wordmark: "MoneyBin" Newsreader semibold — the `components/brand/Wordmark.jsx` lock-up composes the Mark, with the optical baseline nudge and brand-gold on "Bin" (bright gilt on dark, brass on light — the wordmark is identity text) baked in. **Duck-key** (`components/brand/DuckKey.jsx`): reusable glyph — the negative of the "bill-hole" keyhole; mono in app chrome, full-color eyed in docs/marketing; never rotated, bill always right. **Mascot ("Bill")** is docs/CLI only, never app chrome; NEVER a duck swimming/splashing in coins, no coin piles, no top hat/cane/spats (Disney IP adjacency).

## Index
- `styles.css` → `tokens/` (colors, typography, shape) — global CSS entry
- `guidelines/` — specimen cards (Colors ×5, Type ×3, Shape ×3, Brand ×3, Charts ×12, Voice ×2, Iconography)
- `charts.md` — binding chart grammar (prose companion to the 12 `guidelines/charts-*.html` specimens)
- `motion.md` — binding motion doctrine (state-bearing motion only; the tokens live in `tokens/motion.css`)
- `patterns.md` — app-pattern grammar (FilterBar, SegmentedControl, PageHeader, NavRail, Table, VaultStatusBar, floating layer, ⌘K palette, import mapper) + the chord registry and the canonical synthetic dataset
- `ai-surface.md` — binding AI-surface contract (ask-bar anatomy, consent tiers, provider policy)
- `components/core/` — Button, Chip, Icon · `components/data/` — Amount, WidgetCard · `components/chrome/` — VaultStatusBar · `components/brand/` — Mark, DuckKey, Wordmark
- `ui_kits/web_app/` — dashboard home (static reference extraction of the brand kit §09)

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
