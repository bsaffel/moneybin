---
name: moneybin-design
description: Design MoneyBin interfaces and assets on-brand in the "Ledger-grade" style (dark-lead, three-tier metal accent, money in mono, SQL provenance). Use when building, mocking, or theming any MoneyBin UI, screen, artifact, or component.
---

MoneyBin's design system — the source of truth — lives at `design-system/` in this
repo (it also feeds the claude.ai/design project via `/design-sync`). Read these
before producing anything:

- `design-system/readme.md` — brand direction, voice, and content rules
- `design-system/tokens/` — colors, typography, shape (CSS custom properties)
- `design-system/guidelines/*.html` — specimen cards (Colors, Type, Shape, Brand, Charts, Iconography, Voice)
- **The four binding grammar docs** — each wins in its own domain over anything summarized here or elsewhere. Read the one that covers what you are building:
  - `design-system/charts.md` — chart grammar (interpolation, color tiers, provenance ladder, selection)
  - `design-system/motion.md` — motion doctrine (state-bearing motion only; amounts never animate)
  - `design-system/patterns.md` — app chrome (rail shell, floating layer, ⌘K palette, table anatomy, import mapper, chord registry)
  - `design-system/ai-surface.md` — the ask surface, consent tiers, provider policy
- `design-system/components/<group>/<Name>.jsx` + `.prompt.md` — the real components (Button, Chip, Icon, Amount, WidgetCard, VaultStatusBar, Mark, Wordmark, DuckKey)
- `design-system/ui_kits/web_app/index.html` — a full dashboard exemplar

Non-negotiables: dark theme leads (light is a first-class peer); one metal accent
in three tiers — brass (`--accent-brass`) speaks (provenance text), gilt
(`--accent-gilt`) fills (bars, buttons; identity marks use `--brand-gold`,
point-markers on a line use ink), verdigris
(`--accent-verdigris`) responds (selection, filters, links) — never blue; money is ALWAYS JetBrains Mono
with explicit +/- signs on income/expense flows (balances unsigned) — use the `Amount` component; hairline borders, no resting
shadows; every data widget carries a SQL provenance chip (rung one of a three-rung provenance ladder — a global deep-audit strip, then pinned tooltips snapping to real points; see `design-system/charts.md`); linear chart
interpolation only; icons come from the `Icon` component (19 glyphs, 20×20 grid,
1.5px stroke) — never an inline one-off SVG, and the ask/AI surface is the caret
`▸_`, never an icon; no emoji, no exclamation points, no superlatives.

For throwaway artifacts (mocks, slides, prototypes): copy assets out and produce
self-contained static HTML using the tokens. For production code: follow the token
and component conventions in `design-system/`. When the design system itself needs
to evolve, edit the source in `design-system/` and re-run `/design-sync` to
republish to claude.ai/design.
