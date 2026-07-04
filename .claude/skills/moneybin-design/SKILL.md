---
name: moneybin-design
description: Design MoneyBin interfaces and assets on-brand in the "Ledger-grade" style (dark-lead, brass-only accent, money in mono, SQL provenance). Use when building, mocking, or theming any MoneyBin UI, screen, artifact, or component.
---

MoneyBin's design system — the source of truth — lives at `design-system/` in this
repo (it also feeds the claude.ai/design project via `/design-sync`). Read these
before producing anything:

- `design-system/readme.md` — brand direction, voice, and content rules
- `design-system/tokens/` — colors, typography, shape (CSS custom properties)
- `design-system/guidelines/*.html` — specimen cards (Colors, Type, Shape, Brand, Charts, Iconography, Voice)
- `design-system/components/<group>/<Name>.jsx` + `.prompt.md` — the real components (Button, Chip, Amount, WidgetCard, VaultStatusBar, Mark, DuckKey)
- `design-system/ui_kits/web_app/index.html` — a full dashboard exemplar

Non-negotiables: dark theme leads (light is a first-class peer); brass
(`--accent-brass`) is the only accent, never blue; money is ALWAYS JetBrains Mono
with explicit +/- signs — use the `Amount` component; hairline borders, no resting
shadows; every data widget carries a SQL provenance chip; linear chart
interpolation only; no emoji, no exclamation points, no superlatives.

For throwaway artifacts (mocks, slides, prototypes): copy assets out and produce
self-contained static HTML using the tokens. For production code: follow the token
and component conventions in `design-system/`. When the design system itself needs
to evolve, edit the source in `design-system/` and re-run `/design-sync` to
republish to claude.ai/design.
