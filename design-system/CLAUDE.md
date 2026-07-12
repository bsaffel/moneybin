# MoneyBin design system

Read `readme.md` first — it is the source of truth for this system. This file is
a pointer, not a second copy; when the two disagree, `readme.md` wins.

## Non-negotiables

- **Dark theme leads.** Dark is the bare `:root`; light is the
  `[data-theme="light"]` peer mapping — never an inversion.
- **Brass is the only accent, never blue.** `var(--accent-brass)`. Single-series
  charts are brass; multi-series draw `--chart-1..8` in order, six max before
  grouping to "Other", and a category keeps its hue in every view.
- **Money is always mono** (`--font-data`) via the `Amount` component, with an
  explicit +/− (U+2212) on income/expense flows. Balances stay unsigned.
- **Every data widget carries a SQL provenance chip** — rung one of the
  three-rung provenance ladder (see `charts.md`).
- **Hairline borders, no resting shadows. Linear chart interpolation only** —
  gaps are never bridged.
- **Icons come from `components/core/Icon.jsx`** — never an inline one-off SVG.
  A new glyph is a system change. The AI/ask surface is the caret `▸_`, never ✨.
- **No emoji, no exclamation points, no superlatives.**
- **Tokens only.** Never hardcode a hex — every color reads `var(--*)`, or the
  asset breaks in light theme.

## Map

- `tokens/` — colors, typography, shape. The vocabulary everything else reads.
- `components/` — `core/` (Button, Chip, Icon) · `data/` (Amount, WidgetCard) ·
  `chrome/` (VaultStatusBar) · `brand/` (Mark, Wordmark, DuckKey). Each ships a
  `.jsx` + `.d.ts` + `.prompt.md`.
- `guidelines/*.html` — specimen cards (the visual contract).
- `charts.md` — the binding chart grammar. It wins over any other chart prose.

## Direction of truth

This repo is canonical. The claude.ai **Design System** project is a generated
mirror, published from here — never hand-edit it as the source. Design work is
prototyped in the claude.ai **Design Kit** project and promoted back into this
tree by review. See `readme.md` → "Updating the design system".
