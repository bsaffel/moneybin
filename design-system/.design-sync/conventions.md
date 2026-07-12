# MoneyBin Design System — "Ledger-grade"

Engraved print ledger fused with terminal precision: exact, calm, auditable.
Dark theme leads; light is first-class. Components render from
`window.MoneyBinDS.*` (Button, Chip, Icon, Amount, WidgetCard, VaultStatusBar,
Mark, Wordmark, DuckKey).

## Setup — no provider, but set a token surface

There is **no provider/wrapper component**. Tokens and fonts load from the bound
`styles.css`. Two rules make output look right:

- **Dark is the default theme** (defined on `:root`). Opt into light by setting
  `data-theme="light"` on a root ancestor (`<html>`/`<body>`); every color token
  has a light mapping.
- **Put UI on a token surface.** Component text uses light-on-dark tokens by
  default, so a bare white page hides it. Wrap the app canvas in
  `background: var(--bg-base)`; put cards on `var(--bg-surface)` /
  `var(--bg-raised)`.

## Styling idiom — CSS custom-property tokens

Components are **self-styling** (inline styles reading `var(--*)`). Style your
own layout and surfaces with the same tokens — **no utility classes, no style
props**. Real names:

- **Color** — surfaces `--bg-base --bg-surface --bg-raised --bg-inset`; borders
  `--border-hairline --border-strong`; text `--text-primary --text-secondary
  --text-faint`; the one accent `--accent-brass` (+`--accent-brass-strong`),
  **never blue**; money `--pos-income` (green) / `--neg-expense` (red); series
  `--chart-1` … `--chart-8`; `--focus`.
- **Type** — `--font-display` (Newsreader serif; headlines/wordmark only),
  `--font-ui` (Schibsted Grotesk; all UI), `--font-data` (JetBrains Mono;
  **every** amount/timestamp/axis label/SQL). Sizes `--text-body-size
  --text-table-size --text-amount-size --text-hero-amount-size --text-h2-size
  --text-overline-size` (+`--text-overline-tracking`) `--text-axis-size`.
- **Shape** — radii `--r-chip --r-control --r-card --r-modal`; spacing (4px grid)
  `--sp-inside-control --sp-related --sp-widget-pad --sp-between-widgets
  --sp-page-gutter`; density `--row-compact --row-cozy --row-touch`;
  `--shadow-floating` (floating layers ONLY — no resting shadows; hairline
  borders carry structure).

## Non-negotiables

- **Money → `<Amount>`**, never hand-formatted: JetBrains Mono, tabular figures,
  **explicit +/− sign always** (income +, expense −; negative = expense).
- **Every data widget carries a SQL provenance chip** — `<WidgetCard sql="…">`
  (brass chip reveals the exact query) or `<Chip variant="sql" />`.
- **Icons → `<Icon name="…" />`**, never an inline one-off `<svg>`. 19 glyphs:
  `home accounts transactions reports investments budgets console settings`
  (nav) · `vault key` (trust) · `search add close chevron pin sync import export
  sidebar` (actions). Size 16 in controls/table rows, 20 in nav rails; color
  inherits `currentColor`. An icon never appears without a visible label except
  in a collapsed rail or an icon-only control, where `title` is required. The
  AI/ask surface is the caret `▸_` (`--font-data`), never an icon, never ✨.
- Brass is the only accent. Hairline borders, no resting shadows. **No emoji, no
  exclamation points, no superlatives.** Sentence case; overline labels are
  ALL-CAPS mono, tracked `--text-overline-tracking`.

## Where the truth lives

Read before styling: `_ds/<folder>/styles.css` → `_ds_bundle.css` (every token
`:root` definition) + `fonts/fonts.css`. Per-component API in `<Name>.d.ts`,
usage in `<Name>.prompt.md`.

## Build snippet

```jsx
const { Button, WidgetCard, Amount } = window.MoneyBinDS;

<div style={{ background: 'var(--bg-base)', padding: 'var(--sp-page-gutter)',
              display: 'grid', gap: 'var(--sp-between-widgets)' }}>
  <WidgetCard title="NET WORTH" meta="as of today"
              sql="SELECT sum(balance) FROM accounts;">
    <Amount value={487231.09} kind="plain" size="hero" auditable />
  </WidgetCard>
  <Button variant="primary">Add widget</Button>
</div>
```
