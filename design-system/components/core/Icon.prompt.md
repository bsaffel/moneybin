The single icon vocabulary — 19 glyphs, custom-drawn to the grammar: 20×20 grid, 1.5px stroke, squared caps (butt linecap, miter joins), no fills, one weight, literal metaphors. Color inherits `currentColor`.

```jsx
<Icon name="search" size={16} />
<Icon name="chevron" direction="down" size={16} />
<Icon name="pin" title="Pin to overview" />   // title required when no visible label
```

## Names

`home` `accounts` `transactions` `reports` `investments` `budgets` `console` `settings` — navigation · `vault` `key` — trust · `search` `add` `close` `chevron` `pin` `sync` `import` `export` `sidebar` — actions.

`Icon.jsx` also carries a reserve set drawn ahead of need (tagging, planning, connections, alerts, utility). Reserve glyphs render but are untyped and unlisted in `Icon.names`: promote one into `Icon.d.ts` + the vocabulary card only when a shipping surface needs it — never before.

## Rules (binding)

- **Icons are sparing.** An icon never appears without a visible label except in a collapsed nav rail or an icon-only control — and then `title` (tooltip + aria-label) is mandatory.
- **Sizes:** 16px inside controls and table rows, 20px in nav rails. Never larger in app chrome.
- **Color:** inherit text color. Brass only when the element itself is active/brass — never as decoration.
- **The ask/AI surface is the terminal caret `▸_`** (text, `--font-data`), never an icon, never ✨. Unicode glyph budget stays: ⌘K, ⇄ transfers, ▲▼ deltas, ● status.
- **New glyphs are a system change:** draw to the grammar and add to `Icon.jsx` + `guidelines/icons-grammar.html` — never an inline one-off SVG in a screen. If a stock icon is unavoidable, restroke Lucide to 1.5px and flag it.
- **Fills are exceptional** — only the settings hub dot, the warning point, and the `more` dots. Dots are marks, not shapes.
- Banned metaphors: coins raining, sparkles, magic wands, emoji. No icon fonts.

See `guidelines/icons-grammar.html` for the specimen card.
