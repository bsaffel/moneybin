The single icon vocabulary — 19 glyphs, custom-drawn to the grammar: 20×20 grid, 1.5px stroke, squared caps (butt linecap, miter joins), no fills, one weight, literal metaphors. Color inherits `currentColor`.

```jsx
<Icon name="search" size={16} />
<Icon name="chevron" direction="down" size={16} />
<Icon name="pin" title="Pin to overview" />   // title required when no visible label
```

## Names

`home` `accounts` `transactions` `reports` `investments` `budgets` `console` `settings` — navigation · `vault` `key` — trust · `search` `add` `close` `chevron` `pin` `sync` `import` `export` `sidebar` — actions.

`Icon.jsx` also carries a reserve set drawn ahead of need (tagging, planning, connections, alerts, utility). Reserve glyphs render but are untyped and unlisted in `Icon.names`: promote one into `Icon.d.ts` + the vocabulary card only when a shipping surface needs it — never before.

**"Shipping surface" means shipped app code.** A design spec *earmarks* a glyph; it does not promote it. A reserve glyph is fine in a mockup, but it must move to core before it reaches the product.

### Earmarked, not promoted

`eye` / `eye-off` — earmarked by the vault passphrase reveal. Both glyphs stay in the reserve until the v1 vault screen ships.

**What the promotion adds.** Only `eye` enters the public vocabulary — the type union, `Icon.names`, and the vocabulary card — together with the `off` prop below, as one API change. `eye-off` stays an *internal* glyph id backing the `off` state and is deliberately never a public name: two public names would let a caller swap one component for another and lose the shared-outline transition the toggle exists to provide.

**Composed toggle — specified, not yet implemented.** When the promotion lands, the pair ships as one composed state on `Icon`, never a page-level one-off SVG and never two separate glyph names in the markup:

```jsx
<Icon name="eye" off />   {/* specified; the `off` prop does not exist yet */}
```

The slash draws and undraws via `stroke-dashoffset` over `--motion-fast` (120ms) — the redaction stroke. The shared eye outline never moves and the pupil fades; only the strokes that differ animate. The masked *value* swaps instantly per `motion.md` ("Amounts never animate"); only the glyph transitions.

## Rules (binding)

- **Icons are sparing.** An icon never appears without a visible label except in a collapsed nav rail or an icon-only control — and then `title` (tooltip + aria-label) is mandatory.
- **Sizes:** 16px inside controls and table rows, 20px in nav rails. Never larger in app chrome.
- **Color:** inherit text color (`currentColor`). The glyph itself is never gold — a gold glyph reads as a fill and breaks the no-fills grammar. Active *location* (the you-are-here nav row) = a brass edge tick beside an ink glyph (`box-shadow: inset 2px 0 0 var(--accent-brass)`), not a brass drawing; a toggled-on icon-only control = verdigris via `currentColor` (interaction, per the accent tiers).
- **The ask/AI surface is the terminal caret `▸_`** (text, `--font-data`), never an icon, never ✨. Unicode glyph budget stays: ⌘K, ⇄ transfers, ▲▼ deltas, ● status.
- **New glyphs are a system change:** draw to the grammar and add to `Icon.jsx` + `guidelines/icons-grammar.html` — never an inline one-off SVG in a screen. If a stock icon is unavoidable, restroke Lucide to 1.5px and flag it.
- **Fills are exceptional** — only the settings hub dot, the warning point, and the `more` dots. Dots are marks, not shapes.
- Banned metaphors: coins raining, sparkles, magic wands, emoji. No icon fonts.

See `guidelines/icons-grammar.html` for the specimen card.
