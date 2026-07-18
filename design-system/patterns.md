# MoneyBin app-pattern grammar — binding rules

The binding grammar for the app's structural patterns — the chrome and
controls that frame every screen, drawn from the app-screens reference in the
Design Kit project. These patterns are now load-bearing; each is documented
here as grammar. Some are candidate components for the app build; until a
component ships they are pattern specs, not code. Where any other doc's
guidance on these patterns differs, this one wins — `charts.md` still owns
chart grammar, `motion.md` still owns motion, and `ai-surface.md` still owns
the ask surface and its consent contract; each wins in its own domain. Sample
data throughout; the grammar is what ships. Never hardcode hex — all values
come from `tokens/`.

## Accent tiers (applies to every pattern)

One metal family, three tiers, never blue: **brass** (`--accent-brass`) marks
provenance text and active identity; **gilt** (`--accent-gilt`) fills;
**verdigris** (`--accent-verdigris`) responds — interaction and selection. A
pattern that fills reaches for gilt; a pattern that responds to a pointer or
holds a selection reaches for verdigris; a pattern that marks provenance or an
active row reaches for brass. Serif (`--font-display`) is sanctioned for room
and page titles only — never inside a widget, table, control, overline, or any
other data surface. Money is mono (`--font-data`) through the `Amount`
component, explicit `+` / `−` (U+2212) on income and expense flows, balances
unsigned. Hairline borders, no resting shadows.

## 01 FilterBar

One bar, four rungs of the same ladder — a single continuous escalation from
plain text to SQL, not a set of modes to switch between. Each rung is a more
literal view of one query; the user can read or edit at whichever rung suits
them and every rung stays in sync.

```mermaid
flowchart LR
  A[Omnisearch<br/>free text] --> B[Tokens<br/>parsed]
  B --> C[Pills<br/>compiled query]
  C --> D[WHERE readout<br/>live SQL]
  D --> E[Console<br/>direct edit]
```

- **Rung 1 — omnisearch.** A free-text field. Typing is parsed into tokens;
  tokens compile to SQL predicates. Plain language in, structure out.
- **Rung 2 — pills.** Each committed token renders as a pill — the compiled,
  editable form of the query. A pill holding the current selection carries the
  verdigris interaction tier; removing a pill drops its predicate.
- **Rung 3 — the WHERE readout.** The pills together render the live `WHERE`
  clause, mono (`--font-data`), exactly as the predicate stands. It is a
  provenance surface, so it follows the brass ladder from `charts.md` — the
  same reveal gesture as a widget's `SQL` chip.
- **Rung 4 — the console.** Clicking the readout opens it in the SQL console
  for direct editing. The console is the bottom rung, not a separate tool.

Settled: the unified omnisearch-plus-pills approach is the answer to the
transactions filter exploration. A separate third filter mode was considered
and rejected — it split one ladder into two parallel surfaces and broke the
"same query, four views" invariant.

## 02 SegmentedControl

A row of mutually exclusive segments, each label mono (`--font-data`) and
ALL-CAPS — `BY TOTAL` / `BY CATEGORY`, or `6M` `1Y` `ALL`. Exactly one segment
is selected; the selected segment holds the verdigris selection tier
(`--accent-verdigris`), the rest sit in `--text-secondary` and step to
`--text-primary` on hover. Hairline container, `--r-control` radius, no
resting shadow. Selecting a segment swaps state instantly, per `motion.md`.
Candidate component when the app build starts; a pattern spec for now.

## 03 PageHeader

A serif room or page title (`--font-display`) set on one baseline with mono
meta (`--font-data`) alongside it — the room's name in the engraved voice, its
counts and timestamps in the ledger voice. This is the one sanctioned place
for the serif display face in app chrome; it never crosses into a widget,
table, control, or data surface. Meta reads left-to-right as numbers first,
verbs second ("184,203 rows · synced 4 min ago").

## 04 NavRail + rail item

The active row reads as `background: var(--bg-raised)` with an inset 2px brass
edge tick — `box-shadow: inset 2px 0 0 var(--accent-brass)` — and a
`--text-primary` glyph and label. The tick carries the active signal; the
**glyph itself is never gold**, active or not. This agrees with the icon
active-treatment grammar (`readme.md` → Iconography): the active *location* is a
brass edge tick beside an ink glyph, and here the tick is that element — the
glyph is never gold.

- **Glyphs** come from the `Icon` component at 20px (nav-rail size), never an
  inline SVG.
- **Collapsed rail.** The same inset brass tick marks the active row; because
  the label is hidden, each item takes a `title` tooltip — mandatory for any
  icon-only control per the iconography grammar.
- **Library rows** carry mono counts (`--font-data`), right-aligned, as a
  secondary read on each row.

## 05 Table anatomy

The table is the app's densest surface; three parts are load-bearing.

- **Sparkline column.** Shape without axes — trend, not a value. It is **never
  a number source**; the `Amount` beside it is (see `charts.md` §04, including
  the amplitude rule). No axis, no gridline, no label inside the cell.
- **Checkbox rows.** Row-level selection via a leading checkbox; a selected row
  holds the verdigris selection tier. Selection drives bulk actions without a
  separate mode.
- **Two densities.** Compact 32px (`--row-compact`, the app default) and cozy
  40px (`--row-cozy`, a setting). Density is a swap, not a redesign — it must
  **not reflow** the layout (same grammar at every size); 44px is the touch
  minimum. Amounts stay right-aligned mono with an explicit sign.

## 06 VaultStatusBar variants + privacy mode

The persistent trust line has two variants sharing one vocabulary — the green
status dot is `--pos-income`, mono throughout (`--font-data`), and the right
edge always ends `local only · no telemetry · AGPL`.

- **App-floor variant.** A single line pinned to the bottom of every app
  surface (the shipping `VaultStatusBar` component) — file · cipher · rows ·
  accounts · sync.
- **Spec footer variant.** A denser four-cell layout for spec docs and
  expanded chrome, the same fields grouped into cells rather than one run.

**Privacy mode.** Masked amounts swap **instantly** to their masked form and
back — no scramble, no ticking, no count-down of characters — per `motion.md`
("Amounts never animate": a value change swaps instantly). Toggling privacy is
a state swap, not a transition.

## 07 Floating layer

One anatomy for every layer that floats — the ⌘K palette, the consent gate, the
no-provider sheet. There is no second modal shape.

- **Surface** `--bg-raised`, **border** `--border-strong`, **radius** `--r-modal`,
  **shadow** `--shadow-floating`.
- **Scrim** — `color-mix(in srgb, var(--bg-base) 55%, transparent)`. This is a
  recipe, not a new token; use it verbatim rather than minting one.
- **Dismissal** — a mono `esc` affordance sits top-right; `esc` always closes.
- **Motion** — opens per `--motion-slow` (180ms). No entrance choreography.

**Floating layers are the only shadow carriers in the system.** Every resting
surface stays hairline-bordered and flat; if something has a shadow, it floats.

## 08 Command palette (⌘K)

One field, three grammars — the palette is a router, not a search page.

- **Plain text** filters grouped results under ALL-CAPS mono group heads: ROOMS /
  SAVED REPORTS / ACTIONS. The active row holds `--accent-verdigris-tint`, the
  same selection thread as a tinted table row and a chart's verdigris anchor.
- **A `>` or `SELECT` prefix** flips the field to `--font-data` and treats the
  input as SQL; `⏎` opens the query in a new console tab, already run. The
  palette is not a privileged path: auto-run goes through the **same read-only
  validation as every other SQL surface**, and a statement that fails it opens
  in the console *unrun*, with the reason stated. The palette never executes
  what the console would refuse.
- **Keys** — `↑↓` move, `⏎` opens, `esc` closes (it is a floating layer, §07).

The ask action renders as the `▸_` caret row, never an icon — see `ai-surface.md`
for the ask surface itself.

## 09 Import mapper + receipt

First contact **always confirms**. The only exception is a hard balance proof,
which is arithmetic rather than inference. A confidence score shapes the
*ergonomics* of the confirm — never whether one happens.

- **Mapper rows** carry a mono status vocabulary, not a graded meter:
  `HEADER ALIAS ✓` · `CONTENT MATCH · EYEBALL` · `MISSING REQUIRED`. Color follows
  the error semantics sanctioned in `guidelines/colors-semantic.html` and is never
  the sole channel — the words carry the meaning.
- **Row errors are facts with exactly two actions**: fix, or skip. No third path,
  no silent drop.
- **Landing yields a receipt** — row accounting (read, imported, skipped) and a
  revert. The revert is stated up front, not discovered later.
- **Overrides are partial-merge and become part of the format**, so the next file
  of the same shape lands quieter than the first.

## Widget composition

Two layout rules, binding in every room.

- **Boxes line up.** Widgets sit on the room's 12-column grid: card edges land on
  shared column lines with one `--sp-between-widgets` gap everywhere. No bespoke
  fractional grids per row.
- **A visual fills its card.** Charts stretch to the card's inner width, and their
  SVG aspect is chosen so row neighbors land at equal heights. **Dead space under
  a chart is a layout bug, not breathing room.** Bands and tables run the card's
  full width; card controls (range and series toggles) live on the legend line and
  never wrap inside the hero row.

**The console composes chart-first.** The SQL console's result chart is the
largest surface on the screen: the result table is a 340px adjunct column, and the
chart owns the remaining width and carries the type switcher plus Pin / Save /
Export. The table is the adjunct, not the peer.

## Keyboard chords

The registry, seeded. Chords print in mono chip style — hairline border,
`--r-chip` radius.

| Chord | Action |
|---|---|
| `⌘K` | Command palette (§08) |
| `⌘⏎` | Ask — the console editor wins this chord when focused |
| `⌘L` | Lock vault |
| `esc` | Close any floating layer (§07) |
| `↑↓` / `⏎` | List navigation / open |

Conflicts resolve **room-first**: a room may claim a chord its surface needs, and
the console keeps its editor chords. A global chord yields to the focused room
rather than the reverse.

## Canonical synthetic dataset

One persona backs every specimen, so sample data stops drifting across cards.
A new specimen uses this persona; a drifted one is retrofitted to it as it is
touched.

- **8 accounts · 184,203 rows · net worth $487,231.09 · June 2026 ledger.**
- Categories draw from the fixed chart map (`charts.md` → Category color), so a
  category reads as one hue in every view:

```
Housing=chart-1  Groceries=chart-2  Transport=chart-3  Insurance=chart-4
Dining=chart-5   Utilities=chart-6  Travel=chart-7     Other=chart-8
```

## Reference files

- **The app-screens reference in the Design Kit project** — the interactive
  source these patterns were recreated from. It lives in the claude.ai Design
  Kit project, not this repo; read it as source.
- **`charts.md`** — chart grammar, the sparkline amplitude rule (§04), the
  provenance ladder, density, and the category color map.
- **`motion.md`** — the motion doctrine these patterns defer to for every
  state change (segment select, privacy toggle, value swap).
- **`ai-surface.md`** — the ask surface, consent tiers, and provider policy. The
  palette's ask row (§08) is the entry point; the contract lives there.
- **`tokens/`** (colors, typography, shape) — all values; never hardcode hex.
