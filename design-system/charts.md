# MoneyBin chart grammar — binding rules

The binding grammar for every analytics surface. The visual rules here are
demonstrated by the twelve `charts-*.html` specimen cards in `guidelines/`; this
doc is the prose companion. Where any other doc's chart section differs, this
one wins. Sample data throughout; the grammar is what ships. Never hardcode hex
— all values come from `../tokens/`.

## Global grammar (applies to every chart)

- **Axes**: no axis strokes. Horizontal hairlines only, max 5, never vertical. The zero baseline is the only emphasized rule (`--border-strong`).
- **Labels**: JetBrains Mono (`--font-data`), **11px minimum** (`--text-axis-size`) — axis labels, disclosures, legends alike. Currency abbreviated on axes ($480K), exact in tooltips ($487,231.09).
- **Interpolation**: linear only, never splines. Gaps never bridged — stepped carry-forward is the honest form for balance data.
- **Area fills**: ≤ 8% opacity.
- **Signs**: printed in the glyph, never color alone. Income `+`, spend `−` (U+2212), transfers/savings unsigned in neutral. This applies to chart labels too (sankey, donut legends, stacked bars) — not just Amount components.
- **Color**: three tiers by job (see **Annotation ladder** below) — **gilt** (`--accent-gilt`) fills (bars, areas, dots), **brass** (`--accent-brass`) derived lines and provenance text, **verdigris** interaction only. A lone value-over-time line is brass; a lone bar/area/dot is a gilt fill. Comparisons draw `--chart-1..8` in order, six max, then group to Other. A category keeps its hue in every view (Housing = chart-1 everywhere; see **Category color** below). A single-measure bar chart *of categories* may opt into category hue; every other lone series (e.g. spend-by-weekday) stays gilt. Income/expense pair is `--pos-income`/`--neg-expense`. Never blue as accent.
- **Disclosure over decoration**: a clipped axis says so on the chart ("axis clipped · zero not shown", mono 11px, top-right). Independent scales say so. "Other" says what it absorbed.
- **Focus**: every interactive control gets `outline: 2px solid var(--focus)` on `:focus-visible`. Chart SVGs carry `role="img"` + `aria-label`.

### Annotation ladder

Every mark takes its color from its *job*, not its series:

- **Gilt (`--accent-gilt`) = fills** — bars, areas, marker dots. One bright hex both themes; on the light surface a fill's extent is carried by a 1px `--accent-brass` edge, so gold stays gold instead of darkening to brown to chase the 3:1 floor.
- **Brass (`--accent-brass`) = derived lines and provenance text** — the value-over-time line, avg (dashed) and trend (solid) lines, SQL chips, WHERE labels, overlines. Theme-responsive so it stays legible as text on both surfaces.
- **Verdigris (`--accent-verdigris`) = interaction only** — hover/selected states, pinned-tooltip accents, clickable legend entries. Never a data encoding (its dark value equals `--chart-2`; keep verdigris in chart slot 2 so a categorical chart never double-encodes it).
- **Ink (`--text-secondary` / `--text-faint`) = axes and labels only** — never on bars.

Rationale: ink on a gilt fill is ~1.04:1 (isoluminant), so labels never sit on gilt; brass reads ~1.9:1 on a gilt bar and clears the 3:1 graphics floor on the dark canvas.

### Category color

Categories draw from a fixed map so a category reads as the same hue in every view (stacked, share, donut, ranked, column):

```
Housing=chart-1  Groceries=chart-2  Transport=chart-3  Insurance=chart-4
Dining=chart-5   Utilities=chart-6  Travel=chart-7     Other=chart-8
```

**Single-measure category bars** (ranked or column of one measure) default to **gilt** (bars are fills) — one measure, one color. They *may* opt into coloring each bar by its category hue from the map above: the category label sits beside each bar, so hue is a reinforcing channel, not the sole encoding, and it keeps a category's color consistent across ranked / column / stacked / share / donut. Gilt stays the default; category-hue is the opt-in, and it must use this same fixed map. Non-category single series (spend-by-weekday — days carry no palette identity) stay gilt. (Single-series value-over-time *lines* are brass — a derived line, not a fill.)

The categorical `--chart-1..8` ramp was re-reviewed against a desaturated ramp and a brass-anchored ramp and **stands unchanged**: *charts pop, chrome recedes* — the categorical ramp is the one surface sanctioned to outrank the ink/brass restraint around it; its chroma is intentional, not drift.

## The provenance ladder (three rungs)

1. **SQL chip** — every data widget carries a brass `SQL` chip revealing the exact query, prefixed `-- this number, verbatim`.
2. **Deep-audit strip** — a global toggle adds a one-line `AUDIT` strip to *every* widget: n=, scale/clip range, exclusions, method (e.g. "n=24 monthly observations · axis clipped $400K–$490K · transfers excluded").
3. **Pinned tooltips** — tooltips are ledger rows snapping to real data points (never interpolated positions). Deep audit appends the per-point source ("reports.net_worth_history · 1 row"). Click pins the tooltip; click again releases. Tooltips are the ONLY floating shadow (`--shadow-floating`).

## Per-form rules

### 01 Line & area (value over time) — `charts-line-area.html`
Default for anything temporal. Three stances: **A interpolated** (default, brass 1.75px line, 7% gilt area), **B stepped** as-observed (dots = statements, no interpolation), **C prior-year ghost** (dashed `--text-faint` 1.25px, shared scale, disclosed).

### 02 Cash flow (signed quantity) — `charts-cashflow-diverging.html` — diverging is the default
Income bars up, spending bars down from one emphasized $0 line; net traced in brass with dots on the shared axis. **Canonical net trace** (Overview and Analytics must match — they had drifted): net line `stroke:var(--accent-brass)`, `stroke-width:2.5`, no pointer events (bars own hover); net dots one per month, `r:2.9`, `fill:var(--accent-gilt)` (a marker fill), each with a 1px `var(--bg-surface)` halo (`stroke:var(--bg-surface); stroke-width:1`) so markers read where they cross the bars. Legend glyphs carry the sign ("+ INCOME", "− SPENDING"). **Grouped side-by-side pairs are fine when the sign rides an explicit glyph** — the "+ INCOME" / "− SPENDING" legend and signed labels, never color alone; diverging stays the default because it also puts the sign in the geometry (income up, spending down). Stacked composition: six groups max, Other absorbs the tail.

### 03 Rollup bars (horizontal) — `charts-rollup-bars.html`
One measure stays gilt (bars are fills). A **prior-period tick** per bar marks comparison without a second series: `var(--text-primary)`, ~2px × 14px, 1px radius — a neutral light annotation, **never a `--chart-*` hue** (a palette color would imply a second data series), matching the histogram-median marker convention. It must be **keyed**: a brass swatch for the current period + the light tick labelled with the prior period (e.g. "June" / "May (prior month)"). Scale max + exclusions go in the audit strip. Amounts right-aligned mono with explicit −.

### 04 Sparklines — `charts-sparklines.html`
Shape without axes; **never a number source** — the Amount beside it is. **Amplitude ∝ |30d Δ| ÷ balance, full at 6%**: near-flat accounts render flat instead of dramatizing noise. Scales independent, disclosed in audit.

### 05 Calendar heatmap — `charts-heatmap.html`
Rhythm, not precision. **Five quantized gilt bins, edges $25/$50/$75/$100 per day**, dollar endpoints printed on the legend ("$0 … $100+ / DAY"). Empty days stay `--bg-inset` with a hairline. Debits only, transfers excluded.

### 06 Flow / sankey — `charts-flow.html`
Ribbon height ∝ dollars on one scale. Resting fill-opacity **0.55** (hover 0.8), 2.5px gaps between ribbons at the source node. Labels signed; label rows keep ≥13px separation (collision pass). Categories keep their hues.

### 07 Histogram — `charts-histogram.html`
Shape of spending, not its sum. Uneven bucket widths labeled, never hidden. Median marked on the axis. n= printed in the meta; counts printed above bars in deep audit. Single measure = gilt fill at 85% (count labels stay brass).

### 08 Waterfall — `charts-waterfall.html`
Signed pair colors for flows, one categorical hue for market effects, computed anchors (`--bg-inset` + strong border). Dashed carry links. Clipped baseline printed on the chart.

### 09 Small multiples — `charts-small-multiples.html`
Independent y-scales disclosed in the meta AND printed per tile in deep audit ("y: $618 – $728 /mo"). Δ color follows expense semantics — red = spending up, green = down — and the audit strip says so. True zero renders `±0.0%`.

### 10 Proportion — `charts-proportion.html`
Stacked bar (one straight axis) is the default; 2px gaps between segments. Donut tolerated: thin ring (15px stroke on 62px radius), six slices max, mono labels, **signed** total in the center. Legend amounts signed and colored `--neg-expense`.

The remaining two cards are cross-cutting, not a single form: `charts-grammar.html`
(the grammar exemplar) and `charts-provenance.html` (the provenance ladder above).

## Chart-type per report (authoring)

A saved report declares the chart forms that fit its **data shape**, in recommended order; the first is the recommended form. The report builder offers **exactly** those forms, marks the first as recommended, and **never disables** the alternates — the recommendation encodes the honest read; the alternates stay available because provenance, not paternalism, is the system's stance. Selecting a report resets the chart type to its recommendation.

| Report data shape | Chart types (first = recommended; § = per-form rule) |
|---|---|
| time series | Line (§01), Step (§01) |
| signed months (income / spend / net) | Diverging (§02), Grouped (§02), Net line (§02) |
| category × month | Stacked (§10), Share (§10), Donut (§10) |
| ranked categories (+ prior) | Ranked (§03), Column (§03), Share (§10), Donut (§10) |
| single-series categories | Column (§03), Ranked (§03) |
| daily | Heatmap (§05) |

Every name maps to a per-form rule above — no new forms: **Step** = §01's stepped/as-observed stance · **Net line** = §02's signed net trace drawn on its own axis · **Grouped** = §02's grouped pair · **Share** = §10's full-width single-bar proportion (vs. Stacked's per-month columns) · **Column** = §03's rollup bar rotated vertical (same grammar) · **Ranked** = §03 as specimen'd (horizontal).

## Selection (anchored dim, no outlines)

Selecting a mark never draws on the canvas. The selected mark keeps its full hue and geometry; every non-selected mark drops to **35% opacity**; and exactly **one** verdigris furniture anchor names the selected member, by fixed precedence:

1. **Axis tick + label** — when the member is a position on a labeled axis (a column, bar, histogram bucket, heatmap week, or point on a time line): a 2px verdigris tick under the position, its axis label turned verdigris. The default.
2. **Legend entry** — when the selection is a *category across marks* (stacked, multi-series): the swatch row already names it, so per-mark ticks would multiply.
3. **Amount readout** — when the form has neither an axis row nor a legend (allocation band, donut slice).

Never more than one anchor. Hover is distinct and dims nothing — it shows the ledger-row tooltip. Hue is never repainted; no strokes, rings, or inset outlines on marks, ever (a stroke shrinks the mark and falsifies the encoding; a ring is ornament that grows with the selection). Where a chart selection filters an adjacent table, the tick, the verdigris-tinted rows, and the WHERE pill share one verdigris thread.

## Interaction rules (not visible in the static specimens)

- Tooltips snap to real data points only — never interpolated x positions.
- Deep audit is ONE global toggle that adds the `AUDIT` strip to every widget at once — not a per-widget control.
- Density (32px compact / 40px cozy rows) is a setting that must **not reflow** layouts — same grammar at every size.
- `focus-visible` = `2px solid var(--focus)`, 2px offset. Chart SVGs get `role="img"` + `aria-label`.

## Density & layout

- Rows: `--row-compact` 32px (app default), `--row-cozy` 40px (a setting, not a redesign), 44px touch minimum.
- Widgets: `--bg-surface`, 1px `--border-hairline`, radius `--r-card`, no resting shadows. Dashboard cards stretch to equal row heights (`height:100%`).
- Table columns give identifying columns (institution, merchant) generous minimums — truncating the audit column defeats the table.

## Reference files

- **The twelve `charts-*.html` specimen cards in `guidelines/`** — one per settled decision; each `@dsCard` header carries its rule text.
- **`Analytics Perspective v2.dc.html`** — the full interactive spec (10 chart forms × stances, deep-audit toggle, composed dashboard at both densities and themes). It lives in the Claude Design project, **not this repo**, and will not render standalone. Read it as source, not by opening it. When building the live analytics screen, port these reference implementations from its `<script data-dc-script>` rather than re-deriving:
  - `buildNw()` — line-chart scale/tick/clip computation + dynamic audit text.
  - sparkline amplitude — `amp = min(1, (|30dΔ|/balance) / 0.06)`.
  - heatmap quantization — 5 bins, $25 edges, opacities `[.12, .32, .55, .78, 1]`.
  - sankey label collision — `label y = max(nodeCenter, prev + 13)`.
  - tooltip pin/release state machine — click pins, click releases, hover no-ops while pinned.
- **`../tokens/`** (colors, typography, shape) — all values; never hardcode hex.
