The shell for every dashboard widget — carries the trust-as-furniture affordances (overline title + brass SQL chip revealing the exact query). Compose chart/table content as children.

The optional `audit` prop renders the deep-audit strip — rung 2 of the three-rung provenance ladder (SQL chip → deep-audit strip → pinned tooltips; see `charts.md`). In-app the strip is driven by the ONE global deep-audit toggle, never a per-widget control; nothing renders when `audit` is omitted.

```jsx
<WidgetCard title="SPEND BY CATEGORY" meta="June 2026"
  sql="SELECT category, sum(amount) FROM txns WHERE month = '2026-06' GROUP BY 1;"
  audit="n=1,732 debits · June 2026 · transfers excluded">
  {/* bars, tables, charts */}
</WidgetCard>
```

No shadows; hairline border does the work. Padding 20px 24px, radius 6.
