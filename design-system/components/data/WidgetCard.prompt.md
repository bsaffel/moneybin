The shell for every dashboard widget — carries the trust-as-furniture affordances (overline title + brass SQL chip revealing the exact query). Compose chart/table content as children.

```jsx
<WidgetCard title="SPEND BY CATEGORY" meta="June 2026"
  sql="SELECT category, sum(amount) FROM txns WHERE month = '2026-06' GROUP BY 1;">
  {/* bars, tables, charts */}
</WidgetCard>
```

No shadows; hairline border does the work. Padding 20px 24px, radius 6.
