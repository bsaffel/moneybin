Renders a money figure per the hard rules: always mono, tabular, with an explicit sign on directional flows (income `+`, expense `−`). Balances and hero figures (kind `plain`) and transfers (kind `neutral`) are unsigned — a balance has no flow direction. Use for EVERY amount — table cells, deltas, hero figures.

```jsx
<Amount value={6240} arrow />                      {/* +$6,240.00 ▲ in pos-income */}
<Amount value={-428.6} />                          {/* −$428.60 in neg-expense */}
<Amount value={487231.09} kind="plain" size="hero" auditable />  {/* $487,231.09 — unsigned balance */}
<Amount value={-2000} kind="neutral" />            {/* $2,000.00 — transfer, unsigned, text-secondary */}
```
