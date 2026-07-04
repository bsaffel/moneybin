Renders a money figure per the hard rules: always mono, tabular, sign explicit. Use for EVERY amount — table cells, deltas, hero figures.

```jsx
<Amount value={6240} arrow />                      {/* +$6,240.00 ▲ in pos-income */}
<Amount value={-428.6} />                          {/* −$428.60 in neg-expense */}
<Amount value={487231.09} kind="plain" size="hero" auditable />
<Amount value={-2000} kind="neutral" />            {/* transfers stay text-secondary */}
```
