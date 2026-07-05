Standard MoneyBin button; use `primary` (brass) for the single main action on a surface, `secondary` for everything else, `ghost` for tertiary/inline actions.

```jsx
<Button variant="primary" onClick={add}>Add widget</Button>
<Button variant="secondary" size="sm">Export CSV</Button>
```

Notes: never place two primaries side by side; no exclamation points in labels; icons inherit the 20×20/1.5px stroke grammar.
