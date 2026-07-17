Bordered chip in three registers: `category` (transaction category tags, radius 2), `sql` (the brass provenance chip — part of the trust-as-furniture system), `meta` (status chips like "synced 4 min ago", ⌘K).

```jsx
<Chip>Groceries</Chip>
<Chip active onClick={toggle}>Groceries</Chip>   // selected filter → verdigris text + border
<Chip variant="sql" active={open} onClick={toggle} />
<Chip variant="meta">synced 4 min ago</Chip>
```

A selected/active `category` chip renders in verdigris (the interaction tier); the `sql` chip stays brass whether open or not (provenance is identity, not interaction).
