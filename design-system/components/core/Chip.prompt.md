Bordered chip in three registers: `category` (transaction category tags, radius 2), `sql` (the brass provenance chip — part of the trust-as-furniture system), `meta` (status chips like "synced 4 min ago", ⌘K).

```jsx
<Chip>Groceries</Chip>
<Chip variant="sql" active={open} onClick={toggle} />
<Chip variant="meta">synced 4 min ago</Chip>
```
