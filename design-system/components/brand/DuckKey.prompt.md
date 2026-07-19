The reusable duck-key glyph (see guidelines/brand-duckkey.html). Mono for app chrome, full-color eyed for docs/marketing.

```jsx
<DuckKey size={24} />                          {/* brand-gold mono — unlock affordance */}
<DuckKey size={24} color="var(--text-secondary)" />
<DuckKey size={48} variant="full" />           {/* docs/marketing */}
```

Never rotate it; the bill always points right. The vault-unlock animation (key seats into keyhole) is the one sanctioned brand animation.

## The key seat — the sanctioned animation and its only home

The vault unlock is the system's one ceremony (`motion.md`). This is its pinned
spec; it exists nowhere else, and "never rotate it" holds everywhere but here.

- **One transform**, not a sequence: `translateY(-10px → 11px)` and
  `rotate(0 → 90deg)`, easing `--motion-ease`, `fill-mode: forwards`.
- **Duration** `--motion-ceremony` (400ms) — the ceiling, not a target.
- **Once per unlock, success only.** A wrong passphrase gets the error fact and
  no animation.
- Facts and the `VaultStatusBar` engage **after** the seat completes; the unlock
  screen shows zero data until it does.
- `prefers-reduced-motion: reduce` resolves the unlock **instantly** — the state
  change still reports, the ceremony does not play.
