The terminal-style status line at the bottom of every app surface — the most visible piece of the trust story.

```jsx
<VaultStatusBar rows={42318} accounts={9} syncText="plaid broker: synced 4 min ago" />
```

Right side always ends "local only · no telemetry · AGPL".

## Lifecycle states — app-owned in v1

The vault lifecycle defines states this API does not carry. They ship **app-owned
for v1** — do not add state props to `VaultStatusBar.jsx` now; the component is
promoted only after the states survive v1 unchanged. Recorded here so the API
does not fossilize without them and the v2 promotion has a written contract.

- **Locked.** The bar shows the lock fact only — no rows, no accounts, no sync
  text. The unlock screen shows **zero data** until the key seats: no row counts,
  no account names, nothing derived from the encrypted file.
- **Unlocking.** Engages with the key-seat ceremony (≤400ms, `--motion-ceremony`,
  see `DuckKey.prompt.md`), once per unlock, **success only** — a wrong passphrase
  gets the error fact, never the animation. Facts and the bar engage only after
  the seat completes.
- **Unlocked.** The current form — file · cipher · rows · accounts · sync.

**Manual lock** is `⌘L` or the key control: an icon-only control, so `title` is
mandatory per the iconography grammar. An **auto-lock timeout** is proposed, not
settled. Both variants of the bar (app floor, spec footer) carry the same three
states; see `patterns.md` §06 for the variant anatomy and privacy mode.
