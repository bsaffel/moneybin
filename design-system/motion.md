# MoneyBin motion doctrine — binding rules

Motion reports a state change — nothing else moves.

The binding grammar for every animated surface: UI transitions, SVG glyph
states, chrome, brand marks. Motion is mechanical, like a well-oiled drawer —
decisive, short, and always the consequence of a state change the user or the
system just made. It is never organic, ambient, or decorative. If a change of
motion doesn't report a change of state, it doesn't ship. Where any other doc's
motion or animation guidance differs, this one wins. Reference tokens by name —
never hardcode a duration, easing, or color.

## Durations & easing

- **State flips: `--motion-fast` (120ms).** Chevron rotate, pin set/unset,
  deep-audit-strip reveal — anything that snaps between two discrete states.
- **Surface changes: `--motion-slow` (180ms).** A drawer sliding, a tooltip
  appearing — a surface entering or leaving. Nothing in chrome runs longer.
- **Easing: `--motion-ease` (`cubic-bezier(0.2, 0, 0, 1)`).** Decisive out, no
  overshoot. This is the only easing curve; there is no "in" variant and no
  bounce.

## Loops

- **`sync` is the only loop, and only while a sync is in flight.** Linear
  rotation over `--motion-sync` (~1.2s per turn), stopping dead at 0° the instant
  the sync completes. Continuous rotation is the one motion that is *not* eased — it
  uses `linear` timing, never `--motion-ease`, since a spinner that accelerates and
  decelerates each turn reads as broken. This is the sole exception to "reference the
  easing token by name."
- **No other looping or ambient motion.** No pulse, no breathing dot, no
  shimmer, no idle drift. A resting screen is still.

## Forbidden motion

- **No springs, no bounce, no overshoot.** `--motion-ease` settles once and
  stops.
- **No hover scale or translate.** Hover changes color only — never geometry.
- **No entrance choreography.** Nothing staggers, fades, or slides in on page or
  list load. Content is simply there.

## Amounts never animate

A number is a fact, not a slot machine.

- **No count-up, no ticking digits, no rolling odometers.** Ever.
- **A value change swaps instantly.** The one permitted "changed" cue is a
  single `--motion-fast` (120ms) fade-out of an `--accent-gilt-tint` wash behind
  the value — the tint marks that the number moved; the digits themselves do not
  animate.

## Glyph-state toggles

Paired icon states (eye ↔ eye-off today, any future pair) animate **only the
strokes that differ**:

- The differing stroke draws or undraws via `stroke-dashoffset` over
  `--motion-fast` (120ms) — for eye-off, that is the redaction slash.
- Secondary differing marks (the pupil) fade over the same 120ms.
- **The shared outline never moves.** What is common to both states holds still,
  so the eye reads as one glyph changing state, not two glyphs cross-fading.
- Ships as a composed state inside the `Icon` component — e.g.
  `<Icon name="eye" off>` — never a page-level one-off SVG.
- The masked value behind the toggle swaps instantly, per **Amounts never
  animate** above.

## The vault unlock — the one ceremony

The single sanctioned brand animation. On unlock, the `DuckKey` does a
quarter-turn as it seats into the keyhole and the `VaultStatusBar` engages —
**≤ `--motion-ceremony` (400ms), once per unlock.** This is the sole exception to the DuckKey's
"never rotated" brand rule and the only motion in the system allowed to exceed
chrome durations. It fires on the unlock transition only, never on a resting or
already-unlocked vault.

## Reduced motion

Under `prefers-reduced-motion: reduce`, **all motion is disabled** — flips,
surface changes, glyph draws, the value-change tint, and the vault ceremony all
resolve instantly to their end state.

The one exception is the *meaning* of the `sync` loop, which must survive
without moving: replace the spinning `sync` glyph with a static mono
"syncing…" label. The state is still reported; only the rotation is gone.
