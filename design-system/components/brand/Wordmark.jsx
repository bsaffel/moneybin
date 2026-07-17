import { Mark } from './Mark.jsx';

// Size presets → [wordmark px, Mark px, gap px]. The three numbers are a
// normative set: never pair a wordmark size with a foreign Mark size or gap.
// A numeric `size` is the wordmark px and derives Mark ≈1.5× / gap ≈0.6×
// (reproduces the 'nav' preset; a rough escape hatch — prefer a named preset).
const PRESETS = {
  nav: [17, 26, 10],
  bar: [15, 22, 9],
  hero: [52, 52, 20],
};

export function Wordmark({ size = 'bar', bin = 'gold', plate, style }) {
  const [wm, mk, gap] =
    typeof size === 'number'
      ? [size, Math.round(size * 1.5), Math.round(size * 0.6)]
      : PRESETS[size] ?? PRESETS.bar;
  // "Bin" carries --brand-gold by default — the wordmark is identity text, so it
  // is bright gilt on dark and deepens to brass on light (like the logo mark, not
  // a flat fill). Switch to 'mono' when another gold element shares the bar.
  const binColor = bin === 'mono' ? 'var(--text-primary)' : 'var(--brand-gold)';
  return (
    <span style={{ display: 'inline-flex', alignItems: 'center', gap: gap + 'px', ...style }}>
      <Mark size={mk} plate={plate} />
      {/* Optical baseline nudge (spec Rule 1): Newsreader's "y" descender sits
          the wordmark high against the square Mark under align-items:center.
          Nudge the text ONLY — never the Mark or the whole lock-up. em-relative
          so it scales with size. Baked in (not a prop) so it can't be dropped. */}
      <span
        style={{
          fontFamily: 'var(--font-display)',
          fontWeight: 600,
          fontSize: wm + 'px',
          letterSpacing: '-0.012em',
          color: 'var(--text-primary)',
          transform: 'translateY(0.06em)',
          display: 'inline-block',
        }}
      >
        Money<span style={{ color: binColor }}>Bin</span>
      </span>
    </span>
  );
}
