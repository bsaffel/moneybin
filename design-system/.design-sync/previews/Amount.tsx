import { Amount } from 'moneybin-design-system';

// Money is always JetBrains Mono with explicit +/- signs — this card is the
// font/sign-convention test: income green +, expense red −, tabular figures.
const panel = {
  background: 'var(--bg-base)',
  padding: '20px 24px',
  borderRadius: 6,
  display: 'flex',
  gap: 28,
  alignItems: 'baseline',
};

export const Semantics = () => (
  <div style={panel}>
    <Amount value={6240} arrow />
    <Amount value={-428.6} arrow />
    <Amount value={-2000} kind="neutral" />
    <Amount value={487231.09} kind="plain" size="hero" auditable />
  </div>
);

export const Sizes = () => (
  <div style={panel}>
    <Amount value={-42.5} size="sm" />
    <Amount value={-42.5} size="md" />
    <Amount value={-42.5} size="lg" />
    <Amount value={1240} size="hero" />
  </div>
);
