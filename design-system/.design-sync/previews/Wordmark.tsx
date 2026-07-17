import { Wordmark } from 'moneybin-design-system';

// The three size presets. Mark size and gap are paired to each wordmark size.
export const Sizes = () => (
  <div
    style={{
      background: 'var(--bg-base)',
      padding: 24,
      borderRadius: 6,
      display: 'flex',
      flexDirection: 'column',
      gap: 24,
      alignItems: 'flex-start',
    }}
  >
    <Wordmark size="hero" />
    <Wordmark size="nav" />
    <Wordmark size="bar" />
  </div>
);

// bin="mono" — when another gold element shares the bar, drop the gold on "Bin".
export const Mono = () => (
  <div
    style={{
      background: 'var(--bg-base)',
      padding: 24,
      borderRadius: 6,
      display: 'flex',
      gap: 28,
      alignItems: 'center',
    }}
  >
    <Wordmark size="nav" />
    <Wordmark size="nav" bin="mono" />
  </div>
);
