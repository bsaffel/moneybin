import { DuckKey } from 'moneybin-design-system';

// Duck-key glyph (negative of the bill-hole keyhole); never rotated, bill right.
// mono (brand gold) and mono (secondary) for app chrome; "full" (eyed) is docs only.
export const Variants = () => (
  <div
    style={{
      background: 'var(--bg-base)',
      padding: '24px',
      borderRadius: 6,
      display: 'flex',
      gap: 28,
      alignItems: 'center',
    }}
  >
    <DuckKey size={40} />
    <DuckKey size={40} color="var(--text-secondary)" />
    <DuckKey size={40} variant="full" />
  </div>
);
