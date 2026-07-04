import { Mark } from 'moneybin-design-system';

// Coin & slot logo. plate="light" (paper plate, dark-brass coin) reads on dark
// surroundings; plate="dark" (ink plate, brass coin) reads on light — shown on
// a paper swatch so both plates are legible.
export const Plates = () => (
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
    <Mark size={48} plate="light" />
    <span style={{ background: '#F6F4EF', borderRadius: 8, padding: 10, display: 'inline-flex' }}>
      <Mark size={48} plate="dark" />
    </span>
  </div>
);
