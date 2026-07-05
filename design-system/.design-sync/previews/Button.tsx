import { Button } from 'moneybin-design-system';

// Dark theme leads (audience lives in dark editors); the cell renders on light,
// so wrap in the base surface to show the real rendering.
const panel = {
  background: 'var(--bg-base)',
  padding: '20px 24px',
  borderRadius: 6,
  display: 'flex',
  flexWrap: 'wrap' as const,
  gap: 12,
  alignItems: 'center',
};

export const Variants = () => (
  <div style={panel}>
    <Button variant="primary">Add widget</Button>
    <Button variant="secondary">Export CSV</Button>
    <Button variant="ghost">View all →</Button>
    <Button variant="primary" disabled>Add widget</Button>
    <Button variant="secondary" size="sm">Small</Button>
  </div>
);
