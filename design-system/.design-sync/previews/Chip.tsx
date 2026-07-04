import { Chip } from 'moneybin-design-system';

const panel = {
  background: 'var(--bg-base)',
  padding: '20px 24px',
  borderRadius: 6,
  display: 'flex',
  flexWrap: 'wrap' as const,
  gap: 12,
  alignItems: 'center',
};

export const Registers = () => (
  <div style={panel}>
    <Chip>Groceries</Chip>
    <Chip>Travel</Chip>
    <Chip>Transfer ⇄</Chip>
    <Chip variant="sql" />
    <Chip variant="sql" active />
    <Chip variant="meta">synced 4 min ago</Chip>
    <Chip variant="meta">⌘K</Chip>
  </div>
);
