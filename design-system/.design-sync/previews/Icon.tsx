import { Icon } from 'moneybin-design-system';

// Dark theme leads (audience lives in dark editors); the cell renders on light,
// so wrap in the base surface to show the real rendering.
const panel = {
  background: 'var(--bg-base)',
  padding: '20px 24px',
  borderRadius: 6,
  display: 'flex',
  flexWrap: 'wrap' as const,
  gap: 18,
  alignItems: 'center',
  color: 'var(--text-secondary)',
};

// Icons inherit currentColor — the last cell is a toggled-on icon-only control in
// verdigris (interaction). A glyph is never gold; an active nav location is a brass
// edge tick beside an ink glyph, not a brass drawing.
export const Vocabulary = () => (
  <div style={panel}>
    <Icon name="home" />
    <Icon name="accounts" />
    <Icon name="transactions" />
    <Icon name="reports" />
    <Icon name="vault" />
    <Icon name="search" size={16} />
    <Icon name="chevron" direction="down" size={16} />
    <Icon name="sync" size={16} />
    <Icon name="pin" title="Pin to overview" size={16} />
    <span style={{ color: 'var(--accent-verdigris)', display: 'inline-flex' }}>
      <Icon name="console" />
    </span>
  </div>
);
