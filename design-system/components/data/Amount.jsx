export function Amount({ value, currency = '$', kind, arrow = false, auditable = false, size = 'md', style }) {
  const k = kind || (value > 0 ? 'income' : value < 0 ? 'expense' : 'neutral');
  const colors = {
    income: 'var(--pos-income)',
    expense: 'var(--neg-expense)',
    neutral: 'var(--text-secondary)',
    plain: 'var(--text-primary)',
  };
  const sizes = { sm: '11px', md: '12.5px', lg: '15px', hero: 'var(--text-hero-amount-size)' };
  const abs = Math.abs(value).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  const sign = k === 'income' ? '+' : k === 'expense' || value < 0 ? '\u2212' : '';
  const arr = arrow ? (k === 'income' ? ' \u25B2' : k === 'expense' ? ' \u25BC' : '') : '';
  return (
    <span style={{
      fontFamily: 'var(--font-data)',
      fontSize: sizes[size],
      fontWeight: size === 'hero' ? 500 : 400,
      letterSpacing: size === 'hero' ? '-0.02em' : 0,
      color: colors[k],
      borderBottom: auditable ? '1px dotted var(--text-faint)' : 'none',
      paddingBottom: auditable ? '2px' : 0,
      fontVariantNumeric: 'tabular-nums',
      whiteSpace: 'nowrap',
      ...style,
    }}>{sign}{currency}{abs}{arr}</span>
  );
}
