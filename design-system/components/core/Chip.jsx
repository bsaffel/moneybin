export function Chip({ variant = 'category', children, onClick, active = false, style }) {
  const base = {
    fontFamily: variant === 'category' ? 'var(--font-ui)' : 'var(--font-data)',
    display: 'inline-flex',
    alignItems: 'center',
    gap: '5px',
    borderRadius: variant === 'sql' ? '3px' : 'var(--r-chip)',
    border: '1px solid var(--border-strong)',
    cursor: onClick ? 'pointer' : 'default',
    userSelect: 'none',
  };
  const variants = {
    category: {
      fontSize: '11px',
      padding: '2px 6px',
      // Selected/active filter chip = interaction → verdigris (§0 tiers). Text sits
      // on the surface, not on --accent-verdigris-tint: verdigris-on-tint is 4.18:1 in
      // light (below AA), verdigris-on-surface is 4.76:1. The tint is for row backgrounds.
      color: active ? 'var(--accent-verdigris)' : 'var(--text-secondary)',
      borderColor: active ? 'var(--accent-verdigris)' : 'var(--border-strong)',
    },
    sql: { fontSize: '10px', color: 'var(--accent-brass)', padding: '2px 7px', borderColor: active ? 'var(--accent-brass)' : 'var(--border-strong)' },
    meta: { fontSize: '11px', color: 'var(--text-secondary)', padding: '5px 10px', borderColor: 'var(--border-hairline)', borderRadius: 'var(--r-control)', fontFamily: 'var(--font-data)' },
  };
  return (
    <span
      style={{ ...base, ...variants[variant], ...style }}
      onClick={onClick}
      role={onClick ? 'button' : undefined}
      tabIndex={onClick ? 0 : undefined}
      onKeyDown={onClick ? (e) => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); onClick(e); } } : undefined}
      onMouseEnter={(e) => { if (onClick && variant === 'sql') e.currentTarget.style.borderColor = 'var(--accent-brass)'; }}
      onMouseLeave={(e) => { if (onClick && variant === 'sql' && !active) e.currentTarget.style.borderColor = 'var(--border-strong)'; }}
    >{variant === 'sql' && !children ? 'SQL' : children}</span>
  );
}
