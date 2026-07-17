export function Button({ variant = 'primary', size = 'md', disabled = false, type = 'button', children, onClick, style }) {
  const base = {
    fontFamily: 'var(--font-ui)',
    fontWeight: 500,
    fontSize: size === 'sm' ? '12px' : '13px',
    padding: size === 'sm' ? '4px 10px' : '6px 14px',
    borderRadius: 'var(--r-control)',
    cursor: disabled ? 'default' : 'pointer',
    opacity: disabled ? 0.45 : 1,
    border: '1px solid transparent',
    display: 'inline-flex',
    alignItems: 'center',
    gap: '7px',
    lineHeight: 1.35,
    userSelect: 'none',
  };
  const variants = {
    // Gilt fill + a 1px brass edge: on the light theme the edge carries the control
    // boundary (4.9:1) where the fill alone is only ~2.5:1; on dark it is a faint engraving.
    primary: { background: 'var(--accent-gilt)', color: 'var(--on-accent-gilt)', border: '1px solid var(--accent-brass)' },
    secondary: { background: 'transparent', color: 'var(--text-primary)', border: '1px solid var(--border-strong)' },
    ghost: { background: 'transparent', color: 'var(--text-secondary)' },
  };
  return (
    <button
      type={type}
      disabled={disabled}
      onClick={disabled ? undefined : onClick}
      style={{ ...base, ...variants[variant], ...style }}
      onMouseEnter={(e) => {
        if (disabled) return;
        if (variant === 'primary') e.currentTarget.style.background = 'var(--accent-gilt-strong)';
        if (variant === 'secondary') e.currentTarget.style.borderColor = 'var(--accent-brass)';
        if (variant === 'ghost') e.currentTarget.style.color = 'var(--text-primary)';
      }}
      onMouseLeave={(e) => {
        if (variant === 'primary') e.currentTarget.style.background = 'var(--accent-gilt)';
        if (variant === 'secondary') e.currentTarget.style.borderColor = 'var(--border-strong)';
        if (variant === 'ghost') e.currentTarget.style.color = 'var(--text-secondary)';
      }}
    >{children}</button>
  );
}
