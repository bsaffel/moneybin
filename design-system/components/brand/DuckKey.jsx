export function DuckKey({ size = 48, variant = 'mono', color = 'var(--accent-brass)', style }) {
  // Duck-key: circle head + rounded-pill bill pointing right — the exact negative of the bill-hole keyhole.
  // Never rotated; bill always points right. 'full' (light face, brass bill, eye) is docs/marketing only.
  const head = variant === 'full' ? '#E9E4DB' : color;
  return (
    <svg width={size} height={size} viewBox="0 0 48 48" style={style}>
      <circle cx="12.5" cy="23.5" r="8.5" fill={head} />
      <rect x="18" y="20" width="18" height="7" rx="3.5" fill={variant === 'full' ? '#C79B3B' : color} />
      {variant === 'full' ? <circle cx="13.5" cy="21" r="1.5" fill="#141311" /> : null}
    </svg>
  );
}
