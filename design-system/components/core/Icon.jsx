// MoneyBin Icon — the single icon vocabulary. 20×20 grid, 1.5px stroke,
// squared caps, no fills, literal metaphors. New glyphs are a system change:
// draw to this grammar and add them here — never inline a one-off SVG.
// Nearest CDN substitute if unavoidable: Lucide restroked to 1.5px (flag it).

const GLYPHS = {
  // navigation
  home: (
    <>
      <rect x="2.75" y="2.75" width="6" height="6" rx="1" />
      <rect x="11.25" y="2.75" width="6" height="6" rx="1" />
      <rect x="2.75" y="11.25" width="6" height="6" rx="1" />
      <rect x="11.25" y="11.25" width="6" height="6" rx="1" />
    </>
  ),
  accounts: (
    <>
      <rect x="2.75" y="4.25" width="14.5" height="11.5" rx="1" />
      <line x1="2.75" y1="8" x2="17.25" y2="8" />
    </>
  ),
  transactions: (
    <>
      <line x1="3" y1="5.5" x2="17" y2="5.5" />
      <line x1="3" y1="10" x2="17" y2="10" />
      <line x1="3" y1="14.5" x2="12" y2="14.5" />
    </>
  ),
  reports: (
    <>
      <line x1="3" y1="3.5" x2="3" y2="17" />
      <line x1="3" y1="17" x2="17" y2="17" />
      <line x1="6.5" y1="10.5" x2="6.5" y2="17" />
      <line x1="10.5" y1="6.5" x2="10.5" y2="17" />
      <line x1="14.5" y1="3" x2="14.5" y2="17" />
    </>
  ),
  investments: <polyline points="3 15 8 8.75 11.5 11.5 17 4.5" />,
  budgets: (
    <>
      <rect x="2.75" y="7.25" width="14.5" height="5.5" />
      <line x1="8.5" y1="7.25" x2="8.5" y2="12.75" />
      <line x1="12.75" y1="7.25" x2="12.75" y2="12.75" />
    </>
  ),
  console: (
    <>
      <polyline points="4.5 5.5 9 10 4.5 14.5" />
      <line x1="11.5" y1="14.5" x2="16.5" y2="14.5" />
    </>
  ),
  settings: (
    <>
      <circle cx="10" cy="10" r="7" />
      <circle cx="10" cy="10" r="1.5" fill="currentColor" stroke="none" />
    </>
  ),
  // trust
  vault: (
    <>
      <rect x="3" y="3" width="14" height="14" rx="2.5" />
      <circle cx="10" cy="8.75" r="1.9" />
      <line x1="10" y1="10.65" x2="10" y2="13.25" />
    </>
  ),
  key: (
    <>
      <circle cx="7.5" cy="10" r="4.75" />
      <rect x="12.25" y="8.5" width="5" height="3" rx="1.5" />
    </>
  ),
  // actions
  search: (
    <>
      <circle cx="9" cy="9" r="5.25" />
      <line x1="12.9" y1="12.9" x2="17" y2="17" />
    </>
  ),
  add: (
    <>
      <line x1="10" y1="3.5" x2="10" y2="16.5" />
      <line x1="3.5" y1="10" x2="16.5" y2="10" />
    </>
  ),
  close: (
    <>
      <line x1="5" y1="5" x2="15" y2="15" />
      <line x1="15" y1="5" x2="5" y2="15" />
    </>
  ),
  chevron: <polyline points="7 4.5 12.5 10 7 15.5" />,
  pin: (
    <>
      <path d="M7 3.25h6v4.25l2 3.5H5l2-3.5z" />
      <line x1="10" y1="11" x2="10" y2="17" />
    </>
  ),
  sync: (
    <>
      <path d="M17 10a7 7 0 0 0-12-4.9" />
      <polyline points="5 1.9 5 5.1 8.2 5.1" />
      <path d="M3 10a7 7 0 0 0 12 4.9" />
      <polyline points="15 18.1 15 14.9 11.8 14.9" />
    </>
  ),
  import: (
    <>
      <line x1="10" y1="3.5" x2="10" y2="12" />
      <polyline points="6.5 8.5 10 12 13.5 8.5" />
      <path d="M3.5 12.5v2.75a1.25 1.25 0 0 0 1.25 1.25h10.5a1.25 1.25 0 0 0 1.25-1.25V12.5" />
    </>
  ),
  export: (
    <>
      <line x1="10" y1="3" x2="10" y2="12" />
      <polyline points="6.5 6.5 10 3 13.5 6.5" />
      <path d="M3.5 12.5v2.75a1.25 1.25 0 0 0 1.25 1.25h10.5a1.25 1.25 0 0 0 1.25-1.25V12.5" />
    </>
  ),
  sidebar: (
    <>
      <rect x="2.75" y="3.75" width="14.5" height="12.5" rx="1" />
      <line x1="7.75" y1="3.75" x2="7.75" y2="16.25" />
    </>
  ),

  // ── reserve — drawn ahead of need, dormant until a surface ships. Reserve
  // glyphs render if asked for, but are deliberately absent from CORE_NAMES and
  // from Icon.d.ts: a glyph is promoted to core only when a shipping surface
  // needs it. Do not delete them — they are drawn to the grammar and waiting.
  // transactions & categorization
  tag: (
    <>
      <path d="M3.25 3.25h6.25l7.25 7.25-6.25 6.25-7.25-7.25z" />
      <circle cx="6.9" cy="6.9" r="1.4" />
    </>
  ),
  split: (
    <>
      <line x1="10" y1="3.5" x2="10" y2="8" />
      <path d="M10 8l-4.75 4.75v3.75" />
      <path d="M10 8l4.75 4.75v3.75" />
    </>
  ),
  rule: (
    <>
      <path d="M4.5 3.5v6.75a2 2 0 0 0 2 2h9" />
      <polyline points="12.5 9.25 15.5 12.25 12.5 15.25" />
    </>
  ),
  check: <polyline points="3.5 10.5 8 15 16.5 5.5" />,
  flag: <path d="M5.25 17V3.5h9.5l-2.5 3.25 2.5 3.25h-9.5" />,
  note: (
    <>
      <path d="M4 3h8.75l3.75 3.75V17H4z" />
      <polyline points="12.75 3 12.75 6.75 16.5 6.75" />
    </>
  ),
  receipt: (
    <>
      <path d="M5 3h10v14l-1.67-1.3-1.66 1.3-1.67-1.3-1.67 1.3-1.66-1.3L5 17z" />
      <line x1="7.5" y1="7" x2="12.5" y2="7" />
      <line x1="7.5" y1="10" x2="12.5" y2="10" />
    </>
  ),
  filter: <path d="M3.5 4.5h13l-5 6v5.75l-3-1.75v-4z" />,
  // planning
  goal: (
    <>
      <circle cx="10" cy="10" r="5.5" />
      <circle cx="10" cy="10" r="1.75" />
      <line x1="10" y1="2.5" x2="10" y2="4.5" />
      <line x1="10" y1="15.5" x2="10" y2="17.5" />
      <line x1="2.5" y1="10" x2="4.5" y2="10" />
      <line x1="15.5" y1="10" x2="17.5" y2="10" />
    </>
  ),
  calendar: (
    <>
      <rect x="3" y="4.5" width="14" height="12" rx="1" />
      <line x1="3" y1="8.25" x2="17" y2="8.25" />
      <line x1="6.75" y1="2.75" x2="6.75" y2="5.5" />
      <line x1="13.25" y1="2.75" x2="13.25" y2="5.5" />
    </>
  ),
  repeat: (
    <>
      <path d="M14.5 6.25h-7a3 3 0 0 0-3 3v1" />
      <polyline points="12.25 4 14.5 6.25 12.25 8.5" />
      <path d="M5.5 13.75h7a3 3 0 0 0 3-3v-1" />
      <polyline points="7.75 11.5 5.5 13.75 7.75 16" />
    </>
  ),
  allocation: (
    <>
      <circle cx="10" cy="10" r="7" />
      <line x1="10" y1="10" x2="10" y2="3" />
      <line x1="10" y1="10" x2="16.1" y2="13.4" />
    </>
  ),
  // connections
  link: (
    <>
      <path d="M9 6.5l1.75-1.75a3.18 3.18 0 0 1 4.5 4.5L13.5 11" />
      <path d="M11 13.5l-1.75 1.75a3.18 3.18 0 0 1-4.5-4.5L6.5 9" />
      <line x1="8" y1="12" x2="12" y2="8" />
    </>
  ),
  unlink: (
    <>
      <path d="M11.5 6L13 4.5a3.18 3.18 0 0 1 4.5 4.5L16 10.5" />
      <path d="M8.5 14L7 15.5a3.18 3.18 0 0 1-4.5-4.5L4 9.5" />
      <line x1="8.75" y1="6" x2="7.5" y2="4.75" />
      <line x1="11.25" y1="14" x2="12.5" y2="15.25" />
    </>
  ),
  bank: (
    <>
      <polyline points="3 8.25 10 3.5 17 8.25" />
      <line x1="5.5" y1="10.75" x2="5.5" y2="14" />
      <line x1="10" y1="10.75" x2="10" y2="14" />
      <line x1="14.5" y1="10.75" x2="14.5" y2="14" />
      <line x1="3" y1="16.5" x2="17" y2="16.5" />
    </>
  ),
  // state & alerts
  eye: (
    <>
      <path d="M2.75 10C4.5 6.5 7 4.75 10 4.75s5.5 1.75 7.25 5.25c-1.75 3.5-4.25 5.25-7.25 5.25S4.5 13.5 2.75 10z" />
      <circle cx="10" cy="10" r="2.25" />
    </>
  ),
  'eye-off': (
    <>
      <path d="M2.75 10C4.5 6.5 7 4.75 10 4.75s5.5 1.75 7.25 5.25c-1.75 3.5-4.25 5.25-7.25 5.25S4.5 13.5 2.75 10z" />
      <line x1="4.25" y1="15.75" x2="15.75" y2="4.25" />
    </>
  ),
  bell: (
    <>
      <path d="M10 3.5c-2.9 0-4.75 2-4.75 5v3.75L3.5 14.5h13l-1.75-2.25V8.5c0-3-1.85-5-4.75-5z" />
      <path d="M8.5 16.5a1.6 1.6 0 0 0 3 0" />
    </>
  ),
  warning: (
    <>
      <path d="M10 3.25L17.75 16.5H2.25z" />
      <line x1="10" y1="8.5" x2="10" y2="12" />
      <circle cx="10" cy="14" r="0.9" fill="currentColor" stroke="none" />
    </>
  ),
  // utility
  edit: (
    <>
      <path d="M3.5 16.5l.75-3.25 9.5-9.5 2.5 2.5-9.5 9.5z" />
      <line x1="12.25" y1="5.25" x2="14.75" y2="7.75" />
    </>
  ),
  bin: (
    <>
      <line x1="3.5" y1="5.75" x2="16.5" y2="5.75" />
      <path d="M5.25 5.75L6 16.5h8l.75-10.75" />
      <path d="M8 5.75V3.5h4v2.25" />
    </>
  ),
  duplicate: (
    <>
      <rect x="6.5" y="6.5" width="10" height="10" rx="1" />
      <path d="M3.5 13.5v-9a1 1 0 0 1 1-1h9" />
    </>
  ),
  more: (
    <>
      <circle cx="4.5" cy="10" r="1.1" fill="currentColor" stroke="none" />
      <circle cx="10" cy="10" r="1.1" fill="currentColor" stroke="none" />
      <circle cx="15.5" cy="10" r="1.1" fill="currentColor" stroke="none" />
    </>
  ),
  external: (
    <>
      <polyline points="12 3.5 16.5 3.5 16.5 8" />
      <line x1="16.5" y1="3.5" x2="9.5" y2="10.5" />
      <path d="M14 11.5v4a1 1 0 0 1-1 1H4.5a1 1 0 0 1-1-1V7a1 1 0 0 1 1-1h4" />
    </>
  ),
  user: (
    <>
      <circle cx="10" cy="7" r="3.25" />
      <path d="M4 16.5c0-3 2.5-4.75 6-4.75s6 1.75 6 4.75" />
    </>
  ),
};

// The 19 shipped glyphs. Mirrors the IconName union in Icon.d.ts — keep both in
// step when a reserve glyph is promoted.
const CORE_NAMES = [
  'home', 'accounts', 'transactions', 'reports', 'investments', 'budgets', 'console', 'settings',
  'vault', 'key',
  'search', 'add', 'close', 'chevron', 'pin', 'sync', 'import', 'export', 'sidebar',
];

export function Icon({ name, size = 20, direction = 'right', title, style }) {
  const glyph = GLYPHS[name];
  if (!glyph) return null;
  const rot = { right: 0, down: 90, left: 180, up: 270 }[direction] || 0;
  const body =
    name === 'chevron' && rot ? <g transform={`rotate(${rot} 10 10)`}>{glyph}</g> : glyph;
  return (
    <svg
      width={size}
      height={size}
      viewBox="0 0 20 20"
      role={title ? 'img' : undefined}
      aria-label={title}
      aria-hidden={title ? undefined : true}
      fill="none"
      stroke="currentColor"
      strokeWidth="1.5"
      strokeLinecap="butt"
      strokeLinejoin="miter"
      style={{ display: 'block', flex: 'none', ...style }}
    >
      {body}
    </svg>
  );
}

Icon.names = CORE_NAMES;
