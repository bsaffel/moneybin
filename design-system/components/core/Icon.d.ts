/**
 * The single icon vocabulary. 20×20 grid, 1.5px stroke, squared caps, no fills;
 * color inherits currentColor. New glyphs are a system change, never an inline
 * one-off SVG.
 *
 * Only the 19 shipped core glyphs are typed. Icon.jsx also carries a reserve set
 * drawn ahead of need; a reserve glyph is promoted into this union only when a
 * shipping surface needs it.
 */
export type IconName =
  | 'home'         // overview / dashboard
  | 'accounts'     // accounts index
  | 'transactions' // transactions list
  | 'reports'      // reports & analytics
  | 'investments'  // investments (upcoming)
  | 'budgets'      // budgets (upcoming)
  | 'console'      // SQL console (nav only — inline ask stays the ▸_ text glyph)
  | 'settings'
  | 'vault'        // vault / lock state
  | 'key'          // unlock action
  | 'search'
  | 'add'
  | 'close'
  | 'chevron'      // disclosure; rotate via direction
  | 'pin'          // pin widget to overview
  | 'sync'         // manual sync / refresh
  | 'import'       // import CSV / OFX
  | 'export'       // export / download
  | 'sidebar';     // collapse / expand rail

export interface IconProps {
  name: IconName;
  /** Rendered box in px. 16 in controls and table rows, 20 in nav rails. Default 20. */
  size?: number;
  /** chevron only. Default 'right'. */
  direction?: 'right' | 'down' | 'left' | 'up';
  /** Accessible label. Required when the icon appears without visible text. */
  title?: string;
  style?: React.CSSProperties;
}
export declare function Icon(props: IconProps): JSX.Element;
export declare namespace Icon {
  const names: IconName[];
}
