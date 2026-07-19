/**
 * The duck-key glyph — the key that fits the bill-hole keyhole (DuckDB opens the vault).
 * Uses: unlock/lock affordances, API & MCP token settings, the vault-unlock moment, docs/CLI shorthand.
 * Rules: never rotated, bill always points right; 'mono' in app chrome, 'full' (eyed) in docs/marketing only.
 */
export interface DuckKeyProps {
  /** Rendered square size in px. Default 48. */
  size?: number;
  /** 'mono' single-color (app chrome) or 'full' light face + gilt bill + eye (docs/marketing). Default 'mono'. */
  variant?: 'mono' | 'full';
  /** Fill for 'mono'; pick from accent or text ramps. Default var(--brand-gold) — bright gilt on dark, brass on light (an identity mark, like the logo). */
  color?: string;
  style?: React.CSSProperties;
}
export declare function DuckKey(props: DuckKeyProps): JSX.Element;
