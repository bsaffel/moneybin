/**
 * Bordered chip. 'category' for transaction categories, 'sql' for the brass provenance chip, 'meta' for status chips (synced, ⌘K).
 */
export interface ChipProps {
  /** Default 'category'. 'sql' renders the brass SQL provenance chip (defaults to "SQL" text). */
  variant?: 'category' | 'sql' | 'meta';
  /**
   * Selected state. On 'category' → verdigris text + border (an active filter —
   * interaction, per the accent tiers); on 'sql' → brass border when the SQL panel
   * is open (provenance stays brass, never verdigris).
   */
  active?: boolean;
  onClick?: () => void;
  children?: React.ReactNode;
  style?: React.CSSProperties;
}
export declare function Chip(props: ChipProps): JSX.Element;
