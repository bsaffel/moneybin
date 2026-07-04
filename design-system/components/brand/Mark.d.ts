/**
 * The MoneyBin logo mark — "coin & slot": solid coin poised over a slot cut through the rounded-square plate.
 * The slot is a true cut (mask), so the background shows through.
 */
export interface MarkProps {
  /** Rendered square size in px. Default 44. */
  size?: number;
  /**
   * Plate color. Omit for theme-aware auto-contrast — plate and coin follow
   * the `--mark-*` tokens (paper plate on dark surfaces, ink plate on light),
   * so the mark is always legible without the caller choosing. Pass 'dark'
   * (ink plate, for light surroundings) or 'light' (paper plate, for dark
   * surroundings) to force a specific plate regardless of theme.
   */
  plate?: 'dark' | 'light';
  style?: React.CSSProperties;
}
export declare function Mark(props: MarkProps): JSX.Element;
