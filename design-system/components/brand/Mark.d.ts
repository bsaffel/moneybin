/**
 * The MoneyBin logo mark — "coin & slot": solid coin poised over a slot cut through the rounded-square plate.
 * The slot is a true cut (mask), so the background shows through.
 */
export interface MarkProps {
  /** Rendered square size in px. Default 44. */
  size?: number;
  /** Plate color: 'light' (paper plate, for dark surroundings — app default) or 'dark' (ink plate, for light surroundings). Default 'dark'. */
  plate?: 'dark' | 'light';
  style?: React.CSSProperties;
}
export declare function Mark(props: MarkProps): JSX.Element;
