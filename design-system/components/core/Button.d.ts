/**
 * MoneyBin button. Brass primary (the only large brass fill allowed), bordered secondary, ghost tertiary.
 * @startingPoint section="Core" subtitle="Primary / secondary / ghost buttons" viewport="700x150"
 */
export interface ButtonProps {
  /** Visual weight. Default 'primary'. */
  variant?: 'primary' | 'secondary' | 'ghost';
  /** 'sm' 24px, 'md' 30px. Default 'md'. */
  size?: 'sm' | 'md';
  disabled?: boolean;
  /** Native button type. Default 'button' — set 'submit' only for a form's submit action. */
  type?: 'button' | 'submit' | 'reset';
  onClick?: () => void;
  children?: React.ReactNode;
  style?: React.CSSProperties;
}
export declare function Button(props: ButtonProps): JSX.Element;
