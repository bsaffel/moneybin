/**
 * A money figure. Always JetBrains Mono, always tabular, sign carried by explicit +/− (never color alone).
 * Accounting convention: negative = expense.
 */
export interface AmountProps {
  /** Signed numeric value; sign infers kind unless overridden. */
  value: number;
  /** Currency symbol prefix. Default '$'. */
  currency?: string;
  /** Override inferred semantics. 'plain' = text-primary (hero figures, balances). */
  kind?: 'income' | 'expense' | 'neutral' | 'plain';
  /** Append ▲/▼ (for deltas). Default false. */
  arrow?: boolean;
  /** Dotted underline — marks the figure as auditable (click reveals SQL). */
  auditable?: boolean;
  /** sm 11px, md 12.5px (table default), lg 15px, hero 46px/500. */
  size?: 'sm' | 'md' | 'lg' | 'hero';
  style?: React.CSSProperties;
}
export declare function Amount(props: AmountProps): JSX.Element;
