/**
 * Dashboard widget shell: surface card, overline title, brass SQL provenance chip (toggles the query panel), right-aligned meta.
 * Every widget that shows a number must pass `sql` — a widget that can't state its query doesn't ship.
 * @startingPoint section="Data" subtitle="Widget card with SQL provenance" viewport="700x260"
 */
export interface WidgetCardProps {
  /** Overline title, e.g. "NET WORTH". Rendered mono 11px tracked. */
  title: string;
  /** The exact DuckDB SQL behind this widget's numbers. Toggled by the SQL chip. */
  sql?: string;
  /** Right-aligned meta, e.g. "June 2026". */
  meta?: React.ReactNode;
  /**
   * Optional deep-audit strip — rung 2 of the provenance ladder. One mono line
   * under the widget, e.g. "n=24 monthly observations · axis clipped $400K–$490K
   * · transfers excluded". In-app this is driven by the ONE global deep-audit
   * toggle, never a per-widget control; nothing renders when omitted.
   */
  audit?: string;
  children?: React.ReactNode;
  style?: React.CSSProperties;
}
export declare function WidgetCard(props: WidgetCardProps): JSX.Element;
