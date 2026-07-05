/**
 * The MoneyBin wordmark lock-up — the Mark paired with the "MoneyBin" name.
 * Promotes the ad-hoc Mark + span pairing to a system component so the two
 * decisions that are easy to get wrong stay correct by construction: the
 * optical baseline nudge (Rule 1) and brass on "Bin" (Rule 2), both baked in.
 */
export interface WordmarkProps {
  /**
   * Size preset — 'nav' (17px), 'bar' (15px, default), or 'hero' (52px) — or an
   * explicit wordmark px. Mark size and gap are paired to the wordmark size;
   * prefer a preset (a raw number derives Mark ≈1.5× and gap ≈0.6× the text).
   */
  size?: 'nav' | 'bar' | 'hero' | number;
  /**
   * Color of the "Bin" letters. 'brass' (default) is the brand default; use
   * 'mono' whenever another brass element shares the same bar or header, so
   * brass stays unique to a single lock-up.
   */
  bin?: 'brass' | 'mono';
  /**
   * Passed through to the Mark. Omit for theme-aware auto-contrast (recommended);
   * pass 'dark' / 'light' to force a plate. See MarkProps.
   */
  plate?: 'dark' | 'light';
  style?: React.CSSProperties;
}
export declare function Wordmark(props: WordmarkProps): JSX.Element;
