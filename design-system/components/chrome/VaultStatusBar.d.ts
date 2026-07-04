/**
 * The persistent trust status line pinned to the bottom of the app chrome — encryption, vault file, row count, sync freshness.
 * Part of the signature "trust as furniture" system; every app surface ships with it.
 */
export interface VaultStatusBarProps {
  /** Vault filename. Default 'vault.duckdb'. */
  file?: string;
  /** Default 'AES-256-GCM'. */
  cipher?: string;
  rows?: number;
  accounts?: number;
  /** e.g. "plaid broker: synced 4 min ago" */
  syncText?: string;
  style?: React.CSSProperties;
}
export declare function VaultStatusBar(props: VaultStatusBarProps): JSX.Element;
