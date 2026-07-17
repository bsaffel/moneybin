import { VaultStatusBar } from 'moneybin-design-system';

// Full-width persistent trust status line — static vault status dot, encrypted file,
// row/account counts, sync text, and the local-only / no-telemetry / AGPL creed.
export const Default = () => (
  <div style={{ background: 'var(--bg-base)', paddingTop: 28 }}>
    <VaultStatusBar rows={42318} accounts={9} syncText="plaid broker: synced 4 min ago" />
  </div>
);
