export function VaultStatusBar({ file = 'vault.duckdb', cipher = 'AES-256-GCM', rows, accounts, syncText, style }) {
  return (
    <div style={{
      borderTop: '1px solid var(--border-hairline)',
      background: 'var(--bg-inset)',
      padding: '7px 28px',
      display: 'flex',
      alignItems: 'center',
      gap: '20px',
      fontFamily: 'var(--font-data)',
      fontSize: '11px',
      color: 'var(--text-faint)',
      ...style,
    }}>
      <span style={{ display: 'flex', alignItems: 'center', gap: '7px', color: 'var(--pos-income)' }}>
        <span style={{ width: '6px', height: '6px', borderRadius: '50%', background: 'var(--pos-income)' }}></span>
        {file} — encrypted {cipher}
      </span>
      {rows != null ? <span>{rows.toLocaleString('en-US')} rows{accounts != null ? ' · ' + accounts + ' accounts' : ''}</span> : null}
      {syncText ? <span>{syncText}</span> : null}
      <span style={{ marginLeft: 'auto', color: 'var(--text-secondary)' }}>local only · no telemetry · AGPL</span>
    </div>
  );
}
