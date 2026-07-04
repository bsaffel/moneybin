const { useState } = React;

export function WidgetCard({ title, sql, meta, children, style }) {
  const [sqlOpen, setSqlOpen] = useState(false);
  return (
    <div style={{
      background: 'var(--bg-surface)',
      border: '1px solid var(--border-hairline)',
      borderRadius: 'var(--r-card)',
      padding: '20px 24px',
      ...style,
    }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
        <div style={{ fontFamily: 'var(--font-data)', fontSize: 'var(--text-overline-size)', letterSpacing: 'var(--text-overline-tracking)', color: 'var(--text-secondary)' }}>{title}</div>
        {sql ? (
          <span
            onClick={() => setSqlOpen(!sqlOpen)}
            role="button"
            tabIndex={0}
            aria-expanded={sqlOpen}
            onKeyDown={(e) => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); setSqlOpen(!sqlOpen); } }}
            style={{ fontFamily: 'var(--font-data)', fontSize: '10px', color: 'var(--accent-brass)', border: '1px solid ' + (sqlOpen ? 'var(--accent-brass)' : 'var(--border-strong)'), borderRadius: '3px', padding: '2px 7px', cursor: 'pointer', userSelect: 'none' }}
          >SQL</span>
        ) : null}
        {meta ? <div style={{ marginLeft: 'auto', fontFamily: 'var(--font-data)', fontSize: '11px', color: 'var(--text-faint)' }}>{meta}</div> : null}
      </div>
      {sqlOpen && sql ? (
        <pre style={{ margin: '12px 0 0 0', fontFamily: 'var(--font-data)', fontSize: '11.5px', lineHeight: 1.7, color: 'var(--text-secondary)', background: 'var(--bg-inset)', border: '1px solid var(--border-hairline)', borderRadius: 'var(--r-control)', padding: '8px 12px', whiteSpace: 'pre-wrap' }}>{'-- this number, verbatim\n' + sql}</pre>
      ) : null}
      <div style={{ marginTop: '14px' }}>{children}</div>
    </div>
  );
}
