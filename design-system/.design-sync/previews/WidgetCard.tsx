import { WidgetCard, Amount } from 'moneybin-design-system';

// The signature "trust as furniture" widget: an overline title, a brass SQL
// provenance chip (click reveals the exact query — interaction-only, so the
// static card shows it closed), and mono amounts.
export const SpendByCategory = () => (
  <div style={{ background: 'var(--bg-base)', padding: '20px 24px', borderRadius: 6 }}>
    <WidgetCard
      title="SPEND BY CATEGORY"
      meta="June 2026"
      sql={"SELECT category, sum(amount) FROM txns\nWHERE month = '2026-06' GROUP BY 1 ORDER BY 2;"}
    >
      <div
        style={{
          display: 'grid',
          gridTemplateColumns: '90px 1fr 80px',
          alignItems: 'center',
          gap: 12,
          fontFamily: 'var(--font-ui)',
          fontSize: 13,
          color: 'var(--text-primary)',
        }}
      >
        <span>Housing</span>
        <div style={{ height: 12, background: 'var(--bg-inset)', borderRadius: 2 }}>
          <div style={{ width: '100%', height: '100%', background: 'var(--chart-1)', borderRadius: 2 }} />
        </div>
        <span style={{ textAlign: 'right' }}>
          <Amount value={-2850} />
        </span>
        <span>Travel</span>
        <div style={{ height: 12, background: 'var(--bg-inset)', borderRadius: 2 }}>
          <div style={{ width: '42%', height: '100%', background: 'var(--chart-2)', borderRadius: 2 }} />
        </div>
        <span style={{ textAlign: 'right' }}>
          <Amount value={-1204} />
        </span>
      </div>
    </WidgetCard>
  </div>
);
