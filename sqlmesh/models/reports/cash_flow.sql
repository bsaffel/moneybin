/* Monthly inflow/outflow/net per account x category. Excludes transfers and
   archived accounts. Wide-grain: consumers GROUP BY further as needed. */
MODEL (
  name reports.cash_flow,
  kind VIEW
);

SELECT
  date_trunc('month', t.transaction_date) AS year_month, /* First-of-month for the calendar month */
  t.account_id, /* Owning account (joinable to core.dim_accounts) */
  a.display_name AS account_name, /* Account display name (resolved from app.account_settings if overridden) */
  t.category, /* Spending category text from core.fct_transactions; NULL for uncategorized */
  SUM(CASE WHEN t.amount > 0 THEN t.amount ELSE 0 END) AS inflow, /* Sum of positive amounts in this cell */
  SUM(CASE WHEN t.amount < 0 THEN t.amount ELSE 0 END) AS outflow, /* Sum of negative amounts in this cell (kept negative) */
  SUM(t.amount) AS net, /* inflow + outflow */
  COUNT(*) AS txn_count /* Number of non-transfer transactions in this cell */
FROM core.fct_transactions AS t
INNER JOIN core.dim_accounts AS a ON t.account_id = a.account_id
WHERE NOT t.is_transfer
  AND NOT a.archived
GROUP BY date_trunc('month', t.transaction_date), t.account_id, a.display_name, t.category
