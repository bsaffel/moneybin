MODEL (
  name prep.int_investment_events__opening_projection,
  dialect duckdb,
  kind VIEW,
  depends_on (
    app.account_links,
    app.investment_event_members,
    app.security_links,
    prep.int_investment_events__source_projection,
    prep.stg_plaid__investment_revisions,
    raw.plaid_investment_holding_lots,
    raw.plaid_investment_holdings
  )
);

@golden_opening_projection() /* The macro expands the same canonical opening SQL used by the transition builder. */
