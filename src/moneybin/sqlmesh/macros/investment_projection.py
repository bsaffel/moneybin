"""SQL Golden opening projection from retained membership evidence."""

from sqlglot import exp, parse_one
from sqlmesh import macro
from sqlmesh.core.macros import MacroEvaluator

from moneybin.investments.opening_sql import opening_query


@macro()
def golden_opening_projection(evaluator: MacroEvaluator) -> exp.Expr:
    """Expand the canonical opening calculation without querying a database."""
    transactions = """
        SELECT n.source_account_key, n.source_security_key, n.source_origin,
               p.quantity, p.type, p.trade_date, TRUE AS ledger_include
        FROM JSON_EACH(m.reconstruction_evidence, '$.source_member_ids') AS j
        JOIN app.investment_event_members AS s ON s.membership_id = (j.value ->> '$')
        JOIN prep.stg_plaid__investment_revisions AS n
          ON n.source_type = s.source_type AND n.source_origin = s.source_origin
          AND n.investment_transaction_id = s.native_reference AND n.observation_version = s.observation_version
        JOIN prep.int_investment_events__source_projection AS p
          ON p.projection_scope = m.membership_id AND p.investment_transaction_id = s.investment_transaction_id
        WHERE s.source_type = 'plaid'
        UNION ALL
        SELECT n.source_account_key, n.source_security_key, n.source_origin,
               n.quantity, n.type, n.trade_date, n.ledger_include
        FROM JSON_EACH(m.reconstruction_evidence, '$.split_revisions') AS j
        JOIN prep.stg_plaid__investment_revisions AS n
          ON n.source_type = (j.value ->> '$.source_type')
          AND n.source_origin = (j.value ->> '$.source_origin')
          AND n.investment_transaction_id = (j.value ->> '$.native_reference')
          AND n.observation_version = (j.value ->> '$.observation_version')
    """
    snapshot = """SELECT m.reconstruction_evidence ->> '$.snapshot.account_id' AS account_id,
                         m.reconstruction_evidence ->> '$.snapshot.source_origin' AS source_origin,
                         m.reconstruction_evidence ->> '$.snapshot.source_file' AS source_file"""
    reconstruction = opening_query(transactions, snapshot).sql(dialect="duckdb")
    return parse_one(
        f"""
        SELECT m.investment_transaction_id, m.bound_account_id AS account_id,
               m.bound_security_id AS security_id, m.event_group_id,
               r.type, r.subtype, r.provider_type, r.provider_subtype,
               m.source_type, m.source_origin, r.description,
               m.bound_effective_currency AS currency_code,
               r.quantity, r.price, r.amount, r.fees,
               r.trade_date, r.settlement_date, r.original_acquisition_date,
               m.created_at, m.projection_changed_at AS updated_at
        FROM app.investment_event_members AS m
        CROSS JOIN LATERAL ({reconstruction}) AS r
        WHERE m.retired_at IS NULL AND m.member_kind = 'opening_lot_reconstruction'
          AND r.lot_key = (m.reconstruction_evidence ->> '$.lot_key')
          AND r.source_security_key = (m.reconstruction_evidence ->> '$.source_security_key')
          AND CASE WHEN m.member_kind <> 'opening_lot_reconstruction' OR m.retired_at IS NOT NULL THEN TRUE
                   WHEN (m.reconstruction_evidence ->> '$.algorithm_version') = 'opening_membership_v1'
                   THEN TRUE ELSE ERROR('Unsupported Golden opening algorithm') END
    """,  # noqa: S608  # fixed canonical reconstruction AST, no user SQL
        dialect="duckdb",
    )
