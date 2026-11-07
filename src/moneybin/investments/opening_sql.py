"""One canonical opening query for transition preflight and SQLMesh projection."""

from importlib import resources
from typing import cast

from sqlglot import exp, parse_one
from sqlmesh.core.dialect import parse


def model_query(name: str) -> exp.Query:
    """Parse a fixed repository model without creating a SQLMesh Context."""
    text = (
        resources
        .files("moneybin")
        .joinpath(f"sqlmesh/models/prep/{name}.sql")
        .read_text()
    )
    return next(
        node
        for node in parse(text, default_dialect="duckdb")
        if isinstance(node, exp.Query)
    )


def opening_query(transaction_sql: str, snapshot_sql: str) -> exp.Query:
    """Bind the canonical gap and lot calculation to selected evidence queries."""
    positions = model_query("int_plaid__opening_positions")
    first = next(
        cte
        for cte in positions.find_all(exp.CTE)
        if cte.alias_or_name == "first_snapshot"
    )
    first.set("this", parse_one(snapshot_sql, dialect="duckdb"))
    position_sql = positions.sql(dialect="duckdb").replace(
        "prep.stg_plaid__investment_transactions", "selected_transactions"
    )
    opening = cast(exp.Select, model_query("stg_plaid__opening_lots"))
    opening = opening.select(
        "a.lot_key",
        "b.source_file AS first_snapshot_source_file",
        "b.window_start AS reconstruction_window_start",
        "b.snapshot_date AS reconstruction_snapshot_date",
        append=True,
    )
    opening = opening.join(
        "bootstrappable AS b",
        on="b.source_account_key = a.source_account_key AND b.source_security_key = a.source_security_key AND b.source_origin = a.source_origin",
        join_type="INNER",
    )
    opening_sql = opening.sql(dialect="duckdb").replace(
        "prep.int_plaid__opening_positions", "selected_positions"
    )
    return cast(
        exp.Query,
        parse_one(
            f"WITH selected_transactions AS ({transaction_sql}), selected_positions AS ({position_sql}), reconstructed AS ({opening_sql}) SELECT * FROM reconstructed",  # noqa: S608  # canonical models and bound evidence queries, no user SQL
            dialect="duckdb",
        ),
    )
