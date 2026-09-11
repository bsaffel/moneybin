"""Live manual identity reads share the staging model's routing semantics."""

from importlib import resources


def manual_identity_sql() -> str:
    """Return the model query for use before SQLMesh has refreshed its views."""
    model = (
        resources
        .files("moneybin")
        .joinpath("sqlmesh/models/prep/int_manual__investment_identity.sql")
        .read_text()
    )
    # The model has one metadata statement followed by one SELECT statement.
    return model.split(";", 1)[1].strip().removesuffix(";")
