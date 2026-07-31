"""Live catalog coverage for the privacy classification registry."""

from __future__ import annotations

import pytest

from moneybin.database import Database
from moneybin.privacy.taxonomy import CLASSIFICATION
from tests.moneybin.price_model_helpers import close_source_ctes

pytestmark = pytest.mark.unit

_TARGET_SCHEMAS = {"app", "core"}


def _catalog_columns(db: Database) -> set[tuple[str, str, str]]:
    rows = db.execute(
        """
        SELECT schema_name, table_name, column_name
        FROM duckdb_columns()
        """
    ).fetchall()
    return {
        (schema, table, column)
        for schema, table, column in rows
        if schema in _TARGET_SCHEMAS
    }


def _registry_columns() -> set[tuple[str, str, str]]:
    return {
        (schema, table, column)
        for (schema, table), columns in CLASSIFICATION.items()
        if schema in _TARGET_SCHEMAS
        for column in columns
    }


def _format_columns(columns: set[tuple[str, str, str]]) -> str:
    return "\n".join(
        f"- {schema}.{table}.{column}" for schema, table, column in sorted(columns)
    )


def test_classification_registry_covers_every_app_and_core_column(
    schema_catalog_db: Database,
) -> None:
    """Every live app/core column must have a privacy classification."""
    missing = _catalog_columns(schema_catalog_db) - _registry_columns()

    assert not missing, (
        f"CLASSIFICATION is missing live catalog columns:\n{_format_columns(missing)}"
    )


def test_classification_registry_has_no_stale_app_or_core_columns(
    schema_catalog_db: Database,
) -> None:
    """Registry entries must point at columns that still exist."""
    stale = _registry_columns() - _catalog_columns(schema_catalog_db)

    assert not stale, (
        "CLASSIFICATION contains stale columns not present in the catalog:\n"
        f"{_format_columns(stale)}"
    )


# Each CTE unioned into core.fct_security_prices.close, mapped to the column
# whose value it carries through verbatim. ``None`` marks a provider feed: a
# market close is public reference data, with no column of the user's own behind
# it. Held to the model by set equality below, never trusted as a standing list.
_CLOSE_SOURCES: dict[str, tuple[str, str, str] | None] = {
    "provider": None,
    "marks": ("app", "security_price_overrides", "close"),
    "trade_implied": ("core", "fct_investment_transactions", "price"),
}


def test_every_close_contributor_is_accounted_for() -> None:
    """The mapping above must equal the model's union, not merely overlap it.

    A subset check would pass while a fourth source went unclassified, which is
    the exact way this problem arrived: `trade_implied` joined the union and the
    classification below it was never revisited.
    """
    assert close_source_ctes() == set(_CLOSE_SOURCES)


def test_a_resolved_close_is_never_less_sensitive_than_what_flows_into_it() -> None:
    """`close` must carry the tier of the strictest value it can hold.

    `sql_query` serves `core`, so it advertises and returns whatever tier this
    column declares. With a `trade_implied` row the value IS
    `fct_investment_transactions.price` — the user's own fill — and with an
    `override` row it is a valuation the user authored. Classifying the resolved
    column below its sources would return a personal transaction amount as
    low-sensitivity public data.
    """
    close = CLASSIFICATION[("core", "fct_security_prices")]["close"]
    for cte, source in sorted(_CLOSE_SOURCES.items()):
        if source is None:
            continue
        schema, table, column = source
        contributor = CLASSIFICATION[(schema, table)][column]
        assert close.tier >= contributor.tier, (
            f"core.fct_security_prices.close is {close.tier.name}, below the "
            f"{contributor.tier.name} of {schema}.{table}.{column}, which the "
            f"`{cte}` branch copies into it verbatim"
        )
