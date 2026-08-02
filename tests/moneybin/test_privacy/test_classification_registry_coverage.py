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


# Each resolved column of core.fct_security_prices, mapped to the column each
# unioned CTE carries into it verbatim. ``None`` marks a provider feed: a market
# close and the calendar date it closed on are public reference data, with no
# column of the user's own behind either. Held to the model by set equality
# below, never trusted as a standing list.
#
# Both columns are listed because a resolved row leaks through whichever one a
# query selects. Classifying `close` alone left `price_date` LOW, so a projection
# of security_id + price_date + source_type still returned the security and day
# of a personal execution as public data.
_RESOLVED_SOURCES: dict[str, dict[str, tuple[str, str, str] | None]] = {
    "close": {
        "provider": None,
        "marks": ("app", "security_price_overrides", "close"),
        "trade_implied": ("core", "fct_investment_transactions", "price"),
    },
    "price_date": {
        "provider": None,
        "marks": ("app", "security_price_overrides", "price_date"),
        "trade_implied": ("core", "fct_investment_transactions", "trade_date"),
    },
}


@pytest.mark.parametrize("resolved", sorted(_RESOLVED_SOURCES))
def test_every_union_contributor_is_accounted_for(resolved: str) -> None:
    """Each mapping above must equal the model's union, not merely overlap it.

    A subset check would pass while a fourth source went unclassified, which is
    the exact way this problem arrived: `trade_implied` joined the union and the
    classification below it was never revisited.
    """
    assert close_source_ctes() == set(_RESOLVED_SOURCES[resolved])


@pytest.mark.parametrize("resolved", sorted(_RESOLVED_SOURCES))
def test_a_resolved_column_is_never_less_sensitive_than_its_sources(
    resolved: str,
) -> None:
    """Every resolved column must carry the tier of the strictest value it holds.

    `sql_query` serves `core`, so it advertises and returns whatever tier each
    column declares — and a caller chooses which columns to select. With a
    `trade_implied` row `close` IS `fct_investment_transactions.price` and
    `price_date` IS its `trade_date`; with an `override` row `close` is a
    valuation the user authored. Classifying either below its sources returns
    personal transaction data as low-sensitivity public data.
    """
    declared = CLASSIFICATION[("core", "fct_security_prices")][resolved]
    for cte, source in sorted(_RESOLVED_SOURCES[resolved].items()):
        if source is None:
            continue
        schema, table, column = source
        contributor = CLASSIFICATION[(schema, table)][column]
        assert declared.tier >= contributor.tier, (
            f"core.fct_security_prices.{resolved} is {declared.tier.name}, below "
            f"the {contributor.tier.name} of {schema}.{table}.{column}, which the "
            f"`{cte}` branch copies into it verbatim"
        )
