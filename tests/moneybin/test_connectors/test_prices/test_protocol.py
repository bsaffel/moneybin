"""Tests for the shared price-adapter types.

The basis guards matter because raw.security_prices is append-only: a row
carrying a wrong or absent price_basis can never be corrected in place, and
core.fct_security_prices values holdings from price_basis = 'raw' exclusively.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from moneybin.connectors.prices.protocol import PriceObservation
from moneybin.vocabulary import PRICE_BASES


def _observation(*, price_basis: str) -> PriceObservation:
    return PriceObservation(
        provider_security_key="AAPL",
        price_date=date(2026, 7, 24),
        quote_currency="USD",
        close=Decimal("212.50"),
        source_type="tiingo",
        price_basis=price_basis,
    )


def test_an_observation_rejects_an_undeclared_basis() -> None:
    """An adapter that states no basis cannot produce an observation at all."""
    with pytest.raises(ValueError, match="price_basis"):
        _observation(price_basis="")


def test_an_observation_rejects_a_basis_outside_the_vocabulary() -> None:
    """A plausible-looking basis the DDL CHECK would reject fails here first."""
    with pytest.raises(ValueError, match="price_basis"):
        _observation(price_basis="adjusted")


def test_the_basis_vocabulary_matches_the_raw_table_check() -> None:
    """The Python vocabulary and the append-only table's CHECK cannot drift.

    raw.security_prices is a one-way door: a basis this vocabulary admits but
    the CHECK rejects fails at write time, and one the CHECK admits but this
    rejects is unreachable. Read the constraint rather than restating it.
    """
    from pathlib import Path

    import moneybin

    ddl = (
        Path(moneybin.__file__).parent / "sql" / "schema" / "raw_security_prices.sql"
    ).read_text()
    checked = {
        token.strip().strip("'")
        for token in ddl.split("CHECK (price_basis IN (")[1].split("))")[0].split(",")
    }
    assert checked == set(PRICE_BASES)
