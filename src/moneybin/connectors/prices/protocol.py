"""The shared shape every price adapter speaks.

One Protocol, two concrete adapters. The types here are deliberately the shape
of a raw.security_prices row rather than a provider payload: an adapter's whole
job is to turn whatever a provider serves into rows this codebase can store
without a second translation step.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Protocol

from moneybin.vocabulary import PRICE_BASES


@dataclass(frozen=True, slots=True)
class SecurityRef:
    """One security to fetch, named by the key the target provider understands.

    `quote_currency` is supplied by the caller from app.securities.currency_code
    rather than read back from the response. Tiingo's end-of-day payload carries
    no currency field at all, and CoinGecko takes the currency as a *request*
    parameter — so in neither case can the adapter learn it from the provider.
    Hard-coding USD would mislabel every non-US listing permanently, on an
    append-only table; sourcing it from the catalog makes a wrong currency one
    fixable catalog row instead.
    """

    provider_security_key: str
    quote_currency: str


@dataclass(frozen=True, slots=True)
class PriceObservation:
    """One stored-shape price row, ready for raw.security_prices."""

    provider_security_key: str
    price_date: date
    quote_currency: str
    close: Decimal
    source_type: str
    price_basis: str

    def __post_init__(self) -> None:
        """Refuse an undeclared or unknown basis at construction.

        The spec requires that an undeclared price_basis fail ingest. Enforcing
        it on the type means an adapter *cannot* emit a basis-less observation,
        which is stronger than a check the writer might be refactored past.
        """
        if self.price_basis not in PRICE_BASES:
            raise ValueError(
                f"price_basis must be one of {sorted(PRICE_BASES)}, "
                f"got {self.price_basis!r}"
            )


@dataclass(frozen=True, slots=True)
class PriceFetchFailure:
    """One security the provider did not price, and why.

    Carries no response body beyond a truncated status line: a provider error
    can echo the request URL, and for Tiingo that would put the API token in a
    stored reason string.
    """

    provider_security_key: str
    reason: str


@dataclass(frozen=True, slots=True)
class PriceFetchResult:
    """What one batch produced. Partial success is the normal outcome."""

    observations: tuple[PriceObservation, ...]
    failures: tuple[PriceFetchFailure, ...]


class PriceAdapter(Protocol):
    """A market-data provider that can price securities over a date window."""

    source_type: str
    price_basis: str

    def fetch(
        self, securities: Sequence[SecurityRef], start: date, end: date
    ) -> PriceFetchResult:
        """Return observations for `securities` over the inclusive window.

        Never raises for a single security's failure — that lands in
        `result.failures`. Raises only on a whole-batch condition, such as a
        missing or rejected credential.
        """
        ...
