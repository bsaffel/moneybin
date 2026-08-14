"""The shared shape every exchange-rate adapter speaks.

As in ``connectors.prices.protocol``, the observation is the shape of a stored
row rather than a provider payload, so an adapter's output reaches
raw.exchange_rates without a second translation step.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Protocol


@dataclass(frozen=True, slots=True)
class RateObservation:
    """One stored-shape rate row, ready for raw.exchange_rates.

    `rate_date` is the business day the provider answered with, which is not
    always the day asked for — a weekend or holiday resolves back to the last
    published day. Storing the resolved day rather than the requested one keeps
    the table an honest record of what the provider published.

    `rate` is Decimal rather than float because raw.exchange_rates is
    append-only and DECIMAL(18,8): a float that reaches it loses precision on a
    row nothing later rewrites. An adapter parsing a provider's JSON is the one
    place that annotation cannot enforce itself, so each adapter converts before
    constructing rather than handing the number straight through.
    """

    from_currency: str
    to_currency: str
    rate_date: date
    rate: Decimal
    source: str


class RateAdapter(Protocol):
    """A reference-rate provider that can price one currency pair on one date."""

    source: str

    def fetch(
        self, from_currency: str, to_currency: str, on: date
    ) -> RateObservation | None:
        """Return the rate for the pair, or None when the provider has no series.

        Returns None for an absence the caller can route around: an unsupported
        currency, or a date before the provider's series begins. Raises for
        every condition that is the provider's fault or the network's, so an
        offline run can never be mistaken for an unsupported pair.
        """
        ...
