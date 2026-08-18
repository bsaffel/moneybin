"""Typed payloads for the ``fx`` surface: one rate, a series, one override.

Classes follow the ``app.exchange_rate_overrides`` registry entries rather than
being chosen here, so the CLI cannot declare a looser tier than the table it
reads. Two of them are worth restating because they are not the obvious pick:

- ``rate`` is CURRENCY, not TXN_AMOUNT. A daily reference rate is a market fact
  that discloses no balance and no amount, and Requirement 10 requires showing
  the exact rate behind any converted figure — a class that masked it would
  fight the requirement it exists beside.
- the dates are TXN_DATE. The reason to ask about one particular day is that
  money moved on it, so the date carries the same signal a transaction date
  does even though the rate itself carries none.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import TYPE_CHECKING, Annotated

from moneybin.privacy.taxonomy import DataClass

if TYPE_CHECKING:
    # Type-only: `currency_service` imports polars, and this module is reached
    # from the CLI's eager import chain.
    from moneybin.services.currency_service import ResolvedRate


@dataclass(frozen=True, slots=True)
class FxRatePayload:
    """Payload for ``fx rate`` — one resolved rate and where it came from.

    ``requested_date`` and ``rate_date`` are both carried because they
    genuinely differ: a weekend is priced with the preceding Friday's rate, and
    a caller shown only one of them cannot tell a rate published for the day it
    asked about from one carried forward.
    """

    from_currency: Annotated[str, DataClass.CURRENCY]
    to_currency: Annotated[str, DataClass.CURRENCY]
    requested_date: Annotated[date, DataClass.TXN_DATE]
    rate_date: Annotated[date, DataClass.TXN_DATE]
    rate: Annotated[Decimal, DataClass.CURRENCY]
    source: Annotated[str, DataClass.TXN_TYPE]

    @classmethod
    def from_resolved(cls, rate: ResolvedRate) -> FxRatePayload:
        """Publish one resolved rate as Requirement 10's provenance record.

        Shared so every surface that shows a rate shows the same six fields:
        ``fx rate`` answering about one pair, and the rates that priced a
        converted figure elsewhere.
        """
        return cls(
            from_currency=rate.from_currency,
            to_currency=rate.to_currency,
            requested_date=rate.requested_date,
            rate_date=rate.rate_date,
            rate=rate.rate,
            source=rate.source,
        )


@dataclass(frozen=True, slots=True)
class FxRateRow:
    """One date in an ``fx list`` series, and which layer answered for it."""

    rate_date: Annotated[date, DataClass.TXN_DATE]
    rate: Annotated[Decimal, DataClass.CURRENCY]
    source: Annotated[str, DataClass.TXN_TYPE]


@dataclass(frozen=True, slots=True)
class FxRatesPayload:
    """Payload for ``fx list`` — the resolved series for one pair, newest first."""

    from_currency: Annotated[str, DataClass.CURRENCY]
    to_currency: Annotated[str, DataClass.CURRENCY]
    rows: list[FxRateRow]


@dataclass(frozen=True, slots=True)
class FxOverridePayload:
    """Payload for ``fx set`` / ``fx delete`` — the override's identity.

    ``rate`` is ``None`` on the delete path, and ``removed`` distinguishes a
    deletion that found a row from one that found nothing. Both are ordinary
    successes, and only that flag tells them apart.
    """

    from_currency: Annotated[str, DataClass.CURRENCY]
    to_currency: Annotated[str, DataClass.CURRENCY]
    rate_date: Annotated[date, DataClass.TXN_DATE]
    rate: Annotated[Decimal | None, DataClass.CURRENCY]
    removed: Annotated[bool, DataClass.AGGREGATE]
