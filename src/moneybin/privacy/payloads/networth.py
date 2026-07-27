"""Typed payload dataclasses for the networth surface.

Each field carries ``Annotated[T, DataClass.X]`` metadata so the Phase 6
middleware can derive sensitivity via ``derive_tier`` without inspecting
tool source code directly.

``account_id`` is RECORD_ID (spec D6 — opaque canonical surrogate, not PII).
NetWorthSnapshotPayload resolves to ``Tier.HIGH`` (BALANCE is the highest
class among per-account breakdown rows). NetWorthHistoryPayload also resolves
to ``Tier.HIGH`` (BALANCE and TXN_DATE only).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from typing import Annotated

from moneybin.privacy.taxonomy import DataClass


@dataclass(frozen=True, slots=True)
class NetWorthAccountRow:
    """One per-account breakdown row in the networth snapshot."""

    account_id: Annotated[str, DataClass.RECORD_ID]
    display_name: Annotated[str | None, DataClass.USER_NOTE]
    balance: Annotated[Decimal, DataClass.BALANCE]
    observation_source: Annotated[str | None, DataClass.TXN_TYPE]
    currency_code: Annotated[str | None, DataClass.CURRENCY] = None


@dataclass(frozen=True, slots=True)
class NetWorthCurrencySegment:
    """Totals for the accounts denominated in one currency."""

    currency_code: Annotated[str | None, DataClass.CURRENCY]
    net_worth: Annotated[Decimal | None, DataClass.BALANCE]
    total_assets: Annotated[Decimal | None, DataClass.BALANCE]
    total_liabilities: Annotated[Decimal | None, DataClass.BALANCE]
    account_count: Annotated[int, DataClass.AGGREGATE]


@dataclass(frozen=True, slots=True)
class NetWorthSnapshotPayload:
    """Net worth at a point in time + per-currency and per-account breakdowns.

    The headline scalars carry a figure only when every contributing account
    shares one currency. Holding two currencies makes a single total
    meaningless until conversion ships (multi-currency.md Requirement 5), so
    `net_worth`, `total_assets`, `total_liabilities`, and `currency_code` go
    null and `per_currency` carries each currency's own totals. A
    single-currency profile — the common case — sees the same figures it
    always did, plus the currency they are denominated in.
    """

    balance_date: Annotated[date | None, DataClass.TXN_DATE]
    currency_code: Annotated[str | None, DataClass.CURRENCY]
    net_worth: Annotated[Decimal | None, DataClass.BALANCE]
    total_assets: Annotated[Decimal | None, DataClass.BALANCE]
    total_liabilities: Annotated[Decimal | None, DataClass.BALANCE]
    account_count: Annotated[int, DataClass.AGGREGATE]
    per_currency: list[NetWorthCurrencySegment] = field(default_factory=list)
    per_account: list[NetWorthAccountRow] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class NetWorthHistoryPoint:
    """One time-bucketed networth observation with period-over-period change."""

    # period as string (ISO date) per the existing wire format
    period: Annotated[str | None, DataClass.TXN_DATE]
    currency_code: Annotated[str | None, DataClass.CURRENCY]
    net_worth: Annotated[Decimal, DataClass.BALANCE]
    change_abs: Annotated[Decimal | None, DataClass.BALANCE]
    change_pct: Annotated[Decimal | float | None, DataClass.AGGREGATE]


@dataclass(frozen=True, slots=True)
class NetWorthHistoryPayload:
    """Payload for reports_networth_history."""

    points: list[NetWorthHistoryPoint]
