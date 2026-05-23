"""Typed payload dataclasses for the networth surface.

Each field carries ``Annotated[T, DataClass.X]`` metadata so the Phase 6
middleware can derive sensitivity via ``derive_tier`` without inspecting
tool source code directly.

NetWorthSnapshotPayload contains ACCOUNT_IDENTIFIER (CRITICAL) via
per-account breakdown rows, so ``derive_tier`` resolves to ``Tier.CRITICAL``.
NetWorthHistoryPayload contains only BALANCE and TXN_DATE, so it resolves to
``Tier.HIGH``.
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

    account_id: Annotated[str, DataClass.ACCOUNT_IDENTIFIER]
    display_name: Annotated[str | None, DataClass.USER_NOTE]
    balance: Annotated[Decimal, DataClass.BALANCE]
    observation_source: Annotated[str | None, DataClass.TXN_TYPE]


@dataclass(frozen=True, slots=True)
class NetWorthSnapshotPayload:
    """Net worth at a point in time + per-account breakdown."""

    balance_date: Annotated[date, DataClass.TXN_DATE]
    net_worth: Annotated[Decimal, DataClass.BALANCE]
    total_assets: Annotated[Decimal, DataClass.BALANCE]
    total_liabilities: Annotated[Decimal, DataClass.BALANCE]
    account_count: Annotated[int, DataClass.AGGREGATE]
    per_account: list[NetWorthAccountRow] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class NetWorthHistoryPoint:
    """One time-bucketed networth observation with period-over-period change."""

    # period as string (ISO date) per the existing wire format
    period: Annotated[str | None, DataClass.TXN_DATE]
    net_worth: Annotated[Decimal, DataClass.BALANCE]
    change_abs: Annotated[Decimal | None, DataClass.BALANCE]
    change_pct: Annotated[Decimal | float | None, DataClass.AGGREGATE]


@dataclass(frozen=True, slots=True)
class NetWorthHistoryPayload:
    """Payload for reports_networth_history."""

    points: list[NetWorthHistoryPoint]
