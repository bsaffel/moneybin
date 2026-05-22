"""Typed payload dataclasses for the balance surface.

Each field carries ``Annotated[T, DataClass.X]`` metadata so the Phase 6
middleware can derive sensitivity via ``derive_tier`` without inspecting
tool source code directly.

Balance data includes ACCOUNT_IDENTIFIER (CRITICAL), BALANCE (HIGH), and
TXN_DATE (MEDIUM), so ``derive_tier`` resolves to ``Tier.CRITICAL`` for
all payloads here.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Annotated

from moneybin.privacy.taxonomy import DataClass


@dataclass(frozen=True, slots=True)
class BalanceObservationRow:
    """One row of core.fct_balances_daily for list/history/reconcile."""

    account_id: Annotated[str, DataClass.ACCOUNT_IDENTIFIER]
    balance_date: Annotated[date, DataClass.TXN_DATE]
    balance: Annotated[Decimal, DataClass.BALANCE]
    is_observed: Annotated[bool, DataClass.TXN_TYPE]
    observation_source: Annotated[str | None, DataClass.TXN_TYPE]
    reconciliation_delta: Annotated[Decimal | None, DataClass.BALANCE]


@dataclass(frozen=True, slots=True)
class BalanceObservationListPayload:
    """Payload for accounts_balances, accounts_balance_history, accounts_balance_reconcile."""

    observations: list[BalanceObservationRow]


@dataclass(frozen=True, slots=True)
class BalanceAssertionRow:
    """One row of app.balance_assertions."""

    account_id: Annotated[str, DataClass.ACCOUNT_IDENTIFIER]
    assertion_date: Annotated[date, DataClass.TXN_DATE]
    balance: Annotated[Decimal, DataClass.BALANCE]
    notes: Annotated[str | None, DataClass.USER_NOTE]
    created_at: Annotated[str, DataClass.TIMESTAMP_OBSERVABILITY]


@dataclass(frozen=True, slots=True)
class BalanceAssertionListPayload:
    """Payload for accounts_balance_assertions."""

    assertions: list[BalanceAssertionRow]


@dataclass(frozen=True, slots=True)
class BalanceAssertionPayload:
    """Single-row wrapper for accounts_balance_assert (insert/upsert result)."""

    assertion: BalanceAssertionRow


@dataclass(frozen=True, slots=True)
class BalanceAssertionDeletePayload:
    """Status payload for accounts_balance_assertion_delete."""

    account_id: Annotated[str, DataClass.ACCOUNT_IDENTIFIER]
    assertion_date: Annotated[date, DataClass.TXN_DATE]
    deleted: Annotated[bool, DataClass.TXN_TYPE]
