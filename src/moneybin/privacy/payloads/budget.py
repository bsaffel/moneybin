"""Typed payload dataclasses for the budget reports surface.

Each field carries ``Annotated[T, DataClass.X]`` metadata so the Phase 6
middleware can derive sensitivity via ``derive_tier`` without inspecting
tool source code directly.

BudgetStatusPayload contains TXN_AMOUNT (HIGH) and CATEGORY (LOW) fields,
so ``derive_tier`` resolves to ``Tier.HIGH``.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Annotated, Literal

from moneybin.privacy.taxonomy import DataClass


@dataclass(frozen=True, slots=True)
class BudgetCategoryStatusRow:
    """Per-category budget status row."""

    category: Annotated[str, DataClass.CATEGORY]
    budget: Annotated[Decimal, DataClass.TXN_AMOUNT]
    spent: Annotated[Decimal, DataClass.TXN_AMOUNT]
    remaining: Annotated[Decimal, DataClass.TXN_AMOUNT]
    status: Annotated[Literal["OK", "WARNING", "OVER"], DataClass.TXN_TYPE]


@dataclass(frozen=True, slots=True)
class BudgetStatusPayload:
    """Result of reports_budget — per-category status + month."""

    month: Annotated[str, DataClass.TXN_DATE]
    categories: list[BudgetCategoryStatusRow]
